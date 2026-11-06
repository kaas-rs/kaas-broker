#!/usr/bin/env bash
# Shared helpers for the kafka-*.sh test scripts. Source this from each script.
#
#   . "$(dirname "$0")/_common.sh"
#
# Provides:
#   $BOOTSTRAP     bootstrap server (override with env var, default in-cluster Service DNS)
#   $KAFKA_BIN     Kafka shell tools, wrapped so every call authenticates (see below);
#                  the real tools default to /opt/kafka/bin
#   $CLIENT_PROPS  client config every tool call carries (default: built from the
#                  kaas-scripts KafkaUser's credentials via kubectl)
#   $NAMESPACE     Kubernetes namespace holding the cluster (defaults to kaas)
#   $STS           broker StatefulSet name (defaults to kaas)
#   $TOPIC         per-run unique test topic name
#   $TMP           per-run scratch dir, auto-cleaned on exit
#   skip "<reason>"   print reason and exit 77 (autoconf "skipped" exit code)
#   need <bin>     skip if a required tool is not on PATH
#   delete_on_exit <topic>...  delete these topics however the script ends
#   cleanup_on_exit [status]   the EXIT trap, for a script that needs its own
#   newer_flag <tool> <new> <old>  whichever spelling of a flag KAFKA_BIN has
#   wait_cluster_ready [timeout]  block until the brokers are serving again

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kaas.kaas.svc.cluster.local:9095}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
NAMESPACE="${NAMESPACE:-kaas}"
STS="${STS:-kaas}"
TOPIC="${TOPIC:-kaas-test-$$-$(date +%s)}"
TMP="$(mktemp -d)"

# delete_on_exit <topic>...
#
# Delete these topics however the script ends, so a failed run doesn't leave
# them behind. Call it after the create has succeeded, never before: TOPIC can
# be overridden, and a create refused because a real topic has that name must
# not get the real topic deleted.
EXIT_TOPICS=()
delete_on_exit() {
  EXIT_TOPICS+=("$@")
}

# cleanup_on_exit [status]
#
# The EXIT trap. A script that needs its own trap should end it with this,
# passing the status it saved first, or its topics and $TMP stay behind:
#
#   trap 'rc=$?; my_cleanup; cleanup_on_exit "$rc"' EXIT
cleanup_on_exit() {
  local rc="${1:-$?}" t
  set +e
  for t in "${EXIT_TOPICS[@]}"; do
    # Bounded, or an unreachable broker holds each delete for the client's API
    # timeout, on top of whatever just failed.
    timeout 30 "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
      --delete --topic "$t" >/dev/null 2>&1
  done
  rm -rf "$TMP"
  exit "$rc"
}
trap cleanup_on_exit EXIT

skip() {
  echo ">> SKIP: $*" >&2
  exit 77
}

need() {
  command -v "$1" >/dev/null 2>&1 || skip "missing required tool: $1"
}

# newer_flag <tool> <new flag> <old flag>
#
# Print <new flag> if this KAFKA_BIN's tool has it, otherwise <old flag>. The 4.x
# CLI renamed some flags and prints a deprecation notice to stdout for the old
# spelling, which a script reading the tool's output takes for a record. --help
# exits non-zero.
newer_flag() {
  local help
  help="$("$KAFKA_BIN/$1" --help 2>&1 || true)"
  case "$help" in
    *"$2"*) echo "$2" ;;
    *) echo "$3" ;;
  esac
}

# wait_cluster_ready [timeout_seconds]
#
# Block until the broker StatefulSet is fully Ready again and the
# cluster answers an admin call that needs a coordinator lookup. Any
# script that deliberately disturbs the cluster must call this before it
# returns, so the next script in the suite starts from a settled cluster
# rather than inheriting a half-finished rollout.
#
# Two signals, because they mean different things:
#
#   * Pod readiness. `/readyz` is serving-gated — a Ready broker has
#     finished taking over every partition assignment.json gives it — so
#     "every replica Ready" is the honest cluster-settled signal, not a
#     bind-time latch. Checking the StatefulSet's readyReplicas covers
#     the peers too, not just the pod that was bounced: a bounce moves
#     partitions onto the survivors, and they are taking over while the
#     restarted pod comes back.
#
#   * A consumer-group admin call. Group coordinators move independently
#     of partition leadership (coordinator-of-G is a hash over the broker
#     set), so a client can find every partition healthy and still time
#     out in FIND_COORDINATOR. That is the failure this helper exists to
#     prevent, so probe it directly rather than inferring it.
#
# No-op when kubectl is unavailable or the StatefulSet is absent, so
# scripts that source this outside a cluster are unaffected.
wait_cluster_ready() {
  local deadline=$(( SECONDS + ${1:-180} ))
  local ns="$NAMESPACE"
  local sts="$STS"
  local want have

  command -v kubectl >/dev/null 2>&1 || return 0
  want="$(kubectl -n "$ns" get sts "$sts" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  [ -n "$want" ] || return 0

  echo ">> waiting for the cluster to settle"
  # Deadline rather than an iteration count: each `kubectl` is a process
  # spawn plus an API round trip, so counting iterations would make the
  # timeout argument mean something much longer than seconds.
  while [ "$SECONDS" -lt "$deadline" ]; do
    have="$(kubectl -n "$ns" get sts "$sts" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    [ "${have:-0}" = "$want" ] && break
    sleep 1
  done
  [ "${have:-0}" = "$want" ] ||
    echo ">> WARN: only ${have:-0}/$want brokers Ready before the deadline" >&2

  # Coordinator probe. Bounded and advisory — a persistent failure is
  # the next script's problem to report, not this helper's to mask.
  while [ "$SECONDS" -lt "$deadline" ]; do
    "$KAFKA_BIN/kafka-consumer-groups.sh" \
      --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo ">> WARN: consumer-group admin calls still failing after the settle wait" >&2
}

if [ ! -x "$KAFKA_BIN/kafka-topics.sh" ]; then
  skip "Kafka CLI not found at $KAFKA_BIN; set KAFKA_BIN=/path/to/kafka/bin"
fi

# --- Authentication ---------------------------------------------------------
#
# The cluster has no anonymous listener, so every tool call has to carry
# credentials. Rather than thread a config flag through each of the ~110
# call sites, KAFKA_BIN is pointed at a directory of thin wrappers that add
# `--command-config $CLIENT_PROPS` to any call that names a broker
# (--bootstrap-server, or bootstrap.servers= in --producer-props) and then
# runs the real tool. The scripts keep reading like plain Apache
# Kafka CLI usage.
#
# CLIENT_PROPS defaults to SCRAM-SHA-512 over TLS as the `kaas-scripts`
# KafkaUser (an admin principal — the suite exercises ACLs, configs,
# delete-records and group deletes), built from the operator-generated
# password Secret and the chart's broker CA. Set CLIENT_PROPS to run as
# someone else, or KAAS_SCRIPTS_USER to pick another KafkaUser's Secret.
# Missing credentials fail the script outright: a skip would let the whole
# suite report green without having exercised anything.
KAAS_SCRIPTS_USER="${KAAS_SCRIPTS_USER:-kaas-scripts}"
if [ -z "${CLIENT_PROPS:-}" ]; then
  if ! command -v kubectl >/dev/null 2>&1; then
    echo ">> ERROR: no CLIENT_PROPS set and no kubectl to build one from the" \
         "${KAAS_SCRIPTS_USER} KafkaUser" >&2
    exit 1
  fi
  # Written with a 077 umask and removed with $TMP on exit; the password is
  # never echoed.
  (
    umask 077
    pw="$(kubectl -n "$NAMESPACE" get secret "${KAAS_SCRIPTS_USER}-kafka-credentials" \
      -o jsonpath='{.data.password}' 2>/dev/null | base64 -d)"
    [ -n "$pw" ] || exit 1
    kubectl -n "$NAMESPACE" get secret "${STS}-broker-internal-tls" \
      -o jsonpath='{.data.ca\.crt}' 2>/dev/null | base64 -d > "$TMP/ca.crt"
    [ -s "$TMP/ca.crt" ] || exit 1
    cat > "$TMP/client.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAAS_SCRIPTS_USER}" password="${pw}";
ssl.truststore.type=PEM
ssl.truststore.location=$TMP/ca.crt
EOF
  ) || {
    echo ">> ERROR: could not build client credentials from Secret" \
         "${NAMESPACE}/${KAAS_SCRIPTS_USER}-kafka-credentials and the broker CA" \
         "(${NAMESPACE}/${STS}-broker-internal-tls); set CLIENT_PROPS instead" >&2
    exit 1
  }
  CLIENT_PROPS="$TMP/client.properties"
fi
export CLIENT_PROPS

# The wrapper. Config flags a script already passes (--producer.config,
# --consumer.config, --config-file, …) are folded into the one
# --command-config file after CLIENT_PROPS, so the script's own settings
# win on conflict and each tool still sees a single config flag.
# kafka-e2e-latency's positional form, the only one 3.7 has, takes the
# properties file as its optional 6th argument instead.
REAL_KAFKA_BIN="$KAFKA_BIN"
KAFKA_BIN="$TMP/bin"
mkdir -p "$KAFKA_BIN"
cat > "$KAFKA_BIN/.auth-exec" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
real="$1"; shift
tool="$(basename "$real")"

if [ "$tool" = kafka-e2e-latency.sh ] && [ $# -eq 5 ] && [ "${1#-}" = "$1" ]; then
  exec "$real" "$@" "$CLIENT_PROPS"
fi

# A network call names its brokers either with --bootstrap-server or, for
# the perf tools' --producer-props form, as a bootstrap.servers= property.
has_bootstrap=0
for a in "$@"; do
  case "$a" in --bootstrap-server|--bootstrap-server=*|bootstrap.servers=*) has_bootstrap=1 ;; esac
done
[ "$has_bootstrap" = 1 ] || exec "$real" "$@"

cfg_flag() {
  case "$1" in
    --command-config|--producer.config|--consumer.config|--config-file|--admin.config) return 0 ;;
    --config) [ "$tool" = kafka-cluster.sh ] ;;
    *) return 1 ;;
  esac
}

merged="$(mktemp "${TMPDIR:-/tmp}/kaas-client.XXXXXX")"
trap 'rm -f "$merged"' EXIT
cat "$CLIENT_PROPS" > "$merged"
args=()
while [ $# -gt 0 ]; do
  if cfg_flag "$1" && [ $# -ge 2 ]; then
    { echo; cat "$2"; } >> "$merged"; shift 2
  elif cfg_flag "${1%%=*}" && [ "${1#*=}" != "$1" ]; then
    { echo; cat "${1#*=}"; } >> "$merged"; shift
  else
    args+=("$1"); shift
  fi
done
# Placement matters only for the subcommand tools: kafka-transactions takes
# --command-config as a global option (before the subcommand),
# kafka-cluster as an option of its subcommand (after it). The jopt-based
# tools accept it anywhere.
if [ "$tool" = kafka-cluster.sh ]; then
  "$real" "${args[@]}" --command-config "$merged"
else
  "$real" --command-config "$merged" "${args[@]}"
fi
EOF
chmod +x "$KAFKA_BIN/.auth-exec"
for real in "$REAL_KAFKA_BIN"/kafka-*.sh; do
  printf '#!/usr/bin/env bash\nexec "%s" "%s" "$@"\n' \
    "$KAFKA_BIN/.auth-exec" "$real" > "$KAFKA_BIN/$(basename "$real")"
  chmod +x "$KAFKA_BIN/$(basename "$real")"
done

echo ">> bootstrap: $BOOTSTRAP"
echo ">> kafka bin: $REAL_KAFKA_BIN (authenticated wrappers in $KAFKA_BIN)"
echo ">> client:    $CLIENT_PROPS"
