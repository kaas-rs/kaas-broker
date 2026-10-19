{{/*
Common template helpers.
*/}}

{{- /* Pinned to "kaas", not .Chart.Name: the chart was renamed to
       kaas-broker, and this name feeds the StatefulSet/Deployment
       selector labels (immutable) and the default fullname. Deriving it
       from the chart name would make a plain upgrade from a "kaas"
       chart fail on the selector and orphan the PVCs. */ -}}
{{- define "kaas.name" -}}
{{- default "kaas" .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kaas.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "kaas.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "kaas.headlessName" -}}
{{- printf "%s-headless" (include "kaas.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kaas.pvcName" -}}
{{- printf "%s-data" (include "kaas.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kaas.controlPlanePvcName" -}}
{{- printf "%s-cluster-state" (include "kaas.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kaas.poolPvcName" -}}
{{- printf "%s-pool-%s" (include "kaas.fullname" .ctx) .name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- /* gh #221 phase 2: KAAS_LOG_DIRS JSON for broker + operator.
       Mount path convention: /vols/<name>. */ -}}
{{- define "kaas.logDirsJSON" -}}
{{- $dirs := list -}}
{{- range .Values.storage.pool -}}
{{- $dirs = append $dirs (dict "name" .name "path" (printf "/vols/%s" .name) "defaultEligible" (ne .defaultEligible false) "cordoned" (eq .cordoned true) "labels" (.labels | default dict)) -}}
{{- end -}}
{{- toJson $dirs -}}
{{- end -}}

{{- define "kaas.brokerSAName" -}}
{{- if .Values.serviceAccount.broker.create -}}
{{- default (printf "%s-broker" (include "kaas.fullname" .)) .Values.serviceAccount.broker.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.broker.name -}}
{{- end -}}
{{- end -}}

{{- define "kaas.operatorSAName" -}}
{{- if .Values.serviceAccount.operator.create -}}
{{- default (printf "%s-operator" (include "kaas.fullname" .)) .Values.serviceAccount.operator.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.operator.name -}}
{{- end -}}
{{- end -}}

{{- define "kaas.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kaas.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: broker
{{- end -}}

{{- define "kaas.operatorSelectorLabels" -}}
app.kubernetes.io/name: {{ include "kaas.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: operator
{{- end -}}

{{- define "kaas.labels" -}}
{{ include "kaas.selectorLabels" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{- define "kaas.operatorLabels" -}}
{{ include "kaas.operatorSelectorLabels" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{/*
kaas.brokerImage / kaas.operatorImage — an explicit .repository
always wins. Otherwise the repository defaults to the GHCR name plus
a "-preview" suffix when the resolved tag is a pre-release (contains
"-"), mirroring the exact rule docker-publish.yml uses to compute
image names.
*/}}
{{- define "kaas.brokerImage" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- $repo := .Values.image.repository -}}
{{- if not $repo -}}
  {{- $pre := contains "-" $tag | ternary "-preview" "" -}}
  {{- $repo = printf "ghcr.io/kaas-rs/kaas-broker%s" $pre -}}
{{- end -}}
{{ $repo }}:{{ $tag }}
{{- end -}}

{{/*
kaas.listenersJSON — gh #126 helper. Iterates the user-facing
listeners array (Strimzi shape) and emits the KAAS_LISTENERS env
value as a JSON list-of-objects matching the broker's listener
spec (crates/sk-broker/src/cli.rs).

Listener entries with no `enabled` key are treated as enabled
(always-on listeners). Entries with `enabled: false` are skipped.
This preserves the "internal is always on, external/authed are
opt-in" convention from earlier versions while letting custom
listeners freely toggle.

The broker's parser validates the result; constraint violations
(mtls without tls, duplicate ports/names) fail at startup with a
clear error. Output is single-line JSON — fits cleanly into an env
var without escape gymnastics.
*/}}
{{- define "kaas.listenersJSON" -}}
{{- $out := list -}}
{{- range .Values.listeners -}}
{{- if or (not (hasKey . "enabled")) .enabled -}}
{{- $authDict := dict "type" "none" -}}
{{- if .authentication -}}
{{- /* gh #42: pass the whole authentication block through (minus
       chart-only keys, of which there are none today) so oauth's
       validIssuerUri / jwksEndpointUri / userNameClaim /
       checkAudience / clientId / jwksRefreshSeconds /
       maxSecondsWithoutReauthentication reach the broker verbatim.
       The broker's parser owns validation and fails boot on a
       partial oauth block. */ -}}
{{- $authDict = deepCopy .authentication -}}
{{- if not (hasKey $authDict "type") -}}
{{- $_ := set $authDict "type" "none" -}}
{{- end -}}
{{- end -}}
{{- $tls := false -}}
{{- if hasKey . "tls" -}}
{{- $tls = .tls -}}
{{- end -}}
{{- /* Per-listener client-certificate trust: a non-external TLS
       listener with `clientCA.existingSecret` gets the broker's object
       form of `tls`, whose clientCaPath arms client-cert verification
       on that listener alone (KAAS_TLS_CLIENT_CA_FILE would arm it on
       every TLS listener). The CA is mounted by broker-statefulset.yaml
       at the path built by kaas.listenerClientCAPath. */ -}}
{{- $clientCA := include "kaas.listenerClientCASecret" . -}}
{{- if and $clientCA (not $tls) -}}
{{- fail (printf "listener %q: clientCA needs tls: true" .name) -}}
{{- end -}}
{{- if and (eq $authDict.type "mtls") (ne .type "external") (not $clientCA) -}}
{{- fail (printf "listener %q: authentication.type mtls needs clientCA.existingSecret (the CA that signs client certificates)" .name) -}}
{{- end -}}
{{- if $clientCA -}}
{{- $tls = dict "certPath" "/tls/tls.crt" "keyPath" "/tls/tls.key" "clientCaPath" (include "kaas.listenerClientCAPath" .) -}}
{{- end -}}
{{- $out = append $out (dict
  "name" .name
  "port" (.port | int)
  "type" .type
  "tls" $tls
  "authentication" $authDict) -}}
{{- end -}}
{{- end -}}
{{- $out | toJson -}}
{{- end -}}

{{/*
kaas.listenerClientCASecret — the per-listener client-CA Secret name for
one `.Values.listeners` entry, or "" when it has none. External
listeners are excluded: they keep their own `clientCA.enabled` block
wired through KAAS_TLS_CLIENT_CA_FILE.

kaas.listenerClientCAPath — where broker-statefulset.yaml mounts that
Secret's CA key for the listener, as the broker reads it.
*/}}
{{- define "kaas.listenerClientCASecret" -}}
{{- if and (ne .type "external") .clientCA .clientCA.existingSecret -}}
{{- .clientCA.existingSecret -}}
{{- end -}}
{{- end -}}

{{- define "kaas.listenerClientCAPath" -}}
{{- printf "/tls-client-ca/%s/%s" .name (.clientCA.key | default "ca.crt") -}}
{{- end -}}

{{/*
kaas.findListener — return the listener entry matching `name`, or
an empty dict if no such entry exists. Templates that need the
legacy "look up one listener by name" pattern (e.g. the
KafkaCluster CR emitter, the external-listener-only TLS/Gateway
plumbing) consult this instead of `.Values.listeners.<name>`.

Usage:
  {{- $ext := include "kaas.findListener" (dict "ctx" . "name" "external") | fromYaml -}}
  {{- if $ext.enabled }}
    port: {{ $ext.port }}
  {{- end }}

Returns YAML (parseable via `fromYaml`); empty `{}` when no match.
*/}}
{{- define "kaas.findListener" -}}
{{- $name := .name -}}
{{- $found := dict -}}
{{- range .ctx.Values.listeners -}}
{{- if eq .name $name -}}
{{- $found = . -}}
{{- end -}}
{{- end -}}
{{- $found | toYaml -}}
{{- end -}}

{{/*
kaas.firstByType — return the first listener entry with the given
type, or an empty dict. Used by the external-listener machinery
(cert-manager Certificate, per-broker Service, TLSRoute) which
today supports one external listener. The multi-external follow-up
will rewire these to iterate; this helper isolates the assumption.
*/}}
{{- define "kaas.firstByType" -}}
{{- $type := .type -}}
{{- $found := dict -}}
{{- range .ctx.Values.listeners -}}
{{- if and (eq .type $type) (or (not (hasKey . "enabled")) .enabled) -}}
{{- if not $found -}}
{{- $found = . -}}
{{- else if eq (len $found) 0 -}}
{{- $found = . -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $found | toYaml -}}
{{- end -}}

{{/*
kaas.primaryListenerName — name of the first enabled listener in
.Values.listeners. Used as the container-port name for the broker's
startup/liveness probes (which need *some* listener port to TCP-probe
but don't care which one). Fails the chart render if no listener is
enabled — that would mean no Kafka traffic, which is never the intent.
*/}}
{{- define "kaas.primaryListenerName" -}}
{{- $name := "" -}}
{{- range .Values.listeners -}}
{{- if and (eq $name "") (or (not (hasKey . "enabled")) .enabled) -}}
{{- $name = .name -}}
{{- end -}}
{{- end -}}
{{- if eq $name "" -}}
{{- fail "kaas chart: at least one entry in .Values.listeners must be enabled" -}}
{{- end -}}
{{- $name -}}
{{- end -}}

{{/*
kaas.hasInternalTLSListener — true (returns "1") if any enabled
listener has type: internal + tls: true. Distinct from the external
TLS path, which is reconciled by the operator from the KafkaCluster
CR — the internal-TLS case is chart-managed end-to-end via a
selfSigned cert-manager Issuer + Certificate (gh #131).
*/}}
{{- define "kaas.hasInternalTLSListener" -}}
{{- $hit := "" -}}
{{- range .Values.listeners -}}
{{- if and (eq .type "internal") .tls (or (not (hasKey . "enabled")) .enabled) -}}
{{- $hit = "1" -}}
{{- end -}}
{{- end -}}
{{- $hit -}}
{{- end -}}

{{/*
kaas.hasAnyTLSListener — true if any enabled listener has tls: true,
regardless of type. Drives the volume mount + KAAS_TLS_CERT_FILE env
in broker-statefulset.yaml — internal-TLS and external-TLS share the
same broker-side cert loader (one TLS config across all listeners,
per crates/sk-protocol/src/server.rs).
*/}}
{{- define "kaas.hasAnyTLSListener" -}}
{{- $hit := "" -}}
{{- range .Values.listeners -}}
{{- if and .tls (or (not (hasKey . "enabled")) .enabled) -}}
{{- $hit = "1" -}}
{{- end -}}
{{- end -}}
{{- $hit -}}
{{- end -}}

{{/*
kaas.internalTLSSecretName — Secret name for the chart-managed
internal-TLS cert. Cert-manager populates it with tls.crt + tls.key
(no separate ca.crt; the self-signed leaf cert IS the trust anchor).
*/}}
{{- define "kaas.internalTLSSecretName" -}}
{{- printf "%s-broker-internal-tls" (include "kaas.fullname" .) -}}
{{- end -}}

{{/*
kaas.hasEnabledExternalListener — true if any listener has type:
external + (enabled missing or true). Convenience predicate so
templates don't have to range-fold themselves.
*/}}
{{- define "kaas.hasEnabledExternalListener" -}}
{{- $hit := "" -}}
{{- range .Values.listeners -}}
{{- if and (eq .type "external") (or (not (hasKey . "enabled")) .enabled) -}}
{{- $hit = "1" -}}
{{- end -}}
{{- end -}}
{{- $hit -}}
{{- end -}}

{{/*
kaas.superUsersList — emit the cluster-wide superUsers as a
comma-separated string for KAAS_SUPER_USERS. Empty when the list
is empty (broker treats unset env as "no superUsers").
*/}}
{{- define "kaas.superUsersList" -}}
{{- if .Values.authorization -}}
{{- join "," (.Values.authorization.superUsers | default list) -}}
{{- end -}}
{{- end -}}

{{- define "kaas.operatorImage" -}}
{{- $tag := .Values.operator.image.tag | default .Chart.AppVersion -}}
{{- $repo := .Values.operator.image.repository -}}
{{- if not $repo -}}
  {{- $pre := contains "-" $tag | ternary "-preview" "" -}}
  {{- $repo = printf "ghcr.io/kaas-rs/kaas-broker-operator%s" $pre -}}
{{- end -}}
{{ $repo }}:{{ $tag }}
{{- end -}}
