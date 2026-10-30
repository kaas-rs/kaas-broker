# Releasing kaas

Releases are tag-driven. Pushing a semver tag to `main` triggers the
`release` workflow (`.github/workflows/docker-publish.yml`), which builds and
publishes the broker image, the operator image, and the Helm chart to GHCR,
then creates the matching GitHub Release.

Every `v*` tag builds the broker and operator from `bins/*/Dockerfile`
and publishes to `kaas-broker[-preview]` / `kaas-broker-operator[-preview]`. The
`v0.2.x` line is the first cut from the Rust workspace; `v0.1.x` tags
predate it (the version line jumped from `v0.1.190-preview` to
`v0.2.0-preview` at the cutover — the one pre-authorised exception to
patch-bump-only).

## Versioning

- Semver: `vMAJOR.MINOR.PATCH[-PRERELEASE]`.
- While the project is pre-1.0, every release uses the `-preview` suffix
  (e.g. `v0.1.3-preview`). Pre-release tags publish to separate image and
  chart names so they can't be mistaken for stable artifacts.
- **Bump the patch for every release. Never re-cut an existing tag.** Tags
  are immutable; downstream consumers (Helm, ArgoCD, image pulls) cache by
  digest, and re-pointing a tag silently breaks them.

History so far:

| Tag                | Headline change                                                  |
| ------------------ | ---------------------------------------------------------------- |
| `v0.1.0-preview`   | First preview cut — broker registers discovered topics on start. |
| `v0.1.1-preview`   | Helm chart honours `auth.enabled=false` via env.                 |
| `v0.1.2-preview`   | Broker shares one `FlockLock` between engine and produce path.   |
| `v0.1.3-preview`   | Coordinator caps initial rebalance delay for new groups.         |
| …                  | (patch line through `v0.2.47-preview`)                           |
| `v0.3.0-preview`   | SASL/OAUTHBEARER listener auth (gh #42, KIP-255). Minor bump for the new authentication surface; CRDs unchanged, so it rolls in place from `v0.2.47-preview`. |
| `v0.3.1-preview`   | Authorization-only `KafkaUser`s (gh #42): `spec.authentication` optional, for OAuth principals. CRD change is a required→optional relaxation (existing CRs still validate), so it still rolls in place — but the chart does not upgrade CRDs, so apply `deploy/crds/kaas.rs_kafkausers.yaml` before the CR. |
| `v0.3.2-preview`   | Remove the `KafkaClusterAssignments` CRD and the `CrMirror` seam (gh #191/#262): the status writer was never ported from Go, so the CR sat empty on every cluster. CRD-*removing* change, but it rolls in place — nothing ever read or wrote the CR. The orphaned CRD/CR on existing clusters is inert; clean up with `kubectl delete crd kafkaclusterassignments.kaas.rs`. Also drops the CR's RBAC, the `KAAS_CLUSTER_NAME` env, the `cr_mirror_writes` metric, and the never-firing `KaasCRMirrorErrorSustained` alert. |

> **Note on the `v0.3.0` minor bump.** The default rule is patch-only.
> This release takes a minor bump deliberately — it adds a new
> authentication mechanism (a user-visible capability), which is what a
> minor version signals. It changes no CRD schemas, so the
> CRD-unchanged compatibility promise below still holds: it is an
> in-place rolling upgrade from the immediately preceding preview.

## Compatibility policy (pre-v1)

**kaas makes no backwards-compatibility promises before a v1 release.**
Preview releases (`v0.x`) may freely break:

- the on-disk layout (segment/manifest/snapshot formats, directory
  structure, `__cluster/` state files),
- CRD schemas and their semantics,
- env-var and chart-values contracts,
- rolling-upgrade behavior between adjacent previews.

Backwards compatibility at this stage adds complexity that is not worth
carrying. Migration shims, legacy-layout adoption paths, and
mixed-version guards are **optional** — add one only when it is
near-free, and feel free to drop existing ones when they get in the way.

**One promise is kept: a release that leaves the CRD schemas unchanged
supports an in-place rolling update from the immediately preceding
preview release.** The transient mixed-version window of a normal
rolling restart (heartbeats, assignment/state file formats, wire
surface between adjacent versions) must keep working in that case — a
routine version bump must not require touching the deployment. CRD
schema changes are the breaking-release signal: a release that changes
them may break anything, and its supported upgrade path is
**delete the deployment and start fresh.** Call the break out in the
release notes / tag message so the operator knows a fresh deploy is
required; that is the entire obligation. (In practice purely *additive*
CRD changes have rolled fine too, but the promise only covers
CRD-unchanged releases.)

This policy flips at v1: from then on, on-disk formats and CR schemas
version properly and upgrades are supported.

## What gets published

The workflow strips the leading `v` and uses the remainder as both the image
tag and the Helm chart version (so `v0.1.3-preview` → `0.1.3-preview`).

Pre-release tags (`v*-*`) publish to:

- `ghcr.io/kaas-rs/kaas-broker-preview:<version>`
- `ghcr.io/kaas-rs/kaas-broker-operator-preview:<version>`
- `oci://ghcr.io/kaas-rs/charts/kaas-broker:<version>`

Stable tags (no pre-release suffix) publish to the un-suffixed names
(`kaas-broker`, `kaas-broker-operator`). No stable tag has been cut yet.

Once all three are pushed, the workflow creates a **GitHub Release** for the
tag (marked pre-release for `v*-*` tags). Its title and notes come from the
**tag message**: the first line is the title (a leading `vX.Y.Z-preview:` or
`vX.Y.Z-preview —` is stripped, the tag name is prepended), the rest is the
notes, and an artifact list is appended. A lightweight tag falls back to the
tagged commit's message, so use an annotated tag. A tag that already has a
Release is left alone, so re-running the workflow is safe.

Images are built `linux/amd64` only.

## Cutting a release

1. Make sure `main` is green and contains the changes you want to ship.
2. Pick the next patch version. Look at the latest tag:
   ```bash
   git tag -l 'v*' | sort -V | tail -n1
   ```
   Bump the patch (`v0.1.3-preview` → `v0.1.4-preview`).
3. Tag the tip of `main` with an annotated tag and push it. The message
   becomes the GitHub Release: a short first line, then what changed and
   whether CRDs changed (in-place roll vs. fresh deploy, see the policy
   above).
   ```bash
   git tag -a v0.1.4-preview -m "v0.1.4-preview: <one-line headline>

   <what changed>. CRDs unchanged, so it rolls in place from v0.1.3-preview."
   git push origin v0.1.4-preview
   ```
   The release workflow runs automatically on the tag push; no manual
   approval gate.
4. After the run completes, verify the artifacts exist:
   ```bash
   # chart
   helm pull oci://ghcr.io/kaas-rs/charts/kaas-broker --version 0.1.4-preview
   # images (any registry-aware tool works)
   docker buildx imagetools inspect \
     ghcr.io/kaas-rs/kaas-broker-preview:0.1.4-preview
   # GitHub Release
   gh release view v0.1.4-preview
   ```

The `Chart.yaml` `version`/`appVersion` fields are overridden by the CI
packaging step (`helm package --version --app-version`); you do **not** need
to bump them in-tree before tagging.

## Hotfixes

Same flow — there is no separate hotfix branch. Land the fix on `main`,
bump the patch, tag, push. If a bad release ships, cut a new patch with the
fix; do not delete or move the broken tag.

## Infrastructure notes

- The release job runs on the in-cluster `arc-runner-set` (ARC runners
  defined in the `k3s-cluster` repo under `apps/arc-runners`). It needs the
  DinD sidecar for `docker buildx`.
- Image and chart pushes use the workflow's `GITHUB_TOKEN` with
  `packages: write`, and the GitHub Release step uses it with
  `contents: write`; no PATs required.
- Cosign signing was removed (commits `54bcf9e`, `60e1810`); released
  artifacts are not signed today.
