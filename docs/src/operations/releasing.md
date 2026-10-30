# Releasing

Tag-driven releases: broker + operator images and the Helm chart, published to GHCR on every release tag.

The canonical, step-by-step procedure lives in `docs/RELEASING.md` in the
repository — this chapter is the orientation summary.

## The model

Pushing a semver tag to `main` triggers the release workflow
(`.github/workflows/docker-publish.yml`), which builds and publishes three
artifacts to GHCR:

- the broker image — `ghcr.io/kaas-rs/kaas-broker[-preview]`
- the operator image — `ghcr.io/kaas-rs/kaas-broker-operator[-preview]`
- the Helm chart — `oci://ghcr.io/kaas-rs/charts/kaas-broker`

and then creates the tag's GitHub Release, titled and described from the
annotated tag message.

Pre-release tags (anything containing `-`, like `v0.3.1-preview`) get the
`-preview` image-name suffix automatically; the chart's image helpers
derive the same suffix from the tag, so the chart default always points at
images that exist.

## The two rules

1. **Tags are immutable.** Never re-cut or force-move a tag. A bad release
   is fixed by the next patch number, not by rewriting the old one.
2. **Bump the patch by default**: `v0.3.N-preview` → `v0.3.N+1-preview`.
   Minor bumps are deliberate markers for new surfaces, not routine —
   two so far: `v0.1.190-preview` → `v0.2.0-preview` at the Go→Rust
   cutover, and `v0.2.47-preview` → `v0.3.0-preview` for the new
   SASL/OAUTHBEARER authentication surface (see `docs/RELEASING.md`).

## Upgrades before v1

Pre-v1, kaas makes no general backwards-compatibility promises between
previews, with exactly one carve-out:

- **A release that leaves the CRD schemas unchanged** supports an
  in-place rolling upgrade (`helm upgrade`) from the immediately
  preceding preview — adjacent-version heartbeat, state, and wire
  contracts keep working during the roll. Upgrade one release at a
  time; skipping previews is not covered.
- **A release that changes the CRDs** may break anything — on-disk
  layout, wire contracts, chart values. Its supported upgrade path is
  delete-and-redeploy, and the tag message says so explicitly.

Check the tag message before upgrading; it states which case a release
is.

## Before tagging

`cargo xtask ci` green locally, CRDs regenerated if the operator API
changed (`cargo xtask gen-crds` — CI fails on drift), and the book building
cleanly (`cargo xtask docs`). If CRDs changed, remember the chart
[does not upgrade them automatically](./helm.md) — release notes should
say so.
