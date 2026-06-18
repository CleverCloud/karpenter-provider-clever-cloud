# Changelog

## [Unreleased]

## 0.10.0 - 2026-06-18

CKE flavor prices and the available-flavor list can now refresh automatically from Clever Cloud's public, token-less pricing API — **enabled by default**. The built-in static catalogue stays as the fallback, and `settings.flavors` becomes a per-flavor overlay that coexists with the refresher and always wins.

### ✨ Added

- **`feat(pricing)`: dynamic flavor-catalogue refresher** — a 12 h singleton (`PRICING_REFRESH_PERIOD`) resolves the per-resource rates (`/v4/billing/price-system`, keyed by `zone_id = CLEVER_CLOUD_REGION`) and the available-flavor list for `CLEVER_CLOUD_TOPOLOGY` (`/v4/kubernetes-product`) from Clever Cloud's public, unauthenticated API, recomputes each flavor price (`cpu × vcpu_rate + nominalGB × ram_rate`) and swaps the catalogue. cpu/memory sizing is not exposed by the API, so it stays seeded statically and keeps self-correcting at runtime from observed node capacity. A failed refresh keeps the last-known-good catalogue and retries sooner; it never blocks startup.
- **`feat(charts)`: `settings.pricing`** — `enabled` (default `true`), `refreshPeriod`, `apiURL`, `topology`, and independent per-endpoint `kubernetesProductURL` / `priceSystemURL` overrides, mirrored as `PRICING_*` / `CLEVER_CLOUD_TOPOLOGY` env in `deploy/karpenter.yaml`.

### 🔄 Changed

- **The refresher is enabled by default** — a default install now makes outbound HTTPS to `api.clever-cloud.com` (still token-less, no credentials). Set `settings.pricing.enabled=false` to keep the controller fully in-cluster; the binary's own `PRICING_REFRESH_ENABLED` gate still defaults off as a safe fallback.
- **`settings.flavors` is now a per-flavor overlay**, not a full replacement — every field except `name` is optional, set values win per field over the base/dynamic catalogue, and the overrides are re-applied after each refresh. (The replace-everything override shipped only after `v0.9.2` and was never released.)

## 0.9.2 - 2026-06-12

The project moved to the [CleverCloud organization](https://github.com/CleverCloud/karpenter-provider-clever-cloud); this release is the first to publish under the organization's registry namespace. Functionally identical to 0.9.1 apart from dependency bumps — the changes are in where the artifacts live and what they are called.

### 🔄 Changed

- **The controller image is now `ghcr.io/clevercloud/karpenter`** — the image name no longer repeats the repository name, and the release workflow derives the owner from `GITHUB_REPOSITORY_OWNER` so forks publish under their own namespace. The previous `ghcr.io/diodonfrost/karpenter-provider-clever-cloud` packages remain available but frozen at v0.9.1.
- **Charts publish at `oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/karpenter` and `.../karpenter-crd`** — the redundant `charts/` path segment is gone; install commands across the docs use direct OCI references (`helm repo add` does not support `oci://`).
- **Badges, clone instructions, issue links and chart `home`/`sources`** point at `github.com/CleverCloud/karpenter-provider-clever-cloud`.
- **`chore(deps)`: Kubernetes libraries bumped** — `k8s.io/*` 0.35.0 → 0.36.1, `sigs.k8s.io/controller-runtime` 0.22.4 → 0.24.1, plus grouped GitHub Actions updates.

## 0.9.1 - 2026-06-11

Patch release: point the published artifacts at their real home. The `v0.9.0` charts, docs and the chart's default `image.repository` all referenced `ghcr.io/clevercloud/...`, but the release workflow publishes to `ghcr.io/${GITHUB_REPOSITORY,,}` — i.e. `ghcr.io/diodonfrost/...`. A verbatim `helm install` from the documented OCI path therefore 404'd, and even a hand-supplied chart left the controller in `ImagePullBackOff` because the default image did not exist under `clevercloud`. This release makes the documented install path work as written, with no `--set image.repository` override.

### 🐛 Fixed

- **`fix(charts)`: default `image.repository` now matches the publishing registry** (`charts/karpenter/values.yaml`). The chart's `appVersion`-pinned image now resolves to `ghcr.io/diodonfrost/karpenter-provider-clever-cloud:v0.9.1`, so a default install pulls a real image.
- **`fix(docs)`: every install/upgrade command and registry reference points at `ghcr.io/diodonfrost/...`** across `README.md`, `docs/getting-started/installation.md` and both chart READMEs, and the project/clone/issue/CI-badge URLs point at `github.com/diodonfrost/karpenter-provider-clever-cloud`. The Go module path (`github.com/CleverCloud/...`) is unchanged — it is the import identifier, not a fetch URL.
- **`fix(deploy)`: the raw manifest references a published image** (`deploy/karpenter.yaml`) — `ghcr.io/diodonfrost/karpenter-provider-clever-cloud:v0.9.1` instead of the non-existent `clevercloud/...:dev` — and `make image`'s default `IMAGE` matches the same namespace.

## 0.9.0 - 2026-06-11

Initial release: a [Karpenter](https://karpenter.sh) cloud provider for Clever Kubernetes Engine (CKE). Every operation goes through the cluster's own in-cluster NodeGroup API (`nodegroups.api.clever-cloud.com/v1`) — no Clever Cloud HTTP client, no API token to manage. Validated end-to-end against live CKE clusters ([docs/E2E-RESULTS.md](docs/E2E-RESULTS.md)): node registered in ~44 s end-to-end, consolidation, drift and organisation-quota behaviour all exercised on real hardware.

### ✨ Added

- **`feat(cloudprovider)`: the karpenter `CloudProvider` implementation** (9816d44). One NodeClaim = one dedicated `nodeCount: 1` NodeGroup named after it; provider IDs are `clevercloud://<nodegroup>`; drift detection through the CleverNodeClass hash annotation stamped at creation; organisation-quota rejections map to karpenter's `InsufficientCapacityError` so the scheduler relaxes to other options instead of waiting out the 15-minute registration TTL; terminating NodeGroups read as NotFound so the Clever Cloud finalizer can complete once karpenter releases the node.
- **`feat(nodegroup)`: quota-aware NodeGroup lifecycle** (ca2dfab). Creations are serialized and polled up to 15 s for upstream acceptance; a quota rejection deletes the NodeGroup immediately (frees the reservation) and arms a one-minute fail-fast backoff that any delete clears. Only NodeGroups carrying `karpenter.clever-cloud.com/managed=true` are ever touched — groups created by hand or by CKE's own autoscaler are off-limits.
- **`feat(instancetype)`: static flavor catalog** (ec34228). 2XS through XL with EUR/hour pricing; 2XS/XS/S/M capacities measured on live nodes, L/XL derived estimates self-corrected at runtime from observed node capacity.
- **`feat(controllers)`: four CKE-specific controllers** (61a5a3c). `providerid` stamps `spec.providerID` on nodes (CKE leaves it empty and karpenter matches nodes to NodeClaims through it), `garbagecollection` reaps managed NodeGroups whose NodeClaim is gone, `nodeclass` owns CleverNodeClass validation, readiness conditions and a termination finalizer, `instancetypecapacity` feeds observed node capacity back into the catalog.
- **`feat(apis)`: the two API groups** (92b484d). `CleverNodeClass` (v1alpha1) under `karpenter.clever-cloud.com`, and a deliberately loose hand-written typed client for the Clever Cloud-owned NodeGroup CRD.
- **`feat(charts)`: `karpenter` and `karpenter-crd` Helm charts** (c9fd652) published as OCI artifacts on ghcr.io, plus raw manifests and vendored CRDs under `deploy/` (b16d670).

### 📚 Documentation

- README, getting-started installation walkthrough and the live-cluster E2E validation report (baa8eeb).
- Ten self-contained NodePool/CleverNodeClass examples and seven demo workloads covering the flavor catalog, flavor pinning, pool limits, weighted pools, dedicated team nodes, disruption budgets and node lifetime (8d2f25d).
- Contributing guide and agent instructions, with `AGENTS.md` symlinked to `CLAUDE.md` (c3309a0).

### 🤖 CI

- golangci-lint, unit-test, conventional-commit and CodeQL workflows, dependabot, and a tag-driven release pipeline pushing the container image and both Helm charts to ghcr.io (1d545f2).
