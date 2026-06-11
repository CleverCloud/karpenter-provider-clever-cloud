# Changelog

## [Unreleased]

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
