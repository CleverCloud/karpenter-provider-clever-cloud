# Automated integration and end-to-end testing

Unit tests (`make test`) run against the controller-runtime fake client:
fast, deterministic, and blind to everything a real cluster does — CRD
schema validation, admission (CEL), status subresources, watch-driven
reconciles, and the actual Clever Cloud platform. Two additional stages
close that gap. The manual validation runs they automate are recorded in
[E2E-RESULTS.md](E2E-RESULTS.md); the suite's wait bounds derive from the
timing envelope measured there.

## Stage 1 — envtest (`make test-envtest`, every PR)

Runs the provider controllers against a **real kube-apiserver + etcd**
(controller-runtime [envtest](https://book.kubebuilder.io/reference/envtest)),
no cluster needed. `setup-envtest` downloads the binaries on first run
(version pinned by `ENVTEST_K8S_VERSION` in the Makefile, tracking the
`k8s.io/*` minor in go.mod).

What it pins down:

- **Every CRD in `deploy/crds/` installs and reaches `Established`** on the
  targeted apiserver version — the regression a karpenter bump (the
  `karpenter.sh` CRDs are synced automatically from the module) or a
  controller-gen change would otherwise reveal only on a live cluster.
- **CleverNodeClass lifecycle** with real admission: reserved-prefix labels
  are rejected by the CRD's CEL rule at create (the fake client never
  exercises this), everything CEL cannot express degrades to
  `ValidationSucceeded=False`; the readiness conditions (including the
  NodeGroup API probe against real discovery) and the termination finalizer
  blocking deletion while a NodeClaim references the class.
- **providerid stamping** through a real watch: managed worker nodes get
  `clevercloud://<nodegroup>`, unmanaged ones are left alone.

The NodeGroup CRD is owned by Clever Cloud and deliberately not vendored;
the suite installs a loose stand-in from `test/envtest/testdata/` (the typed
client is deliberately tolerant, so the loose schema is representative).
Without `KUBEBUILDER_ASSETS` the package skips itself, keeping plain
`go test ./...` green.

## Stage 2 — e2e suite (`make e2e`, local only)

Runs the full provider against a **real CKE cluster**: real VMs, the real
quota engine, the real node-group operator. The controller is built from the
working tree and runs out-of-cluster (the `make run` shape used by every
manual validation), so the suite always tests the current commit without
needing an image registry.

**This stage is deliberately not wired into CI.** The CKE test clusters are
ephemeral (created for a working session, destroyed at its end), so there is
no stable cluster a scheduled workflow could target — and cluster
credentials (CKE kubeconfigs are cluster-admin) must not live in GitHub
Actions secrets. Run it from a workstation against the test cluster of the
day, typically before a release or after a karpenter-core bump.

```sh
E2E_CONTEXT=<kubeconfig-context> make e2e
```

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `E2E_CONTEXT` | **yes** | — | kubeconfig context of the dedicated test cluster. The suite refuses to run against an implicit current-context: it creates and deletes real VMs, and test-cluster kubeconfigs can have their current-context rotated externally. |
| `KUBECONFIG` | no | `~/.kube/config` | kubeconfig file; the suite writes a private copy with `E2E_CONTEXT` pinned so the controller subprocess cannot drift to another cluster mid-run. |
| `E2E_TIMEOUT` | no | `40m` | suite-wide deadline. The suite reserves ~22 minutes of cleanup budget inside `go test -timeout` (90m in the Makefile) and clamps `E2E_TIMEOUT` down if it would eat into that reserve — a go test timeout kills the process without running cleanups, leaking billed VMs, so the invariant is enforced at startup rather than documented and hoped for. |
| `E2E_METRICS_PORT` / `E2E_HEALTH_PORT` | no | `8090` / `8091` | local ports of the controller subprocess (scenarios assert on the provider metrics). |
| `E2E_ARTIFACTS` | no | `$TMPDIR/karpenter-e2e` | controller log + pinned kubeconfig destination. |
| `E2E_KEEP` | no | — | skip cleanup, for debugging a failed run. Everything left behind **bills hourly**. |

### Scenarios (serial — they share the org quota)

1. **Provision** — 2 pending pods → running pods; every NodeClaim
   `Registered`, its NodeGroup managed + `nodeCount: 1` + owner-referenced,
   its node stamped with `clevercloud://<nodegroup>`.
2. **Consolidation / scale to zero** — workload deleted → claims drained,
   NodeGroups (and VMs) gone: billing stops.
3. **Drift** — a `CleverNodeClass` label change rolls the node; the
   replacement's node carries the new label.
4. **Garbage collection** — both faces of the safety net: a hand-made
   NodeGroup wearing the managed label but no NodeClaim owner reference is
   *refused* (event `GarbageCollectionRefused`, `gc_refused_nodegroups` ≥ 1)
   and survives (it is tainted so the scheduler never treats its VM as
   reschedulable capacity); the NodeGroup of a force-deleted NodeClaim
   *cannot survive* — kube-controller-manager's owner-reference cascade
   usually reaps it within seconds, the provider's 2-minute sweep is the
   backstop, and the suite accepts either winner (the deterministic proof of
   the sweep itself lives in the unit tests). The workload then self-heals
   onto a replacement node.
5. **Quota fast-fail** — an XL-only pool drives provisioning into the org
   quota: `nodegroup_quota_rejections_total` moves, pods stay `Pending`, and
   once the workload is gone **nothing leaks** — the historical failure mode
   of the beta quota engine.

A full green run takes ~25–35 minutes and transiently creates a handful of
small VMs plus one or two XL (the quota scenario); everything is destroyed
before the suite exits.

### Cleanup guarantees

Every object carries the `e2e.karpenter.clever-cloud.com/suite=true` label
and a per-run ID; NodeGroups inherit the `e2e-<run>` name prefix from their
NodeClaims. Cleanup is layered:

1. **In-suite** (deferred, fresh 20-minute context so it survives the suite
   deadline): workloads → NodePools (karpenter drains gracefully) →
   NodeClasses → direct sweep of anything still carrying the run prefix,
   failing the run loudly with the leftover names.
2. **Out-of-band** — [`hack/e2e-cleanup.sh`](../hack/e2e-cleanup.sh): needs
   no controller (after a hard kill of the suite the NodeClaim finalizers
   are stuck — it strips them by hand and deletes NodeGroups directly, VM
   teardown being platform-side). Safe to run at any time; it exits
   non-zero if a live e2e NodeGroup survives the sweep. If the test cluster
   is about to be destroyed anyway, destroying it is also a complete
   cleanup — nothing the suite creates lives outside the cluster and its
   nodegroup VMs.

The controller log lands in `E2E_ARTIFACTS` and its tail is inlined into
the test output on failure, so a red run is diagnosable from the terminal
alone.
