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
  blocking deletion while a NodeClaim references the class. A NodeClass
  admitted under v0.12.0's narrower CEL rule (installed for the test) with keys
  the current rule rejects: validation ratcheting admits every write that
  leaves `spec.labels` alone, the controller keeps it Ready with
  `LabelsIgnored`, an edit of `spec.labels` that keeps such a key is refused,
  and removing it is admitted.
- **providerid stamping** through a real watch: managed worker nodes get
  `clevercloud://<nodegroup>`, unmanaged ones are left alone.
- **The NodeGroup payload against Clever Cloud's own schema**: for every
  built-in flavor, the NodeGroup `Create` builds (labels from a NodeClaim and
  a NodeClass, the unregistered taint, the owner reference) is admitted and
  stored exactly as sent — nothing pruned or defaulted away — and the
  operator's first status write passes the status schema. A flavor outside
  the CRD's `spec.flavor` enum comes back as the typed admission refusal
  (`ErrFlavorRejected`, reason `Invalid`), built from the apiserver's real
  error, with no NodeGroup created and the flavor held out.
- **The nodegroupstatus watches**: a quota rejection published after the
  launch fails the NodeClaim through the NodeClaim watch, and a Registered
  claim next to an identical refusal is never touched.

The NodeGroup CRD is owned by Clever Cloud and installed by the platform;
this repository never ships it. The suite installs a verbatim capture of it
from a CKE cluster, `test/envtest/testdata/nodegroup-crd.yaml` (server-set
metadata and status removed; the header records the capture date and
Kubernetes version), so its flavor enum, reserved label prefixes, label and
taint patterns, immutability rules and pruning apply as they do live. When
Clever Cloud changes the CRD, refresh the fixture from a test cluster
(`kubectl get crd nodegroups.api.clever-cloud.com -o yaml`, the same
sanitising) and update the header.
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
| `E2E_TIMEOUT` | no | `40m` | suite-wide deadline. The suite reserves 22 minutes plus `E2E_CLEANUP_RECHECK` (24 minutes by default) of cleanup budget inside `go test -timeout` (90m in the Makefile) and clamps `E2E_TIMEOUT` down if it would eat into that reserve — a go test timeout kills the process without running cleanups, leaking billed VMs, so the invariant is enforced at startup rather than documented and hoped for. |
| `E2E_METRICS_PORT` / `E2E_HEALTH_PORT` | no | free ports | local ports of the controller subprocess (scenarios assert on the provider metrics). Unset, the suite picks a free port for each at startup, so suites against sibling clusters run in parallel without clashing. It picks them at random outside the kernel's ephemeral range (`net.ipv4.ip_local_port_range`): outbound connections take their source ports from that range, so none can take a port between its reservation and the controller's bind. The suite binds the ports set here before it picks the others, so a picked port is never one of them. A port set here that another process holds fails the run at once, naming it; the same port for both, or a value that is not a TCP port (1-65535), fails the run too. |
| `E2E_CLEANUP_RECHECK` | no | `2m` | how long after the cleanup the suite looks for the run's NodeGroups once more (Go duration; `0` skips it). See [Cleanup guarantees](#cleanup-guarantees). |
| `E2E_ARTIFACTS` | no | `$TMPDIR/karpenter-e2e` | controller log + pinned kubeconfig destination. |
| `E2E_KEEP` | no | — | skip cleanup, for debugging a failed run. Everything left behind **bills hourly**. |

The suite runs on Linux only (it reads `/proc` and ties the controller's
lifetime to its own). Before the scenarios start it requires the health
endpoint to answer **and** the controller process to be the one listening
on both of its ports, so another local process answering on a port cannot
pass for it. If the controller exits while the suite still needs it (a
panic at startup, a crash mid-run), the suite fails at once with the exit
status and the tail of the controller log, and the running scenario aborts
instead of waiting out its bound against a cluster nobody reconciles.
Suites running in parallel against sibling clusters of one organisation
still draw on the same organisation quota: size it for both.

The harness's own mechanics (the port reservation, the listener ownership
check, the exit watcher, what aborts a wait, how the cleanup treats each
NodeGroup of the run) have tests that need no cluster and no
`E2E_CONTEXT`; `make e2e` runs them with the suite:

```sh
go test -tags e2e -run '^TestHarness' ./test/e2e/
```

**The target cluster must not be running the provider itself.** The suite
starts its own controller with leader election disabled
(`DISABLE_LEADER_ELECTION=true`), so an installed release does not stand by
as a follower: both controllers act at once, provision for the same pending
pods (extra billed VMs, the org quota reached early), and the scenarios,
which assert on the suite controller's metrics, can fail whenever the
in-cluster one does the work — the run stops testing the working tree alone.
Scale an installed release down for the run and back to its own replica
count afterwards (the chart's `replicas` value may be above 1):

```sh
replicas=$(kubectl --context <kubeconfig-context> -n karpenter get deployment/karpenter -o jsonpath='{.spec.replicas}')
kubectl --context <kubeconfig-context> -n karpenter scale deployment/karpenter --replicas=0
E2E_CONTEXT=<kubeconfig-context> make e2e
kubectl --context <kubeconfig-context> -n karpenter scale deployment/karpenter --replicas="$replicas"
```

### Scenarios (serial — they share the org quota)

1. **Provision** — 2 pending pods → running pods; every NodeClaim
   `Registered`, its NodeGroup managed + `nodeCount: 1` + owner-referenced,
   its node stamped with `clevercloud://<nodegroup>`; the node's
   `status.capacity` matches the built-in catalogue (cpu exactly, memory
   within 1%) and the controller refused no capacity report
   (`instancetype_observed_capacity_rejections_total` scraped and = 0; it
   counts every node of a managed group the controller watches — in a
   dedicated test cluster, the suite's own), and no launch ended in an
   acceptance timeout (`nodegroup_acceptance_timeouts_total` delta = 0: the
   node-group operator acknowledges a group about 1 s after its creation,
   and `Create` returns there instead of waiting for the VM). The
   memory the kernel exposes moves with Clever Cloud's node image, and a
   fresh controller packs pods against the built-in value, so a node image
   change fails here first: re-measure `FlavorSizing` (see
   [E2E-RESULTS.md](E2E-RESULTS.md#node-capacity-re-measurement-2026-09-30)).
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

A full green run takes about 12 minutes: 10 for the scenarios (599 s
measured on 2026-09-30) and the 2-minute cleanup re-check. It transiently
creates a handful of small VMs plus one or two XL (the quota scenario);
everything is destroyed before the suite exits.

### Cleanup guarantees

Every object carries the `e2e.karpenter.clever-cloud.com/suite=true` label
and a per-run ID; NodeGroups inherit the `e2e-<run>` name prefix from their
NodeClaims. Cleanup is layered:

1. **In-suite** (deferred, fresh 20-minute context so it survives the suite
   deadline): workloads → NodeGroups without a NodeClaim owner, deleted
   directly (karpenter never drains them) → NodePools (karpenter drains
   gracefully) → NodeClasses → direct sweep of anything still carrying the
   run prefix, failing the run loudly with the leftover names. When the
   controller is already dead nothing can drain, so the suite skips the
   graceful wait and goes straight to the direct sweep; the claims, nodes
   and NodeClasses it leaves on their finalizers need the out-of-band
   script below.
2. **Re-created groups**: the platform was once seen re-creating a deleted
   NodeGroup (same name, new uid, no labels or owner references, a fresh
   VM) about 6 minutes after its deletion, during an upstream incident.
   Nothing in the provider acts on such a group. Every group karpenter
   launches carries its NodeClaim's owner reference, and the only group the
   suite makes without one is the garbage collection decoy
   (`e2e-<run>-decoy`, carrying the run label). Any other ownerless group
   of the run fails the run wherever the in-suite cleanup meets it (the
   first sweep, the graceful wait, the final sweep): its uid, creation time
   and labels are logged and it is deleted. The graceful wait deletes it at
   once instead of waiting on it, since nothing would ever drain it. In a
   10-minute run, this is what catches a 6-minute resurrection of the
   groups deleted early (consolidation, drift): they reappear before the
   final sweep.
3. **Re-check** (`E2E_CLEANUP_RECHECK`, 2 minutes by default): the final
   sweep checks absence at one instant, so the suite lists the run's
   NodeGroups once more after the delay: one that is live again fails the
   run (its uid, creation time and labels are logged) and is deleted again;
   one still terminating is only reported. The re-check only covers groups
   that reappear after the final sweep, within the delay. The garbage
   collection and quota scenarios delete their groups last and the cleanup
   deletes the remaining ones, so a 6-minute resurrection of those lands
   after the final sweep and beyond the 2-minute default: only a longer
   delay covers them. Set `E2E_CLEANUP_RECHECK=7m`, for instance before a
   release.
4. **Out-of-band** — [`hack/e2e-cleanup.sh`](../hack/e2e-cleanup.sh): needs
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
