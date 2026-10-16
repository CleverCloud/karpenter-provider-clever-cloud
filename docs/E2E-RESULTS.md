# E2E validation on a real CKE cluster

All scenarios below were executed on 2026-06-10 against a live Clever
Kubernetes Engine cluster (`kubernetes_01KTREHTR6E4JEY46EPYBKY8XG`, Paris,
Kubernetes v1.36.0, 2 control-plane nodes, org quota 40 vCPU / 40 GB RAM),
with the controller running out-of-cluster (`make run`).

## 1. Provisioning

Workload: deployment `inflate`, pods requesting 1 CPU / 1500Mi with
`nodeSelector: clever-cloud.com/cluster-node-role: worker`.

- Pending pod detected → NodeClaim computed → cheapest fitting flavor chosen (2XS).
- NodeGroup created with `nodeCount: 1` and the `karpenter.sh/unregistered:NoExecute` taint.
- Node `default-66vj5-node0` Ready and **registered in 44 s** end-to-end:
  providerID stamped by the providerid controller, labels synced by
  karpenter-core (instance-type, zone, capacity-type, nodepool), startup
  taint removed. Zero `UnregisteredTaintMissing` events.
- 3 replicas → 2× 2XS nodes, pods bin-packed 2+1.

## 2. Consolidation

Scaled 3 → 1 replica:

- `disrupting node(s) ... Empty/delete ... (savings: $0.03)` — the
  documented flavor prices feed the savings computation.
- Node tainted `karpenter.sh/disrupted`, drained, deleted; NodeClaim and
  NodeGroup removed. Node object gone ~40 s after NodeGroup deletion.
  No finalizer deadlock (Get reports terminating NodeGroups as not found,
  releasing Karpenter's node finalizer before Clever Cloud needs it).

## 3. Organisation quota

Scaled to 12 replicas with NodePool limits above the org quota:

- The upstream rejection (`Quota exceeded: RAM max: limit = 40.0 GB ...`)
  surfaced within ~3 s of NodeGroup creation as an
  `InsufficientCapacityError`; the rejected NodeGroup was cleaned up and the
  pods stayed Pending.
- **Observed CKE beta behaviour**: bursts of create→reject→delete cycles
  leaked upstream quota reservations for several minutes (upstream "actual"
  stayed 16 GB above real in-cluster commitments). Mitigated in the
  provider by serializing NodeGroup creations and a 60 s quota backoff
  after any rejection (cleared by any real node deletion). With the
  backoff, upstream churn dropped from ~6 create/delete cycles per minute
  to 1.

## 4. Drift (NodeClass change)

Patched the `CleverNodeClass` with `labels: {environment: production}`:

- NodeClaim marked `Drifted` (reason `NodeClassDrifted`) **9 s** after the
  patch (hash comparison via the NodeGroup's nodeclass-hash annotation).
- Disruption controller decided `replace`, provisioned `default-p9fst`,
  whose node carried `environment=production` from the NodeGroup at join
  time, drained the old node, moved the 2 pods, deleted the old
  NodeClaim/NodeGroup. Full roll completed in ~60 s.

## 5. Scale to zero

Deleting the workload consolidated the last node away; the cluster
returned to control-plane-only with zero managed NodeGroups left.

## Example catalog validation

Every example under `examples/` was applied and exercised on the live
cluster (sequentially, to stay inside the organisation quota):

| Example | Verified behaviour | Time |
|---|---|---|
| v1/general-purpose + workloads/inflate | 2 pods → one 2XS node, pods Running | 42 s |
| v1/small-flavors | 5Gi pod skips 2XS, lands on XS | 33 s |
| v1/large-instances | 10Gi pod → M node (L/XL blocked by org quota, as documented — see the correction below) | 175 s |
| v1/flavor-pinned | XS provisioned although 2XS is cheaper (pool pinned via `clever-cloud.com/flavor`) | 41 s |
| v1/cpu-limit | exactly 2 nodes (8 vCPU cap), 5th pod Pending, log `all available instance types exceed limits for nodepool` | — |
| v1/team-dedicated + workloads/team-workload | node carries taint `team=platform:NoSchedule`, NodePool label `team` and NodeClass label `cost-center` | 41 s |
| v1/weighted-nodepools | claim created by `preferred-small` (weight 100), not the fallback pool | 42 s |
| v1/min-values | claim requirement keeps `minValues: 2` with 3 candidate flavors | 42 s |
| v1/max-node-lifetime (expireAfter patched to 2m) | replacement claim created at node age 2m06s | — |
| v1/disruption-budgets | empty node NOT consolidated during the business-hours `nodes: "0"` window; consolidated 41 s after the budget opened | — |
| workloads/one-pod-per-node | 3 replicas → 3 distinct nodes via required pod anti-affinity | 61 s |
| workloads/do-not-disrupt | empty nodes consolidated around the protected node; node released 41 s after the pod was removed | — |
| workloads/flavor-selector | plain nodeSelector `clever-cloud.com/flavor: XS` → XS node | 41 s |
| workloads/prefer-small | preferred affinity steers provisioning to 2XS | 42 s |
| workloads/disruption-budget | deployment + PDB schedule onto existing capacity, PDB active | 8 s |

> **Correction (2026-09-30):** the org quota is not what kept L and XL out of
> `v1/large-instances` in this run and the second one: they were never even
> attempted, because the example's own NodePool limits (16 vCPU / 16Gi)
> excluded them. karpenter-core never launches an instance type whose
> capacity exceeds the pool's remaining limits, and L and XL both carry more
> than 16Gi of memory. The 10Gi pod landing on M stands — M is the cheapest
> allowed flavor that fits it. The example's limits now admit L and XL.

One finding from testing: a `topologySpreadConstraint` on
`kubernetes.io/hostname` does **not** force one replica per node (with a
single existing node the constraint is trivially satisfied), which is why
the catalog ships `one-pod-per-node.yaml` based on required pod
anti-affinity instead.

## Measured timings

| Operation | Duration |
|---|---|
| NodeGroup creation → node Ready | 26–60 s |
| Node Ready → registered (providerID + label sync) | < 5 s |
| NodeGroup deletion → Node object gone | ~40 s |
| Quota rejection visibility in NodeGroup conditions | 1–3 s |
| NodeClass change → Drifted condition | < 10 s |

---

# Second validation run (2026-06-11)

Re-validation of the full example catalog on a fresh CKE cluster
(`kubernetes_01KTTX1BTYGBZ2VB0N38QAF35X`, Paris, Kubernetes v1.36.0,
2 control-plane S nodes, org quota 40 vCPU / 40 GB RAM), controller
out-of-cluster (`make run`, commit `6df2b61-dirty`). Two changes vs the
first run: the CRDs were installed through the new **karpenter-crd Helm
chart** (first live validation: all four CRDs `Established` with Helm
ownership metadata), and the instance-type catalog now carries the
capacities self-corrected from live nodes (commit `e0cbd0d`).

## Results

All 15 scenarios passed. Times are pod-pending → pod-Running.

| Scenario | Verified behaviour | Time |
|---|---|---|
| general-purpose + inflate ×3 | one XS node fits all 3 pods — corrected catalog makes 1× XS (€0.0611/h) beat the first run's 2× 2XS (€0.0666/h) | 40 s |
| — consolidation (3→1) | **replace** consolidation: XS swapped for a cheaper 2XS, pod moved, no finalizer deadlock | 102 s |
| — org quota (→12) | upstream `Quota exceeded: RAM max: limit = 40.0 GB \| actual = 48.0 GB` mapped to `InsufficientCapacityError`; backoff fails retries fast (~10 s apart); zero leaked NodeGroups | rejection +34 s |
| — drift | `Drifted` < 1 s after NodeClass patch; replacement NodeGroup carries `environment=production`; full roll | 53 s |
| — scale to zero | last node consolidated away, zero managed NodeGroups | 31 s |
| v1/small-flavors | 5Gi pod skips 2XS, lands on XS | 46 s |
| v1/large-instances | 10Gi pod → M node (L/XL quota-blocked — see the 2026-09-30 correction under the first run) | 185 s |
| v1/flavor-pinned | XS provisioned (NodeGroup `spec.flavor=XS`) despite cheaper 2XS | 183 s |
| v1/cpu-limit | exactly 2× 2XS (`status.resources.cpu=8` at the cap), 5th pod Pending | — |
| v1/team-dedicated + team-workload | node carries `team=platform:NoSchedule` taint + `team` label | 42 s |
| v1/weighted-nodepools | claim created by `preferred-small` (weight 100) | 42 s |
| v1/min-values | claim keeps `minValues: 2` with 3 candidate flavors | 42 s |
| v1/max-node-lifetime (expireAfter 2m) | replacement claim at node age 2m02s | — |
| v1/disruption-budgets | empty node NOT consolidated for 3+ min inside the business-hours window; consolidated 31 s after the budget opened | — |
| workloads/one-pod-per-node ×3 | 3 distinct nodes via required anti-affinity (see incident below) | — |
| workloads/do-not-disrupt | 2 empty nodes consolidated around the protected node (92 s); node released 31 s after the pod was removed | — |
| workloads/flavor-selector | nodeSelector `clever-cloud.com/flavor: XS` → XS node | 115 s |
| workloads/prefer-small | preferred affinity steers to 2XS | 42 s |
| workloads/disruption-budget | 2 pods + PDB on existing capacity; `minAvailable=50%`, `disruptionsAllowed=1` | 42 s |

## New CKE beta behaviour: asynchronous NodeGroup deletion upstream

During one-pod-per-node, one of three NodeGroups was accepted by the
upstream operator (`Synced` within a second of creation, claim `Launched`)
and then **deleted upstream several seconds later** — no provider delete,
no NodeGroup events, suspected quota-edge race (the third node landed the
org RAM accounting exactly at the limit). The provider gets no signal once
its 15 s post-create poll has seen `Synced`.

The designed backstop handled it: karpenter-core's **15-minute
registration TTL** reaped the never-registered claim at exactly +15 min, a
replacement claim provisioned ~45 s later, and all 3 pods went Running
without intervention. Cost: one pod Pending ~16 min. This is the
documented trade-off of treating the post-create poll optimistically;
acceptable while the quota engine stays beta.

## Controller log audit

160 ERROR-level lines over the 70-minute run, all expected: 61 `could not
schedule pod` (quota and cpu-limit tests), 49 `failed launching nodeclaim`
(quota fast-fail backoff working as designed), 50 transient
`NodePool not found` reconciler errors during scenario cleanups (NodePool
deleted while its claims were still draining). No unexpected errors, no
panics.

## Timing envelope

Same envelope as the first run, with more upstream variance: NodeGroup
creation → pods Running ranged 40–185 s (XS once took 183 s where another
XS took 46 s — VM pool variance, not provider-side). Deletion ~30–40 s,
drift detection < 1 s, quota rejection surfaced in ~3 s of NodeGroup
creation.

---

# Node capacity re-measurement (2026-09-30)

`status.capacity` read on one live node of each flavor on a CKE cluster
running Kubernetes 1.37.0 (kernel 7.2.8, worker-only topology). These are
the built-in catalogue's figures from this date on
(`FlavorSizing` in `pkg/providers/instancetype`).

| Flavor | cpu | Memory (Ki) | Previous seed (Ki) | Gap |
|---|---|---|---|---|
| 2XS | 4 | 3715344 | 3911884 | −5.0% |
| XS | 6 | 7553664 | 7937580 | −4.8% |
| S | 8 | 11385832 | 11957148 | −4.8% |
| M | 10 | 15229256 | 15988992 | −4.8% |
| L | 12 | 22896304 | 23983488 | −4.5% |
| XL | 16 | 30584176 | 31977984 | −4.4% |

On every flavor, allocatable memory is exactly capacity − 100Mi, and the
other figures did not move: ephemeral-storage 40971488Ki (allocatable
37759323279 bytes), 110 pods. The previous seed came from an earlier node
image (L and XL extrapolated from M, never measured). Until a node of a
flavor reports, karpenter packs pods against the seed. Fed the previous seed,
karpenter-core's own scheduler promised two 1 CPU / 1700Mi pods plus 224Mi of
DaemonSets to one 2XS, whose measured allocatable cannot hold them (a second
node follows), and chose an XL for a 29.5Gi pod that no measured flavor
holds — a launch repeated after every controller restart while no XL node
runs.

---

# Third validation run (2026-10-01)

Live validation of the 2026-09-30 fix series on a CKE cluster
(`kubernetes_01M3S499F1ZK2EA68TECG770GT`, Paris, Kubernetes v1.37.0,
worker-only topology, kernel `7.2.8-clevercloud-vm-dirty`, containerd 2.3.5,
Cilium, org quota 100 vCPU / 100 GB RAM), with an **unmanaged** 2× S
NodeGroup as baseline capacity. The controller ran out-of-cluster, built from
the tip of the series; the "before" figures come from the same cluster at
`1beb82e` (v0.12.0 plus its P1 fixes) on 2026-09-30.

## `make e2e`

| Scenario | Result | Notes |
|---|---|---|
| Provision | PASS (50 s) | pods Running in 45 s; the 2XS node reported exactly the catalogue's memory (3715344Ki, +0.00%); no acceptance timeout on the healthy launches |
| ConsolidationScaleToZero | PASS (71 s) | pool consolidated to zero in 1 min 10 s |
| Drift | PASS (115 s) | replacement registered 1 min 25 s after the NodeClass patch |
| GarbageCollection | PASS (252 s) | ownerless decoy refused in 2 min 5 s; owner-reference cascade reaped the force-deleted claim's group in 5 s |
| QuotaFastFail | PASS (106 s) | rejection surfaced in **10 s** (35 s at `1beb82e`), 4/4 pods Pending, no leaked NodeGroup |

Whole suite: 1020 s, including a 7-minute `E2E_CLEANUP_RECHECK` that found no
NodeGroup of the run re-created upstream.

## Operator acknowledgement timeline

Watched event by event (`kubectl get nodegroups -w`) on hand-made groups:

- the operator's **first** status write lands ~1 s after creation and is
  already the decision: an accepted group gets `phase=Creating` +
  `ReconcileInProgress=True(Creating)`; a quota-rejected one gets
  `phase=QuotaExceeded` + `ReconcileFailed=True(QuotaExceeded)` directly,
  never preceded by `Creating`. The quota condition message is now empty
  (it used to carry `Quota exceeded: RAM max: limit = …`);
- karpenter registers the node ~28 s after creation while the group is still
  `Creating`; `phase=Synced` and `Ready=True` follow together at 38–58 s
  (one outlier stayed `Creating` for 6 min 30 s).

This is what the acceptance poll now waits for: the acknowledgement, not
`Ready`.

## Upgrade from `1beb82e` and an external resize

1. `1beb82e` created a managed 2XS group (`spec.labels` carrying
   `karpenter.sh/nodepool`, hash generation `v2`), then was stopped.
2. The new controller adopted it: the stored hash was migrated to `v3` with
   **no drift** (the NodeClaim kept every condition `True`) and the NodeClass
   stayed `Ready`.
3. `nodeCount: 1 → 2`: the extra `-node1` joined with `karpenter.sh/nodepool`
   and no provider ID. The controller refused to stamp it (by design) and
   **removed `karpenter.sh/nodepool` from it** half a second later.
4. Controller restarted while the group was still resized, then the workload
   scaled up: `karpenter_cluster_state_synced` was `1` and the new NodeClaim
   existed **4 s** after the restart. At `1beb82e` the same sequence kept the
   cluster state unsynced for 4 minutes — no provisioning, no disruption —
   until the resize was reverted.
5. The new group's `spec.labels` no longer carry `karpenter.sh/nodepool`; its
   node got `karpenter.sh/nodepool`, `topology.kubernetes.io/zone=par` and
   `topology.kubernetes.io/region=par` from registration.

## Region selector

One pod with `nodeSelector: topology.kubernetes.io/region: par`: **one**
NodeClaim, pod Running in 45 s. At `1beb82e` the same pod made karpenter
launch three VMs in 60 s (stopped only by the NodePool limit) and was never
scheduled.

## Before / after

| Measure | `1beb82e` | This series |
|---|---|---|
| `CloudProvider.Create` duration (healthy launch) | 15.06 s | 1.16 s |
| `nodegroup_acceptance_timeouts_total` on healthy launches | 7/7 | 0 |
| Quota rejection surfaced by the e2e quota scenario | 35 s | 10 s |
| Restart with an externally resized group | cluster state unsynced until the resize is reverted | synced, provisioning in 4 s |
| Pod with a region `nodeSelector` | 3 VMs, never scheduled | 1 VM, Running in 45 s |
| Outbound HTTP to Clever Cloud | pricing refresher (failing: `/v4/kubernetes-product` now answers 401) | none |

---

# DELETE error and quota backoff validation (2026-10-13)

Validated the updated `nodegroup.Provider.Delete` against
`karpenter-test@kubernetes_01M4TR2F3NY1WHFEJWG1RP86DT` (CKE 1.37)
with a direct provider harness and real Kubernetes API calls. A disposable
managed `2XS` NodeGroup used `nodeCount: 0` and a test-owned finalizer;
no NodeClaim or worker VM was created. The quota rejection was seeded in
provider memory, without exhausting the organisation quota.

- At 16:37:15 CEST (Europe/Paris, UTC+02:00), a DELETE impersonating an
  identity without write access returned a real API `Forbidden`. The
  rejected `2XS` flavor and larger `XS` flavor both remained unavailable.
- Retrying through the authorized client succeeded and cleared the older
  quota backoff. The NodeGroup still had its deletion timestamp and
  finalizers: accepted deletion does not confirm VM destruction or quota
  release.
- Cleanup removed only the test-owned finalizer. The platform reconciled
  its own finalizer, and the disposable NodeGroup disappeared.

Unit tests additionally cover service unavailability, cancellation, stale
`NotFound` retries, and a new or refreshed quota rejection arriving while
DELETE is in flight, including when the fake clock has not advanced.
This targeted API test does not reproduce CKE `UPDATING` or `FAILED`.


---

# Cluster upgrade blocks platform operations (2026-10-13)

Manual investigation on `karpenter-test`
(`kubernetes_01M4TR2F3NY1WHFEJWG1RP86DT`, Paris, `DEDICATED_COMPUTE`),
upgrading Kubernetes **1.36 → 1.37** with an unmanaged 2× XS worker group
as baseline. The controller ran out-of-cluster and reported build
`33a6c2a-dirty`. This investigation used the **existing controller before the
deletion-overdue feature**; it is evidence for the failure mode and recovery,
not a live test of the new metric or event.

A managed 2XS NodeClaim/NodeGroup (`upgrade-probe-before`) was provisioned
before the upgrade. During `UPDATING`, its NodeGroup was deleted and a second
2XS NodeClaim (`upgrade-probe-during`) created. The first NodeClaim was then
deleted to exercise Karpenter's termination flow. The test NodePool disabled
consolidation and voluntary disruption; no finalizer was forcibly removed.
Platform state was observed separately with the operator's authenticated
CLI; the provider continued to use only the Kubernetes API.

Times below are Europe/Paris (CEST, UTC+02:00). Platform and Kubernetes
snapshots were sampled every 5 seconds; controller log times are exact.

| Time | Observation |
|---|---|
| 15:57:36 | Platform CLI accepted the upgrade to 1.37. |
| 15:57:40 | Platform state was `UPDATING`. Kubernetes accepted the NodeGroup DELETE and the new NodeClaim. The deleting group retained `clever-nodegroup-operator` and its old `Ready=True` condition. |
| 15:57:56 | The new group's acceptance poll timed out after 15 s with `ReconcileFailed=True(UpstreamError)`, message `Upstream creation failed: failed to reconcile resource, upstream creation failed, CreationFailedUpstream`. The provider emitted `NodeGroupTransientFailure` and `NodeGroupAcceptanceTimeout`, returned optimistically, and core set `Launched=True`; no new node existed yet. |
| 15:58:01–15:58:09 | Deleting the old NodeClaim started drain; the sampled claim reached `Drained=True`, `VolumesDetached=True`, `InstanceTerminating=True`. Its NodeGroup stayed visible with the platform finalizer. |
| 16:01:01 | First platform sample back at `ACTIVE`; the queued operations resumed automatically. |
| 16:01:05–16:01:06 | Core deleted the old Node and NodeClaim. The 16:01:06 Kubernetes sample no longer contained the old NodeGroup, while the platform still listed it as `READY` with one node. |
| 16:01:32–16:01:40 | The newly requested NodeClaim registered and initialized without intervention. The platform showed its group `READY` by 16:01:56. |
| 16:03:02 | The old group's platform record reported `DELETED`, `currentNodeCount: 0`, with `updatedAt: 14:03:02.777502Z`; confirmed by the later direct group read. |

The deletion remained visible in Kubernetes for about 3 min 26 s, shorter
than the new 5-minute observation threshold. Platform confirmation of its
completion arrived about **two minutes after** the Kubernetes NodeGroup
and NodeClaim disappeared. Disappearance from the in-cluster API therefore
does not prove that the platform has finished deleting the VM; the new
`nodegroup_deletion_overdue` gauge deliberately measures only the former.

`UPDATING` with automatic recovery was reproduced. A persistently `FAILED`
cluster was **not** induced, so no recovery bound is claimed for that state.
A separate characterization test runs the existing core termination
controllers after drain: across repeated reconciles a Ready Node remains,
a NotReady Node can be released, and the NodeClaim keeps waiting. Deletion
resumes when the test simulates the platform completing its finalizer. This
pins the wait/recovery contract, not a maximum duration or the new overdue
signal; the latter has its own deterministic threshold and event tests.

Cleanup removed both managed probe groups, their NodeClaims, the temporary
NodePool/NodeClass, the out-of-cluster controller and the temporary
Karpenter CRDs. Final platform state was `ACTIVE` at 1.37 with only the
baseline two workers; quota usage returned to 18 vCPU / 24 GB (including
the control plane).

## Deletion-overdue signal validation

The new reporting feature was tested separately on the same cluster on
2026-10-13 using a disposable managed NodeGroup, `pr-deletion-probe`
(`flavor: 2XS`, `nodeCount: 0`). A test-owned finalizer retained the deleting
object; no NodeClaim or worker was created. The actual GC `Reconcile` ran
against the real Kubernetes API with the real `events.Recorder`, called
every 10 seconds for this probe (production remains every 2 minutes).

| Time (Europe/Paris, CEST) | Observation |
|---|---|
| 16:31:49 | Deletion age 4 min 58 s: `nodegroup_deletion_overdue=0`. |
| 16:32:00 | Deletion age 5 min 9 s: gauge `1`; a real Warning `NodeGroupDeletionOverdue` event was recorded on the NodeGroup and named its remaining test finalizer. |
| 16:32:00 | Removing only the test-owned finalizer let the NodeGroup disappear; the next reconcile returned the gauge to `0`. |

The platform had already cleared its own finalizer; it was never forcibly
removed. This validates the feature's threshold, Kubernetes event and gauge
recovery with no NodeClaim. It does not reproduce `FAILED` or validate VM
creation/deletion: this probe had no worker, unlike the upgrade investigation
above.


---

# Deletion diagnostic follow-up (2026-10-14)

Validated the revised message on `karpenter-review`
(`kubernetes_01M4WSYD7Q0BS4FRV9GSTCT4A2`, Kubernetes 1.36.3). The
actual GC and event recorder ran against the real API every ten seconds,
using a disposable managed 2XS NodeGroup with `nodeCount: 0` and a
test-owned finalizer. No NodeClaim or worker was created for this probe.
Times below are Europe/Paris (CEST, UTC+02:00).

- At 12:45:24, deletion age 4 min 55 s, the gauge was still zero.
- At 12:45:35, age 5 min 6 s, the gauge was one. The Warning recorded the
  age at observation (5 min 4 s), `phase: DeletionRequested`, the last
  `Ready` and `Terminating` true conditions, and the test finalizer.
- CKE had already removed its own finalizer. The message correctly pointed
  to the remaining finalizer's controller and the current NodeGroup YAML,
  without directing this case to Clever Cloud support or claiming VM
  cleanup was complete.
- Removing only the test-owned finalizer let the object disappear; the
  next reconcile returned the gauge to zero at 12:45:38.

The exact five-minute boundary and same-name/new-UID behavior are covered
separately by deterministic tests with the real event deduplication logic.
The production GC cadence remains two minutes. The prior upgrade and
this zero-worker diagnostic probe establish different properties; neither
reproduces a persistently `FAILED` cluster.


---

# Expanded worker disk capacity (2026-10-14)

The platform rollout enlarges the disk of every newly created worker,
including workers launched in older clusters; existing workers retain
their disk size. This rollout contract was confirmed by the platform owner.

Measured on `karpenter-review`
(`kubernetes_01M4WSYD7Q0BS4FRV9GSTCT4A2`, Kubernetes 1.36.3,
kernel 7.2.10) on 2026-10-14: the baseline XS and a freshly created,
NoSchedule-tainted 2XS worker reported the same disk values. The new 2XS
Node appeared at 12:38:58 Europe/Paris (CEST, UTC+02:00). Read-only checks
of eight workers created on 2026-10-07 in the older `csi-migration-tests`
cluster also returned those disk values. No Kubernetes 1.37 cluster was
available for this measurement; the current disk size was not inferred
from its Kubernetes version.

| Resource | Current worker | Former worker profile |
|---|---|---|
| Ephemeral-storage capacity | 102624184Ki (about 97.9Gi) | 40971488Ki (about 39.1Gi) |
| Ephemeral-storage allocatable | 94578447818 bytes (about 88.1Gi) | 37759323279 bytes (about 35.2Gi) |

The new seed reserves 10262419Ki (10% rounded up), advertising 92361765Ki
allocatable before observation: 458 bytes below the measured value.
The former profile remains valid evidence from the September measurement,
but cannot reduce the disk advertised for new launches. Its CPU, memory
and pod observations still contribute to their normal per-resource minima.
Existing-node placement continues to use that Node's actual capacity;
the change does not resize or drift old workers. Unknown intermediate or
inflated disk reports remain refused rather than widening the tolerance
across the gap between the two profiles.

The disposable measurement group was deleted through the NodeGroup API;
no platform finalizer was removed manually. Unit tests cover both profile
orders, missing-disk overhead fallback, and invalid profile pairs. A test
using core's scheduler shows a 1Gi request using an old worker, a 50Gi
request getting a new worker, and the same request staying on that in-flight
claim without another launch; the old claim does not drift.


## Combined end-to-end validation

The complete suite passed against `karpenter-review` with the security,
quota, deletion-observability and disk fixes combined (run `60416a`,
2026-10-14; Go 1.26.9). The integration tree was `b412b5b`; stacking the
PR branches reproduced that tree exactly before the final documentation
and test-comment clarification. No capacity assertion was disabled.

| Scenario | Result | Duration |
|---|---|---|
| Provision | PASS | 60.71 s |
| ConsolidationScaleToZero | PASS | 65.40 s |
| Drift | PASS | 115.71 s |
| GarbageCollection | PASS | 346.20 s |
| QuotaFastFail | PASS | 10.61 s |

`TestE2E` completed in 725.28 s including setup and cleanup. There were no
managed workers at startup: the two pending pods, each requesting 50Gi of
ephemeral storage, became Running on two freshly provisioned 2XS workers
in 55 s. Both reported the new disk capacity and allocatable above. The
Provision assertions verified zero capacity-rejection and acceptance-timeout
deltas. A metrics snapshot during final cleanup still showed both counters
at zero, the deletion-overdue gauge at zero, and one expected quota
rejection. No overdue-deletion diagnostic appeared in the controller log.

The ownerless decoy survived the GC, and the orphaned group's owner-reference
cascade reaped it before the provider sweep. After normal cleanup and its
two-minute recheck, Kubernetes showed only the original manual NodeGroup
and Node, with their original UIDs, and no NodeClaims, NodePools,
CleverNodeClasses or test namespace. The platform API independently showed
an ACTIVE cluster with only its original worker group. Pre-existing Karpenter
CRDs were retained.

The full local validation also passed on the standalone changes and their
combined tree: vet, lint, build, unit/race tests, envtest, chart lint/tests,
vulnerability scan, and generated CRD/manifest checks without drift.


---

# Controller-node disk reference (2026-10-14)

The complete suite passed on `karpenter-review`
(`kubernetes_01M4WSYD7Q0BS4FRV9GSTCT4A2`, Kubernetes 1.36.3,
kernel 7.2.10) with the dynamic ephemeral-storage reference enabled.
Run `f27ee4` took place around 14:23–14:35 Europe/Paris
(CEST, UTC+02:00), using Go 1.26.9. The production source did not change
between the controller build and completion of the run.

The controller ran outside the cluster with
`E2E_NODE_NAME=manual-nodegroup-node0`, explicitly selecting the existing
fixed XS worker as the reference. Chart tests separately verify injection
of the pod's own node name through the Downward API; this run did not
deploy a controller pod.

| Scenario | Result | Duration |
|---|---|---|
| Provision | PASS | 61.05 s |
| ConsolidationScaleToZero | PASS | 65.28 s |
| Drift | PASS | 120.90 s |
| GarbageCollection | PASS | 336.49 s |
| QuotaFastFail | PASS | 10.95 s |

`TestE2E` completed in 720.79 s including setup and cleanup. Before
provisioning, the reference metrics reported readiness `1`, capacity
`105087164416` bytes (`102624184Ki`) and allocatable `94578447818` bytes.
Each of the two pending pods requested `56747068689` bytes of ephemeral
storage, 60% of the reference's allocatable disk. Both became Running on
freshly provisioned 2XS workers in 55 s. Their actual disk capacity and
allocatable exactly matched the reference; the launch snapshot annotations
on both NodeGroups contained these same values. No capacity assertion was
disabled.

The final controller metrics showed zero observed-capacity rejections,
zero acceptance timeouts, zero overdue deletions and one expected quota
rejection. The ownerless decoy survived the GC; the orphaned group's owner
reference triggered Kubernetes garbage collection before the provider
sweep. Cleanup and its two-minute recheck passed. Kubernetes then contained
only the original manual NodeGroup and Node, with their original UIDs, and
no NodeClaims, NodePools, CleverNodeClasses or test namespace. The local
controller stopped and pre-existing CRDs were retained. This cleanup check
does not independently confirm VM deletion through the platform API.

No disk was resized on the platform for this run. Increases, decreases,
invalid-reference recovery and replacement of the reference Node are
covered by unit tests and an envtest watch against a real Kubernetes 1.37
API server. Tests using Karpenter's scheduler verify that a remote 200Gi
PVC, both already bound and waiting for its first consumer, is independent
of a worker's 40Gi local disk; they do not provision a real remote volume.
Adoption tests preserve an existing launch's disk snapshot after a reference
change or outage and refuse to recreate a vanished or replaced NodeGroup.

The full local validation passed: vet, lint, build, unit and race tests,
envtest, chart lint/tests, e2e harness tests, generated CRD/manifest checks
without drift, and govulncheck with no reachable vulnerabilities.

## Provisioning without a disk measurement

A subsequent change on 2026-10-14 removes the global startup gate. When
the dynamic reference is unavailable, future workers advertise zero
guaranteed ephemeral storage while retaining CPU, memory and pod capacity.
This permits requests without local disk reservations. Positive disk
requests, including DaemonSet overhead, and NodePools with an explicit
ephemeral-storage limit still require a valid reference.

Tests using the real Karpenter scheduler pass for mixed batches at startup
and after reference invalidation: only the pod requiring disk waits, the
other launches exactly one worker and reuses it while in flight. A zero/zero
launch snapshot survives adoption and controller restart; once the Node
registers, its actual disk status permits scheduling onto that worker.
Other regressions cover DaemonSet disk reservations, NodePool disk limits,
recovery, and remote 200Gi PVCs without a local disk request. Envtest checks
the zero-disk catalogue through the real Node watch and its recovery.

The follow-up passed vet, build, lint, all unit/race tests, envtest, chart
lint/tests and e2e harness tests on Go 1.26.9. The live run `f27ee4` above
predates this follow-up. An additional live probe was deferred because
another review was actively using the test cluster; no controller was
started and no cluster resources were changed for this follow-up.


## Independent review of the zero-disk follow-up

The independent PR #82 review subsequently tested commit `7c80175` on the
same cluster on 2026-10-14. Its full suite, run `65bacc`, passed **5/5** in
736.38 s (15:55–16:07 Europe/Paris, CEST, UTC+02:00): Provision 65.57 s,
ConsolidationScaleToZero 67.55 s, Drift 110.30 s, GarbageCollection 343.83 s,
and QuotaFastFail 8.75 s. Cleanup and a later recheck restored the original
Node/NodeGroup and left no test NodeClaims, NodePools or CleverNodeClasses.

Separate review probes demonstrated CPU-only provisioning with a `0/0`
launch snapshot while the reference was unavailable, recovery to actual Node
status after registration, and snapshot adoption across controller restart.
They also found two gaps fixed by the subsequent revision: an unlaunched
claim could hold core's cluster synchronization after losing its disk
reference, and an error from the catalogue prevented empty-node cleanup in
a pool with a disk limit. This historical run predates those fixes and the
configurable static/host/named-node chart modes.


## Review fixes: upgrade safety and reference outages

The revised PR #82 working tree based on `7c80175` passed a fresh full run,
`9ec339`, on 2026-10-14 around 17:26–17:38 Europe/Paris (CEST, UTC+02:00).
The cluster and reference were unchanged: CKE 1.36.3, kernel 7.2.10,
`E2E_NODE_NAME=manual-nodegroup-node0`. Production and e2e source hashes
were unchanged between build and completion.

| Scenario | Result | Duration |
|---|---|---|
| Provision | PASS | 70.23 s |
| ConsolidationScaleToZero | PASS | 70.75 s |
| Drift | PASS | 119.70 s |
| GarbageCollection | PASS | 335.56 s |
| QuotaFastFail | PASS | 12.50 s |

`TestE2E` completed in 742.64 s, including cleanup and its two-minute
recheck. Both provisioned workers exactly matched the reference's disk
capacity and allocatable; their pods became Running in 61 s. The harness
now directly verifies both NodeGroup snapshot annotations against the
reference and NodeClaim status. The reference still equals the static seed
on this cluster; this run does not by itself distinguish those profiles.
Final metrics showed zero capacity rejections, acceptance timeouts and
overdue deletions, plus one expected quota rejection.

The final Kubernetes object identities matched the captured baseline:
only the original manual NodeGroup and Node, no NodeClaims, NodePools,
CleverNodeClasses or test namespace. The controller stopped; its temporary
binary and kubeconfig copy were removed. This is a Kubernetes cleanup
check, not an independent confirmation through the platform VM API.

The local checks all passed under Go 1.26.9: vet, build, unit/race tests,
lint, envtest, chart lint/tests, harness tests, generated-file hashes and
govulncheck (zero reachable vulnerabilities). New regression tests run
core's actual lifecycle, cluster-state and provisioning controllers to
prove a lost reference releases an unlaunched claim and allows CPU-only
provisioning in another pool. Core's disruption candidate path verifies
that a disk-limited pool remains eligible for empty-node cleanup while
new launches are unavailable. API-reader failures remain retriable.
Adoption tests cover legacy groups without snapshots, quota backoff and
owner UID replacement. Metric and watch tests verify invalidation and
recovery after the reference was actually read.

Envtest additionally starts the real controller binary against an isolated
API server with a 23Gi/19Gi reference, distinct from the static seed, and
checks static versus dynamic wiring. Chart tests send 24 strategic-merge
and server-side-apply transitions through a real API server, including
upgrades from a deployment without `NODE_NAME`. The live run uses an
out-of-cluster binary; it does not deploy the chart's controller pod or
resize physical disks. Control-plane disk equivalence remains unmeasured.

# karpenter-core 1.15 upgrade validation (2026-10-16)

Live validation of the karpenter-core 1.14.1 → 1.15.0 bump on a fresh CKE
cluster (`kubernetes_01M51XVYXJ0GKWQHWF0HSJZFP8`, Paris, Kubernetes v1.36.3,
worker-only topology, kernel `7.2.11-clevercloud-vm-dirty`, containerd 2.4.1,
Cilium) with an **unmanaged** 1× XS NodeGroup as baseline capacity. Its node
reported 7553640Ki of memory (catalogue: 7553664Ki) and the measured
102624184Ki disk: the built-in catalogue still matches this node image.

## In-place upgrade from v0.14.0, under the chart's own RBAC

1. The published v0.14.0 charts (`karpenter-crd`, then `karpenter`) were
   installed in the cluster. A CleverNodeClass carrying one label, a NodePool
   without capacity-type requirement and three pods of 1 CPU / 3Gi gave one
   S node, Ready 45 s after the workload was applied.
2. The `karpenter-crd` chart of the branch was upgraded in (`Unhealthy`
   appears in the budget reasons), then the controller chart of the branch
   with `replicas=0`, so its RBAC applied while no in-cluster controller ran.
   The controller built from the branch ran out-of-cluster **impersonating
   `system:serviceaccount:karpenter:karpenter`**, bound to nothing but the
   chart's ClusterRole and Roles, leader election on.
3. Every core and provider controller started (39), no request was refused,
   `karpenter_cluster_state_synced` read 1, and the v0.14.0 node kept its
   NodeClaim: same `clevernodeclass-hash` and `nodepool-hash` stamps
   (version `v3` for both), never `Drifted`.
4. Scaled to six pods, the new controller launched a second S node: claim
   created, NodeGroup acknowledged 1.4 s later, node registered at 28 s and
   initialized at 36 s. Scaled back to two pods, an `Empty` command deleted
   the emptied node 54 s after the scale-down, NodeGroup included; an
   `Underutilized` replacement of the remaining S by a cheaper XS started
   next. The restart of step 5 abandoned it half-way (the orchestration
   queue lives in the controller's memory): its XS came up, and the S stayed.
5. Restarted with `FEATURE_GATES=NodeRepair=true` and a `SCHEDULER_CONFIG`
   (hostname spread, `ScheduleAnyway`), the controller started (the repair
   policies compile) but **froze**: the chart's ClusterRole did not grant
   `services`, the Services informer logged `forbidden` in a loop, and a
   goroutine dump showed both the provisioner and a consolidation simulation
   waiting in `WaitForCacheSync` under
   `DefaultTopologySpreadInjector.services`. A Pending pod stayed Pending.
   With `services` added to the ClusterRole (as the chart now ships it), the
   next start placed it on a new 2XS within 45 s of taking the lease, with no
   refused request.
6. Rolled back to the in-cluster v0.14.0 image over the 1.15 CRDs and RBAC,
   the old controller deleted all three nodes (the v0.14.0 S, the abandoned
   replacement's XS and the 2XS of step 5) within 98 s of the workload's
   deletion, logging no error.

## `make e2e`

| Scenario | Result | Notes |
|---|---|---|
| Provision | PASS (81 s) | both pods Running in 75 s: the main pool's 8-CPU limit lets one claim through per scheduling pass, the second follows 10 s later |
| ConsolidationScaleToZero | PASS (65 s) | pool consolidated to zero in 1 min 5 s |
| Drift | PASS (145 s) | replacement registered 1 min 20 s after the NodeClass patch |
| GarbageCollection | PASS (372 s) | ownerless decoy refused in 3 min 10 s; the owner-reference cascade reaped the force-deleted claim's group in 5 s |
| QuotaFastFail | PASS (126 s) | rejection surfaced in 10 s, 4/4 pods Pending, each told why (below), no leaked NodeGroup |

Whole suite: 915 s on the final tree; the same suite passed (830 s) on the
bare bump before the repair, RBAC and test changes. The quota scenario now
also asserts karpenter-core's new pod event. Observed messages:

```
NodeClaim e2e-386b78-quota-9qwkx nominated for this pod failed to launch: insufficient capacity, clever cloud quota exceeded for flavor XL
NodeClaim e2e-386b78-quota-gw4z6 nominated for this pod failed to launch: insufficient capacity, resolving instance type, no available clever cloud flavor satisfies the nodeclaim requirements and resource requests (1 flavor(s) that would satisfy them are currently unavailable after a quota rejection or an upstream refusal)
```

Every gate of the local validation chain passed under Go 1.26.9 (vet,
build, unit and race tests, envtest, chart lint and tests, generated files,
lint, govulncheck).

## A worker whose kubelet stops: the platform heals it first

With the final tree running as the chart's ServiceAccount,
`FEATURE_GATES=NodeRepair=true`, a NodePool of two 2XS nodes with one pod
each (required anti-affinity), the kubelet of one worker was stopped
(`kubectl debug node/… --profile=sysadmin -- chroot /host systemctl stop
kubelet`). Times from the node turning `Unknown`:

| t | What happened |
|---|---|
| 0 | `Ready=Unknown` (`NodeStatusUnknown`); core matches the repair policy |
| +11 s | **CKE starts a replacement VM in the same NodeGroup**: `<group>-node1` joins with the group's labels and the `karpenter.sh/unregistered` taint while `nodeCount` stays 1. The providerid controller refuses it a provider ID (its sibling holds it), so it never registers and no pod can use it |
| +36 s | CKE cordons the old node (`spec.unschedulable`, label `clever-cloud.com/draining: "true"`) and evicts its pod through the Eviction API, well before the 300 s taint-based eviction |
| +1 min 24 s | the evicted pod, recreated and Pending (the replacement VM is unregistered), runs on a new 2XS Karpenter launched for it |
| +5 min 36 s | CKE deletes the old Node object: core's termination deletes the NodeClaim, drains, and the provider deletes the NodeGroup |
| +7 min 9 s | the replacement VM is gone with its NodeGroup: nothing leaked |

Karpenter's own repair, due at +10 min, never had to act. On CKE, node repair
is the backstop for failures the platform does not heal; the platform's
replacement VM costs one idle node for a few minutes.
