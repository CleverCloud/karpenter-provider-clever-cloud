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
The existing core termination behavior for an indefinitely held finalizer
is covered separately by fake-clock tests: a Ready Node remains, a NotReady
Node can be released, the NodeClaim keeps waiting, and deletion resumes
when the platform completes its finalizer.

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
