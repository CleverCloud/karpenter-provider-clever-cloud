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
| v1/large-instances | 10Gi pod → M node (L/XL blocked by org quota, as documented) | 175 s |
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
| v1/large-instances | 10Gi pod → M node (L/XL quota-blocked) | 185 s |
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
