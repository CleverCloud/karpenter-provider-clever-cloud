# Observability

The controller serves Prometheus metrics on `:8080/metrics` (scraped via the
`karpenter` Service) — karpenter-core's series and the provider's own on the
same endpoint — and publishes Kubernetes Events on the objects users actually
look at (`kubectl describe nodeclaim`, `kubectl get events`).

## Provider metrics

All provider series are prefixed `karpenter_clevercloud_`.

| Metric | Type | Meaning | When it moves, do this |
|---|---|---|---|
| `nodegroup_acceptance_timeouts_total` | counter | The node-group operator neither acknowledged nor refused a new NodeGroup within the 15 s poll window (it wrote no status at all, or only a transient failure), and the launch proceeded optimistically. A healthy operator decides with its first status write, about 1 s after the group's creation: `phase: Creating` with `ReconcileInProgress=True` on a group it accepted, `QuotaExceeded` directly on one the quota rejects. The launch returns there, long before the VM is up (the group turns `Ready` 38–58 s after its creation), so **a healthy launch never ticks this counter**. | Every tick deserves a look: an isolated one can be an operator restart. **Sustained growth means the node-group operator is down or wedged**: every launch will burn the 15-minute registration TTL, and each one also holds the create lock for the whole 15 s window, so launches slow to 4 a minute. The node-group operator is Clever Cloud's: on the topologies whose control plane runs outside the cluster (only worker nodes in it) there is no control-plane VM to look at, so check what the cluster shows. In `kubectl get nodegroup <name> -o yaml` (a NodeGroup is named after its NodeClaim), no `status` at all means the operator never picked the group up, and a `phase` and conditions that stop moving mean it is stuck; `nodegroup_sync_overdue` and its `NodeGroupSyncOverdue` event tell a launch actually stuck from a slow one. Report the NodeGroup names (and their `status.upstreamId` when set) to Clever Cloud support. Scale-ups stall until the operator recovers. If the same NodeClaims also carry a `NodeGroupTransientFailure` event, the operator is up but retrying failed Clever Cloud API calls (`UpstreamError`): a platform incident, which it recovers from on its own. |
| `nodegroup_quota_rejections_total` | counter | The organisation quota rejected a NodeGroup creation (fresh upstream rejections; cached-backoff fast-fails are not counted). The rejected flavor is then [unavailable](#unavailable-flavors) to karpenter's scheduler for 1 minute and every other flavor at least as large for 2 minutes, while smaller flavors stay available. A rejection published after the acceptance poll counts too: the nodegroupstatus controller then deletes the launched NodeClaim so karpenter re-plans, and makes the same flavors unavailable. | Normal near the quota ceiling: karpenter plans the pending pods onto the flavors still available, retries the rejected flavor once its minute has passed and the larger ones a minute later. On an exhausted quota expect a burst of up to one tick per flavor size the pending pods walk down through (at most six), then about one a minute while the smallest of them is retried alone. If it keeps moving while workloads stay Pending, the quota cannot hold the sizes they need. The quota belongs to the **organisation**: the control plane and every other cluster of the organisation draw on it, and a rejection caused by another cluster's usage is indistinguishable from one caused by this cluster's. Check the organisation's usage across all its clusters, then raise the org quota or lower NodePool limits. |
| `nodegroup_rejections_total` | counter | The node-group operator refused a NodeGroup for a reason that is **not** the organisation quota — a flavor this cluster cannot provision, a spec it will not accept, or any reason the provider does not know. The refused flavor is [unavailable](#unavailable-flavors) to karpenter's scheduler for 5 minutes, so it relaxes to another flavor, or to the next NodePool by weight. A refusal published after the acceptance poll counts too: the nodegroupstatus controller then deletes the launched NodeClaim so karpenter re-plans, and holds the flavor out the same way. So does a create the API server refused **at admission** because the NodeGroup CRD's `spec.flavor` enum (`2XS`…`XL`) does not carry the flavor: no NodeGroup is created, the flavor is unavailable the same way, and the launch fails over to another flavor when the claim allows one (pods only that flavor fits stay pending). An admission error that also names another field (a label value the CRD refuses) is not counted: it fails whatever the flavor and stays a plain launch error. **Not counted**: a `ReconcileFailed` with reason `UpstreamError`, which the operator reports when one of its Clever Cloud API calls fails and then retries on its own (seen recovering after about an hour). That is a platform incident, not a verdict on the flavor: the group is left in place, nothing is held out, the launch proceeds once the operator acknowledges the group (or keeps waiting when it has not yet), and a `NodeGroupTransientFailure` event reports it instead. | Unlike a quota rejection this is not normal operation: the request was well-formed as far as the provider knows. Read the `NodeGroupRejected` event on the NodeClaim — it carries the operator's own reason and message, which is what to report to Clever Cloud. A refusal **at admission** (the event says so, with the API server's `Unsupported value` message; the controller logs it as an error) names a flavor the NodeGroup API does not know, which a `settings.flavors` override added — a new flavor declared before Clever Cloud's CRD carries it: fix or remove that override. A built-in flavor refused this way means Clever Cloud withdrew it from the enum: report it, and exclude it in the NodePool requirements meanwhile (see below). Every flavor being refused in turn points at the platform, not at the flavors: report the reasons to Clever Cloud rather than trimming the catalogue. If a single flavor is refused persistently, the 5-minute hold-out already makes launches pick another flavor between attempts when the claim allows one; to stop the attempts, exclude it in the NodePool requirements (`node.kubernetes.io/instance-type` with `NotIn`) — nodes already running that flavor are then replaced as `RequirementsDrifted` under the disruption budgets. `settings.flavors` cannot remove a flavor from the catalogue: its overrides only add or adjust entries. |
| `nodegroup_vanished_total` | counter | A NodeGroup disappeared after creation while its NodeClaim was still unregistered — usually the quota engine reclaiming an accepted group. Each tick is a launch fast-failed instead of burning the 15-minute registration TTL. Two paths count it. The acceptance poll fails the launch at once, and its flavor is then [unavailable](#unavailable-flavors) for 1 minute and every other flavor at least as large for 2 minutes, as after a quota rejection; but the poll only runs until the operator's decision, about 1 s after the group's creation, and a group it sees terminating counts as vanished too. A group that vanishes after the operator acknowledged it is caught by the GC sweep instead, which deletes the NodeClaim 2 to 4 minutes after its creation (the sweep runs every 2 minutes and skips claims younger than 2 minutes) and makes no flavor unavailable. | Occasional ticks near the quota ceiling are the documented upstream race. Sustained growth means the platform keeps reclaiming accepted groups — check quota headroom and the node-group operator. |
| `nodegroup_sync_overdue` | gauge | Launched NodeGroups the node-group operator has not synced (no `Ready=True`) 5 minutes after their creation while their NodeClaim is still unregistered. Healthy launches sync in under a minute (node Ready at 26–60 s, see [E2E-RESULTS](E2E-RESULTS.md)). | **Non-zero means launches are stuck upstream**: they will not register, and karpenter-core deletes each NodeClaim at its 15-minute registration timeout, then re-plans. The `NodeGroupSyncOverdue` event on the NodeClaim names the group and what the operator last wrote. In `kubectl get nodegroup <name> -o yaml`: no `status` means the operator never picked the group up; a `ReconcileInProgress` (`Creating`) that never ends means the operator is still building, or failing to build, the machine; a `ReconcileFailed` with reason `UpstreamError` means the operator keeps retrying failed Clever Cloud API calls (a `NodeGroupTransientFailure` event says so too). None of this can be fixed from the cluster: report the NodeGroup names (and their `status.upstreamId` when set) to Clever Cloud support. Nothing needs deleting by hand. |
| `nodegroup_external_resizes` | gauge | Managed NodeGroups whose `nodeCount` is not 1 — something outside karpenter resizes them (the platform's alert-driven scaler via an inherited `autoscalingEnabled`, a human, anything with `nodegroups/scale` RBAC). | **Non-zero breaks the 1 NodeClaim = 1 NodeGroup invariant**: the extra nodes are never registered — they get no provider ID and keep the `karpenter.sh/unregistered` taint — so karpenter neither manages nor prices them. Find the resizer; ensure the cluster's `autoscalingEnabled` feature is off (the two autoscalers must never run together), then set `nodeCount` back to 1. **Also check karpenter-core's `karpenter_cluster_state_synced`**: a node carrying `karpenter.sh/nodepool` without a `spec.providerID` keeps it at 0 after every controller restart — provisioning and disruption stop cluster-wide and `cluster is waiting on sync for extended duration` is logged every 10 s. NodeGroups created by the current version never put that label on their nodes; for older groups, whose immutable `spec.labels` still carry it, the providerid controller removes it from the extra node it refuses to stamp (logged once per node). The controller never touches nodes of unmanaged groups, and that includes an older group recreated upstream without its `karpenter.clever-cloud.com/managed` label but with its old `spec.labels`: its single node carries the label with no provider ID and freezes the sync the same way. If `karpenter_cluster_state_synced` stays at 0 anyway, list the culprits with `kubectl get nodes -l karpenter.sh/nodepool -o custom-columns=NAME:.metadata.name,PROVIDERID:.spec.providerID` and remove the label from any node with an empty provider ID, or revert the resize. |
| `gc_reaped_nodegroups_total` | counter | The GC safety net deleted an orphaned NodeGroup (its NodeClaim was force-deleted outside the normal flow). | Occasional ticks are the safety net working. Frequent ticks mean something force-deletes NodeClaims — find it. |
| `gc_refused_nodegroups` | gauge | NodeGroups the last GC sweep refused to reap: managed label present, but no verified dead NodeClaim owner. | **Non-zero needs attention** — each one is a VM billing hourly. A copied manifest: remove the `karpenter.clever-cloud.com/managed` label. A deliberately orphaned group: delete it manually. Details in the `GarbageCollectionRefused` event on the NodeGroup. |
| `instancetype_flavors_config_invalid` | gauge | 1 while the `settings.flavors` overrides file failed to load: the controller runs on the base catalogue WITHOUT the configured overrides instead of crashlooping. Any key the controller does not know invalidates the whole file, including `priceHourly`, which earlier releases accepted (prices are now derived from `cpu` and `memoryKi`), and so does a name that cannot be a Clever Cloud flavor (`2xs`, `CUSTOM`: the NodeGroup API could never create it), which earlier releases accepted too. | Fix `settings.flavors` (the chart also validates it at install time via values.schema.json, so this mostly fires when the ConfigMap was written outside the chart): remove `priceHourly` and any other unknown key, and fix or remove any name the error log refuses (it suggests the uppercase spelling when that one is a flavor name); the next pod roll picks it up. **Note**: a flavor that only the overrides kept in the catalogue leaves it while the gauge is 1 — its nodes are then rolled by drift (see [Flavor removal semantics](#flavor-removal-semantics)). |
| `instancetype_observed_capacity_rejections_total` | counter | A node's capacity report was refused by the instance-type catalogue. The catalogue follows what nodes report (the memory the kernel exposes moves with the node image), but a kubelet can rewrite its own node's labels and status, and an accepted report sets the flavor's capacity for the whole cluster. A report is refused when the node's `clever-cloud.com/nodegroup` label names a managed NodeGroup the node is not a node of (the platform names a group's nodes `<nodegroup>-node<N>`, and a kubelet cannot rename its Node, so the name decides), when its `clever-cloud.com/flavor` label contradicts that NodeGroup's `spec.flavor`, when its `cpu` differs from the catalogue entry, when its memory, ephemeral storage or pod count (capacity or allocatable) is more than 10% off the entry, when its allocatable exceeds its capacity, or when a resource appears in only one of capacity and allocatable. Counted on every reconcile of the node (each status update); logged at most once an hour per node. Nodes whose nodegroup label names no managed NodeGroup are not reports and are never counted. What the bounds leave by design: the kubelet of a node of a managed group can move its own group's flavor by up to 10%. | **Non-zero needs a look.** The log line names the node and the reason. A single node: inspect it — a kubelet claiming another nodegroup, reporting another flavor's figures or rewriting its flavor label is misconfigured or compromised (drain and replace it). Every node of a flavor: either a `settings.flavors` `cpu`/`memoryKi` pin more than 10% off reality (fix or remove it — until then the pin is served as-is), or a platform node image that moved more than 10% (report it; pin `memoryKi` to the measured value meanwhile, on every flavor the same way — a lone pin skews the relative prices, see the README's Flavor catalogue). Every node, each refused as "not one of its nodes": the platform changed how it names nodes (report it). The catalogue keeps serving its previous figures in all cases. Related, without a metric: an **accepted** report more than 1% off the catalogue entry is logged once per flavor (`node memory differs from the flavor's catalogue entry`) — the node image moved; the catalogue already follows the smallest node of that flavor, but a restarted controller packs pods against the entry until it sees one, so pin `memoryKi` (every flavor the same way) if the entry is above the nodes. |
| `instancetype_unknown_flavor_lookups_total` | counter | An instance-type lookup referenced a flavor absent from the served catalogue. | A running NodeGroup uses a flavor the catalogue lost (a removed or invalid `settings.flavors` override, a release whose built-in catalogue dropped it). GC and termination keep working on a synthesized type, and the affected nodes are **rolled by drift under disruption budgets** (see [Flavor removal semantics](#flavor-removal-semantics)); restore the flavor via `settings.flavors` to stop the roll. |

Suggested alert expressions:

```promql
# Node-group operator suspected down (worth paging). Healthy launches never
# move it: the operator acknowledges a group about 1 s after its creation. A
# wedged operator produces ~1 timeout per registration-TTL cycle (~15 min) per
# concurrent claim, so alert on any movement over an hour, not on a burst.
increase(karpenter_clevercloud_nodegroup_acceptance_timeouts_total[1h]) > 0

# A NodeGroup the GC refuses to reap keeps billing — use `for: 30m` in the
# alert rule to skip refusals the operator fixes quickly.
karpenter_clevercloud_gc_refused_nodegroups > 0

# Something outside karpenter resizes its nodegroups
karpenter_clevercloud_nodegroup_external_resizes > 0

# Launches stuck upstream: the node-group operator has not synced a launched
# group 5 minutes after its creation (healthy launches sync in under a minute)
karpenter_clevercloud_nodegroup_sync_overdue > 0

# karpenter-core's cluster state has not synced for 15 minutes: provisioning and
# disruption are paused. Launches in flight hold it unsynced legitimately: each
# NodeClaim until its NodeGroup is created and acknowledged by the node-group
# operator, and creations are serialized at about 1-2 s each, so a burst of N
# NodeClaims keeps this rising for about N x 2 s with no fault. A down operator
# stretches each one to the 15 s poll window (4 a minute, so 60 claims reach
# 900 s), which the acceptance-timeout alert above already reports. Beyond
# that, see nodegroup_external_resizes above.
karpenter_cluster_state_unsynced_time_seconds > 900

# The flavors overrides file is broken; the configured overrides are inactive
karpenter_clevercloud_instancetype_flavors_config_invalid > 0

# Running nodes reference a flavor the catalogue lost
increase(karpenter_clevercloud_instancetype_unknown_flavor_lookups_total[30m]) > 0

# A node reported capacity the catalogue refused (forged label or status, a
# settings.flavors pin far from reality, or a node image that moved > 10%)
increase(karpenter_clevercloud_instancetype_observed_capacity_rejections_total[1h]) > 0
```

All counters and gauges are pre-seeded at startup so the series exist from the
first scrape.

The controller no longer exports `pricing_refresh_failures_total` nor
`pricing_last_successful_refresh_timestamp_seconds`: the dynamic pricing
refresher that fed them was removed, and the catalogue is now the built-in one
plus `settings.flavors`. Delete any alert or dashboard built on them.

## Flavor removal semantics

When a flavor leaves the served catalogue (a removed or invalid
`settings.flavors` override, a release whose built-in catalogue dropped it)
while nodes of that flavor still run, the degradation is deliberate and
bounded:

- `Get`/`List` keep describing the affected NodeGroups with a **synthesized
  instance type** (seed sizing when the name is known, observed capacity when
  a live node reported it) — garbage collection and node termination keep
  working, and the claims never read as orphaned.
- The flavor disappears from the provisioning catalog, so **nothing new is
  created or priced with it**.
- karpenter-core's drift controller marks the affected NodeClaims
  `InstanceTypeNotFound` (for nodes older than 1 h, within ~35 min of the
  catalogue change) and **replaces them under the NodePool's disruption
  budgets** and PDBs — budgets are the pacing lever for the roll. Pods that
  fit no remaining flavor leave the node parked with a `Blocked` disruption
  event until capacity or budgets allow.
- `instancetype_unknown_flavor_lookups_total` moves and a per-flavor log line
  names it. Remediation: restore the flavor via `settings.flavors` (a flavor
  outside the built-in catalogue must set `cpu` and `memoryKi`; its price is
  derived from them).

## Unavailable flavors

karpenter-core remembers nothing of a failed launch: it deletes the
NodeClaim and plans again over the instance types the provider serves. So the
provider serves the flavors a launch is known to fail for with **unavailable
offerings**, for a limited time, and karpenter's scheduler plans around them:

- A **quota rejection** of a flavor (`nodegroup_quota_rejections_total`,
  `NodeGroupQuotaExceeded`), or a NodeGroup of it reclaimed during its
  acceptance poll (`nodegroup_vanished_total`), makes that flavor unavailable
  for 1 minute, and every other flavor at least as large (as many vCPUs
  **and** as much memory, per the catalogue) for 2 minutes. Smaller flavors
  stay available: the remaining quota may hold them. Launches of the
  unavailable flavors fail fast without reaching Clever Cloud. Once its minute
  has passed, the rejected flavor — the cheapest probe of whether capacity
  came back — is tried again on its own, and the larger flavors only a minute
  later, if it was not rejected again. Deleting a node that held capacity ends
  every quota rejection at once (a refusal's hold-out stays). The quota
  belongs to the organisation, so the usage that exhausted it may be another
  cluster's: a rejection says nothing about whose.
- A **refusal** of a flavor for any other reason (`nodegroup_rejections_total`,
  `NodeGroupRejected`), by the node-group operator or by the API server at
  admission (a `settings.flavors` flavor the NodeGroup CRD's enum does not
  carry), makes that flavor alone unavailable for 5 minutes. A NodePool left
  with no available flavor is skipped, so the next NodePool by weight is
  tried.

Pods that only an unavailable flavor can hold stay `Pending` until the window
ends, with a karpenter-core scheduling error on them; karpenter then tries
again. On a quota that stays exhausted, pending pods that several sizes can
hold first walk down the catalogue: karpenter packs them onto the fewest
nodes the available flavors allow, and each rejection sends the next pass to
a smaller flavor — one rejected NodeGroup per flavor size tried, at most six,
within seconds. Once the smallest of them is rejected too, only it is
retried, about once a minute, while the larger ones stay unavailable. That is
what `nodegroup_quota_rejections_total` shows: a burst, then about one a
minute, not a create and delete on every scheduling pass. When capacity
comes back (a retry is accepted, or a node is deleted), the larger flavors
reopen and a new walk down may follow. An unavailable flavor stays in the
catalogue: unlike a [removed flavor](#flavor-removal-semantics), it never
drifts the nodes running it.

## Price units

Offering prices are not EUR: every flavor is priced by its cpu and memory
relative to the built-in `2XS`, which costs 1.0 (formula and table in the
[README](../README.md#flavor-catalogue)). karpenter-core only compares and
sums them, so its decisions depend on how prices rank, not on their unit;
but every price-based figure it reports is in this unit, whatever its label
says:

- `karpenter_nodepools_cost_total` (help text: "Units are determined by the
  cloud provider") is the NodePool's total in 2XS-equivalents: 3.0 is the
  price of three 2XS nodes, not 3 EUR an hour.
- `savings: $…` in disruption log lines and in the `ConsolidationCandidate` /
  `ConsolidationRejected` events is the same unit: `$1.85` is the price of an
  XS, not a sum of money. The `$` is hard-coded by karpenter-core.

Earlier releases served EUR/hour prices, so a dashboard or alert threshold
built on these figures must be rescaled, not reused.

## CloudProvider call metrics

The provider is wrapped in karpenter-core's metrics decorator, so the standard
`karpenter_cloudprovider_duration_seconds` and
`karpenter_cloudprovider_errors_total` series (labeled by `method` and typed
`error`, e.g. `InsufficientCapacityError`) cover Create/Delete/Get/List
latency and failure rates. `Create` duration includes the acceptance poll by
design: about 1–2 s, since the poll ends on the node-group operator's first
status write (its decision), not when the VM is up. It stretches to the full
15 s window only when the operator does not answer, which
`nodegroup_acceptance_timeouts_total` counts. Creations are serialized, so it
also includes the wait behind the launches queued ahead: a burst of N
launches adds up to N × 1–2 s.

Also useful from karpenter-core: `karpenter_nodeclaims_disrupted_total{reason="registration_timeout"}`
(NodeClaims that never registered — fires 15 minutes after each optimistic
launch that went nowhere; `nodegroup_sync_overdue` reports a launch stuck
upstream about 10 minutes earlier, while it is still in flight) and the
NodePool `NodeRegistrationHealthy` status condition.

## Kubernetes Events

| Event reason | On | Type | Meaning |
|---|---|---|---|
| `NodeGroupQuotaExceeded` | NodeClaim | Warning | The org quota freshly rejected this claim's NodeGroup; names the flavor, carries the quota detail when the operator gives one (live, it gives none), and says which flavors are unavailable and for how long. Backoff fast-fails emit no provider event — karpenter-core already publishes an `InsufficientCapacityError` event per attempt. When the rejection came after the acceptance poll, the message says so: the nodegroupstatus controller deleted the launched NodeClaim so karpenter re-plans. |
| `NodeGroupRejected` | NodeClaim | Warning | The node-group operator refused the NodeGroup for a non-quota reason; carries the operator's reason and message, and names the flavor held out of provisioning. When the refusal came after the acceptance poll, the message says so: the nodegroupstatus controller deleted the launched NodeClaim so karpenter re-plans. When the API server refused the create at admission because the NodeGroup CRD's enum does not carry the flavor, the message says so too, carries the API server's message, and points at the `settings.flavors` override that added the flavor; no NodeGroup was created. |
| `NodeGroupTransientFailure` | NodeClaim | Normal / Warning | The node-group operator reported a failure it retries on its own (`ReconcileFailed` with reason `UpstreamError`: a Clever Cloud API call failed) at some point during the acceptance poll; carries its reason and message, and what then ended the poll. It says the poll saw the failure, not that it is still current or was resolved: the operator can keep it set next to `Ready`. Not a refusal: nothing is deleted or held out for it. **Normal** when the group was `Ready` by the end of the poll: its machine is up. That includes a group the poll found `Ready` with the failure already set next to it (a claim adopting its group after a restart, for instance). **Warning** otherwise: the operator acknowledged the group (`ReconcileInProgress`) while retrying, so the launch proceeded with the machine still being built and the nodegroupstatus controller follows it; or the poll window closed before any acknowledgement, so the launch proceeded optimistically with the registration TTL as the backstop (a `NodeGroupAcceptanceTimeout` event accompanies it); or the launch then failed — the group vanished, or was refused afterwards, whose own event accompanies it and can be tied back to the incident through this one. Controller shutdowns mid-poll are excluded: the claim's next attempt polls the group again. Also published, as a Warning, by the nodegroupstatus controller when a launched group reports such a failure after the poll; it shares the poll's dedupe key, so it does not repeat within minutes what the poll has just said. |
| `NodeGroupAcceptanceTimeout` | NodeClaim | Warning | The operator neither acknowledged nor refused the NodeGroup within the 15 s poll window (no status at all, or only a transient failure); the launch proceeded optimistically. A healthy launch never publishes it: the operator acknowledges a group about 1 s after its creation. Controller shutdowns mid-poll are excluded. |
| `NodeGroupVanished` | NodeClaim | Warning | The NodeGroup disappeared after launch before the node registered, and the launch was failed instead of waiting out the registration TTL. From the acceptance poll, the launch failed at once: the poll runs until the operator's decision, about 1 s after the group's creation, and also fails the launch on a group it sees terminating. When the acceptance poll saw it, its flavor is [unavailable](#unavailable-flavors) for a minute and every other flavor at least as large for two, as after a quota rejection. From the GC sweep, the claim was deleted: a group that vanishes after the operator acknowledged it is caught there, 2 to 4 minutes after the NodeClaim's creation, and no flavor is made unavailable. |
| `NodeGroupSyncOverdue` | NodeClaim | Warning | The launched NodeGroup is still not synced 5 minutes after its creation and the claim has not registered; carries what the operator last wrote (phase and true conditions), or says the operator never picked the group up. Nothing is deleted: karpenter-core's registration timeout retires the claim at 15 minutes. Counted by `nodegroup_sync_overdue`. |
| `NodeGroupExternallyResized` | NodeGroup | Warning | The group's nodeCount is not 1; republished hourly while the condition persists. |
| `GarbageCollected` | NodeGroup | Normal | The GC safety net deleted this orphaned group. |
| `GarbageCollectionRefused` | NodeGroup | Warning | The GC found this group orphan-like but refused to reap it (no verified dead owner); republished hourly while the condition persists so it survives etcd's event TTL. |
| `LabelsIgnored` | CleverNodeClass | Warning | `spec.labels` carries keys that v0.12.0 admitted and the current rule rejects: a subdomained `kubernetes.io/` key such as `app.kubernetes.io/part-of`, or a `karpenter.sh` key. No node launched now gets them (a subdomained `kubernetes.io/` key never reached any node; a `karpenter.sh` key reached the nodes v0.12.0 built, which keep it). The NodeClass stays Ready and keeps provisioning, and its `LabelsIgnored` status condition, which does not gate readiness, names them for as long as they remain. The event is published whenever the controller reconciles the NodeClass (at start, on every change, and every minute while the condition's reason is `HashMigrationPending`), at most once an hour per key set and reason. Its message and the condition's say whether removing the keys drifts nodes. The controller re-stamps the NodeGroups v0.12.0 built (which hashed the keys) with hash version v3 (which leaves them out) before it reports the keys; reason `LegacyLabelKeys` means no NodeGroup of the NodeClass still carries an older stamp. `HashMigrationPending` counts the NodeGroups that still do: their NodeClaim is already drifted (it keeps its stamp until it is replaced), the NodeClass changed since they were built (they drift anyway), or a read or write failed (retried; the error is in the controller's logs). It gives no count when the NodeGroups could not be listed. **Action**: remove the keys once the reason is `LegacyLabelKeys`: no node drifts for it. Removing them while it is `HashMigrationPending`, or before the upgraded controller has reconciled the NodeClass (before the upgrade, in the same change as the upgrade, or while v0.12.0 still runs), drifts the NodeGroups still carrying an older stamp, and Karpenter replaces their nodes. Until the keys are removed, the CRD refuses any edit of `spec.labels` that keeps them. |

Refusal logs are deduplicated to one line per NodeGroup per controller
lifetime — the gauge, not the log volume, is the persistent signal. The
nodegroupstatus controller likewise logs a transient failure or an overdue
sync once per NodeGroup while it follows the launch.

Deliberate descope: unknown-flavor lookups are metrics/log-only — the lookup
site holds no natural involved object for an Event (instance-type lookups have
no client).
