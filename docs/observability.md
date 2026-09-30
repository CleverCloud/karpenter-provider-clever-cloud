# Observability

The controller serves Prometheus metrics on `:8080/metrics` (scraped via the
`karpenter` Service) — karpenter-core's series and the provider's own on the
same endpoint — and publishes Kubernetes Events on the objects users actually
look at (`kubectl describe nodeclaim`, `kubectl get events`).

## Provider metrics

All provider series are prefixed `karpenter_clevercloud_`.

| Metric | Type | Meaning | When it moves, do this |
|---|---|---|---|
| `nodegroup_acceptance_timeouts_total` | counter | A NodeGroup creation was not accepted by the node-group operator within the poll window and proceeded optimistically. | One-off ticks are benign (slow reconcile). **Sustained growth means the node-group operator is down or wedged**: every launch will burn the 15-minute registration TTL. Check the operator on the control-plane VM (platform-side); scale-ups stall until it recovers. If the same NodeClaims also carry a `NodeGroupTransientFailure` event, the operator is up but retrying failed Clever Cloud API calls (`UpstreamError`): a platform incident, which it recovers from on its own. |
| `nodegroup_quota_rejections_total` | counter | The organisation quota rejected a NodeGroup creation (fresh upstream rejections; cached-backoff fast-fails are not counted). | Normal near the quota ceiling — karpenter relaxes to other options. If persistent while workloads stay Pending, raise the org quota or lower NodePool limits. |
| `nodegroup_rejections_total` | counter | The node-group operator refused a NodeGroup for a reason that is **not** the organisation quota — a flavor this cluster cannot provision, a spec it will not accept, or any reason the provider does not know. The refused flavor is held out of provisioning for 5 minutes so the scheduler relaxes to another one. **Not counted**: a `ReconcileFailed` with reason `UpstreamError`, which the operator reports when one of its Clever Cloud API calls fails and then retries on its own (seen recovering after about an hour). That is a platform incident, not a verdict on the flavor: the group is left in place, nothing is held out, the launch keeps waiting as for any group in progress, and a `NodeGroupTransientFailure` event reports it instead. | Unlike a quota rejection this is not normal operation: the request was well-formed as far as the provider knows. Read the `NodeGroupRejected` event on the NodeClaim — it carries the operator's own reason and message, which is what to report to Clever Cloud. Every flavor being refused in turn points at the platform, not at the flavors: report the reasons to Clever Cloud rather than trimming the catalogue. If a single flavor is refused persistently, the 5-minute hold-out already makes launches pick another flavor between attempts when the claim allows one; to stop the attempts, exclude it in the NodePool requirements (`node.kubernetes.io/instance-type` with `NotIn`) — nodes already running that flavor are then replaced as `RequirementsDrifted` under the disruption budgets. `settings.flavors` cannot remove a flavor from the catalogue: its overrides only add or adjust entries. |
| `nodegroup_vanished_total` | counter | A NodeGroup disappeared after creation while its NodeClaim was still unregistered — usually the quota engine reclaiming an accepted group. Each tick is a launch fast-failed instead of burning the 15-minute registration TTL. | Occasional ticks near the quota ceiling are the documented upstream race. Sustained growth means the platform keeps reclaiming accepted groups — check quota headroom and the node-group operator. |
| `nodegroup_external_resizes` | gauge | Managed NodeGroups whose `nodeCount` is not 1 — something outside karpenter resizes them (the platform's alert-driven scaler via an inherited `autoscalingEnabled`, a human, anything with `nodegroups/scale` RBAC). | **Non-zero breaks the 1 NodeClaim = 1 NodeGroup invariant**: the extra nodes are never registered — they get no provider ID and keep the `karpenter.sh/unregistered` taint — so karpenter neither manages nor prices them. Find the resizer; ensure the cluster's `autoscalingEnabled` feature is off (the two autoscalers must never run together), then set `nodeCount` back to 1. **Also check karpenter-core's `karpenter_cluster_state_synced`**: a node carrying `karpenter.sh/nodepool` without a `spec.providerID` keeps it at 0 after every controller restart — provisioning and disruption stop cluster-wide and `cluster is waiting on sync for extended duration` is logged every 10 s. NodeGroups created by the current version never put that label on their nodes; for older groups, whose immutable `spec.labels` still carry it, the providerid controller removes it from the extra node it refuses to stamp (logged once per node). The controller never touches nodes of unmanaged groups, and that includes an older group recreated upstream without its `karpenter.clever-cloud.com/managed` label but with its old `spec.labels`: its single node carries the label with no provider ID and freezes the sync the same way. If `karpenter_cluster_state_synced` stays at 0 anyway, list the culprits with `kubectl get nodes -l karpenter.sh/nodepool -o custom-columns=NAME:.metadata.name,PROVIDERID:.spec.providerID` and remove the label from any node with an empty provider ID, or revert the resize. |
| `gc_reaped_nodegroups_total` | counter | The GC safety net deleted an orphaned NodeGroup (its NodeClaim was force-deleted outside the normal flow). | Occasional ticks are the safety net working. Frequent ticks mean something force-deletes NodeClaims — find it. |
| `gc_refused_nodegroups` | gauge | NodeGroups the last GC sweep refused to reap: managed label present, but no verified dead NodeClaim owner. | **Non-zero needs attention** — each one is a VM billing hourly. A copied manifest: remove the `karpenter.clever-cloud.com/managed` label. A deliberately orphaned group: delete it manually. Details in the `GarbageCollectionRefused` event on the NodeGroup. |
| `instancetype_flavors_config_invalid` | gauge | 1 while the `settings.flavors` overrides file failed to load: the controller runs on the base catalogue WITHOUT the configured overrides instead of crashlooping. | Fix `settings.flavors` (the chart also validates it at install time via values.schema.json); the next pod roll picks it up. **Note**: a flavor that only the overrides kept in the catalogue leaves it while the gauge is 1 — its nodes are then rolled by drift (see [Flavor removal semantics](#flavor-removal-semantics)). |
| `instancetype_unknown_flavor_lookups_total` | counter | An instance-type lookup referenced a flavor absent from the served catalogue. | A running NodeGroup uses a flavor the catalogue lost (a removed or invalid `settings.flavors` override, a release whose built-in catalogue dropped it). GC and termination keep working on a synthesized type, and the affected nodes are **rolled by drift under disruption budgets** (see [Flavor removal semantics](#flavor-removal-semantics)); restore the flavor via `settings.flavors` to stop the roll. |

Suggested alert expressions:

```promql
# Node-group operator suspected down (worth paging). A wedged operator
# produces ~1 timeout per registration-TTL cycle (~15 min) per concurrent
# claim, so alert on any movement over an hour, not on a burst.
increase(karpenter_clevercloud_nodegroup_acceptance_timeouts_total[1h]) > 0

# A NodeGroup the GC refuses to reap keeps billing — use `for: 30m` in the
# alert rule to skip refusals the operator fixes quickly.
karpenter_clevercloud_gc_refused_nodegroups > 0

# Something outside karpenter resizes its nodegroups
karpenter_clevercloud_nodegroup_external_resizes > 0

# karpenter-core's cluster state has not synced for 15 minutes: provisioning and
# disruption are paused. Launches in flight hold it unsynced legitimately: each
# NodeClaim until its NodeGroup is created, and creations are serialized at
# about 15 s each (4 a minute), so a burst of N NodeClaims keeps this rising for
# about N/4 minutes with no fault (60 reach 900 s). Set the threshold above your
# largest expected burst (NodePool limits and the org quota cap it); beyond it,
# see nodegroup_external_resizes above.
karpenter_cluster_state_unsynced_time_seconds > 900

# The flavors overrides file is broken; the configured overrides are inactive
karpenter_clevercloud_instancetype_flavors_config_invalid > 0

# Running nodes reference a flavor the catalogue lost
increase(karpenter_clevercloud_instancetype_unknown_flavor_lookups_total[30m]) > 0
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
  outside the built-in catalogue must set `cpu`, `memoryKi` and
  `priceHourly`).

## CloudProvider call metrics

The provider is wrapped in karpenter-core's metrics decorator, so the standard
`karpenter_cloudprovider_duration_seconds` and
`karpenter_cloudprovider_errors_total` series (labeled by `method` and typed
`error`, e.g. `InsufficientCapacityError`) cover Create/Delete/Get/List
latency and failure rates — `Create` duration includes the up-to-15s
acceptance poll by design.

Also useful from karpenter-core: `karpenter_nodeclaims_disrupted_total{reason="registration_timeout"}`
(NodeClaims that never registered — fires 15 minutes after each optimistic
launch that went nowhere) and the NodePool `NodeRegistrationHealthy` status
condition.

## Kubernetes Events

| Event reason | On | Type | Meaning |
|---|---|---|---|
| `NodeGroupQuotaExceeded` | NodeClaim | Warning | The org quota freshly rejected this claim's NodeGroup (message carries the quota detail). Backoff fast-fails emit no provider event — karpenter-core already publishes an `InsufficientCapacityError` event per attempt. |
| `NodeGroupRejected` | NodeClaim | Warning | The node-group operator refused the NodeGroup for a non-quota reason; carries the operator's reason and message, and names the flavor held out of provisioning. |
| `NodeGroupTransientFailure` | NodeClaim | Normal / Warning | The node-group operator reported a failure it retries on its own (`ReconcileFailed` with reason `UpstreamError`: a Clever Cloud API call failed) at some point during the acceptance poll; carries its reason and message, and what then ended the poll. It says the poll saw the failure, not that it is still current or was resolved: the operator can keep it set next to `Ready`. Not a refusal: nothing is deleted or held out for it. **Normal** when the group was then accepted within the poll, including a group the poll found `Ready` with the failure already set next to it (a claim adopting its group after a restart, for instance). **Warning** otherwise: the poll window closed first, so the launch proceeded optimistically with the registration TTL as the backstop (a `NodeGroupAcceptanceTimeout` event accompanies it), or the launch then failed — the group vanished, or was refused afterwards, whose own event accompanies it and can be tied back to the incident through this one. Controller shutdowns mid-poll are excluded: the claim's next attempt polls the group again. |
| `NodeGroupAcceptanceTimeout` | NodeClaim | Warning | The operator did not accept the NodeGroup in time; the launch proceeded optimistically. Controller shutdowns mid-poll are excluded. |
| `NodeGroupVanished` | NodeClaim | Warning | The NodeGroup disappeared after launch before the node registered; the launch was failed (acceptance poll) or the claim deleted (GC sweep) instead of waiting out the registration TTL. |
| `NodeGroupExternallyResized` | NodeGroup | Warning | The group's nodeCount is not 1; republished hourly while the condition persists. |
| `GarbageCollected` | NodeGroup | Normal | The GC safety net deleted this orphaned group. |
| `GarbageCollectionRefused` | NodeGroup | Warning | The GC found this group orphan-like but refused to reap it (no verified dead owner); republished hourly while the condition persists so it survives etcd's event TTL. |

Refusal logs are deduplicated to one line per NodeGroup per controller
lifetime — the gauge, not the log volume, is the persistent signal.

Deliberate descope: unknown-flavor lookups are metrics/log-only — the lookup
site holds no natural involved object for an Event (instance-type lookups have
no client).
