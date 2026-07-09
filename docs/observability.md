# Observability

The controller serves Prometheus metrics on `:8080/metrics` (scraped via the
`karpenter` Service) — karpenter-core's series and the provider's own on the
same endpoint — and publishes Kubernetes Events on the objects users actually
look at (`kubectl describe nodeclaim`, `kubectl get events`).

## Provider metrics

All provider series are prefixed `karpenter_clevercloud_`.

| Metric | Type | Meaning | When it moves, do this |
|---|---|---|---|
| `nodegroup_acceptance_timeouts_total` | counter | A NodeGroup creation was not accepted by the node-group operator within the poll window and proceeded optimistically. | One-off ticks are benign (slow reconcile). **Sustained growth means the node-group operator is down or wedged**: every launch will burn the 15-minute registration TTL. Check the operator on the control-plane VM (platform-side); scale-ups stall until it recovers. |
| `nodegroup_quota_rejections_total` | counter | The organisation quota rejected a NodeGroup creation (fresh upstream rejections; cached-backoff fast-fails are not counted). | Normal near the quota ceiling — karpenter relaxes to other options. If persistent while workloads stay Pending, raise the org quota or lower NodePool limits. |
| `nodegroup_vanished_total` | counter | A NodeGroup disappeared after creation while its NodeClaim was still unregistered — usually the quota engine reclaiming an accepted group. Each tick is a launch fast-failed instead of burning the 15-minute registration TTL. | Occasional ticks near the quota ceiling are the documented upstream race. Sustained growth means the platform keeps reclaiming accepted groups — check quota headroom and the node-group operator. |
| `nodegroup_external_resizes` | gauge | Managed NodeGroups whose `nodeCount` is not 1 — something outside karpenter resizes them (the platform's alert-driven scaler via an inherited `autoscalingEnabled`, a human, anything with `nodegroups/scale` RBAC). | **Non-zero breaks the 1 NodeClaim = 1 NodeGroup invariant**: the extra nodes are neither tracked nor priced by karpenter. Find the resizer; ensure the cluster's `autoscalingEnabled` feature is off (the two autoscalers must never run together). |
| `gc_reaped_nodegroups_total` | counter | The GC safety net deleted an orphaned NodeGroup (its NodeClaim was force-deleted outside the normal flow). | Occasional ticks are the safety net working. Frequent ticks mean something force-deletes NodeClaims — find it. |
| `gc_refused_nodegroups` | gauge | NodeGroups the last GC sweep refused to reap: managed label present, but no verified dead NodeClaim owner. | **Non-zero needs attention** — each one is a VM billing hourly. A copied manifest: remove the `karpenter.clever-cloud.com/managed` label. A deliberately orphaned group: delete it manually. Details in the `GarbageCollectionRefused` event on the NodeGroup. |
| `instancetype_flavors_config_invalid` | gauge | 1 while the `settings.flavors` overrides file failed to load: the controller runs on the base catalogue WITHOUT the configured overrides instead of crashlooping. | Fix `settings.flavors` (the chart also validates it at install time via values.schema.json); the next pod roll picks it up. **Note**: a flavor that only the overrides kept in the catalogue leaves it while the gauge is 1 — its nodes are then rolled by drift (see [Flavor removal semantics](#flavor-removal-semantics)). |
| `pricing_refresh_failures_total` | counter | A catalogue refresh from the public price API failed; the last-known-good catalogue stays in use. | Transient failures are harmless. Persistent failures mean prices/flavors drift from reality: check egress to `api.clever-cloud.com` and the error log, or pin the catalogue via `settings.flavors`. |
| `pricing_last_successful_refresh_timestamp_seconds` | gauge | Unix time of the last successful catalogue refresh. **Absent until the first success, and exports no series when the refresher is disabled** (`settings.pricing.enabled=false`). | Alert on staleness only when > 0 (see below). |
| `instancetype_unknown_flavor_lookups_total` | counter | An instance-type lookup referenced a flavor absent from the served catalogue. | A running NodeGroup uses a flavor the catalogue lost (upstream change, topology misconfig, removed override). GC and termination keep working on a synthesized type, and the affected nodes are **rolled by drift under disruption budgets** (see [Flavor removal semantics](#flavor-removal-semantics)); restore the flavor via `settings.flavors` or fix `CLEVER_CLOUD_TOPOLOGY` to stop the roll. |

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

# The flavors overrides file is broken; the configured overrides are inactive
karpenter_clevercloud_instancetype_flavors_config_invalid > 0

# Catalogue stale for more than two refresh periods (only when refresher active)
karpenter_clevercloud_pricing_last_successful_refresh_timestamp_seconds > 0
  and time() - karpenter_clevercloud_pricing_last_successful_refresh_timestamp_seconds > 86400

# Running nodes reference a flavor the catalogue lost
increase(karpenter_clevercloud_instancetype_unknown_flavor_lookups_total[30m]) > 0
```

All counters and the refused gauge are pre-seeded at startup so the series
exist from the first scrape; only the pricing timestamp gauge is deliberately
absent until its first success.

## Flavor removal semantics

When a flavor leaves the served catalogue (upstream removal, a topology
change, a removed override) while nodes of that flavor still run, the
degradation is deliberate and bounded:

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
  names it. Remediation: restore the flavor via `settings.flavors` (an
  override resurrecting a flavor absent from the live catalogue is accepted
  but logged loudly — the platform may reject new NodeGroups using it) or fix
  `CLEVER_CLOUD_TOPOLOGY`.

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
| `NodeGroupAcceptanceTimeout` | NodeClaim | Warning | The operator did not accept the NodeGroup in time; the launch proceeded optimistically. Controller shutdowns mid-poll are excluded. |
| `NodeGroupVanished` | NodeClaim | Warning | The NodeGroup disappeared after launch before the node registered; the launch was failed (acceptance poll) or the claim deleted (GC sweep) instead of waiting out the registration TTL. |
| `NodeGroupExternallyResized` | NodeGroup | Warning | The group's nodeCount is not 1; republished hourly while the condition persists. |
| `GarbageCollected` | NodeGroup | Normal | The GC safety net deleted this orphaned group. |
| `GarbageCollectionRefused` | NodeGroup | Warning | The GC found this group orphan-like but refused to reap it (no verified dead owner); republished hourly while the condition persists so it survives etcd's event TTL. |

Refusal logs are deduplicated to one line per NodeGroup per controller
lifetime — the gauge, not the log volume, is the persistent signal.

Deliberate descope: pricing-refresh failures and unknown-flavor lookups are
metrics/log-only — neither site holds a natural involved object for an Event
(the refresher has no NodeClass in hand; instance-type lookups have no client).
