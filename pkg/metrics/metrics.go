/*
Copyright 2026 The karpenter-provider-clever-cloud Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package metrics registers the provider-specific Prometheus metrics on the
// controller-runtime registry, next to karpenter-core's own metrics on the
// same endpoint. Metrics are package-level singletons: operatorpkg's
// constructors MustRegister at call time, so each must be created exactly
// once per process. docs/observability.md documents what each metric means
// and what to do when it moves.
package metrics

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const namespace = "karpenter_clevercloud"

var (
	// NodeGroupAcceptanceTimeouts counts Creates that hit the acceptance-poll
	// timeout and proceeded optimistically: the node-group operator neither
	// acknowledged nor refused the group within the window. A healthy
	// operator acknowledges a group about 1 s after its creation, so healthy
	// launches never count here; growth is the signature of a down or wedged
	// operator, which otherwise looks like normal (slow) provisioning until
	// the registration TTL fires.
	NodeGroupAcceptanceTimeouts = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "nodegroup",
			Name:      "acceptance_timeouts_total",
			Help:      "NodeGroup creations the node-group operator neither acknowledged nor refused within the poll window, which proceeded optimistically. Healthy launches are acknowledged within seconds; growth usually means the operator is down or wedged.",
		},
		nil,
	)

	// NodeGroupQuotaRejections counts fresh upstream quota rejections (not
	// the fail-fast hits on the cached backoff), whether the acceptance poll
	// saw them or the nodegroupstatus controller did after the launch.
	// Rejections are normal operation near the organisation quota ceiling.
	NodeGroupQuotaRejections = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "nodegroup",
			Name:      "quota_rejections_total",
			Help:      "NodeGroup creations rejected by the organisation quota. Rejections are normal operation near the quota ceiling.",
		},
		nil,
	)

	// NodeGroupVanished counts NodeGroups that disappeared after creation
	// while their NodeClaim had not registered — the quota engine reclaiming
	// an accepted group is the documented cause. Each tick is a fast-failed
	// launch that would otherwise have burned the 15-minute registration TTL.
	NodeGroupVanished = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "nodegroup",
			Name:      "vanished_total",
			Help:      "NodeGroups that disappeared after creation while their NodeClaim was still unregistered. Each tick is a fast-failed launch.",
		},
		nil,
	)

	// NodeGroupRejections is the number of NodeGroup creations the upstream
	// operator refused for a reason that is NOT the organisation quota — a
	// flavor the cluster cannot provision, a spec it will not accept. Unlike a
	// quota rejection these are not normal operation: the request was
	// well-formed as far as this provider knows, and the flavor is held out of
	// provisioning for a few minutes so the scheduler relaxes to another one.
	// Refusals published after the acceptance poll, which the nodegroupstatus
	// controller turns into a failed launch, count too. A failure the operator
	// retries on its own (UpstreamError) is not a refusal and is not counted
	// here.
	NodeGroupRejections = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "nodegroup",
			Name:      "rejections_total",
			Help:      "NodeGroup creations refused by the node-group operator for a non-quota reason. The refused flavor is held out of provisioning briefly.",
		},
		nil,
	)

	// NodeGroupSyncOverdue is the number of launched NodeGroups the node-group
	// operator has still not synced 5 minutes after their creation while their
	// NodeClaim is still unregistered. A healthy launch syncs within about a
	// minute; each group counted here is a launch stuck upstream, which would
	// otherwise stay silent until karpenter-core's 15-minute registration
	// timeout deletes the claim.
	NodeGroupSyncOverdue = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "nodegroup",
			Name:      "sync_overdue",
			Help:      "Launched NodeGroups the node-group operator has not synced 5 minutes after their creation while their NodeClaim is still unregistered. Non-zero means launches are stuck upstream.",
		},
		nil,
	)

	// NodeGroupExternalResizes is the number of managed NodeGroups whose
	// nodeCount is not 1 — something outside this provider resized them (the
	// platform's alert-driven scaler through the inherited autoscalingEnabled
	// flag, a human, or anything holding nodegroups/scale RBAC). The
	// 1 NodeClaim = 1 NodeGroup invariant is broken while this is non-zero:
	// karpenter neither tracks nor bills the extra nodes.
	NodeGroupExternalResizes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "nodegroup",
			Name:      "external_resizes",
			Help:      "Managed NodeGroups whose nodeCount is not 1. Non-zero means something outside karpenter resizes its groups.",
		},
		nil,
	)

	// GCReapedNodeGroups counts orphaned NodeGroups deleted by the
	// garbage-collection safety net (not deletions through the normal
	// NodeClaim termination flow).
	GCReapedNodeGroups = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "gc",
			Name:      "reaped_nodegroups_total",
			Help:      "Orphaned NodeGroups deleted by the garbage-collection safety net.",
		},
		nil,
	)

	// GCRefusedNodeGroups is the number of NodeGroups the last GC sweep
	// refused to reap: they carry the managed label but no verified NodeClaim
	// ownership. Non-zero means a hand-copied manifest or a stripped owner
	// reference needs operator attention (the VM keeps billing).
	GCRefusedNodeGroups = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "gc",
			Name:      "refused_nodegroups",
			Help:      "NodeGroups the last garbage-collection sweep refused to reap (managed label without a verified, dead NodeClaim owner). Non-zero needs operator attention.",
		},
		nil,
	)

	// FlavorsConfigInvalid is 1 while the FLAVORS_CONFIG_PATH file failed to
	// load and the controller runs WITHOUT the configured overrides. The
	// chart rolls the pod on every settings.flavors change, so a typo used to
	// crashloop the platform's autoscaler; it now degrades — this gauge is
	// what keeps the degradation from being silent.
	FlavorsConfigInvalid = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "instancetype",
			Name:      "flavors_config_invalid",
			Help:      "1 while the flavors overrides file failed to load and the controller runs without the configured overrides.",
		},
		nil,
	)

	// UnknownFlavorLookups counts instance-type lookups for a flavor absent
	// from the served catalogue — a running NodeGroup references a flavor the
	// catalogue no longer carries.
	UnknownFlavorLookups = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "instancetype",
			Name:      "unknown_flavor_lookups_total",
			Help:      "Instance-type lookups for a flavor absent from the served catalogue.",
		},
		nil,
	)

	// ObservedCapacityRejections counts node capacity reports the
	// instance-type catalogue refused. A kubelet can rewrite its own Node's
	// labels and status, and an accepted report sets the flavor's capacity
	// for the whole cluster, so a report only counts when it comes from a node
	// whose name places it in the managed NodeGroup its labels claim, carries
	// that NodeGroup's flavor, and fits the flavor's catalogue entry (cpu
	// exactly, everything else within 10%).
	ObservedCapacityRejections = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "instancetype",
			Name:      "observed_capacity_rejections_total",
			Help:      "Node capacity reports refused by the instance-type catalogue: a node claiming a managed NodeGroup it is not a node of, a flavor label contradicting that NodeGroup, or figures a VM of that flavor cannot report. The catalogue keeps its previous value.",
		},
		nil,
	)
)

// init pre-seeds the unlabeled series so they exist from the first scrape:
// increase()-style alerts cannot credit an absent→1 transition, which would
// make the first incident tick after a controller restart invisible.
func init() {
	NodeGroupAcceptanceTimeouts.Add(0, nil)
	NodeGroupQuotaRejections.Add(0, nil)
	NodeGroupRejections.Add(0, nil)
	NodeGroupVanished.Add(0, nil)
	NodeGroupExternalResizes.Set(0, nil)
	NodeGroupSyncOverdue.Set(0, nil)
	FlavorsConfigInvalid.Set(0, nil)
	GCReapedNodeGroups.Add(0, nil)
	GCRefusedNodeGroups.Set(0, nil)
	UnknownFlavorLookups.Add(0, nil)
	ObservedCapacityRejections.Add(0, nil)
}
