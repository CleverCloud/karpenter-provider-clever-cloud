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
	// timeout and proceeded optimistically. Sustained growth is the signature
	// of a down or wedged node-group operator, which otherwise looks like
	// normal (slow) provisioning until the registration TTL fires.
	NodeGroupAcceptanceTimeouts = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "nodegroup",
			Name:      "acceptance_timeouts_total",
			Help:      "NodeGroup creations that were not accepted by the node-group operator within the poll window and proceeded optimistically. Sustained growth usually means the operator is down or wedged.",
		},
		nil,
	)

	// NodeGroupQuotaRejections counts fresh upstream quota rejections (not
	// the fail-fast hits on the cached backoff). Rejections are normal
	// operation near the organisation quota ceiling.
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

	// PricingRefreshFailures counts failed catalogue refreshes; each failure
	// keeps the last-known-good catalogue in use.
	PricingRefreshFailures = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "pricing",
			Name:      "refresh_failures_total",
			Help:      "Failed refreshes of the flavor/price catalogue. The last-known-good catalogue stays in use.",
		},
		nil,
	)

	// PricingLastSuccessfulRefresh is the unix time of the last successful
	// catalogue refresh. Deliberately NOT pre-seeded in init: the series is
	// absent until the first successful refresh, and exports nothing at all
	// when the refresher is disabled (air-gap installs) — alert on staleness
	// only when > 0.
	PricingLastSuccessfulRefresh = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "pricing",
			Name:      "last_successful_refresh_timestamp_seconds",
			Help:      "Unix time of the last successful catalogue refresh. Absent until the first success; exports no series when the refresher is disabled.",
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
)

// init pre-seeds the unlabeled series so they exist from the first scrape:
// increase()-style alerts cannot credit an absent→1 transition, which would
// make the first incident tick after a controller restart invisible. The
// pricing timestamp gauge is intentionally left absent (see its comment).
func init() {
	NodeGroupAcceptanceTimeouts.Add(0, nil)
	NodeGroupQuotaRejections.Add(0, nil)
	GCReapedNodeGroups.Add(0, nil)
	GCRefusedNodeGroups.Set(0, nil)
	PricingRefreshFailures.Add(0, nil)
	UnknownFlavorLookups.Add(0, nil)
}
