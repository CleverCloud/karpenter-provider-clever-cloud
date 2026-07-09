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

// Package pricing periodically refreshes the instance-type catalog from Clever
// Cloud's public price API. It is a singleton controller: it fires once shortly
// after startup and then on a fixed period. A failed refresh keeps the
// last-known-good catalog (the binary always seeds DefaultFlavors), so the
// network is never on the critical path.
package pricing

import (
	"context"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

const (
	// DefaultRefreshPeriod is how often the catalog is refreshed on success.
	DefaultRefreshPeriod = 12 * time.Hour
	// failureRequeue retries sooner than the period after a failed refresh.
	failureRequeue = 30 * time.Minute
)

// Resolver fetches a freshly priced catalog. *pricing.Provider satisfies it; an
// interface keeps the controller testable without HTTP.
type Resolver interface {
	Resolve(ctx context.Context) ([]instancetype.Flavor, error)
}

type Controller struct {
	resolver             Resolver
	instanceTypeProvider *instancetype.Provider
	period               time.Duration
}

// NewController builds the refresher. A non-positive period falls back to
// DefaultRefreshPeriod.
func NewController(resolver Resolver, instanceTypeProvider *instancetype.Provider, period time.Duration) *Controller {
	if period <= 0 {
		period = DefaultRefreshPeriod
	}
	return &Controller{resolver: resolver, instanceTypeProvider: instanceTypeProvider, period: period}
}

func (c *Controller) Name() string {
	return "instancetype.pricing"
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	flavors, err := c.resolver.Resolve(ctx)
	if err != nil {
		// Keep the last-known-good catalog; never clear it. Returning the error
		// would route through the rate limiter and discard our chosen requeue,
		// so we log and retry sooner than the period instead.
		metrics.PricingRefreshFailures.Inc(nil)
		log.FromContext(ctx).Error(err, "refreshing flavor catalog; keeping last-known-good")
		return reconciler.Result{RequeueAfter: c.failureRequeue()}, nil
	}
	c.instanceTypeProvider.SetBaseFlavors(flavors)
	metrics.PricingLastSuccessfulRefresh.Set(float64(time.Now().Unix()), nil)
	log.FromContext(ctx).Info("refreshed flavor catalog from clever cloud api", "flavors", len(flavors))
	return reconciler.Result{RequeueAfter: c.period}, nil
}

// failureRequeue never retries slower than the success cadence.
func (c *Controller) failureRequeue() time.Duration {
	if c.period < failureRequeue {
		return c.period
	}
	return failureRequeue
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}
