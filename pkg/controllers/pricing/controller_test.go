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

package pricing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/pricing"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

type stubResolver struct {
	flavors []instancetype.Flavor
	err     error
}

func (s stubResolver) Resolve(context.Context) ([]instancetype.Flavor, error) {
	return s.flavors, s.err
}

// withinJitter asserts a requeue landed in the ±10% jitter window around want.
func withinJitter(t *testing.T, got, want time.Duration) {
	t.Helper()
	lo, hi := time.Duration(float64(want)*0.9), time.Duration(float64(want)*1.1)
	if got < lo || got > hi {
		t.Errorf("RequeueAfter = %v, want within [%v, %v]", got, lo, hi)
	}
}

func TestReconcileUpdatesProvider(t *testing.T) {
	itp := instancetype.NewProvider("par", nil, nil)
	resolver := stubResolver{flavors: []instancetype.Flavor{
		{Name: "M", CPU: 10, MemoryKi: 15988992, PriceHourly: 0.20},
	}}
	ctrl := pricing.NewController(resolver, itp, 12*time.Hour)

	result, err := ctrl.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	withinJitter(t, result.RequeueAfter, 12*time.Hour)
	if metricstest.Value(t, "karpenter_clevercloud_pricing_last_successful_refresh_timestamp_seconds") == 0 {
		t.Error("expected the last-successful-refresh gauge to be set")
	}
	it, err := itp.Get("M")
	if err != nil {
		t.Fatalf("Get(M): %v", err)
	}
	if it.Offerings[0].Price != 0.20 {
		t.Errorf("expected refreshed price 0.20, got %v", it.Offerings[0].Price)
	}
	// The refresh narrowed the base to M only.
	if _, err := itp.Get("2XS"); err == nil {
		t.Error("expected 2XS gone after a refresh that omitted it")
	}
}

func TestReconcileKeepsLastKnownGoodOnError(t *testing.T) {
	itp := instancetype.NewProvider("par", nil, nil)
	ctrl := pricing.NewController(stubResolver{err: errors.New("api down")}, itp, 12*time.Hour)
	failuresBefore := metricstest.Value(t, "karpenter_clevercloud_pricing_refresh_failures_total")

	result, err := ctrl.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile must swallow the error, got %v", err)
	}
	withinJitter(t, result.RequeueAfter, 30*time.Minute)
	if delta := metricstest.Value(t, "karpenter_clevercloud_pricing_refresh_failures_total") - failuresBefore; delta != 1 {
		t.Errorf("pricing_refresh_failures_total delta = %v, want 1", delta)
	}
	// The default catalog must be intact.
	if got := len(itp.List()); got != len(instancetype.DefaultFlavors) {
		t.Errorf("expected catalog unchanged (%d), got %d", len(instancetype.DefaultFlavors), got)
	}
	it, err := itp.Get("M")
	if err != nil {
		t.Fatalf("Get(M): %v", err)
	}
	if it.Offerings[0].Price != 0.1167 {
		t.Errorf("expected default M price 0.1167 retained, got %v", it.Offerings[0].Price)
	}
}

func TestReconcileEmptyResultKeepsCatalog(t *testing.T) {
	itp := instancetype.NewProvider("par", nil, nil)
	ctrl := pricing.NewController(stubResolver{flavors: []instancetype.Flavor{}}, itp, time.Hour)

	result, err := ctrl.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	withinJitter(t, result.RequeueAfter, time.Hour)
	if got := len(itp.List()); got != len(instancetype.DefaultFlavors) {
		t.Errorf("empty result must keep the catalog (%d), got %d", len(instancetype.DefaultFlavors), got)
	}
}

func TestFailureRequeueNeverExceedsPeriod(t *testing.T) {
	// A short period must not be overridden by the 30m failure backoff.
	itp := instancetype.NewProvider("par", nil, nil)
	ctrl := pricing.NewController(stubResolver{err: errors.New("boom")}, itp, 5*time.Minute)

	result, _ := ctrl.Reconcile(context.Background())
	withinJitter(t, result.RequeueAfter, 5*time.Minute)
}
