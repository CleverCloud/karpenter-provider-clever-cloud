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

package cloudprovider_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	coremetrics "sigs.k8s.io/karpenter/pkg/cloudprovider/metrics"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
)

// TestCreateFailuresLandInTheDocumentedErrorSeries runs Create through
// karpenter-core's metrics decorator, as main.go does, and pins the
// karpenter_cloudprovider_errors_total series docs/observability.md sends
// operators to for each launch failure. Core picks the value from the error
// type the provider returns, and core 1.15 renamed the untyped one from "" to
// "unknown": a rename fails here instead of silently emptying a dashboard.
func TestCreateFailuresLandInTheDocumentedErrorSeries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		errorValue string
		launch     func(t *testing.T) (corecloudprovider.CloudProvider, *karpv1.NodeClaim)
	}{
		{
			// A capacity failure, which core re-plans around.
			name:       "quota rejection",
			errorValue: "InsufficientCapacityError",
			launch: func(t *testing.T) (corecloudprovider.CloudProvider, *karpv1.NodeClaim) {
				cp, kubeClient := newOperatorProvider(t, &fakeOperator{headroomGB: 0})
				return cp, stored(t, kubeClient, claimFor("default-quota", "1Gi", "2XS"))
			},
		},
		{
			name:       "NodeClass not ready",
			errorValue: "NodeClassNotReadyError",
			launch: func(t *testing.T) (corecloudprovider.CloudProvider, *karpv1.NodeClaim) {
				cp, kubeClient := newTestProvider(t, &v1alpha1.CleverNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
				return cp, stored(t, kubeClient, testNodeClaim("default-unrdy"))
			},
		},
		{
			// A NodeClaim deleted while its launch waited on the create lock:
			// deliberately a plain error, counted as neither a capacity nor a
			// nodeclass failure.
			name:       "NodeClaim gone",
			errorValue: "unknown",
			launch: func(t *testing.T) (corecloudprovider.CloudProvider, *karpv1.NodeClaim) {
				cp, _ := newTestProvider(t, readyNodeClass("default"))
				return cp, testNodeClaim("default-gone1")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			series := map[string]string{"method": "Create", "provider": "clevercloud", "error": tc.errorValue, "nodepool": "default"}
			cp, nodeClaim := tc.launch(t)
			before := metricstest.ValueWith(t, "karpenter_cloudprovider_errors_total", series)
			if _, err := coremetrics.Decorate(cp).Create(context.Background(), nodeClaim); err == nil {
				t.Fatal("expected the launch to fail")
			}
			if delta := metricstest.ValueWith(t, "karpenter_cloudprovider_errors_total", series) - before; delta != 1 {
				t.Errorf("karpenter_cloudprovider_errors_total%v moved by %v, want 1", series, delta)
			}
		})
	}
}
