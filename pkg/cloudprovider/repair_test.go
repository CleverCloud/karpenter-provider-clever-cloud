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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"

	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// TestRepairPoliciesReplaceANodeNotReadyForTenMinutes runs karpenter-core's
// node health controller (enabled by the NodeRepair feature gate) with the
// provider's RepairPolicies over the node of a managed group. A node whose
// kubelet reports NotReady (False), and one the node lifecycle controller
// marked Unknown because its kubelet stopped reporting — a VM gone from under
// it — are tolerated for 10 minutes, then core deletes their NodeClaim so the
// capacity is replaced; a Ready node is left alone.
func TestRepairPoliciesReplaceANodeNotReadyForTenMinutes(t *testing.T) {
	const toleration = 10 * time.Minute
	for _, status := range []corev1.ConditionStatus{corev1.ConditionFalse, corev1.ConditionUnknown, corev1.ConditionTrue} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			clk := clocktesting.NewFakeClock(time.Now())
			group := managedNodeGroup("default-heal1", "XS")
			nodeClaim := launchedNodeClaim(group.Name)
			node := clusterNode(nodeClaim, status, clk.Now())
			kubeClient := newCoreIndexedClient(readyNodeClass("default"), group, nodeClaim, node)
			itp := instancetype.NewProvider("par", nil, nil)
			cp := cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{}))
			controller := health.NewController(kubeClient, cp, clk, noopRecorder{})
			repaired := func() bool {
				t.Helper()
				if _, err := controller.Reconcile(ctx, node); err != nil {
					t.Fatalf("core node health: %v", err)
				}
				current := &karpv1.NodeClaim{}
				err := kubeClient.Get(ctx, types.NamespacedName{Name: nodeClaim.Name}, current)
				if apierrors.IsNotFound(err) {
					return true
				}
				if err != nil {
					t.Fatalf("getting nodeclaim: %v", err)
				}
				return !current.DeletionTimestamp.IsZero()
			}

			clk.Step(toleration - time.Second)
			if repaired() {
				t.Fatalf("node %s for %s: repaired before its toleration elapsed", status, toleration-time.Second)
			}
			clk.Step(time.Second)
			if got, want := repaired(), status != corev1.ConditionTrue; got != want {
				t.Errorf("node Ready=%s for %s: repaired = %v, want %v", status, toleration, got, want)
			}
		})
	}
}
