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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/state"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/instancetypecapacity"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

func TestLargerDiskLaunchesAlongsideLegacyNodes(t *testing.T) {
	ctx := coreContext()
	_, kubeClient := newClusterStateProvider(t)
	itp := instancetype.NewProvider("par", nil, nil)
	cp := cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{}))
	np := topologyNodePool("default", corev1.NodeSelectorRequirement{
		Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"2XS"},
	})
	seed(t, kubeClient, np)
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	initial := provisioningPass(t, cp, kubeClient, cluster, np, pendingPod("initial"))
	if len(initial.NewNodeClaims) != 1 {
		t.Fatal("expected a claim for the initial small pod")
	}
	legacy := launchPlanned(t, cp, kubeClient, initial.NewNodeClaims[0], "default-legacy")
	// Model a claim and registered worker launched before the disk rollout.
	// The node's actual status must remain authoritative for existing-node
	// placement, even while the catalogue describes larger future workers.
	legacy.Status.Capacity[corev1.ResourceEphemeralStorage] = resource.MustParse("40971488Ki")
	legacy.Status.Allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse("37759323279")
	if err := kubeClient.Update(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: legacy.Name}, ng); err != nil {
		t.Fatal(err)
	}
	node := joinNode(t, kubeClient, ng, "0")
	stampNode(t, kubeClient, node.Name)
	registerNode(t, kubeClient, legacy, node.Name)
	reportCapacity(t, kubeClient, node.Name, legacy)
	observeNode(t, kubeClient, cluster, node.Name)
	cluster.UpdateNodeClaim(legacy)
	if _, err := instancetypecapacity.NewController(kubeClient, itp).Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: node.Name}}); err != nil {
		t.Fatal(err)
	}

	small := pendingPod("small")
	small.Spec.Containers[0].Resources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse("1Gi")
	requireAbsorbed(t, provisioningPass(t, cp, kubeClient, cluster, np, small), small, "legacy worker still usable")
	large := pendingPod("large")
	large.Spec.Containers[0].Resources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse("50Gi")
	results := provisioningPass(t, cp, kubeClient, cluster, np, large)
	if len(results.NewNodeClaims) != 1 || results.PodErrors[large] != nil {
		t.Fatalf("50Gi pod must get a new worker, never the legacy disk: new=%d existing=%d error=%v",
			len(results.NewNodeClaims), len(results.ExistingNodes), results.PodErrors[large])
	}
	for _, existing := range results.ExistingNodes {
		if len(existing.Pods) != 0 {
			t.Fatalf("50Gi pod was placed on the legacy worker: %v", results.ExistingNodeToPodMapping())
		}
	}
	launched := launchPlanned(t, cp, kubeClient, results.NewNodeClaims[0], "default-larger")
	if disk := launched.Status.Allocatable[corev1.ResourceEphemeralStorage]; disk.Cmp(resource.MustParse("50Gi")) < 0 {
		t.Fatalf("new claim advertises only %s disk", disk.String())
	}
	cluster.UpdateNodeClaim(launched)
	inflight := provisioningPass(t, cp, kubeClient, cluster, np, large)
	planned := inflight.ExistingNodeToPodMapping()
	if len(inflight.NewNodeClaims) != 0 || inflight.PodErrors[large] != nil || len(planned[launched.Name]) != 1 || len(planned[legacy.Name]) != 0 {
		t.Fatalf("50Gi pod must stay on the larger in-flight claim: new=%d largerPods=%d legacyPods=%d error=%v",
			len(inflight.NewNodeClaims), len(planned[launched.Name]), len(planned[legacy.Name]), inflight.PodErrors[large])
	}

	legacy.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	if _, err := disruption.NewController(clock.RealClock{}, kubeClient, cp).Reconcile(ctx, legacy); err != nil {
		t.Fatalf("legacy claim drift reconcile: %v", err)
	}
	if condition := legacy.StatusConditions().Get(karpv1.ConditionTypeDrifted); condition.IsTrue() {
		t.Errorf("changing the disk seed drifted the old claim: %s", condition.Reason)
	}
}
