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

// These tests cover a NodeGroup the platform is already deleting. Clever
// Cloud's finalizer keeps such a group, with a deletion timestamp, until the
// group's Node is gone — and karpenter's termination finalizer holds that Node
// until core sees the instance gone. The cloud provider is how core sees it:
// if it reported the terminating group as a running instance, neither
// finalizer would ever complete, and every node termination would wedge
// (docs/E2E-RESULTS.md, "No finalizer deadlock").

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/controllers/state"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// terminatingNodeGroup is a managed NodeGroup the platform is deleting: its
// finalizer keeps it, with a deletion timestamp, until its node is gone.
func terminatingNodeGroup(name, flavor string) *ngv1.NodeGroup {
	ng := managedNodeGroup(name, flavor)
	ng.Finalizers = []string{"api.clever-cloud.com/finalizer"}
	now := metav1.Now()
	ng.DeletionTimestamp = &now
	return ng
}

// newCoreIndexedClient is a fake client carrying the field indexes
// karpenter-core's node controllers read through: NodeClaims by provider ID
// and pods by node.
func newCoreIndexedClient(objs ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}, &karpv1.NodeClaim{}).
		WithIndex(&karpv1.NodeClaim{}, "status.providerID", func(o client.Object) []string {
			return []string{o.(*karpv1.NodeClaim).Status.ProviderID}
		}).
		WithIndex(&corev1.Node{}, "spec.providerID", func(o client.Object) []string {
			return []string{o.(*corev1.Node).Spec.ProviderID}
		}).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithIndex(&storagev1.VolumeAttachment{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*storagev1.VolumeAttachment).Spec.NodeName}
		}).
		Build()
}

// launchedNodeClaim is the NodeClaim of a registered node of the group of the
// same name, as karpenter-core keeps it: labelled with its NodePool and
// NodeClass, the provider ID on its status, and core's termination finalizer.
func launchedNodeClaim(name string) *karpv1.NodeClaim {
	nodeClaim := testNodeClaim(name)
	nodeClaim.Labels[karpv1.NodeClassLabelKey(nodeClaim.Spec.NodeClassRef.GroupKind())] = "default"
	nodeClaim.Finalizers = []string{karpv1.TerminationFinalizer}
	nodeClaim.Status.ProviderID = nodegroup.ProviderID(name)
	nodeClaim.Status.NodeName = name + "-node0"
	return nodeClaim
}

// clusterNode is the node <group>-node0 of a launched NodeClaim once
// registered, with its NodeReady condition.
func clusterNode(nodeClaim *karpv1.NodeClaim, ready corev1.ConditionStatus, since time.Time) *corev1.Node {
	labels := map[string]string{}
	for k, v := range nodeClaim.Labels {
		labels[k] = v
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:       nodeClaim.Status.NodeName,
			Labels:     labels,
			Finalizers: []string{karpv1.TerminationFinalizer},
		},
		Spec: corev1.NodeSpec{ProviderID: nodeClaim.Status.ProviderID},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: ready, LastTransitionTime: metav1.NewTime(since)},
		}},
	}
}

// TestGetReleasesTheNodeOfATerminatingNodeGroup runs karpenter-core's node
// termination controller over the node of a group the platform is deleting.
// Once the platform has stopped its VM the node is NotReady, and core then
// asks the cloud provider whether the instance still exists before draining
// anything: NodeClaimNotFound releases the node's finalizer at once, which
// lets Clever Cloud's own finalizer finish. Were the terminating group
// described as running, core would drain the node and wait for the instance
// to go through Delete, which only answers that a deletion is already in
// flight — the group waiting on the node, the node on the group, forever.
func TestGetReleasesTheNodeOfATerminatingNodeGroup(t *testing.T) {
	ctx := context.Background()
	group := terminatingNodeGroup("default-term1", "XS")
	nodeClaim := launchedNodeClaim(group.Name)
	node := clusterNode(nodeClaim, corev1.ConditionUnknown, time.Now().Add(-time.Minute))
	now := metav1.Now()
	node.DeletionTimestamp = &now
	kubeClient := newCoreIndexedClient(readyNodeClass("default"), group, nodeClaim, node)
	itp := instancetype.NewProvider("par", nil, nil)
	cp := cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{}))

	if _, err := cp.Get(ctx, nodeClaim.Status.ProviderID); !corecloudprovider.IsNodeClaimNotFoundError(err) {
		t.Errorf("expected NodeClaimNotFound for a terminating nodegroup, got %v", err)
	}

	clk := clock.RealClock{}
	controller := termination.NewController(clk, kubeClient, cp, terminator.NewTerminator(clk, kubeClient, terminator.NewQueue(clk, kubeClient, noopRecorder{}), noopRecorder{}), noopRecorder{})
	if _, err := controller.Reconcile(ctx, node); err != nil {
		t.Fatalf("core node termination: %v", err)
	}
	// The fake client removes an object whose deletion timestamp is set as soon
	// as its last finalizer goes.
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: node.Name}, &corev1.Node{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected core to release the node of the terminating nodegroup at once, got %v", err)
	}
}

// A cluster upgrade can block the CKE finalizer even though Kubernetes
// accepted DELETE. Core must retain the NodeClaim until the group disappears
// and resume automatically afterwards. Its five-second retry has no timeout:
// the deletion-overdue signal must report this without forcing progress.
// This characterizes the existing core/provider contract after drain; it
// does not exercise the overdue signal or measure a maximum waiting time.
func TestBlockedDeletionWaitsAndResumesWhenThePlatformRecovers(t *testing.T) {
	for _, ready := range []corev1.ConditionStatus{corev1.ConditionTrue, corev1.ConditionUnknown} {
		t.Run(string(ready), func(t *testing.T) {
			ctx := context.Background()
			group := terminatingNodeGroup("default-blocked", "XS")
			claim := launchedNodeClaim(group.Name)
			claim.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
			claim.StatusConditions().SetTrue(karpv1.ConditionTypeDrained)
			node := clusterNode(claim, ready, time.Now().Add(-time.Hour))
			now := metav1.Now()
			node.DeletionTimestamp, claim.DeletionTimestamp = &now, &now
			kubeClient := newCoreIndexedClient(readyNodeClass("default"), group, claim, node)
			clk := clock.RealClock{}
			itp := instancetype.NewProvider("par", nil, nil)
			cp := cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clk))
			nodeController := termination.NewController(clk, kubeClient, cp, terminator.NewTerminator(clk, kubeClient, terminator.NewQueue(clk, kubeClient, noopRecorder{}), noopRecorder{}), noopRecorder{})
			claimController := lifecycle.NewController(clk, kubeClient, cp, noopRecorder{}, state.NewCluster(clk, kubeClient, cp), nil, nil)
			for range 3 {
				err := kubeClient.Get(ctx, client.ObjectKeyFromObject(node), node)
				if err == nil {
					result, err := nodeController.Reconcile(ctx, node)
					if err != nil {
						t.Fatalf("core node termination: %v", err)
					}
					if ready == corev1.ConditionTrue && result.RequeueAfter != 5*time.Second {
						t.Fatalf("Ready node retry = %v, want 5s", result.RequeueAfter)
					}
				} else if !apierrors.IsNotFound(err) {
					t.Fatal(err)
				}
				err = kubeClient.Get(ctx, client.ObjectKeyFromObject(node), node)
				if ready == corev1.ConditionTrue {
					if err != nil || !controllerutil.ContainsFinalizer(node, karpv1.TerminationFinalizer) {
						t.Fatalf("Ready node released while deletion blocked: %v", err)
					}
				} else if !apierrors.IsNotFound(err) {
					t.Fatalf("NotReady node must be released to avoid the CKE finalizer deadlock: %v", err)
				}
				if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), claim); err != nil {
					t.Fatal(err)
				}
				result, err := claimController.Reconcile(ctx, claim)
				// While the Node exists its termination controller performs
				// the retry; once gone the NodeClaim controller takes over.
				if err != nil || (ready == corev1.ConditionUnknown && result.RequeueAfter != 5*time.Second) {
					t.Fatalf("core claim termination = %+v, %v", result, err)
				}
				if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), claim); err != nil {
					t.Fatal(err)
				}
				if !controllerutil.ContainsFinalizer(claim, karpv1.TerminationFinalizer) ||
					!claim.StatusConditions().IsTrue(karpv1.ConditionTypeInstanceTerminating) {
					t.Fatalf("blocked claim lost its finalizer or InstanceTerminating: %+v", claim)
				}
			}
			// Simulate the platform completing its own finalizer. Production
			// code never removes it; only its disappearance allows completion.
			if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(group), group); err != nil {
				t.Fatal(err)
			}
			group.Finalizers = nil
			if err := kubeClient.Update(ctx, group); err != nil {
				t.Fatal(err)
			}
			if ready == corev1.ConditionTrue {
				if _, err := nodeController.Reconcile(ctx, node); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := claimController.Reconcile(ctx, claim); err != nil {
				t.Fatal(err)
			}
			for _, obj := range []client.Object{node, claim, group} {
				if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(obj), obj); !apierrors.IsNotFound(err) {
					t.Errorf("%T did not finish deletion after platform recovery: %v", obj, err)
				}
			}
		})
	}
}

// TestDeleteLeavesATerminatingNodeGroupAlone covers the calls karpenter-core
// keeps making while a group terminates: its termination calls Delete every
// 5 seconds until the instance is reported gone. Those calls are not new
// deletions. They must not send the DELETE again, and above all must not
// forget a quota rejection recorded since the first one — the quota engine
// made it with that deletion already in flight, so repeating the deletion
// frees nothing it had not counted, and clearing it would send the next
// launch straight back into the exhausted quota. The answer stays nil, not
// NodeClaimNotFound: the group still exists, so core keeps waiting for it.
func TestDeleteLeavesATerminatingNodeGroupAlone(t *testing.T) {
	ctx := context.Background()
	group := terminatingNodeGroup("default-term2", "XS")
	var mu sync.Mutex
	deletes := 0
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(readyNodeClass("default"), group).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*ngv1.NodeGroup); ok {
					mu.Lock()
					deletes++
					mu.Unlock()
				}
				return c.Delete(ctx, obj, opts...)
			},
		}).
		Build()
	itp := instancetype.NewProvider("par", nil, nil)
	ngp := nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{})
	cp := cloudprovider.New(kubeClient, kubeClient, itp, ngp)

	// Another claim's launch is rejected by the quota while the group goes.
	rejected := managedNodeGroup("default-quota", "2XS")
	rejected.Status = ngv1.NodeGroupStatus{
		Phase:      ngv1.PhaseQuotaExceeded,
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonQuotaExceeded, "")},
	}
	ngp.RecordLateRefusal(testNodeClaim(rejected.Name), rejected)

	nodeClaim := launchedNodeClaim(group.Name)
	for range 3 {
		if err := cp.Delete(ctx, nodeClaim); err != nil {
			t.Fatalf("Delete of a terminating nodegroup: %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if deletes != 0 {
		t.Errorf("sent %d DELETE requests for a nodegroup already being deleted, want 0", deletes)
	}
	if !ngp.Unavailable("2XS") {
		t.Error("repeating the delete of a terminating nodegroup forgot the quota rejection recorded since")
	}
}
