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

package garbagecollection_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/garbagecollection"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

func newTestController(t *testing.T, objs ...client.Object) (*garbagecollection.Controller, client.Client) {
	t.Helper()
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		Build()
	return garbagecollection.NewController(kubeClient, nodegroup.NewProvider(kubeClient)), kubeClient
}

// managedNodeGroup builds a karpenter-managed NodeGroup backdated by age.
// An empty claimLabel omits the nodeclaim label so the controller falls back
// to the NodeGroup name. The fake client requires a finalizer alongside a
// DeletionTimestamp, hence the test finalizer on deleting groups.
func managedNodeGroup(name, claimLabel string, age time.Duration, deleting bool) *ngv1.NodeGroup {
	labels := map[string]string{v1alpha1.ManagedLabelKey: "true"}
	if claimLabel != "" {
		labels[v1alpha1.NodeClaimLabelKey] = claimLabel
	}
	ng := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Labels:            labels,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-age)),
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1},
	}
	if deleting {
		now := metav1.Now()
		ng.DeletionTimestamp = &now
		ng.Finalizers = []string{"test.finalizer/keep"}
	}
	return ng
}

func testNodeClaim(name string) *karpv1.NodeClaim {
	return &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func nodeGroupExists(t *testing.T, kubeClient client.Client, name string) bool {
	t.Helper()
	err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, &ngv1.NodeGroup{})
	if client.IgnoreNotFound(err) != nil {
		t.Fatalf("getting nodegroup %s: %v", name, err)
	}
	return err == nil
}

func TestReconcileDeletesOrphanedNodeGroup(t *testing.T) {
	ctrl, kubeClient := newTestController(t, managedNodeGroup("ng-orphan", "claim-gone", 10*time.Minute, false))

	result, err := ctrl.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != 2*time.Minute {
		t.Errorf("expected RequeueAfter of 2m, got %v", result.RequeueAfter)
	}
	if nodeGroupExists(t, kubeClient, "ng-orphan") {
		t.Error("expected orphaned nodegroup to be deleted")
	}
}

func TestReconcileKeepsNodeGroupWithLivingNodeClaim(t *testing.T) {
	ctrl, kubeClient := newTestController(t,
		managedNodeGroup("ng-live", "claim-live", 10*time.Minute, false),
		testNodeClaim("claim-live"),
	)

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !nodeGroupExists(t, kubeClient, "ng-live") {
		t.Error("expected nodegroup with living nodeclaim to be retained")
	}
}

func TestReconcileSkipsYoungNodeGroup(t *testing.T) {
	// Explicit CreationTimestamp=now: a zero timestamp would read as ancient.
	ctrl, kubeClient := newTestController(t, managedNodeGroup("ng-young", "claim-gone", 0, false))

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !nodeGroupExists(t, kubeClient, "ng-young") {
		t.Error("expected young nodegroup to be retained")
	}
}

func TestReconcileFallsBackToNodeGroupName(t *testing.T) {
	// Without a nodeclaim label, the NodeGroup name is the claim name.
	ctrl, kubeClient := newTestController(t,
		managedNodeGroup("named-claim", "", 10*time.Minute, false),
		managedNodeGroup("orphan-claim", "", 10*time.Minute, false),
		testNodeClaim("named-claim"),
	)

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !nodeGroupExists(t, kubeClient, "named-claim") {
		t.Error("expected nodegroup matching a nodeclaim by name to be retained")
	}
	if nodeGroupExists(t, kubeClient, "orphan-claim") {
		t.Error("expected nodegroup without a matching nodeclaim to be deleted")
	}
}

func TestReconcileIgnoresUnmanagedNodeGroups(t *testing.T) {
	userNG := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "user-pool",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "M", NodeCount: 3},
	}
	ctrl, kubeClient := newTestController(t, userNG)

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !nodeGroupExists(t, kubeClient, "user-pool") {
		t.Error("expected unmanaged nodegroup to be retained")
	}
}

func TestReconcileSkipsAlreadyDeletingNodeGroup(t *testing.T) {
	ctrl, kubeClient := newTestController(t, managedNodeGroup("ng-deleting", "claim-gone", 10*time.Minute, true))

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// The finalizer keeps the object visible; the controller must not error
	// on a nodegroup already being torn down by the Clever Cloud operator.
	if !nodeGroupExists(t, kubeClient, "ng-deleting") {
		t.Error("expected already-deleting nodegroup to still be present")
	}
}
