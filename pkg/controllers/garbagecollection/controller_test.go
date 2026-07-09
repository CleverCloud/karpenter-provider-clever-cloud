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

// managedNodeGroup builds a karpenter-managed NodeGroup backdated by age,
// carrying the NodeClaim owner reference exactly as Create stamps it.
// An empty claimLabel omits the nodeclaim label so the controller falls back
// to the NodeGroup name. The fake client requires a finalizer alongside a
// DeletionTimestamp, hence the test finalizer on deleting groups.
func managedNodeGroup(name, claimLabel string, age time.Duration, deleting bool) *ngv1.NodeGroup {
	ng := labeledNodeGroup(name, claimLabel, age, deleting)
	claimName := claimLabel
	if claimName == "" {
		claimName = name
	}
	ng.OwnerReferences = []metav1.OwnerReference{
		{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: claimName, UID: types.UID("uid-" + claimName)},
	}
	return ng
}

// labeledNodeGroup builds a NodeGroup carrying the managed label but no owner
// reference — what a user gets by copying a karpenter-created manifest.
func labeledNodeGroup(name, claimLabel string, age time.Duration, deleting bool) *ngv1.NodeGroup {
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

func TestReconcileKeepsNodeGroupWhoseOwnerIsAlive(t *testing.T) {
	// The nodeclaim label is mutable; the owner reference is the identity.
	// A group whose label points at nothing but whose recorded owner lives
	// must never be reaped — kubernetes GC semantics would keep it too.
	ng := labeledNodeGroup("ng-mislabeled", "claim-gone", 10*time.Minute, false)
	ng.OwnerReferences = []metav1.OwnerReference{
		{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: "claim-alive", UID: types.UID("uid-claim-alive")},
	}
	ctrl, kubeClient := newTestController(t, ng, testNodeClaim("claim-alive"))

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !nodeGroupExists(t, kubeClient, "ng-mislabeled") {
		t.Error("expected nodegroup with a living NodeClaim owner to be retained despite its stale label")
	}
}

func TestReconcileTreatsForeignOwnerReferencesAsUnowned(t *testing.T) {
	// Only karpenter.sh NodeClaim references prove this provider created the
	// group; a NodePool ref or a NodeClaim kind from another API group must
	// not unlock reaping.
	nodePoolRef := labeledNodeGroup("ng-nodepool-ref", "claim-gone", 10*time.Minute, false)
	nodePoolRef.OwnerReferences = []metav1.OwnerReference{
		{APIVersion: "karpenter.sh/v1", Kind: "NodePool", Name: "pool-a", UID: types.UID("uid-pool-a")},
	}
	foreignGroup := labeledNodeGroup("ng-foreign-group", "claim-gone", 10*time.Minute, false)
	foreignGroup.OwnerReferences = []metav1.OwnerReference{
		{APIVersion: "karpenter.sh.example.com/v1", Kind: "NodeClaim", Name: "claim-gone", UID: types.UID("uid-x")},
	}
	ctrl, kubeClient := newTestController(t, nodePoolRef, foreignGroup)

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !nodeGroupExists(t, kubeClient, "ng-nodepool-ref") {
		t.Error("expected nodegroup with only a NodePool owner reference to be retained")
	}
	if !nodeGroupExists(t, kubeClient, "ng-foreign-group") {
		t.Error("expected nodegroup with a foreign-group NodeClaim reference to be retained")
	}
}

func TestReconcileReapsAcrossKarpenterAPIVersions(t *testing.T) {
	// The karpenter.sh/ prefix match is deliberate future-proofing: a group
	// stamped by a version using karpenter.sh/v1beta1 still counts as owned.
	ng := labeledNodeGroup("ng-beta-ref", "claim-gone", 10*time.Minute, false)
	ng.OwnerReferences = []metav1.OwnerReference{
		{APIVersion: "karpenter.sh/v1beta1", Kind: "NodeClaim", Name: "claim-gone", UID: types.UID("uid-claim-gone")},
	}
	ctrl, kubeClient := newTestController(t, ng)

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if nodeGroupExists(t, kubeClient, "ng-beta-ref") {
		t.Error("expected orphaned nodegroup with a v1beta1 NodeClaim reference to be reaped")
	}
}

func TestReconcileKeepsLabeledNodeGroupWithoutOwnerReference(t *testing.T) {
	// A user who copies a karpenter-created NodeGroup manifest keeps the
	// managed label but not the owner reference (or strips it). Reaping on
	// the label alone would destroy nodes this provider never created.
	ctrl, kubeClient := newTestController(t, labeledNodeGroup("copied-pool", "", 10*time.Minute, false))

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !nodeGroupExists(t, kubeClient, "copied-pool") {
		t.Error("expected labeled nodegroup without a NodeClaim owner reference to be retained")
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
