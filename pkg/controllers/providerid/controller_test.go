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

package providerid_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/providerid"
)

func node(name string, labels map[string]string, providerID string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec:       corev1.NodeSpec{ProviderID: providerID},
	}
}

func managedNodeGroup(name string) *ngv1.NodeGroup {
	return &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true"},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1},
	}
}

func unmanagedNodeGroup(name string) *ngv1.NodeGroup {
	return &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1},
	}
}

func reconcileNode(t *testing.T, kubeClient client.Client, nodeName string) error {
	t.Helper()
	c := providerid.NewController(kubeClient, kubeClient)
	_, err := c.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: nodeName}})
	return err
}

func getProviderID(t *testing.T, kubeClient client.Client, nodeName string) string {
	t.Helper()
	got := &corev1.Node{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeName}, got); err != nil {
		t.Fatalf("getting node: %v", err)
	}
	return got.Spec.ProviderID
}

func newClient(objs ...client.Object) client.Client {
	return fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).Build()
}

func TestReconcileStampsProviderID(t *testing.T) {
	kubeClient := newClient(
		node("worker-0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""),
		managedNodeGroup("ng1"),
	)
	if err := reconcileNode(t, kubeClient, "worker-0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "worker-0"); got != "clevercloud://ng1" {
		t.Errorf("expected provider id %q, got %q", "clevercloud://ng1", got)
	}
}

func TestReconcileSkipsNodeWithExistingProviderID(t *testing.T) {
	kubeClient := newClient(
		node("worker-0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, "other://x"),
		managedNodeGroup("ng1"),
	)
	if err := reconcileNode(t, kubeClient, "worker-0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "worker-0"); got != "other://x" {
		t.Errorf("expected provider id to stay %q, got %q", "other://x", got)
	}
}

func TestReconcileSkipsNodeWithoutNodeGroupLabel(t *testing.T) {
	kubeClient := newClient(node("worker-0", nil, ""))
	if err := reconcileNode(t, kubeClient, "worker-0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "worker-0"); got != "" {
		t.Errorf("expected provider id to stay empty, got %q", got)
	}
}

func TestReconcileSkipsUnmanagedNodeGroup(t *testing.T) {
	kubeClient := newClient(
		node("worker-0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""),
		unmanagedNodeGroup("ng1"),
	)
	if err := reconcileNode(t, kubeClient, "worker-0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "worker-0"); got != "" {
		t.Errorf("expected provider id to stay empty for unmanaged nodegroup, got %q", got)
	}
}

func TestReconcileSkipsMissingNodeGroup(t *testing.T) {
	kubeClient := newClient(node("worker-0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "absent"}, ""))
	if err := reconcileNode(t, kubeClient, "worker-0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "worker-0"); got != "" {
		t.Errorf("expected provider id to stay empty for missing nodegroup, got %q", got)
	}
}

func TestReconcileMissingNodeNoError(t *testing.T) {
	kubeClient := newClient()
	if err := reconcileNode(t, kubeClient, "ghost"); err != nil {
		t.Fatalf("expected nil error for missing node, got %v", err)
	}
}

// TestReconcileStampsWhenScaledToZero pins the boundary the guard deliberately
// does NOT cover. A group scaled down to zero can still have its last node
// draining, and that node needs its provider ID for karpenter to match it to
// its NodeClaim and terminate it. Only two or more nodes can collide on one ID.
func TestReconcileStampsWhenScaledToZero(t *testing.T) {
	ng := managedNodeGroup("ng1")
	ng.Spec.NodeCount = 0
	kubeClient := newClient(node("ng1-node0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""), ng)
	if err := reconcileNode(t, kubeClient, "ng1-node0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "ng1-node0"); got != "clevercloud://ng1" {
		t.Errorf("a draining node of a scaled-to-zero group must still be stamped, got %q: "+
			"without the provider id karpenter cannot match it to its NodeClaim to terminate it", got)
	}
}

// TestReconcileRefusesWhenASiblingAlreadyOwnsTheProviderID covers the recovery
// path this guard itself recommends. Reverting a resize sets spec.nodeCount
// back to 1 immediately, but the extra Node object outlives the revert by
// ~40 s while Clever Cloud tears its VM down. Any event on that node during the
// window — cordon, drain, kubelet heartbeat — passes a spec.nodeCount check and
// re-creates the very MultipleNodesFound collision the guard exists to prevent.
//
// Desired state is not the invariant; the Nodes that actually exist are.
func TestReconcileRefusesWhenASiblingAlreadyOwnsTheProviderID(t *testing.T) {
	ng := managedNodeGroup("ng1")
	ng.Spec.NodeCount = 1 // the resize has already been reverted
	kubeClient := newClient(
		node("ng1-node0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, "clevercloud://ng1"),
		// The extra node is still around, draining, and still unstamped.
		node("ng1-node1", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""),
		ng,
	)
	if err := reconcileNode(t, kubeClient, "ng1-node1"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "ng1-node1"); got != "" {
		t.Errorf("ng1-node1 must not be stamped while ng1-node0 already owns %q, got %q: "+
			"two nodes on one provider id is the terminal MultipleNodesFound collision", "clevercloud://ng1", got)
	}
}

// TestReconcileStampsExactlyOneNodeOfAResizedGroup pins the behaviour that
// makes the guard non-destructive. Refusing every node of a resized group would
// leave the NodeClaim unregistered until karpenter's 15-minute liveness TTL
// deleted it — taking the NodeGroup and both VMs, which is the outcome the
// guard is supposed to prevent. Exactly one node must be stamped so the claim
// registers; the extra one is left inert and surfaced by the GC.
func TestReconcileStampsExactlyOneNodeOfAResizedGroup(t *testing.T) {
	ng := managedNodeGroup("ng1")
	ng.Spec.NodeCount = 2
	kubeClient := newClient(
		node("ng1-node0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""),
		node("ng1-node1", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""),
		ng,
	)
	stamped := 0
	for _, name := range []string{"ng1-node0", "ng1-node1"} {
		if err := reconcileNode(t, kubeClient, name); err != nil {
			t.Fatalf("Reconcile %s: %v", name, err)
		}
		if getProviderID(t, kubeClient, name) == "clevercloud://ng1" {
			stamped++
		}
	}
	if stamped != 1 {
		t.Errorf("expected exactly one node of the resized group to be stamped, got %d: "+
			"zero wedges registration until liveness deletes the claim, two is the collision", stamped)
	}
}

// TestReconcileStampsAgainOnceTheExtraNodeIsGone proves the refusal is not
// sticky: once Clever Cloud has torn the extra node down, a node of that group
// is stamped normally again.
func TestReconcileStampsAgainOnceTheExtraNodeIsGone(t *testing.T) {
	ng := managedNodeGroup("ng1")
	ng.Spec.NodeCount = 2
	owner := node("ng1-node0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, "clevercloud://ng1")
	kubeClient := newClient(owner, node("ng1-node1", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""), ng)

	if err := reconcileNode(t, kubeClient, "ng1-node1"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "ng1-node1"); got != "" {
		t.Fatalf("expected no provider id while a sibling owns it, got %q", got)
	}

	// The platform removes the node that held the ID; the survivor may now take it.
	if err := kubeClient.Delete(context.Background(), owner); err != nil {
		t.Fatalf("deleting the owning node: %v", err)
	}
	if err := reconcileNode(t, kubeClient, "ng1-node1"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "ng1-node1"); got != "clevercloud://ng1" {
		t.Errorf("expected the surviving node to be stamped once the id is free, got %q", got)
	}
}

// TestReconcileConfirmsOwnershipUncached pins WHERE the uniqueness check reads
// from. Reconciles are serialised, so when the second node is processed the
// first node's patch is already committed to the API server — but it has not
// necessarily reached the informer cache yet. Reading the cache in that window
// tells both nodes the provider ID is free, and both get stamped: the terminal
// MultipleNodesFound collision, back through a few-millisecond door.
//
// The two clients below model exactly that skew: the cache still shows
// ng1-node0 unstamped, the API server already has it stamped.
func TestReconcileConfirmsOwnershipUncached(t *testing.T) {
	ng := managedNodeGroup("ng1")
	ng.Spec.NodeCount = 2
	stale := newClient(
		node("ng1-node0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""),
		node("ng1-node1", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""),
		ng,
	)
	fresh := newClient(
		node("ng1-node0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, "clevercloud://ng1"),
		node("ng1-node1", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""),
		ng,
	)

	c := providerid.NewController(stale, fresh)
	if _, err := c.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "ng1-node1"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, stale, "ng1-node1"); got != "" {
		t.Errorf("ng1-node1 was stamped %q off a stale cache while the API server already shows "+
			"ng1-node0 owning that id: the ownership check must read uncached", got)
	}
}
