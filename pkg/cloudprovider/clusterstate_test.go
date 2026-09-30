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

// These tests run karpenter-core's own cluster state (state.Cluster, fed by its
// node informer) over what this provider and the platform actually put in the
// API server. Provisioning, disruption and the NodePool counters are all gated
// on Cluster.Synced(), and its FIRST sync after a controller start requires
// every Node in the API server to be tracked — while UpdateNode skips a node
// that carries karpenter.sh/nodepool without a provider ID. One such node
// freezes karpenter cluster-wide from the next restart on.

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/providerid"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// newClusterStateProvider builds the provider over a fake client that also
// carries the pod index karpenter-core's cluster state reads through.
func newClusterStateProvider(t *testing.T) (*cloudprovider.CloudProvider, client.Client) {
	t.Helper()
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(readyNodeClass("default")).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		Build()
	itp := instancetype.NewProvider("par", nil, nil)
	cp := cloudprovider.New(kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{}))
	return cp, kubeClient
}

// launchNodeClaim runs a real Create for a NodeClaim labelled the way
// karpenter-core labels it, and records the provider ID on its status as the
// launch controller does.
func launchNodeClaim(t *testing.T, cp *cloudprovider.CloudProvider, kubeClient client.Client, name string) (*karpv1.NodeClaim, *ngv1.NodeGroup) {
	t.Helper()
	ctx := context.Background()
	nodeClaim := testNodeClaim(name)
	nodeClaim.Labels[karpv1.NodeClassLabelKey(nodeClaim.Spec.NodeClassRef.GroupKind())] = "default"
	if err := kubeClient.Create(ctx, nodeClaim); err != nil {
		t.Fatalf("creating nodeclaim: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: name}, &ngv1.NodeGroup{}); err == nil {
				markSynced(t, kubeClient, name)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	launched, err := cp.Create(ctx, nodeClaim)
	<-done
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: name}, nodeClaim); err != nil {
		t.Fatalf("getting nodeclaim: %v", err)
	}
	for k, v := range launched.Labels {
		nodeClaim.Labels[k] = v
	}
	nodeClaim.Status.ProviderID = launched.Status.ProviderID
	if err := kubeClient.Update(ctx, nodeClaim); err != nil {
		t.Fatalf("recording the provider id on the nodeclaim: %v", err)
	}
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: name}, ng); err != nil {
		t.Fatalf("getting nodegroup: %v", err)
	}
	return nodeClaim, ng
}

// joinNode creates node <group>-node<index> the way the platform registers
// it: the group's spec.labels and spec.taints, the platform's own labels, and
// no provider ID.
func joinNode(t *testing.T, kubeClient client.Client, ng *ngv1.NodeGroup, index string) *corev1.Node {
	t.Helper()
	name := ng.Name + "-node" + index
	labels := map[string]string{
		v1alpha1.NodeGroupNodeLabelKey: ng.Name,
		v1alpha1.FlavorLabelKey:        ng.Spec.Flavor,
		v1alpha1.NodeRoleLabelKey:      v1alpha1.NodeRoleWorker,
		corev1.LabelHostname:           name,
	}
	for k, v := range ng.Spec.Labels {
		labels[k] = v
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
	for _, taint := range ng.Spec.Taints {
		node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{Key: taint.Key, Value: taint.Value, Effect: taint.Effect})
	}
	if err := kubeClient.Create(context.Background(), node); err != nil {
		t.Fatalf("creating node %s: %v", name, err)
	}
	return node
}

// stampNode runs the provider's providerid controller on a node.
func stampNode(t *testing.T, kubeClient client.Client, name string) {
	t.Helper()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: name}}
	if _, err := providerid.NewController(kubeClient, kubeClient).Reconcile(context.Background(), req); err != nil {
		t.Fatalf("providerid reconcile %s: %v", name, err)
	}
}

// registerNode does what karpenter-core's registration sync does to the node
// of a NodeClaim: copies the NodeClaim's labels onto it, marks it registered
// and initialized, and lifts the unregistered taint.
func registerNode(t *testing.T, kubeClient client.Client, nodeClaim *karpv1.NodeClaim, nodeName string) {
	t.Helper()
	ctx := context.Background()
	node := &corev1.Node{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		t.Fatalf("getting node: %v", err)
	}
	if node.Spec.ProviderID != nodeClaim.Status.ProviderID {
		t.Fatalf("node %s carries provider id %q, want %q: the providerid controller did not stamp it",
			nodeName, node.Spec.ProviderID, nodeClaim.Status.ProviderID)
	}
	for k, v := range nodeClaim.Labels {
		node.Labels[k] = v
	}
	node.Labels[karpv1.NodeRegisteredLabelKey] = "true"
	node.Labels[karpv1.NodeInitializedLabelKey] = "true"
	var taints []corev1.Taint
	for _, taint := range node.Spec.Taints {
		if taint.Key != karpv1.UnregisteredTaintKey {
			taints = append(taints, taint)
		}
	}
	node.Spec.Taints = taints
	if err := kubeClient.Update(ctx, node); err != nil {
		t.Fatalf("registering node: %v", err)
	}
	nodeClaim.Status.NodeName = nodeName
	if err := kubeClient.Update(ctx, nodeClaim); err != nil {
		t.Fatalf("recording the node name on the nodeclaim: %v", err)
	}
}

// resize changes the group's nodeCount the way something outside karpenter
// does (the platform's scaler, a human).
func resize(t *testing.T, kubeClient client.Client, ng *ngv1.NodeGroup, count int32) {
	t.Helper()
	ng.Spec.NodeCount = count
	if err := kubeClient.Update(context.Background(), ng); err != nil {
		t.Fatalf("resizing nodegroup: %v", err)
	}
}

// restartedClusterState is karpenter-core's cluster state as a freshly
// started controller builds it: a new state.Cluster hydrated by core's node
// informer and the NodeClaims in the API server. It reports Synced(), polled a
// few times as the provisioner and disruption loops do.
func restartedClusterState(t *testing.T, cp *cloudprovider.CloudProvider, kubeClient client.Client) bool {
	t.Helper()
	ctx := context.Background()
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	nodeInformer := informer.NewNodeController(kubeClient, cluster)
	nodes := &corev1.NodeList{}
	if err := kubeClient.List(ctx, nodes); err != nil {
		t.Fatalf("listing nodes: %v", err)
	}
	for i := range nodes.Items {
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: nodes.Items[i].Name}}
		if _, err := nodeInformer.Reconcile(ctx, req); err != nil {
			t.Fatalf("state node reconcile %s: %v", nodes.Items[i].Name, err)
		}
	}
	claims := &karpv1.NodeClaimList{}
	if err := kubeClient.List(ctx, claims); err != nil {
		t.Fatalf("listing nodeclaims: %v", err)
	}
	for i := range claims.Items {
		cluster.UpdateNodeClaim(&claims.Items[i])
	}
	synced := false
	for range 3 {
		synced = cluster.Synced(ctx)
	}
	return synced
}

// TestResizedNodeGroupDoesNotWedgeClusterStateAfterRestart is the regression
// test for a freeze reproduced live: a managed group resized to 2 from outside
// karpenter, then a controller restart. The extra node is deliberately left
// without a provider ID by the providerid controller; when the group's
// spec.labels carried karpenter.sh/nodepool, that node carried it too, core's
// first sync never completed, and no NodeClaim was created for 4 minutes —
// until the resize was reverted.
func TestResizedNodeGroupDoesNotWedgeClusterStateAfterRestart(t *testing.T) {
	cp, kubeClient := newClusterStateProvider(t)
	nodeClaim, ng := launchNodeClaim(t, cp, kubeClient, "default-rs7k2")
	joinNode(t, kubeClient, ng, "0")
	stampNode(t, kubeClient, ng.Name+"-node0")
	registerNode(t, kubeClient, nodeClaim, ng.Name+"-node0")
	if !restartedClusterState(t, cp, kubeClient) {
		t.Fatal("control: a healthy 1:1 nodegroup must let karpenter-core's cluster state sync")
	}

	resize(t, kubeClient, ng, 2)
	extra := joinNode(t, kubeClient, ng, "1")
	// Harmless from the moment it joins, before the providerid controller has
	// looked at it: a restart can land in between.
	if !restartedClusterState(t, cp, kubeClient) {
		t.Errorf("karpenter-core's cluster state never syncs after a restart while the extra node of a resized "+
			"group exists (it joined with %v): provisioning and disruption stay stopped cluster-wide", extra.Labels)
	}

	stampNode(t, kubeClient, extra.Name)
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: extra.Name}, extra); err != nil {
		t.Fatalf("getting the extra node: %v", err)
	}
	if extra.Spec.ProviderID != "" {
		t.Fatalf("the extra node was stamped %q: two nodes on one provider id", extra.Spec.ProviderID)
	}
	if !restartedClusterState(t, cp, kubeClient) {
		t.Error("karpenter-core's cluster state stops syncing once the extra node has been refused a provider id")
	}
}

// TestLegacyNodeGroupDoesNotWedgeClusterStateAfterRestart covers groups
// created before the payload filter, whose immutable spec.labels still carry
// karpenter.sh/nodepool. The extra node joins with it; the providerid
// controller, refusing it a provider ID, strips it. The first assertion proves
// the harness reproduces the freeze, so the second one is meaningful.
func TestLegacyNodeGroupDoesNotWedgeClusterStateAfterRestart(t *testing.T) {
	cp, kubeClient := newClusterStateProvider(t)
	nodeClaim, ng := launchNodeClaim(t, cp, kubeClient, "default-l3gcy")
	// What an earlier version put in the payload.
	if ng.Spec.Labels == nil {
		ng.Spec.Labels = map[string]string{}
	}
	ng.Spec.Labels[karpv1.NodePoolLabelKey] = nodeClaim.Labels[karpv1.NodePoolLabelKey]
	if err := kubeClient.Update(context.Background(), ng); err != nil {
		t.Fatalf("seeding the legacy payload: %v", err)
	}
	joinNode(t, kubeClient, ng, "0")
	stampNode(t, kubeClient, ng.Name+"-node0")
	registerNode(t, kubeClient, nodeClaim, ng.Name+"-node0")

	resize(t, kubeClient, ng, 2)
	joinNode(t, kubeClient, ng, "1")
	if restartedClusterState(t, cp, kubeClient) {
		t.Fatal("harness check: a node carrying karpenter.sh/nodepool without a provider id must keep " +
			"karpenter-core's first sync false — otherwise this test proves nothing")
	}

	stampNode(t, kubeClient, ng.Name+"-node1")
	if !restartedClusterState(t, cp, kubeClient) {
		t.Error("karpenter-core's cluster state still never syncs after a restart: the providerid controller " +
			"must strip karpenter.sh/nodepool from the extra node it refuses to stamp")
	}
}

// TestNodeJoiningWithoutNodePoolLabelIsTrackedOnce covers the join shape the
// payload filter introduces: the node now joins with neither
// karpenter.sh/nodepool nor a provider ID, so core's node informer — which
// fires on the same create event as the providerid controller — can track it
// under its name first, then again under the provider ID once stamped. Core
// re-keys a node whose provider ID changes; this pins that one machine ends up
// as exactly one managed state node, not a ghost plus the real one.
func TestNodeJoiningWithoutNodePoolLabelIsTrackedOnce(t *testing.T) {
	cp, kubeClient := newClusterStateProvider(t)
	nodeClaim, ng := launchNodeClaim(t, cp, kubeClient, "default-gh0st")

	ctx := context.Background()
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	nodeInformer := informer.NewNodeController(kubeClient, cluster)
	cluster.UpdateNodeClaim(nodeClaim)
	node := joinNode(t, kubeClient, ng, "0")
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: node.Name}}
	observe := func() {
		t.Helper()
		if _, err := nodeInformer.Reconcile(ctx, req); err != nil {
			t.Fatalf("state node reconcile: %v", err)
		}
	}

	observe() // core sees the node before it is stamped
	stampNode(t, kubeClient, node.Name)
	observe()
	registerNode(t, kubeClient, nodeClaim, node.Name)
	cluster.UpdateNodeClaim(nodeClaim)
	observe()

	nodes := cluster.DeepCopyNodes()
	if len(nodes) != 1 {
		ids := make([]string, 0, len(nodes))
		for _, n := range nodes {
			ids = append(ids, n.ProviderID())
		}
		t.Fatalf("karpenter-core tracks %d state nodes for one machine: %v", len(nodes), ids)
	}
	if !nodes[0].Managed() || nodes[0].ProviderID() != nodeClaim.Status.ProviderID {
		t.Errorf("the state node is not the managed node of %s: provider id %q, managed %v",
			nodeClaim.Name, nodes[0].ProviderID(), nodes[0].Managed())
	}
}
