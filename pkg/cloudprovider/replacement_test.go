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

// These tests replay what Clever Cloud does to a node during a Kubernetes
// upgrade of the cluster, or when it repairs a NotReady worker: it adds
// <nodegroup>-node1 to the NodeGroup, cordons and drains <nodegroup>-node0,
// then deletes that Node object. Karpenter-core's own NodeClaim lifecycle,
// node termination, disruption and cluster state run over the result, with
// the provider's providerid and nodereplacement controllers in between, as in
// the controller binary.
//
// Measured live on an upgrade from 1.35 to 1.36 before the nodereplacement
// controller existed (docs/E2E-RESULTS.md): the deletion of node0 went through
// core's termination finalizer, which deleted the NodeClaim and the NodeGroup
// with the replacement node in it, and the platform refused every new
// NodeGroup until the upgrade ended.

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/nodereplacement"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/providerid"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// replacementEnv is one cluster with karpenter-core's controllers and the
// provider's, each reconciled by hand in the order the test tells.
type replacementEnv struct {
	t          *testing.T
	ctx        context.Context
	kubeClient client.Client
	clk        *clocktesting.FakeClock
	cp         *cloudprovider.CloudProvider
	claims     *lifecycle.Controller
	nodes      *termination.Controller
	stamps     *providerid.Controller
	replace    *nodereplacement.Controller
}

func newReplacementEnv(t *testing.T) *replacementEnv {
	t.Helper()
	kubeClient := newCoreIndexedClient(readyNodeClass("default"))
	// Core's lifecycle sleeps a second after each patch to read its own
	// write; the fake clock turns that into a step.
	clk := clocktesting.NewFakeClock(time.Now())
	itp := instancetype.NewProvider("par", nil, nil)
	cp := cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clk))
	return &replacementEnv{
		t: t, ctx: coreContext(), kubeClient: kubeClient, clk: clk, cp: cp,
		claims:  lifecycle.NewController(clk, kubeClient, cp, noopRecorder{}, state.NewCluster(clk, kubeClient, cp), nil, nil),
		nodes:   termination.NewController(clk, kubeClient, cp, terminator.NewTerminator(clk, kubeClient, terminator.NewQueue(clk, kubeClient, noopRecorder{}), noopRecorder{}), noopRecorder{}),
		stamps:  providerid.NewController(kubeClient, kubeClient),
		replace: nodereplacement.NewController(kubeClient, kubeClient, noopRecorder{}, clk),
	}
}

// launch creates a NodeClaim and lets karpenter-core launch it through the
// provider's real Create, the fake operator accepting the group meanwhile.
func (e *replacementEnv) launch(name string) {
	e.t.Helper()
	nodeClaim := testNodeClaim(name)
	nodeClaim.Labels[karpv1.NodeClassLabelKey(nodeClaim.Spec.NodeClassRef.GroupKind())] = "default"
	// Core dates a NodeClaim's first conditions from its creation, and its
	// registration timeout counts from there: the fake API server sets none.
	nodeClaim.CreationTimestamp = metav1.NewTime(e.clk.Now())
	if err := e.kubeClient.Create(e.ctx, nodeClaim); err != nil {
		e.t.Fatalf("creating nodeclaim: %v", err)
	}
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			if err := e.kubeClient.Get(e.ctx, types.NamespacedName{Name: name}, &ngv1.NodeGroup{}); err == nil {
				markSynced(e.t, e.kubeClient, name)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	e.coreLifecycle(name)
	<-accepted
	if got := e.claim(name); !got.StatusConditions().Get(karpv1.ConditionTypeLaunched).IsTrue() || got.Status.ProviderID != nodegroup.ProviderID(name) {
		e.t.Fatalf("karpenter-core did not launch %s: provider id %q, conditions %+v", name, got.Status.ProviderID, got.Status.Conditions)
	}
}

// platformAddsNode registers node <group>-node<index> the way the platform
// does, Ready and with its resources reported, as a booted worker is.
func (e *replacementEnv) platformAddsNode(group, index string) string {
	e.t.Helper()
	ng := &ngv1.NodeGroup{}
	if err := e.kubeClient.Get(e.ctx, types.NamespacedName{Name: group}, ng); err != nil {
		e.t.Fatalf("getting nodegroup: %v", err)
	}
	node := joinNodeAt(e.t, e.kubeClient, ng, index, e.clk.Now())
	node.Status = corev1.NodeStatus{
		Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("3715344Ki"), corev1.ResourcePods: resource.MustParse("110"),
		},
		Allocatable: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("3612944Ki"), corev1.ResourcePods: resource.MustParse("110"),
		},
	}
	if err := e.kubeClient.Status().Update(e.ctx, node); err != nil {
		e.t.Fatalf("reporting the status of %s: %v", node.Name, err)
	}
	return node.Name
}

// platformDrains cordons a node and labels it the way the platform does, in
// one write, when it starts draining the node it replaces.
func (e *replacementEnv) platformDrains(name string) {
	e.t.Helper()
	node := e.node(name)
	node.Spec.Unschedulable = true
	node.Labels["clever-cloud.com/draining"] = "true"
	if err := e.kubeClient.Update(e.ctx, node); err != nil {
		e.t.Fatalf("cordoning node %s: %v", name, err)
	}
}

// platformDeletesNode deletes a Node object the way the platform does once it
// has drained it. It returns whether the object is gone: a finalizer keeps it.
func (e *replacementEnv) platformDeletesNode(name string) bool {
	e.t.Helper()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := e.kubeClient.Delete(e.ctx, node); err != nil {
		e.t.Fatalf("deleting node %s: %v", name, err)
	}
	return apierrors.IsNotFound(e.kubeClient.Get(e.ctx, types.NamespacedName{Name: name}, node))
}

func (e *replacementEnv) stamp(node string) {
	e.t.Helper()
	if _, err := e.stamps.Reconcile(e.ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: node}}); err != nil {
		e.t.Fatalf("providerid reconcile %s: %v", node, err)
	}
}

func (e *replacementEnv) replacement(group string) {
	e.t.Helper()
	if _, err := e.replace.Reconcile(e.ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: group}}); err != nil {
		e.t.Fatalf("nodereplacement reconcile %s: %v", group, err)
	}
}

// coreLifecycle runs karpenter-core's NodeClaim lifecycle (launch,
// registration, initialization, liveness) a few times, as its watches do.
func (e *replacementEnv) coreLifecycle(name string) {
	e.t.Helper()
	for range 3 {
		nodeClaim := &karpv1.NodeClaim{}
		if err := e.kubeClient.Get(e.ctx, types.NamespacedName{Name: name}, nodeClaim); err != nil {
			if apierrors.IsNotFound(err) {
				return
			}
			e.t.Fatalf("getting nodeclaim: %v", err)
		}
		if _, err := e.claims.Reconcile(e.ctx, nodeClaim); err != nil {
			e.t.Fatalf("core nodeclaim lifecycle: %v", err)
		}
	}
}

// coreTermination runs karpenter-core's node termination on a node.
func (e *replacementEnv) coreTermination(name string) {
	e.t.Helper()
	node := &corev1.Node{}
	if err := e.kubeClient.Get(e.ctx, types.NamespacedName{Name: name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		e.t.Fatalf("getting node: %v", err)
	}
	if _, err := e.nodes.Reconcile(e.ctx, node); err != nil {
		e.t.Fatalf("core node termination: %v", err)
	}
}

func (e *replacementEnv) claim(name string) *karpv1.NodeClaim {
	e.t.Helper()
	nodeClaim := &karpv1.NodeClaim{}
	if err := e.kubeClient.Get(e.ctx, types.NamespacedName{Name: name}, nodeClaim); err != nil {
		e.t.Fatalf("getting nodeclaim %s: %v", name, err)
	}
	return nodeClaim
}

func (e *replacementEnv) node(name string) *corev1.Node {
	e.t.Helper()
	node := &corev1.Node{}
	if err := e.kubeClient.Get(e.ctx, types.NamespacedName{Name: name}, node); err != nil {
		e.t.Fatalf("getting node %s: %v", name, err)
	}
	return node
}

// registered launches a NodeClaim and brings its node0 up to a registered,
// initialized karpenter node, every step run by the controller that owns it.
func (e *replacementEnv) registered(name string) string {
	e.t.Helper()
	e.launch(name)
	node0 := e.platformAddsNode(name, "0")
	e.stamp(node0)
	e.coreLifecycle(name)
	e.requireKarpenterNode(name, node0)
	return node0
}

// requireKarpenterNode fails unless node is the registered, initialized node
// of the NodeClaim: what a pod needs to schedule on it, and what core needs
// to drain it and count it.
func (e *replacementEnv) requireKarpenterNode(claimName, nodeName string) {
	e.t.Helper()
	nodeClaim, node := e.claim(claimName), e.node(nodeName)
	if !nodeClaim.DeletionTimestamp.IsZero() {
		e.t.Fatalf("nodeclaim %s is being deleted", claimName)
	}
	for _, condition := range []string{karpv1.ConditionTypeLaunched, karpv1.ConditionTypeRegistered, karpv1.ConditionTypeInitialized} {
		if !nodeClaim.StatusConditions().Get(condition).IsTrue() {
			e.t.Errorf("nodeclaim %s: %s is not True: %+v", claimName, condition, nodeClaim.StatusConditions().Get(condition))
		}
	}
	if nodeClaim.Status.NodeName != nodeName {
		e.t.Errorf("nodeclaim %s names node %q, want %q", claimName, nodeClaim.Status.NodeName, nodeName)
	}
	if node.Spec.ProviderID != nodeClaim.Status.ProviderID {
		e.t.Errorf("node %s carries provider id %q, want %q", nodeName, node.Spec.ProviderID, nodeClaim.Status.ProviderID)
	}
	for _, taint := range node.Spec.Taints {
		if taint.Key == karpv1.UnregisteredTaintKey {
			e.t.Errorf("node %s still carries the %s taint: no pod can schedule on it", nodeName, karpv1.UnregisteredTaintKey)
		}
	}
	for _, label := range []string{karpv1.NodePoolLabelKey, karpv1.NodeRegisteredLabelKey, karpv1.NodeInitializedLabelKey, corev1.LabelInstanceTypeStable, corev1.LabelTopologyZone} {
		if node.Labels[label] == "" {
			e.t.Errorf("node %s lacks the %s label", nodeName, label)
		}
	}
	if !controllerutil.ContainsFinalizer(node, karpv1.TerminationFinalizer) {
		e.t.Errorf("node %s lacks karpenter's termination finalizer", nodeName)
	}
}

func (e *replacementEnv) requireNodeGroupKept(name string) {
	e.t.Helper()
	ng := &ngv1.NodeGroup{}
	if err := e.kubeClient.Get(e.ctx, types.NamespacedName{Name: name}, ng); err != nil {
		e.t.Fatalf("the nodegroup %s is gone: %v", name, err)
	}
	if !ng.DeletionTimestamp.IsZero() {
		e.t.Fatalf("the nodegroup %s is being deleted", name)
	}
}

// TestPlatformDeletionOfARegisteredNodeDeletesItsNodeClaim pins what
// karpenter-core does with the platform's deletion of a node that still
// carries its termination finalizer: it deletes the NodeClaim, hence the
// NodeGroup and every node in it. This is why the nodereplacement controller
// removes that finalizer before the platform gets there, and the fallback
// when it did not (the controller was down while the node was replaced).
func TestPlatformDeletionOfARegisteredNodeDeletesItsNodeClaim(t *testing.T) {
	e := newReplacementEnv(t)
	node0 := e.registered("default-kept")
	node1 := e.platformAddsNode("default-kept", "1")
	e.stamp(node1)

	if e.platformDeletesNode(node0) {
		t.Fatal("the node is gone at once: karpenter-core's termination finalizer no longer holds it")
	}
	e.coreTermination(node0)
	if e.claim("default-kept").DeletionTimestamp.IsZero() {
		t.Fatal("karpenter-core left the nodeclaim alone: the release this provider performs would no longer be needed")
	}
}

// TestPlatformReplacementKeepsTheNodeClaimAndItsNodeGroup is the upgrade as
// measured: node1 joins the group of a registered NodeClaim, node0 is deleted
// by the platform, and node1 must end up as the NodeClaim's node — labelled,
// untainted, finalized by core — with the NodeClaim and the NodeGroup intact.
// The stamp of node1 and the reopening of the registration are triggered by
// the same deletion, so both orders are run.
func TestPlatformReplacementKeepsTheNodeClaimAndItsNodeGroup(t *testing.T) {
	for _, stampFirst := range []bool{true, false} {
		name := map[bool]string{true: "stamp then adoption", false: "adoption then stamp"}[stampFirst]
		t.Run(name, func(t *testing.T) {
			e := newReplacementEnv(t)
			const group = "default-upgrade"
			node0 := e.registered(group)
			uid := e.claim(group).UID

			// The platform adds the replacement. It is refused the provider ID
			// node0 holds, and node0 keeps core's finalizer: nobody drains it
			// yet.
			node1 := e.platformAddsNode(group, "1")
			e.stamp(node1)
			e.replacement(group)
			if got := e.node(node1).Spec.ProviderID; got != "" {
				t.Fatalf("the replacement node carries provider id %q next to %s: two nodes on one provider id", got, node0)
			}
			if !controllerutil.ContainsFinalizer(e.node(node0), karpv1.TerminationFinalizer) {
				t.Fatal("node0 lost karpenter's termination finalizer before the platform started draining it")
			}

			// The platform cordons node0 and drains it: node0 is released.
			e.platformDrains(node0)
			e.replacement(group)
			if controllerutil.ContainsFinalizer(e.node(node0), karpv1.TerminationFinalizer) {
				t.Fatal("node0 kept karpenter's termination finalizer although the platform is draining it")
			}
			e.coreLifecycle(group)
			e.coreTermination(node0)
			e.requireNodeGroupKept(group)

			// The platform deletes node0 once drained.
			if !e.platformDeletesNode(node0) {
				t.Fatal("node0 survived its deletion by the platform")
			}
			if stampFirst {
				e.stamp(node1)
				e.replacement(group)
			} else {
				e.replacement(group)
				e.stamp(node1)
			}
			if got := e.claim(group); got.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue() {
				t.Fatalf("the registration of the nodeclaim was not reopened: %+v", got.Status.Conditions)
			}
			e.coreLifecycle(group)

			e.requireKarpenterNode(group, node1)
			e.requireNodeGroupKept(group)
			if got := e.claim(group).UID; got != uid {
				t.Errorf("the nodeclaim was recreated: uid %s, was %s", got, uid)
			}
			// Nothing left to do: a second pass must not reopen anything.
			e.replacement(group)
			e.coreLifecycle(group)
			e.requireKarpenterNode(group, node1)

			// Core's cluster state, as a restarted controller builds it, tracks
			// one managed node for the group and syncs.
			cluster := state.NewCluster(e.clk, e.kubeClient, e.cp)
			nodeInformer := informer.NewNodeController(e.kubeClient, cluster)
			if _, err := nodeInformer.Reconcile(e.ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: node1}}); err != nil {
				t.Fatalf("state node reconcile: %v", err)
			}
			cluster.UpdateNodeClaim(e.claim(group))
			if !cluster.Synced(e.ctx) {
				t.Error("karpenter-core's cluster state does not sync after the replacement")
			}
			if nodes := cluster.DeepCopyNodes(); len(nodes) != 1 || !nodes[0].Managed() || !nodes[0].Initialized() || nodes[0].Node.Name != node1 {
				t.Errorf("karpenter-core does not track %s as the one initialized node of the nodeclaim: %d state nodes", node1, len(nodes))
			}
		})
	}
}

// TestNodeLostWithoutReplacementFallsBackToTheRegistrationTimeout covers the
// release that leads nowhere: the platform deleted the registered node, then
// the replacement went away too. The NodeClaim must not stay Registered on a
// node that no longer exists, holding capacity the scheduler counts on:
// its registration is reopened, and karpenter-core's own registration timeout
// deletes it when no node joins — counted from the reopening, not from the
// launch of a NodeClaim that has been running for a day.
func TestNodeLostWithoutReplacementFallsBackToTheRegistrationTimeout(t *testing.T) {
	e := newReplacementEnv(t)
	const group = "default-lost"
	node0 := e.registered(group)
	e.clk.Step(24 * time.Hour)
	node1 := e.platformAddsNode(group, "1")
	e.platformDrains(node0)
	e.replacement(group)
	if !e.platformDeletesNode(node0) || !e.platformDeletesNode(node1) {
		t.Fatal("the nodes survived their deletion")
	}
	e.replacement(group)
	e.coreLifecycle(group)
	nodeClaim := e.claim(group)
	if nodeClaim.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue() || !nodeClaim.DeletionTimestamp.IsZero() {
		t.Fatalf("want the nodeclaim waiting for a node, not deleted yet: %+v", nodeClaim.Status.Conditions)
	}
	e.clk.Step(14 * time.Minute)
	e.coreLifecycle(group)
	if nodeClaim = e.claim(group); !nodeClaim.DeletionTimestamp.IsZero() {
		t.Fatal("the nodeclaim was deleted before karpenter-core's registration timeout: a replacement that boots in that time would have been adopted")
	}

	e.clk.Step(2 * time.Minute)
	e.coreLifecycle(group)
	nodeClaim = &karpv1.NodeClaim{}
	err := e.kubeClient.Get(e.ctx, types.NamespacedName{Name: group}, nodeClaim)
	if err == nil && nodeClaim.DeletionTimestamp.IsZero() {
		t.Fatal("karpenter-core's registration timeout did not delete a nodeclaim no node registered for")
	}
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

// TestAReplacementBornWithTheRegisteredLabelIsAdopted: v0.12.0 let the
// karpenter.sh domain into a NodeGroup's immutable spec.labels, which the
// platform applies to every node of the group. With karpenter.sh/registered
// there, the replacement carries the label core's registration writes before
// core ever saw it. It must still end up as the NodeClaim's node, whichever of
// the stamp and the adoption comes first.
func TestAReplacementBornWithTheRegisteredLabelIsAdopted(t *testing.T) {
	e := newReplacementEnv(t)
	const group = "default-legacy"
	e.launch(group)
	ng := &ngv1.NodeGroup{}
	if err := e.kubeClient.Get(e.ctx, types.NamespacedName{Name: group}, ng); err != nil {
		t.Fatal(err)
	}
	ng.Spec.Labels[karpv1.NodeRegisteredLabelKey] = "true"
	if err := e.kubeClient.Update(e.ctx, ng); err != nil {
		t.Fatal(err)
	}
	node0 := e.platformAddsNode(group, "0")
	e.stamp(node0)
	e.coreLifecycle(group)
	e.requireKarpenterNode(group, node0)

	node1 := e.platformAddsNode(group, "1")
	e.platformDrains(node0)
	e.replacement(group)
	if !e.platformDeletesNode(node0) {
		t.Fatal("node0 survived its deletion by the platform")
	}
	e.stamp(node1)
	e.replacement(group)
	e.coreLifecycle(group)
	e.requireKarpenterNode(group, node1)
	e.requireNodeGroupKept(group)
}

// TestAReleaseLastsOnlyWhileThePlatformDrains pins both sides of the release.
// A released node is out of karpenter-core's hands: deleting its NodeClaim
// removes the Node object at once, pods and all, where core would have
// drained it. That is the platform's job while it drains the node, and
// nobody's once it stops — a drain a PodDisruptionBudget blocks until the
// platform gives up, a cluster its upgrade left FAILED, both nodes still in
// the group. So the release ends with its window, and core drains again.
func TestAReleaseLastsOnlyWhileThePlatformDrains(t *testing.T) {
	released := func(t *testing.T, group string) (*replacementEnv, string) {
		e := newReplacementEnv(t)
		node0 := e.registered(group)
		pod := pendingPod("web")
		pod.Spec.NodeName = node0
		pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web", UID: "rs", Controller: new(true)}}
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning}
		if err := e.kubeClient.Create(e.ctx, pod); err != nil {
			t.Fatal(err)
		}
		e.platformAddsNode(group, "1")
		e.platformDrains(node0)
		e.replacement(group)
		if controllerutil.ContainsFinalizer(e.node(node0), karpv1.TerminationFinalizer) {
			t.Fatal("node0 was not released")
		}
		return e, node0
	}
	deleteClaim := func(e *replacementEnv, group string) {
		e.t.Helper()
		if err := e.kubeClient.Delete(e.ctx, e.claim(group)); err != nil {
			e.t.Fatal(err)
		}
		e.replacement(group)
		e.coreLifecycle(group)
	}

	t.Run("during the drain, karpenter-core does not drain the node", func(t *testing.T) {
		const group = "default-draining"
		e, node0 := released(t, group)
		deleteClaim(e, group)
		if err := e.kubeClient.Get(e.ctx, types.NamespacedName{Name: node0}, &corev1.Node{}); !apierrors.IsNotFound(err) {
			t.Fatalf("node0 after the deletion of its nodeclaim: %v, want it gone at once — karpenter-core now drains a released node, and the release window can go", err)
		}
	})

	t.Run("past the window, karpenter-core drains it again", func(t *testing.T) {
		const group = "default-stalled"
		e, node0 := released(t, group)
		e.clk.Step(36 * time.Minute)
		e.replacement(group)
		if !controllerutil.ContainsFinalizer(e.node(node0), karpv1.TerminationFinalizer) {
			t.Fatal("node0 is still released 36 minutes after its replacement joined: nobody drains it any more")
		}
		deleteClaim(e, group)
		node := e.node(node0)
		if node.DeletionTimestamp.IsZero() {
			t.Fatal("karpenter-core did not delete the node of the deleted nodeclaim")
		}
		e.coreTermination(node0)
		node = e.node(node0)
		disrupted := false
		for i := range node.Spec.Taints {
			disrupted = disrupted || node.Spec.Taints[i].MatchTaint(&karpv1.DisruptedNoScheduleTaint)
		}
		if !disrupted {
			t.Errorf("karpenter-core's termination did not take the node: taints %v", node.Spec.Taints)
		}
		e.requireNodeGroupKept(group)
	})
}

// TestCoreEmptinessLeavesANodeThePlatformDrainsAlone runs karpenter-core's
// disruption over a node the platform is draining. The pods it evicted are
// terminating, which core does not count: the node reads as empty while they
// stop, and with consolidateAfter 0s core's emptiness deletes its NodeClaim
// 15 seconds later — the NodeGroup and the replacement node with it, while
// the platform refuses every creation. The release annotates the node
// karpenter.sh/do-not-disrupt, which is all that stands in the way.
func TestCoreEmptinessLeavesANodeThePlatformDrainsAlone(t *testing.T) {
	const group = "default-old"
	for name, shielded := range map[string]bool{
		"as the release leaves it":            true,
		"without karpenter.sh/do-not-disrupt": false,
	} {
		t.Run(name, func(t *testing.T) {
			start := time.Now().UTC().Truncate(time.Second)
			evicted := metav1.NewTime(start)
			pod := pendingPod("web")
			pod.Spec.NodeName = group + "-node0"
			pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web", UID: "rs", Controller: new(true)}}
			pod.Status = corev1.PodStatus{Phase: corev1.PodRunning}
			pod.DeletionTimestamp, pod.Finalizers = &evicted, []string{"test/still-stopping"}
			f := newRepairFixture(t, nil, "100%", []repairNode{
				{name: group, flavor: "XS", ready: corev1.ConditionTrue, since: start.Add(-time.Hour), consolidatable: true},
			}, pod)
			nodePool := &karpv1.NodePool{}
			if err := f.kubeClient.Get(f.ctx, types.NamespacedName{Name: "default"}, nodePool); err != nil {
				t.Fatal(err)
			}
			nodePool.Spec.Disruption.ConsolidationPolicy = karpv1.ConsolidationPolicyWhenEmptyOrUnderutilized
			nodePool.Spec.Disruption.ConsolidateAfter = karpv1.MustParseNillableDuration("0s")
			if err := f.kubeClient.Update(f.ctx, nodePool); err != nil {
				t.Fatal(err)
			}
			// The fixture's group and node, as the provider and the platform
			// make them.
			ng := &ngv1.NodeGroup{}
			if err := f.kubeClient.Get(f.ctx, types.NamespacedName{Name: group}, ng); err != nil {
				t.Fatal(err)
			}
			ng.Labels[v1alpha1.NodeClaimLabelKey] = group
			ng.OwnerReferences = []metav1.OwnerReference{{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: group, UID: types.UID("uid-" + group)}}
			ng.Spec.Taints = []ngv1.NodeGroupTaint{{Key: karpv1.UnregisteredTaintKey, Effect: corev1.TaintEffectNoExecute}}
			if err := f.kubeClient.Update(f.ctx, ng); err != nil {
				t.Fatal(err)
			}
			node1 := joinNodeAt(t, f.kubeClient, ng, "1", start)
			if err := f.cluster.UpdateNode(f.ctx, node1); err != nil {
				t.Fatal(err)
			}
			node0 := &corev1.Node{}
			if err := f.kubeClient.Get(f.ctx, types.NamespacedName{Name: group + "-node0"}, node0); err != nil {
				t.Fatal(err)
			}
			node0.Labels[v1alpha1.NodeGroupNodeLabelKey] = group
			node0.Labels["clever-cloud.com/draining"] = "true"
			node0.Spec.Unschedulable = true
			node0.Spec.Taints = append(node0.Spec.Taints, corev1.Taint{Key: corev1.TaintNodeUnschedulable, Effect: corev1.TaintEffectNoSchedule})
			if err := f.kubeClient.Update(f.ctx, node0); err != nil {
				t.Fatal(err)
			}

			// The release, by the controller itself.
			replace := nodereplacement.NewController(f.kubeClient, f.kubeClient, noopRecorder{}, f.clk)
			if _, err := replace.Reconcile(f.ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: group}}); err != nil {
				t.Fatal(err)
			}
			if err := f.kubeClient.Get(f.ctx, types.NamespacedName{Name: group + "-node0"}, node0); err != nil {
				t.Fatal(err)
			}
			if controllerutil.ContainsFinalizer(node0, karpv1.TerminationFinalizer) || node0.Annotations[karpv1.DoNotDisruptAnnotationKey] != "true" {
				t.Fatalf("the node the platform drains was not released: finalizers %v, annotations %v", node0.Finalizers, node0.Annotations)
			}
			if !shielded {
				delete(node0.Annotations, karpv1.DoNotDisruptAnnotationKey)
				if err := f.kubeClient.Update(f.ctx, node0); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.cluster.UpdateNode(f.ctx, node0); err != nil {
				t.Fatal(err)
			}
			f.clk.SetTime(start.Add(time.Second))

			// Emptiness waits out its validation period on the clock: step the
			// fake one while the pass waits.
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			passed := make(chan error, 1)
			go func() {
				_, err := f.controller.Reconcile(ctx)
				passed <- err
			}()
			for deadline := time.After(30 * time.Second); ; {
				select {
				case err := <-passed:
					if err != nil {
						t.Fatalf("disruption pass: %v", err)
					}
					cmds := f.queue.GetCommands()
					if shielded && len(cmds) != 0 {
						t.Fatalf("karpenter-core disrupts the node the platform is draining: %s of %s", cmds[0].Reason(), cmds[0].Candidates[0].NodeClaim.Name)
					}
					if !shielded && (len(cmds) != 1 || cmds[0].Reason() != karpv1.DisruptionReasonEmpty) {
						t.Fatalf("without the annotation karpenter-core left the drained node alone (%v): the release no longer needs to set it", cmds)
					}
					return
				case <-deadline:
					t.Fatal("the disruption pass did not return")
				case <-time.After(10 * time.Millisecond):
					if f.clk.HasWaiters() {
						f.clk.Step(time.Second)
					}
				}
			}
		})
	}
}
