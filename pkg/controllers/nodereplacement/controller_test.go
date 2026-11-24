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

package nodereplacement_test

// The replacement end to end, against karpenter-core's own registration,
// termination and disruption, is in pkg/cloudprovider/replacement_test.go.
// These tests pin what the controller writes, and above all what it leaves
// alone.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/nodereplacement"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

const (
	group        = "default-abc12"
	node0        = group + "-node0"
	node1        = group + "-node1"
	replacements = "karpenter_clevercloud_nodeclaim_node_replacements_total"
	// releaseWindow mirrors the controller's bound on a release: the
	// platform's 30-minute drain timeout and a margin.
	releaseWindow = 35 * time.Minute
)

// start is when every test begins: the replacement node has just joined.
var start = time.Date(2026, 11, 24, 14, 0, 0, 0, time.UTC)

// fakeRecorder captures published events for assertions.
type fakeRecorder struct {
	mu     sync.Mutex
	events []events.Event
}

func (r *fakeRecorder) Publish(evts ...events.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evts...)
}

func (r *fakeRecorder) withReason(reason string) []events.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	var matched []events.Event
	for _, e := range r.events {
		if e.Reason == reason {
			matched = append(matched, e)
		}
	}
	return matched
}

// registeredClaim is the NodeClaim of a launched, registered and initialized
// node, as karpenter-core keeps it.
func registeredClaim(name, nodeName string) *karpv1.NodeClaim {
	nodeClaim := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			UID:        types.UID("uid-" + name),
			Labels:     map[string]string{karpv1.NodePoolLabelKey: "default"},
			Finalizers: []string{karpv1.TerminationFinalizer},
		},
		Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{
			Group: "karpenter.clever-cloud.com", Kind: "CleverNodeClass", Name: "default",
		}},
		Status: karpv1.NodeClaimStatus{ProviderID: nodegroup.ProviderID(name), NodeName: nodeName},
	}
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeLaunched)
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeInitialized)
	return nodeClaim
}

// ownedGroup is the NodeGroup the provider created for nodeClaim.
func ownedGroup(nodeClaim *karpv1.NodeClaim) *ngv1.NodeGroup {
	return &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   nodeClaim.Name,
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true", v1alpha1.NodeClaimLabelKey: nodeClaim.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: nodeClaim.Name, UID: nodeClaim.UID,
			}},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1, Taints: []ngv1.NodeGroupTaint{{Key: karpv1.UnregisteredTaintKey, Effect: corev1.TaintEffectNoExecute}}},
	}
}

// karpenterNode is a node of the group after core's registration: provider
// ID, registered label, termination finalizer, no unregistered taint. It has
// been running for a day and is Ready.
func karpenterNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(start.Add(-24 * time.Hour)),
			Labels: map[string]string{
				v1alpha1.NodeGroupNodeLabelKey: group,
				karpv1.NodePoolLabelKey:        "default",
				karpv1.NodeRegisteredLabelKey:  "true",
				karpv1.NodeInitializedLabelKey: "true",
			},
			Finalizers: []string{karpv1.TerminationFinalizer},
		},
		Spec:   corev1.NodeSpec{ProviderID: nodegroup.ProviderID(group)},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
}

// drained is a registered node as the platform's drain leaves it: cordoned
// and labelled in one write.
func drained(node *corev1.Node) *corev1.Node {
	node.Spec.Unschedulable = true
	node.Labels["clever-cloud.com/draining"] = "true"
	return node
}

// notReady is a registered node whose kubelet stopped reporting: what the
// platform repairs.
func notReady(node *corev1.Node) *corev1.Node {
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}
	return node
}

// platformNode is a node as the platform registers it in the group, at start:
// the group's labels and taints, no provider ID, nothing from karpenter-core.
func platformNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(start),
			Labels:            map[string]string{v1alpha1.NodeGroupNodeLabelKey: group},
		},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: karpv1.UnregisteredTaintKey, Effect: corev1.TaintEffectNoExecute}}},
	}
}

// stamped is a platform node the providerid controller gave the group's
// provider ID to.
func stamped(node *corev1.Node) *corev1.Node {
	node.Spec.ProviderID = nodegroup.ProviderID(group)
	return node
}

type testEnv struct {
	t          *testing.T
	ctrl       *nodereplacement.Controller
	kubeClient client.WithWatch
	recorder   *fakeRecorder
	clock      *clocktesting.FakeClock
}

func newClient(objs ...client.Object) client.WithWatch {
	return fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).WithStatusSubresource(&karpv1.NodeClaim{}).Build()
}

// newTestEnv builds the controller on a fake client that serves as its own
// uncached reader: cache and API server agree unless a test says otherwise.
// Its clock reads 20 seconds after start, when the platform cordons the node
// it replaces.
func newTestEnv(t *testing.T, objs ...client.Object) *testEnv {
	t.Helper()
	kubeClient := newClient(objs...)
	return newTestEnvWith(t, kubeClient, kubeClient)
}

func newTestEnvWith(t *testing.T, kubeClient client.WithWatch, uncached client.Reader) *testEnv {
	recorder := &fakeRecorder{}
	clk := clocktesting.NewFakeClock(start.Add(20 * time.Second))
	return &testEnv{t: t, ctrl: nodereplacement.NewController(kubeClient, uncached, recorder, clk), kubeClient: kubeClient, recorder: recorder, clock: clk}
}

func (e *testEnv) reconcile() reconcile.Result {
	e.t.Helper()
	result, err := e.ctrl.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: group}})
	if err != nil {
		e.t.Fatalf("Reconcile: %v", err)
	}
	return result
}

func (e *testEnv) node(name string) *corev1.Node {
	e.t.Helper()
	node := &corev1.Node{}
	if err := e.kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, node); err != nil {
		e.t.Fatalf("getting node %s: %v", name, err)
	}
	return node
}

func (e *testEnv) update(obj client.Object) {
	e.t.Helper()
	if err := e.kubeClient.Update(context.Background(), obj); err != nil {
		e.t.Fatalf("updating %s: %v", obj.GetName(), err)
	}
}

func (e *testEnv) claim() *karpv1.NodeClaim {
	e.t.Helper()
	nodeClaim := &karpv1.NodeClaim{}
	if err := e.kubeClient.Get(context.Background(), types.NamespacedName{Name: group}, nodeClaim); err != nil {
		e.t.Fatalf("getting nodeclaim: %v", err)
	}
	return nodeClaim
}

func (e *testEnv) held(name string) bool {
	e.t.Helper()
	return controllerutil.ContainsFinalizer(e.node(name), karpv1.TerminationFinalizer)
}

// shielded reports whether the node carries karpenter.sh/do-not-disrupt.
func (e *testEnv) shielded(name string) bool {
	e.t.Helper()
	return e.node(name).Annotations[karpv1.DoNotDisruptAnnotationKey] == "true"
}

func (e *testEnv) registered() bool {
	e.t.Helper()
	return e.claim().StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue()
}

// requireUntouched fails when the controller wrote anything: the registered
// node keeps its finalizer and gets no annotation, the NodeClaim keeps its
// registration.
func (e *testEnv) requireUntouched(nodeName string) {
	e.t.Helper()
	if !e.held(nodeName) {
		e.t.Errorf("node %s lost karpenter's termination finalizer", nodeName)
	}
	if annotations := e.node(nodeName).Annotations; len(annotations) != 0 {
		e.t.Errorf("node %s was annotated: %v", nodeName, annotations)
	}
	if !e.registered() {
		e.t.Errorf("the registration of the nodeclaim was reopened: %+v", e.claim().Status.Conditions)
	}
	if len(e.recorder.events) != 0 {
		e.t.Errorf("published %d events for a group with nothing to do: %+v", len(e.recorder.events), e.recorder.events)
	}
}

// requireReleased fails unless the node is released for its replacement: no
// termination finalizer, and karpenter-core's disruption kept away.
func (e *testEnv) requireReleased(nodeName string) {
	e.t.Helper()
	if e.held(nodeName) {
		e.t.Fatalf("node %s kept karpenter's termination finalizer while the platform replaces it", nodeName)
	}
	if !e.shielded(nodeName) {
		e.t.Errorf("node %s is released without karpenter.sh/do-not-disrupt: core's emptiness can delete its nodeclaim under the platform's drain", nodeName)
	}
}

// requireRestored fails unless the node is back to what core registered: its
// finalizer, and none of the release's annotations.
func (e *testEnv) requireRestored(nodeName string) {
	e.t.Helper()
	if !e.held(nodeName) {
		e.t.Fatalf("node %s stayed without karpenter's termination finalizer: deleting it would neither drain it nor delete its nodeclaim", nodeName)
	}
	if annotations := e.node(nodeName).Annotations; len(annotations) != 0 {
		e.t.Errorf("node %s kept annotations of the release: %v", nodeName, annotations)
	}
	if !e.registered() {
		e.t.Errorf("the registration was reopened: %+v", e.claim().Status.Conditions)
	}
}

func TestSteadyStateIsLeftAlone(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), karpenterNode(node0))
	e.reconcile()
	e.requireUntouched(node0)
}

// TestASecondNodeAloneReleasesNothing: until the platform cordons the
// registered node, nobody is draining it, and a Node object can sit in a
// group for other reasons (a replacement that never takes over, an orphaned
// object). Released then, the node would be deleted without a drain by
// whatever deletes it or its NodeClaim.
func TestASecondNodeAloneReleasesNothing(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), karpenterNode(node0), platformNode(node1))
	e.reconcile()
	e.requireUntouched(node0)
}

// TestACordonAloneReleasesNothing: a node somebody cordoned is not a node the
// platform replaces.
func TestACordonAloneReleasesNothing(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)))
	e.reconcile()
	if !e.held(node0) || e.shielded(node0) {
		t.Error("a cordoned node with no replacement in its group was released")
	}
}

// TestReleasesTheRegisteredNodeOnceThePlatformDrainsIt is the first step of a
// replacement: node1 has joined a group sized for one and the platform has
// cordoned node0. node0 must lose core's termination finalizer, or its
// deletion by the platform deletes the NodeClaim and the NodeGroup, and it
// must be kept out of core's disruption. Nothing else changes yet.
func TestReleasesTheRegisteredNodeOnceThePlatformDrainsIt(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)), platformNode(node1))
	before := metricstest.Value(t, replacements)
	result := e.reconcile()

	e.requireReleased(node0)
	// The release ends by itself: the controller comes back when the
	// replacement is releaseWindow old.
	if want := releaseWindow - 20*time.Second; result.RequeueAfter != want {
		t.Errorf("requeue after %s, want %s: nothing else ends a release the platform does not finish", result.RequeueAfter, want)
	}
	if !e.registered() {
		t.Errorf("the registration was reopened while the registered node still exists: %+v", e.claim().Status.Conditions)
	}
	if got := e.node(node1); got.Spec.ProviderID != "" || len(got.Finalizers) != 0 || len(got.Annotations) != 0 {
		t.Errorf("the replacement node was written to: provider id %q, finalizers %v, annotations %v", got.Spec.ProviderID, got.Finalizers, got.Annotations)
	}
	started := e.recorder.withReason("NodeReplacementStarted")
	if len(started) != 1 || started[0].Type != corev1.EventTypeNormal || started[0].InvolvedObject.(client.Object).GetName() != group ||
		!strings.Contains(started[0].Message, node1) || !strings.Contains(started[0].Message, "drain it by hand") {
		t.Errorf("want one Normal NodeReplacementStarted event on the nodeclaim naming the replacement and the manual drain, got %+v", started)
	}
	if got := metricstest.Value(t, replacements) - before; got != 0 {
		t.Errorf("node_replacements_total moved by %v before any node was replaced", got)
	}
	// A second pass has nothing left to do but wait for the same moment.
	e.clock.Step(time.Minute)
	result = e.reconcile()
	e.requireReleased(node0)
	if want := releaseWindow - 80*time.Second; result.RequeueAfter != want || !e.registered() {
		t.Errorf("second pass: requeue after %s (want %s), registered %v", result.RequeueAfter, want, e.registered())
	}
	if len(e.recorder.events) != 1 {
		t.Errorf("the second pass published again: %+v", e.recorder.events)
	}
}

// TestReleasesANotReadyNodeBeingRepaired: the platform deletes the NotReady
// node it repairs within two seconds of cordoning it (measured), too short to
// release it then. NotReady next to its replacement, it is released at once:
// there is no drain to lose on a node whose kubelet is gone.
func TestReleasesANotReadyNodeBeingRepaired(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), notReady(karpenterNode(node0)), platformNode(node1))
	e.reconcile()
	e.requireReleased(node0)
}

// TestADoNotDisruptAnnotationOfTheUserSurvivesTheRelease: the annotation is
// removed on restore only when the release added it.
func TestADoNotDisruptAnnotationOfTheUserSurvivesTheRelease(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	old := drained(karpenterNode(node0))
	old.Annotations = map[string]string{karpv1.DoNotDisruptAnnotationKey: "true"}
	replacement := platformNode(node1)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), old, replacement)
	e.reconcile()
	e.requireReleased(node0)
	if err := e.kubeClient.Delete(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	e.reconcile()
	if !e.held(node0) || !e.shielded(node0) {
		t.Errorf("restore removed a karpenter.sh/do-not-disrupt annotation the release did not add: held %v, annotations %v", e.held(node0), e.node(node0).Annotations)
	}
	if _, marked := e.node(node0).Annotations[v1alpha1.NodeReplacementAnnotationKey]; marked {
		t.Error("the release marker stayed on a restored node")
	}
}

// TestResizedGroupKeepsItsFinalizer covers a group resized from outside
// karpenter: both nodes stay, so the registered one must keep core's
// finalizer and its drain on deletion.
func TestResizedGroupKeepsItsFinalizer(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	ng := ownedGroup(nodeClaim)
	ng.Spec.NodeCount = 2
	e := newTestEnv(t, nodeClaim, ng, drained(karpenterNode(node0)), platformNode(node1))
	e.reconcile()
	e.requireUntouched(node0)
}

// TestRestoresTheFinalizerWhenThePlatformStopsReplacing covers every way a
// replacement ends without the old node being deleted. The released node must
// get core's finalizer back, or deleting it would neither drain it nor delete
// its NodeClaim.
func TestRestoresTheFinalizerWhenThePlatformStopsReplacing(t *testing.T) {
	cases := map[string]func(e *testEnv){
		"the replacement is gone": func(e *testEnv) {
			if err := e.kubeClient.Delete(context.Background(), e.node(node1)); err != nil {
				e.t.Fatal(err)
			}
		},
		"the node is uncordoned": func(e *testEnv) {
			node := e.node(node0)
			node.Spec.Unschedulable = false
			e.update(node)
		},
		"the group is resized": func(e *testEnv) {
			ng := &ngv1.NodeGroup{}
			if err := e.kubeClient.Get(context.Background(), types.NamespacedName{Name: group}, ng); err != nil {
				e.t.Fatal(err)
			}
			ng.Spec.NodeCount = 2
			e.update(ng)
		},
	}
	for name, platform := range cases {
		t.Run(name, func(t *testing.T) {
			nodeClaim := registeredClaim(group, node0)
			e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)), platformNode(node1))
			e.reconcile()
			e.requireReleased(node0)
			platform(e)
			e.reconcile()
			e.requireRestored(node0)
			if stalled := e.recorder.withReason("NodeReplacementStalled"); len(stalled) != 0 {
				t.Errorf("a replacement that ended was reported as stalled: %+v", stalled)
			}
		})
	}
}

// TestAReleaseEndsWithItsWindow is the cluster left FAILED by its upgrade, or
// a drain a PodDisruptionBudget blocks for good: the platform stops, both
// nodes stay, the old one cordoned. Past releaseWindow nobody drains it any
// more, so it must be core's to drain again — and stay so, however long the
// replacement lingers.
func TestAReleaseEndsWithItsWindow(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)), platformNode(node1))
	e.reconcile()
	e.requireReleased(node0)

	e.clock.SetTime(start.Add(releaseWindow - time.Second))
	if result := e.reconcile(); result.RequeueAfter != time.Second {
		t.Errorf("one second before the window ends: requeue after %s, want 1s", result.RequeueAfter)
	}
	e.requireReleased(node0)

	e.clock.SetTime(start.Add(releaseWindow))
	e.reconcile()
	e.requireRestored(node0)
	stalled := e.recorder.withReason("NodeReplacementStalled")
	if len(stalled) != 1 || stalled[0].Type != corev1.EventTypeWarning || stalled[0].InvolvedObject.(client.Object).GetName() != group ||
		!strings.Contains(stalled[0].Message, node0) || !strings.Contains(stalled[0].Message, node1) {
		t.Errorf("want one Warning NodeReplacementStalled event on the nodeclaim naming both nodes, got %+v", stalled)
	}

	// Still cordoned next to its replacement a day later: not released again.
	e.clock.SetTime(start.Add(24 * time.Hour))
	if result := e.reconcile(); result.RequeueAfter != 0 {
		t.Errorf("a stalled replacement is still scheduled: %+v", result)
	}
	e.requireRestored(node0)
}

// TestANodeLingeringInTheGroupReleasesNothing: an unstamped node older than
// releaseWindow is not a replacement in progress, whatever happens to the
// registered node next to it.
func TestANodeLingeringInTheGroupReleasesNothing(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	lingering := platformNode(node1)
	lingering.CreationTimestamp = metav1.NewTime(start.Add(-releaseWindow))
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(notReady(karpenterNode(node0))), lingering)
	e.reconcile()
	if !e.held(node0) || e.shielded(node0) {
		t.Error("released a node for a node that has been in its group longer than a replacement takes")
	}
}

// TestAManuallyRestoredFinalizerIsReleasedAgain: while the platform drains the
// node, the finalizer must stay off, whoever puts it back.
func TestAManuallyRestoredFinalizerIsReleasedAgain(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)), platformNode(node1))
	e.reconcile()
	node := e.node(node0)
	controllerutil.AddFinalizer(node, karpv1.TerminationFinalizer)
	e.update(node)
	e.reconcile()
	e.requireReleased(node0)
}

// TestAFinalizerPutBackByHandLeavesNoAnnotationBehind: the release ends on a
// node that already has its finalizer again. Its annotations must still go,
// or karpenter.sh/do-not-disrupt would keep core off the node for good.
func TestAFinalizerPutBackByHandLeavesNoAnnotationBehind(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	replacement := platformNode(node1)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)), replacement)
	e.reconcile()
	e.requireReleased(node0)
	if err := e.kubeClient.Delete(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	node := e.node(node0)
	controllerutil.AddFinalizer(node, karpv1.TerminationFinalizer)
	e.update(node)
	e.reconcile()
	e.requireRestored(node0)
}

// TestTheNewestNodeIsTheReplacement: next to a Node object that has lingered
// in the group, a node that just joined is still a replacement.
func TestTheNewestNodeIsTheReplacement(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	lingering := platformNode(group + "-node2")
	lingering.CreationTimestamp = metav1.NewTime(start.Add(-24 * time.Hour))
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)), lingering, platformNode(node1))
	result := e.reconcile()
	e.requireReleased(node0)
	if want := releaseWindow - 20*time.Second; result.RequeueAfter != want {
		t.Errorf("requeue after %s, want %s: the window is the newest node's", result.RequeueAfter, want)
	}
	if started := e.recorder.withReason("NodeReplacementStarted"); len(started) != 1 || !strings.Contains(started[0].Message, node1) {
		t.Errorf("want the event to name %s as the replacement, got %+v", node1, started)
	}
}

// TestADeletedReplacementDoesNotCount: a replacement node the platform is
// already deleting replaces nothing.
func TestADeletedReplacementDoesNotCount(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	replacement := platformNode(node1)
	replacement.Finalizers = []string{"example.com/hold"}
	now := metav1.Now()
	replacement.DeletionTimestamp = &now
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)), replacement)
	e.reconcile()
	e.requireUntouched(node0)
}

// TestAdoptsTheNodeThatTookTheProviderIDOver is the second step: node0 is
// gone and node1 carries the provider ID, but karpenter-core never registered
// it. The NodeClaim's registration must be reopened, which is what makes core
// sync node1; the controller itself writes nothing on the node.
func TestAdoptsTheNodeThatTookTheProviderIDOver(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), stamped(platformNode(node1)))
	before := metricstest.Value(t, replacements)
	e.reconcile()

	got := e.claim()
	for _, condition := range []string{karpv1.ConditionTypeRegistered, karpv1.ConditionTypeInitialized} {
		if c := got.StatusConditions().Get(condition); !c.IsUnknown() || c.Reason != nodereplacement.ReasonNodeReplaced {
			t.Errorf("%s = %s (%s), want Unknown (%s): core's registration only acts on Unknown", condition, c.Status, c.Reason, nodereplacement.ReasonNodeReplaced)
		}
	}
	if !got.StatusConditions().Get(karpv1.ConditionTypeLaunched).IsTrue() {
		t.Error("Launched was reset: karpenter-core would launch a second nodegroup")
	}
	if got.Status.NodeName != "" || got.Status.ProviderID != nodegroup.ProviderID(group) {
		t.Errorf("status names node %q with provider id %q, want no node and the provider id kept", got.Status.NodeName, got.Status.ProviderID)
	}
	if !got.DeletionTimestamp.IsZero() {
		t.Error("the nodeclaim was deleted")
	}
	if node := e.node(node1); len(node.Finalizers) != 0 || len(node.Spec.Taints) != 1 || node.Labels[karpv1.NodeRegisteredLabelKey] != "" || len(node.Annotations) != 0 {
		t.Errorf("the controller synced the node itself; that is core's registration: %+v", node)
	}
	replaced := e.recorder.withReason("NodeReplaced")
	if len(replaced) != 1 || replaced[0].Type != corev1.EventTypeNormal || !strings.Contains(replaced[0].Message, node1) {
		t.Errorf("want one Normal NodeReplaced event naming the adopted node, got %+v", replaced)
	}
	if got := metricstest.Value(t, replacements) - before; got != 1 {
		t.Errorf("node_replacements_total moved by %v, want 1", got)
	}
	// Reopened once: the NodeClaim is core's again until it registers.
	e.reconcile()
	if got := metricstest.Value(t, replacements) - before; got != 1 {
		t.Errorf("a second pass counted the replacement again: %v", got)
	}
}

// TestAdoptsBeforeTheReplacementIsStamped: the old node's deletion triggers
// the providerid controller and this one at once, in either order.
func TestAdoptsBeforeTheReplacementIsStamped(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), platformNode(node1))
	e.reconcile()
	if e.registered() {
		t.Fatalf("the registration was not reopened: %+v", e.claim().Status.Conditions)
	}
	if replaced := e.recorder.withReason("NodeReplaced"); len(replaced) != 1 || replaced[0].Type != corev1.EventTypeNormal {
		t.Errorf("want one Normal NodeReplaced event, got %+v", replaced)
	}
}

// TestAdoptsAReplacementBornWithTheRegisteredLabel: v0.12.0 let the
// karpenter.sh domain into a NodeGroup's immutable spec.labels, which the
// platform applies to every node of the group. A group carrying
// karpenter.sh/registered there gives it to the replacement before core ever
// saw it; taken as proof of registration, the NodeClaim kept naming the
// deleted node and the replacement kept its unregistered taint for good.
func TestAdoptsAReplacementBornWithTheRegisteredLabel(t *testing.T) {
	for name, taints := range map[string][]corev1.Taint{
		"with the unregistered taint of its group": {{Key: karpv1.UnregisteredTaintKey, Effect: corev1.TaintEffectNoExecute}},
		// Whatever its taints say, a node under another name is not the node
		// karpenter-core registered.
		"without any taint": nil,
	} {
		t.Run(name, func(t *testing.T) {
			nodeClaim := registeredClaim(group, node0)
			replacement := stamped(platformNode(node1))
			replacement.Labels[karpv1.NodeRegisteredLabelKey] = "true"
			replacement.Spec.Taints = taints
			e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), replacement)
			e.reconcile()
			if e.registered() {
				t.Fatal("a replacement carrying the registered label from its group's labels was taken for the registered node")
			}
			if e.held(node1) {
				t.Error("karpenter's termination finalizer was put on a node core never registered")
			}
		})
	}
}

// TestAdoptsANodeRecreatedUnderTheSameName covers a Node object deleted and
// registered again by its kubelet: same name, same provider ID once stamped,
// but the group's unregistered taint is back — with or without the registered
// label a group created by v0.12.0 can carry.
func TestAdoptsANodeRecreatedUnderTheSameName(t *testing.T) {
	for name, labels := range map[string]map[string]string{
		"without core's labels":                  nil,
		"with the registered label of its group": {karpv1.NodeRegisteredLabelKey: "true"},
	} {
		t.Run(name, func(t *testing.T) {
			nodeClaim := registeredClaim(group, node0)
			recreated := stamped(platformNode(node0))
			for key, value := range labels {
				recreated.Labels[key] = value
			}
			e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), recreated)
			e.reconcile()
			if e.registered() {
				t.Fatal("a node karpenter-core never registered was left behind a Registered nodeclaim: its unregistered taint would never be lifted")
			}
		})
	}
}

// TestARegisteredNodeWithoutItsRegisteredLabelIsNotReplaced: a kubelet can
// remove karpenter.sh/registered from its own Node. Were the label what
// decides, it could reopen its NodeClaim's registration at will, restarting
// core's registration timeout each time.
func TestARegisteredNodeWithoutItsRegisteredLabelIsNotReplaced(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	node := karpenterNode(node0)
	delete(node.Labels, karpv1.NodeRegisteredLabelKey)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), node)
	e.reconcile()
	e.requireUntouched(node0)
}

// TestNodeGoneWithNothingToAdopt: the NodeClaim must not stay Registered on a
// node that does not exist. The event is a Warning: karpenter-core's
// registration timeout is now counting.
func TestNodeGoneWithNothingToAdopt(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim))
	e.reconcile()
	if e.registered() {
		t.Fatalf("the nodeclaim stayed Registered without any node: %+v", e.claim().Status.Conditions)
	}
	replaced := e.recorder.withReason("NodeReplaced")
	if len(replaced) != 1 || replaced[0].Type != corev1.EventTypeWarning || !strings.Contains(replaced[0].Message, "15 minutes") {
		t.Errorf("want one Warning NodeReplaced event announcing the registration timeout, got %+v", replaced)
	}
}

// TestANodeClaimBeingRepairedIsNotAdopted: karpenter-core's repair deletes
// the NodeClaim it decided to replace once its own replacement is up, without
// looking at the node again, and drains it with a zero grace period. Adopting
// the platform's replacement meanwhile would hand that healthy node, and the
// pods it took back, to that forced drain. The registration reopens if core
// gives the repair up.
func TestANodeClaimBeingRepairedIsNotAdopted(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	nodeClaim.StatusConditions().SetTrueWithReason(karpv1.ConditionTypeDisruptionReason, string(karpv1.DisruptionReasonUnhealthy), "Unhealthy")
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), stamped(platformNode(node1)))
	e.reconcile()
	if !e.registered() || len(e.recorder.events) != 0 {
		t.Fatalf("adopted a node for a nodeclaim karpenter-core is about to delete: %+v", e.claim().Status.Conditions)
	}

	got := e.claim()
	if err := got.StatusConditions().Clear(karpv1.ConditionTypeDisruptionReason); err != nil {
		t.Fatal(err)
	}
	if err := e.kubeClient.Status().Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	e.reconcile()
	if e.registered() {
		t.Error("the registration stayed closed after karpenter-core gave the repair up")
	}
}

// TestAnyOtherDisruptionStillAdopts: a drift or consolidation command drains
// the NodeClaim's node gracefully when it completes, and its replacement
// cannot start while the cluster upgrades. Not adopting would leave the
// evicted pods Pending until then, which is the failed upgrade this
// controller exists to prevent.
func TestAnyOtherDisruptionStillAdopts(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	nodeClaim.StatusConditions().SetTrueWithReason(karpv1.ConditionTypeDisruptionReason, string(karpv1.DisruptionReasonDrifted), "Drifted")
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), stamped(platformNode(node1)))
	e.reconcile()
	if e.registered() {
		t.Error("a drifted nodeclaim whose node the platform replaced was not adopted")
	}
}

// TestARegisteredNodeIsFoundWithoutItsNodeGroupLabel: a kubelet can drop or
// rewrite the clever-cloud.com labels of its own Node. The node core
// registered must still be found, by the name recorded on the NodeClaim, or
// every reconcile would reopen a registration core closes again at once.
func TestARegisteredNodeIsFoundWithoutItsNodeGroupLabel(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	node := karpenterNode(node0)
	delete(node.Labels, v1alpha1.NodeGroupNodeLabelKey)
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), node)
	e.reconcile()
	e.requireUntouched(node0)
}

// TestANodeClaimingTheGroupByLabelOnlyIsIgnored: the name places a node in a
// group, not a label its kubelet can write. A node of another group carrying
// this group's label is not a replacement.
func TestANodeClaimingTheGroupByLabelOnlyIsIgnored(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	intruder := platformNode("other-zzz99-node0")
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)), intruder)
	e.reconcile()
	e.requireUntouched(node0)
}

// TestARegisteredNodeOutsideTheGroupIsNotFollowed: the node the NodeClaim
// names carries its provider ID, but its name places it in no node of this
// group. That is not a state this provider's controllers produce: reopening
// the registration would only make core register the same node again,
// forever, and it is not a node of this group to release.
func TestARegisteredNodeOutsideTheGroupIsNotFollowed(t *testing.T) {
	nodeClaim := registeredClaim(group, "other-zzz99-node0")
	outsider := drained(karpenterNode("other-zzz99-node0"))
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), outsider, platformNode(node1))
	e.reconcile()
	e.requireUntouched("other-zzz99-node0")
}

// TestIgnoresWhatIsNotItsToFollow lists the NodeClaims and NodeGroups the
// controller must never write for, each with a drained node and its
// replacement in place that would otherwise trigger the release.
func TestIgnoresWhatIsNotItsToFollow(t *testing.T) {
	now := metav1.Now()
	cases := map[string]func(nodeClaim *karpv1.NodeClaim, ng *ngv1.NodeGroup){
		"nodeclaim of another provider": func(nodeClaim *karpv1.NodeClaim, _ *ngv1.NodeGroup) {
			nodeClaim.Spec.NodeClassRef.Group = "karpenter.k8s.aws"
		},
		"nodeclaim being deleted": func(nodeClaim *karpv1.NodeClaim, _ *ngv1.NodeGroup) {
			nodeClaim.DeletionTimestamp = &now
		},
		"nodeclaim not registered yet": func(nodeClaim *karpv1.NodeClaim, _ *ngv1.NodeGroup) {
			nodeClaim.StatusConditions().SetUnknown(karpv1.ConditionTypeRegistered)
		},
		"nodeclaim with another provider id": func(nodeClaim *karpv1.NodeClaim, _ *ngv1.NodeGroup) {
			nodeClaim.Status.ProviderID = nodegroup.ProviderID("elsewhere")
		},
		"unmanaged nodegroup": func(_ *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
			delete(ng.Labels, v1alpha1.ManagedLabelKey)
		},
		"nodegroup owned by an earlier nodeclaim of the same name": func(_ *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
			ng.OwnerReferences[0].UID = "uid-of-the-previous-claim"
		},
		"nodegroup being deleted": func(_ *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
			ng.Finalizers = []string{"clever-nodegroup-operator"}
			ng.DeletionTimestamp = &now
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			nodeClaim := registeredClaim(group, node0)
			ng := ownedGroup(nodeClaim)
			mutate(nodeClaim, ng)
			e := newTestEnv(t, nodeClaim, ng, drained(karpenterNode(node0)), platformNode(node1))
			e.reconcile()
			if !e.held(node0) || e.shielded(node0) {
				t.Error("the registered node was released")
			}
			if len(e.recorder.events) != 0 {
				t.Errorf("published events: %+v", e.recorder.events)
			}
		})
	}
	t.Run("no nodeclaim", func(t *testing.T) {
		e := newTestEnv(t, drained(karpenterNode(node0)), platformNode(node1))
		e.reconcile()
		if !e.held(node0) || e.shielded(node0) {
			t.Error("the registered node was released")
		}
	})
}

// TestTwoNodesOnOneProviderIDAreLeftAlone: the providerid controller stamps
// one node per group, so this should not exist. If it does, reopening the
// registration would make karpenter-core find two nodes for the NodeClaim,
// which it answers with a terminal Registered=False and, 15 minutes later,
// the deletion of the NodeClaim. The platform reuses the lowest free index,
// so the registered node is as often node1 as node0.
func TestTwoNodesOnOneProviderIDAreLeftAlone(t *testing.T) {
	for _, registered := range []string{node0, node1} {
		other := map[string]string{node0: node1, node1: node0}[registered]
		t.Run("registered on "+registered, func(t *testing.T) {
			nodeClaim := registeredClaim(group, registered)
			e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(registered)), stamped(platformNode(other)))
			e.reconcile()
			e.requireUntouched(registered)
		})
		// The registered node lost its nodegroup label: the list returns the
		// other node alone, which must not read as the single holder — nor
		// the registered node as one, with a replacement next to it.
		t.Run("registered on "+registered+", unlabelled", func(t *testing.T) {
			nodeClaim := registeredClaim(group, registered)
			unlabelled := drained(karpenterNode(registered))
			delete(unlabelled.Labels, v1alpha1.NodeGroupNodeLabelKey)
			e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), unlabelled, stamped(platformNode(other)), platformNode(group+"-node2"))
			e.reconcile()
			e.requireUntouched(registered)
		})
	}
}

// TestARegisteredNodeBeingDeletedIsLeftToCore: with its finalizer, the
// deletion is karpenter-core's termination at work.
func TestARegisteredNodeBeingDeletedIsLeftToCore(t *testing.T) {
	nodeClaim := registeredClaim(group, node0)
	deleting := drained(karpenterNode(node0))
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	e := newTestEnv(t, nodeClaim, ownedGroup(nodeClaim), deleting, platformNode(node1))
	e.reconcile()
	e.requireUntouched(node0)
}

// TestWritesAreDecidedUncached: the cache can show a node the API server no
// longer has, or miss one it has. Neither a release nor a reopened
// registration may rest on it.
func TestWritesAreDecidedUncached(t *testing.T) {
	t.Run("a replacement only the cache still shows releases nothing", func(t *testing.T) {
		nodeClaim := registeredClaim(group, node0)
		apiServer := newClient(nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)))
		cache := newClient(nodeClaim.DeepCopy(), ownedGroup(nodeClaim), drained(karpenterNode(node0)), platformNode(node1))
		e := newTestEnvWith(t, writingTo(cache, apiServer), apiServer)
		e.reconcile()
		got := &corev1.Node{}
		if err := apiServer.Get(context.Background(), types.NamespacedName{Name: node0}, got); err != nil {
			t.Fatal(err)
		}
		if !controllerutil.ContainsFinalizer(got, karpv1.TerminationFinalizer) {
			t.Error("released a node for a replacement that only the cache still shows")
		}
	})
	t.Run("a cordon only the cache still shows releases nothing", func(t *testing.T) {
		nodeClaim := registeredClaim(group, node0)
		apiServer := newClient(nodeClaim, ownedGroup(nodeClaim), karpenterNode(node0), platformNode(node1))
		cache := newClient(nodeClaim.DeepCopy(), ownedGroup(nodeClaim), drained(karpenterNode(node0)), platformNode(node1))
		e := newTestEnvWith(t, writingTo(cache, apiServer), apiServer)
		e.reconcile()
		got := &corev1.Node{}
		if err := apiServer.Get(context.Background(), types.NamespacedName{Name: node0}, got); err != nil {
			t.Fatal(err)
		}
		if !controllerutil.ContainsFinalizer(got, karpv1.TerminationFinalizer) {
			t.Error("released a node the API server no longer shows cordoned")
		}
	})
	t.Run("a registered node the cache lost is not adopted away", func(t *testing.T) {
		nodeClaim := registeredClaim(group, node0)
		apiServer := newClient(nodeClaim, ownedGroup(nodeClaim), karpenterNode(node0))
		cache := newClient(nodeClaim.DeepCopy(), ownedGroup(nodeClaim))
		e := newTestEnvWith(t, writingTo(cache, apiServer), apiServer)
		e.reconcile()
		got := &karpv1.NodeClaim{}
		if err := apiServer.Get(context.Background(), types.NamespacedName{Name: group}, got); err != nil {
			t.Fatal(err)
		}
		if !got.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue() {
			t.Error("reopened the registration of a nodeclaim whose node only the cache lost")
		}
	})
}

// writingTo is a client that reads from cache and writes to apiServer, as the
// manager's client does.
func writingTo(cache, apiServer client.WithWatch) client.WithWatch {
	return interceptor.NewClient(cache, interceptor.Funcs{
		Patch: func(ctx context.Context, _ client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			return apiServer.Patch(ctx, obj, patch, opts...)
		},
		SubResourcePatch: func(ctx context.Context, _ client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			return apiServer.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	})
}

// TestAConflictIsRetriedNotReported: the node or the NodeClaim changed since
// the read. The write is not forced; the next pass reads again.
func TestAConflictIsRetriedNotReported(t *testing.T) {
	conflict := apierrors.NewConflict(corev1.Resource("nodes"), node0, nil)
	nodeClaim := registeredClaim(group, node0)
	kubeClient := interceptor.NewClient(
		newClient(nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)), platformNode(node1)),
		interceptor.Funcs{Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			return conflict
		}})
	e := newTestEnvWith(t, kubeClient, kubeClient)
	if result := e.reconcile(); result.RequeueAfter != time.Second {
		t.Errorf("a conflicting write was not scheduled again a second later: %+v", result)
	}
	if len(e.recorder.events) != 0 {
		t.Errorf("published an event for a write that did not happen: %+v", e.recorder.events)
	}
}

// TestWritesDoNotUndoAConcurrentChange: finalizers and conditions are lists,
// which a merge patch replaces whole. Each write must be refused when the
// object changed since it was read — here between the read and the patch —
// instead of putting the list back as it was read.
func TestWritesDoNotUndoAConcurrentChange(t *testing.T) {
	t.Run("release", func(t *testing.T) {
		nodeClaim := registeredClaim(group, node0)
		apiServer := newClient(nodeClaim, ownedGroup(nodeClaim), drained(karpenterNode(node0)), platformNode(node1))
		kubeClient := interceptor.NewClient(apiServer, interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				concurrent := &corev1.Node{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), concurrent); err != nil {
					return err
				}
				controllerutil.AddFinalizer(concurrent, "example.com/added-meanwhile")
				if err := c.Update(ctx, concurrent); err != nil {
					return err
				}
				return c.Patch(ctx, obj, patch, opts...)
			}})
		e := newTestEnvWith(t, kubeClient, apiServer)
		if result := e.reconcile(); result.RequeueAfter != time.Second {
			t.Errorf("a write on a node that changed since the read was not refused and retried: %+v", result)
		}
		if got := e.node(node0); !controllerutil.ContainsFinalizer(got, "example.com/added-meanwhile") || !controllerutil.ContainsFinalizer(got, karpv1.TerminationFinalizer) {
			t.Errorf("the release overwrote the finalizers of a node that changed since the read: %v", got.Finalizers)
		}
	})
	t.Run("adoption", func(t *testing.T) {
		nodeClaim := registeredClaim(group, node0)
		apiServer := newClient(nodeClaim, ownedGroup(nodeClaim), stamped(platformNode(node1)))
		kubeClient := interceptor.NewClient(apiServer, interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				concurrent := &karpv1.NodeClaim{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), concurrent); err != nil {
					return err
				}
				concurrent.StatusConditions().SetTrue(karpv1.ConditionTypeDrifted)
				if err := c.Status().Update(ctx, concurrent); err != nil {
					return err
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			}})
		e := newTestEnvWith(t, kubeClient, apiServer)
		if result := e.reconcile(); result.RequeueAfter != time.Second {
			t.Errorf("a write on a nodeclaim that changed since the read was not refused and retried: %+v", result)
		}
		got := e.claim()
		if !got.StatusConditions().Get(karpv1.ConditionTypeDrifted).IsTrue() || !got.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue() {
			t.Errorf("the adoption overwrote the conditions of a nodeclaim that changed since the read: %+v", got.Status.Conditions)
		}
	})
}
