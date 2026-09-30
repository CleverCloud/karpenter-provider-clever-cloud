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

package nodegroup_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis"
	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

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

func (r *fakeRecorder) reasons() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	reasons := make([]string, 0, len(r.events))
	for _, e := range r.events {
		reasons = append(reasons, e.Reason)
	}
	return reasons
}

// catalogue sizes flavors for the providers under test as the controller
// does: the built-in catalogue.
var catalogue = instancetype.NewProvider("par", nil, nil)

func newTestProvider(t *testing.T, objs ...client.Object) (*nodegroup.Provider, client.Client) {
	provider, kubeClient, _ := newTestProviderWithRecorder(t, objs...)
	return provider, kubeClient
}

func newTestProviderWithRecorder(t *testing.T, objs ...client.Object) (*nodegroup.Provider, client.Client, *fakeRecorder) {
	t.Helper()
	provider, kubeClient, recorder, _ := newClockedTestProvider(t, objs...)
	return provider, kubeClient, recorder
}

// newClockedTestProvider is newTestProviderWithRecorder with the fake clock
// that times the provider's quota backoff and refusal hold-out, so that tests
// can step through them. The acceptance poll keeps real time.
func newClockedTestProvider(t *testing.T, objs ...client.Object) (*nodegroup.Provider, client.Client, *fakeRecorder, *clocktesting.FakeClock) {
	t.Helper()
	// No WithStatusSubresource for NodeGroup: status must stay writable via
	// plain Update so the tests can play the Clever Cloud operator, and so
	// that seeded objects keep their status.
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		Build()
	recorder := &fakeRecorder{}
	clk := clocktesting.NewFakeClock(time.Now())
	return nodegroup.NewProvider(kubeClient, recorder, catalogue, clk), kubeClient, recorder, clk
}

func testNodeClass(name string) *v1alpha1.CleverNodeClass {
	return &v1alpha1.CleverNodeClass{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func testNodeClaim(name string) *karpv1.NodeClaim {
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			UID:    types.UID("uid-" + name),
			Labels: map[string]string{karpv1.NodePoolLabelKey: "default"},
		},
	}
}

// eventsWithReason returns the captured events carrying the given reason.
func (r *fakeRecorder) eventsWithReason(reason string) []events.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []events.Event
	for _, e := range r.events {
		if e.Reason == reason {
			out = append(out, e)
		}
	}
	return out
}

// assertOneNormalTransientFailure checks that Create surfaced the transient
// failure its poll saw exactly once, as the Normal event of a group accepted
// within the window.
func assertOneNormalTransientFailure(t *testing.T, recorder *fakeRecorder) {
	t.Helper()
	evts := recorder.eventsWithReason("NodeGroupTransientFailure")
	if len(evts) != 1 {
		t.Fatalf("expected exactly one NodeGroupTransientFailure event, got %v", recorder.reasons())
	}
	if evts[0].Type != corev1.EventTypeNormal || !strings.Contains(evts[0].Message, ngv1.ReasonUpstreamError) {
		t.Errorf("expected a Normal event carrying the operator's reason, got %+v", evts[0])
	}
}

// condTrue builds a True condition of the given type, as the Clever Cloud
// operator writes it.
func condTrue(condType, reason, message string) ngv1.NodeGroupCondition {
	return ngv1.NodeGroupCondition{Type: condType, Status: corev1.ConditionTrue, Reason: reason, Message: message}
}

// syncedConditions is the status the Clever Cloud operator reports once it
// has accepted a NodeGroup.
func syncedConditions() []ngv1.NodeGroupCondition {
	return []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReady, "Synced", "")}
}

// syncedStatus is the full status of an accepted NodeGroup.
func syncedStatus() ngv1.NodeGroupStatus {
	return ngv1.NodeGroupStatus{Phase: ngv1.PhaseSynced, Conditions: syncedConditions()}
}

// acknowledgedStatus is the operator's first status write on a group it
// accepted, measured live about 1 s after the group's creation: phase Creating
// and ReconcileInProgress=True(Creating). Ready comes 38-58 s later.
func acknowledgedStatus() ngv1.NodeGroupStatus {
	return ngv1.NodeGroupStatus{
		Phase:      "Creating",
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileInProgress, "Creating", "")},
	}
}

// upstreamErrorStatus is the status the operator reports while it retries a
// failing Clever Cloud API call. Measured live: phase=UpstreamError with
// Ready=True(Synced) + ReconcileInProgress=True(Scaling) +
// ReconcileFailed=True(UpstreamError), on a group that was already up and
// being scaled; the operator retried until it succeeded. Without ready, the
// same failure lands on a group still in its first reconcile (Creating).
func upstreamErrorStatus(ready bool) ngv1.NodeGroupStatus {
	failed := condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonUpstreamError, "API error: RequestDidntReturnSuccess")
	if !ready {
		return ngv1.NodeGroupStatus{
			Phase:      ngv1.PhaseUpstreamError,
			Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileInProgress, "Creating", ""), failed},
		}
	}
	return ngv1.NodeGroupStatus{
		Phase: ngv1.PhaseUpstreamError,
		Conditions: []ngv1.NodeGroupCondition{
			condTrue(ngv1.ConditionTypeReady, "Synced", ""),
			condTrue(ngv1.ConditionTypeReconcileInProgress, "Scaling", ""),
			failed,
		},
		NodeCount: 1,
	}
}

// unacknowledgedUpstreamErrorStatus is a transient failure reported before the
// operator acknowledged the group: ReconcileFailed=True(UpstreamError) without
// ReconcileInProgress. Not a shape measured live; it pins that a transient
// failure alone is no decision, so the poll keeps waiting for one.
func unacknowledgedUpstreamErrorStatus() ngv1.NodeGroupStatus {
	return ngv1.NodeGroupStatus{
		Phase:      ngv1.PhaseUpstreamError,
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonUpstreamError, "API error: RequestDidntReturnSuccess")},
	}
}

// statusStep separates successive statuses written by setStatusOnceCreated:
// longer than the 1s acceptance-poll interval, so the poll observes each one.
const statusStep = 1500 * time.Millisecond

// creationPatience bounds how long setStatusOnceCreated waits for the
// NodeGroup to appear, far longer than any Create under test takes to post it.
const creationPatience = 20 * time.Second

// setStatusOnceCreated plays the Clever Cloud operator: once the NodeGroup
// appears in the fake client, it writes each status in turn, statusStep apart.
// A status may carry several conditions at once, as the live operator reports
// them. It stops early if the group is deleted in between, so a Create that
// wrongly deletes the group fails its assertions instead of hanging the test,
// and gives up on a group that never appears within creationPatience, so a
// Create that wrongly fails fast without posting it does not hang the test
// until go test's timeout either.
func setStatusOnceCreated(t *testing.T, kubeClient client.Client, name string, statuses ...ngv1.NodeGroupStatus) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(creationPatience)
		for i, status := range statuses {
			if i > 0 {
				time.Sleep(statusStep)
			}
			for {
				ng := &ngv1.NodeGroup{}
				if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, ng); err != nil {
					if i > 0 && apierrors.IsNotFound(err) {
						return
					}
					if time.Now().After(deadline) {
						return
					}
					time.Sleep(time.Millisecond)
					continue
				}
				ng.Status = status
				if err := kubeClient.Update(context.Background(), ng); err != nil {
					t.Errorf("updating nodegroup status: %v", err)
				}
				break
			}
		}
	}()
	return done
}

// acceptOnceCreated simulates the Clever Cloud operator accepting the
// NodeGroup: once it appears in the fake client, it writes the operator's
// first status on an accepted group (acknowledgedStatus), on which Create's
// acceptance poll returns — as live, long before the group turns Ready.
func acceptOnceCreated(t *testing.T, kubeClient client.Client, name string) <-chan struct{} {
	t.Helper()
	return setStatusOnceCreated(t, kubeClient, name, acknowledgedStatus())
}

// failOnceCreated flips the NodeGroup to ReconcileFailed with an arbitrary
// reason once it appears, standing in for the upstream operator.
func failOnceCreated(t *testing.T, kubeClient client.Client, name, reason, message string) <-chan struct{} {
	t.Helper()
	return setStatusOnceCreated(t, kubeClient, name, ngv1.NodeGroupStatus{
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, reason, message)},
	})
}

// rejectOnceCreated simulates the Clever Cloud operator rejecting the
// NodeGroup on quota once it appears in the fake client.
func rejectOnceCreated(t *testing.T, kubeClient client.Client, name, message string) <-chan struct{} {
	t.Helper()
	return failOnceCreated(t, kubeClient, name, ngv1.ReasonQuotaExceeded, message)
}

// ownedNodeGroup seeds a NodeGroup as Create would have made it for the claim
// of the same name, with the given status: what the AlreadyExists path adopts.
func ownedNodeGroup(name, flavor string, status ngv1.NodeGroupStatus) *ngv1.NodeGroup {
	return &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				v1alpha1.ManagedLabelKey:   "true",
				v1alpha1.NodeClaimLabelKey: name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: name},
			},
		},
		Spec:   ngv1.NodeGroupSpec{Flavor: flavor, NodeCount: 1},
		Status: status,
	}
}

func TestProviderIDRoundTrip(t *testing.T) {
	if got := nodegroup.ProviderID("x"); got != "clevercloud://x" {
		t.Errorf("ProviderID = %q, want %q", got, "clevercloud://x")
	}
	name, err := nodegroup.ParseProviderID(nodegroup.ProviderID("x"))
	if err != nil {
		t.Fatalf("ParseProviderID: %v", err)
	}
	if name != "x" {
		t.Errorf("round-trip = %q, want %q", name, "x")
	}
	for _, bad := range []string{"aws:///i-123", "", "clevercloud://"} {
		if _, err := nodegroup.ParseProviderID(bad); err == nil {
			t.Errorf("expected error for provider id %q", bad)
		}
	}
}

func TestNodeGroupOfNode(t *testing.T) {
	for _, tc := range []struct {
		node   string
		want   string
		wantOK bool
	}{
		{"default-abc12-node0", "default-abc12", true},
		{"default-abc12-node1", "default-abc12", true},
		{"default-abc12-node12", "default-abc12", true},
		// Only the last suffix is the node index: a group may itself be
		// named like a node.
		{"pool-node0-node3", "pool-node0", true},
		{"default-abc12", "", false},
		{"default-abc12-node", "", false},
		{"default-abc12-nodex", "", false},
		{"default-abc12-node0a", "", false},
		{"-node0", "", false},
		{"", "", false},
	} {
		got, ok := nodegroup.NodeGroupOfNode(tc.node)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("NodeGroupOfNode(%q) = %q, %v; want %q, %v", tc.node, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestIsManaged(t *testing.T) {
	cases := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"managed true", map[string]string{v1alpha1.ManagedLabelKey: "true"}, true},
		{"managed false", map[string]string{v1alpha1.ManagedLabelKey: "false"}, false},
		{"label absent", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ng := &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: "ng", Labels: tc.labels}}
			if got := nodegroup.IsManaged(ng); got != tc.want {
				t.Errorf("IsManaged = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCreateBuildsNodeGroupForNodeClaim(t *testing.T) {
	provider, kubeClient := newTestProvider(t)
	nodeClass := testNodeClass("default")
	nodeClaim := testNodeClaim("default-abc12")

	done := acceptOnceCreated(t, kubeClient, nodeClaim.Name)
	_, err := provider.Create(context.Background(), nodeClaim, nodeClass, "XS")
	<-done
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, ng); err != nil {
		t.Fatalf("expected nodegroup to exist: %v", err)
	}
	wantLabels := map[string]string{
		v1alpha1.ManagedLabelKey:   "true",
		v1alpha1.NodeClaimLabelKey: "default-abc12",
		v1alpha1.NodePoolLabelKey:  "default",
		v1alpha1.NodeClassLabelKey: "default",
	}
	for k, want := range wantLabels {
		if got := ng.Labels[k]; got != want {
			t.Errorf("label %s = %q, want %q", k, got, want)
		}
	}
	if got := ng.Annotations[v1alpha1.NodeClassHashLabelKey]; got != nodeClass.Hash() {
		t.Errorf("nodeclass hash annotation = %q, want %q", got, nodeClass.Hash())
	}
	if len(ng.OwnerReferences) != 1 {
		t.Fatalf("expected exactly one owner reference, got %+v", ng.OwnerReferences)
	}
	ref := ng.OwnerReferences[0]
	if ref.APIVersion != "karpenter.sh/v1" || ref.Kind != "NodeClaim" || ref.Name != nodeClaim.Name || ref.UID != nodeClaim.UID {
		t.Errorf("unexpected owner reference %+v", ref)
	}
	if ng.Spec.NodeCount != 1 {
		t.Errorf("nodeCount = %d, want 1", ng.Spec.NodeCount)
	}
	if ng.Spec.Flavor != "XS" {
		t.Errorf("flavor = %q, want %q", ng.Spec.Flavor, "XS")
	}
	if len(ng.Spec.Taints) != 1 || ng.Spec.Taints[0].Key != karpv1.UnregisteredTaintKey || ng.Spec.Taints[0].Effect != corev1.TaintEffectNoExecute {
		t.Errorf("expected single unregistered NoExecute taint, got %+v", ng.Spec.Taints)
	}
}

// TestResolveAndCreateBuildsWhatTheResolverDecides pins the contract of the
// resolver cloudprovider.Create decides its launch with once it holds the
// creation lock: its error fails the launch as it is, with nothing created,
// and the NodeGroup is built from the NodeClass and the flavor it returns.
func TestResolveAndCreateBuildsWhatTheResolverDecides(t *testing.T) {
	provider, kubeClient := newTestProvider(t)
	ctx := context.Background()

	unwanted := testNodeClaim("default-gone1")
	errGone := errors.New("nodeclaim default-gone1 is being deleted")
	_, err := provider.ResolveAndCreate(ctx, unwanted, func(context.Context) (*v1alpha1.CleverNodeClass, string, error) {
		return nil, "", errGone
	})
	if !errors.Is(err, errGone) {
		t.Fatalf("want the resolver's error as it is, got %T: %v", err, err)
	}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: unwanted.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("a failed resolution must create nothing, got %v", err)
	}

	nodeClaim := testNodeClaim("default-rslv1")
	nodeClass := testNodeClass("resolved")
	done := acceptOnceCreated(t, kubeClient, nodeClaim.Name)
	ng, err := provider.ResolveAndCreate(ctx, nodeClaim, func(context.Context) (*v1alpha1.CleverNodeClass, string, error) {
		return nodeClass, "S", nil
	})
	<-done
	if err != nil {
		t.Fatalf("ResolveAndCreate: %v", err)
	}
	if ng.Spec.Flavor != "S" || ng.Labels[v1alpha1.NodeClassLabelKey] != nodeClass.Name {
		t.Errorf("nodegroup built with flavor %q from nodeclass %q, want S from %q", ng.Spec.Flavor, ng.Labels[v1alpha1.NodeClassLabelKey], nodeClass.Name)
	}
}

func TestCreateFiltersReservedNodeGroupLabels(t *testing.T) {
	provider, kubeClient := newTestProvider(t)
	nodeClass := testNodeClass("default")
	nodeClass.Spec.Labels = map[string]string{
		"team":                        "data",
		"kubernetes.io/x":             "1",
		"node.kubernetes.io/y":        "1",
		"clever-cloud.com/z":          "1",
		"topology.kubernetes.io/zone": "par",
		"build":                       strings.Repeat("a", 64), // values longer than 63 chars are rejected upstream
	}
	nodeClaim := testNodeClaim("default-lbl01")
	nodeClaim.Labels["app"] = "web"

	done := acceptOnceCreated(t, kubeClient, nodeClaim.Name)
	ng, err := provider.Create(context.Background(), nodeClaim, nodeClass, "2XS")
	<-done
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// karpenter.sh/nodepool, stamped on every NodeClaim, is filtered too: the
	// node receives it from karpenter-core's registration sync.
	want := map[string]string{
		"team": "data",
		"app":  "web",
	}
	if len(ng.Spec.Labels) != len(want) {
		t.Errorf("expected exactly %d nodegroup labels, got %+v", len(want), ng.Spec.Labels)
	}
	for k, v := range want {
		if got := ng.Spec.Labels[k]; got != v {
			t.Errorf("nodegroup label %s = %q, want %q", k, got, v)
		}
	}
	// The stamped hash describes what the NodeClass put in the payload, not
	// its raw labels: dropping a filtered key later must not drift the node.
	delivered := testNodeClass("default")
	delivered.Spec.Labels = map[string]string{"team": "data"}
	if got := ng.Annotations[v1alpha1.NodeClassHashLabelKey]; got != delivered.Hash() {
		t.Errorf("nodeclass hash annotation = %q, want %q (the hash of the delivered labels only)", got, delivered.Hash())
	}
}

// TestCreateKeepsKarpenterLabelsOutOfTheNodeGroup pins the payload against the
// labels karpenter-core actually stamps on a NodeClaim at launch. spec.labels
// is immutable and the platform applies it to EVERY node of the group: when
// it carried karpenter.sh/nodepool, the extra node of an externally resized
// group joined with that label and no provider ID (the providerid controller
// deliberately leaves it unstamped), karpenter-core's cluster state ignored
// it, and its first sync after the next controller restart never completed —
// no provisioning and no disruption, cluster-wide, until the resize was
// reverted. No karpenter.sh key may reach the payload; the provider's own
// domain, which core also stamps (the nodeclass label), is not core's.
func TestCreateKeepsKarpenterLabelsOutOfTheNodeGroup(t *testing.T) {
	provider, kubeClient := newTestProvider(t)
	nodeClaim := testNodeClaim("default-core1")
	nodeClassLabel := karpv1.NodeClassLabelKey(schema.GroupKind{Group: "karpenter.clever-cloud.com", Kind: "CleverNodeClass"})
	for k, v := range map[string]string{
		karpv1.CapacityTypeLabelKey:    karpv1.CapacityTypeOnDemand,
		corev1.LabelInstanceTypeStable: "2XS",
		corev1.LabelTopologyZone:       "par",
		corev1.LabelArchStable:         "amd64",
		nodeClassLabel:                 "default",
		"team":                         "data",
	} {
		nodeClaim.Labels[k] = v
	}

	done := acceptOnceCreated(t, kubeClient, nodeClaim.Name)
	ng, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
	<-done
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	for k, v := range ng.Spec.Labels {
		if domain := karpv1.GetLabelDomain(k); domain == "karpenter.sh" || strings.HasSuffix(domain, ".karpenter.sh") {
			t.Errorf("nodegroup spec.labels carries karpenter-core's %s=%s: the platform applies it to every node "+
				"of the group, including nodes that never get a provider id", k, v)
		}
	}
	for _, k := range []string{"team", nodeClassLabel} {
		if _, ok := ng.Spec.Labels[k]; !ok {
			t.Errorf("nodegroup spec.labels lost %s, got %+v", k, ng.Spec.Labels)
		}
	}
}

func TestCreateReusesExistingManagedNodeGroup(t *testing.T) {
	// Seeded already Synced: waitForAcceptance returns on its first poll, no
	// operator-simulating goroutine needed.
	existing := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-reuse",
			Labels: map[string]string{
				v1alpha1.ManagedLabelKey:   "true",
				v1alpha1.NodeClaimLabelKey: "default-reuse",
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: "default-reuse"},
			},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "M", NodeCount: 1},
		Status: ngv1.NodeGroupStatus{
			Conditions: syncedConditions(),
			Phase:      ngv1.PhaseSynced,
		},
	}
	provider, _ := newTestProvider(t, existing)

	ng, err := provider.Create(context.Background(), testNodeClaim("default-reuse"), testNodeClass("default"), "2XS")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// The existing group is returned as-is: the requested flavor is ignored.
	if ng.Spec.Flavor != "M" {
		t.Errorf("flavor = %q, want existing %q", ng.Spec.Flavor, "M")
	}
}

func TestCreateRejectsForeignNodeGroup(t *testing.T) {
	t.Run("unmanaged nodegroup with the same name", func(t *testing.T) {
		existing := &ngv1.NodeGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "user-pool"},
			Spec:       ngv1.NodeGroupSpec{Flavor: "M", NodeCount: 3},
		}
		provider, _ := newTestProvider(t, existing)
		_, err := provider.Create(context.Background(), testNodeClaim("user-pool"), testNodeClass("default"), "2XS")
		if err == nil || !strings.Contains(err.Error(), "not managed") {
			t.Fatalf("expected 'not managed' error, got %v", err)
		}
	})
	t.Run("managed nodegroup owned by another nodeclaim", func(t *testing.T) {
		existing := &ngv1.NodeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "default-other",
				Labels: map[string]string{
					v1alpha1.ManagedLabelKey:   "true",
					v1alpha1.NodeClaimLabelKey: "some-other-claim",
				},
			},
			Spec: ngv1.NodeGroupSpec{Flavor: "M", NodeCount: 1},
		}
		provider, _ := newTestProvider(t, existing)
		_, err := provider.Create(context.Background(), testNodeClaim("default-other"), testNodeClass("default"), "2XS")
		if err == nil || !strings.Contains(err.Error(), "not managed") {
			t.Fatalf("expected 'not managed' error, got %v", err)
		}
	})
	t.Run("labeled nodegroup without the nodeclaim owner reference", func(t *testing.T) {
		// Matching labels but no ownership proof — a hand-copied manifest
		// must not be adopted (it would later be destroyed at deprovisioning).
		existing := &ngv1.NodeGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "default-copied",
				Labels: map[string]string{
					v1alpha1.ManagedLabelKey:   "true",
					v1alpha1.NodeClaimLabelKey: "default-copied",
				},
			},
			Spec: ngv1.NodeGroupSpec{Flavor: "M", NodeCount: 1},
		}
		provider, _ := newTestProvider(t, existing)
		_, err := provider.Create(context.Background(), testNodeClaim("default-copied"), testNodeClass("default"), "2XS")
		if err == nil || !strings.Contains(err.Error(), "not managed") {
			t.Fatalf("expected 'not managed' error, got %v", err)
		}
	})
}

func TestCreateQuotaRejectionCleansUpAndReturnsTypedError(t *testing.T) {
	provider, kubeClient, recorder := newTestProviderWithRecorder(t)
	nodeClaim := testNodeClaim("default-quota")
	message := "Quota exceeded: RAM max reached"
	rejectionsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_quota_rejections_total")

	done := rejectOnceCreated(t, kubeClient, nodeClaim.Name, message)
	_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
	<-done
	var quotaErr *nodegroup.ErrQuotaExceeded
	if !errors.As(err, &quotaErr) {
		t.Fatalf("expected *ErrQuotaExceeded, got %T: %v", err, err)
	}
	if quotaErr.Message != message {
		t.Errorf("quota message = %q, want %q", quotaErr.Message, message)
	}
	// The rejected NodeGroup must have been deleted to free the reservation.
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected quota-rejected nodegroup to be deleted, got %v", err)
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_quota_rejections_total") - rejectionsBefore; delta != 1 {
		t.Errorf("quota_rejections_total delta = %v, want 1", delta)
	}
	if !slices.Contains(recorder.reasons(), "NodeGroupQuotaExceeded") {
		t.Errorf("expected a NodeGroupQuotaExceeded event on the nodeclaim, got %v", recorder.reasons())
	}
}

func TestCreateAcceptanceTimeoutProceedsOptimisticallyAndSurfaces(t *testing.T) {
	prev := nodegroup.SetQuotaCheckTimeout(300 * time.Millisecond)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	provider, _, recorder := newTestProviderWithRecorder(t)
	timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

	// Nothing plays the operator: the group never gets a status, so the
	// operator never acknowledges it, the poll times out, and Create must
	// still succeed (optimistic launch) while surfacing the timeout through
	// the counter and a NodeClaim event.
	ng, err := provider.Create(context.Background(), testNodeClaim("default-slow"), testNodeClass("default"), "2XS")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if ng.Name != "default-slow" {
		t.Fatalf("nodegroup name = %q, want %q", ng.Name, "default-slow")
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 1 {
		t.Errorf("acceptance_timeouts_total delta = %v, want 1", delta)
	}
	if !slices.Contains(recorder.reasons(), "NodeGroupAcceptanceTimeout") {
		t.Errorf("expected a NodeGroupAcceptanceTimeout event on the nodeclaim, got %v", recorder.reasons())
	}
}

// TestCreateReturnsOnTheOperatorsDecision pins what the acceptance poll waits
// for: the operator's first status write, which is its decision, not the group
// turning Ready. Live, an accepted group gets phase Creating +
// ReconcileInProgress about 1 s after its creation and Ready only at 38-58 s,
// while a quota rejection is written directly. Waiting for Ready outlasted the
// 15 s window on every healthy launch, so each one ran the full window and
// counted as an acceptance timeout.
func TestCreateReturnsOnTheOperatorsDecision(t *testing.T) {
	// Far longer than the decision takes: a poll that waits the window out
	// fails the elapsed-time checks below.
	const window = 10 * time.Second
	prev := nodegroup.SetQuotaCheckTimeout(window)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	t.Run("acknowledged", func(t *testing.T) {
		provider, kubeClient, recorder := newTestProviderWithRecorder(t)
		nodeClaim := testNodeClaim("default-acked")
		timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

		done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, acknowledgedStatus())
		start := time.Now()
		ng, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
		elapsed := time.Since(start)
		<-done
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if elapsed >= window/2 {
			t.Errorf("Create took %s: it must return on the acknowledgement, not wait for Ready", elapsed)
		}
		if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: ng.Name}, &ngv1.NodeGroup{}); err != nil {
			t.Errorf("an acknowledged group must be kept: %v", err)
		}
		// A timeout is optimistic success too, so err alone proves nothing.
		if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 0 {
			t.Errorf("acceptance_timeouts_total delta = %v, want 0: the operator acknowledged the group", delta)
		}
		if len(recorder.reasons()) != 0 {
			t.Errorf("a healthy launch must publish no event, got %v", recorder.reasons())
		}
	})

	quotaCases := map[string]ngv1.NodeGroupStatus{
		// The live shape: written directly as the first status, with an empty
		// message.
		"quota rejected": quotaRejectedStatus(),
		// Not measured live, where a quota rejection is never preceded by
		// Creating: an acknowledgement next to it must not outrank it, or the
		// launch would proceed on a group the quota engine rejected, holding
		// its reservation until the registration TTL.
		"quota rejected while reconciling": func() ngv1.NodeGroupStatus {
			status := quotaRejectedStatus()
			status.Conditions = append(status.Conditions, condTrue(ngv1.ConditionTypeReconcileInProgress, "Creating", ""))
			return status
		}(),
	}
	for name, status := range quotaCases {
		t.Run(name, func(t *testing.T) {
			provider, kubeClient, recorder := newTestProviderWithRecorder(t)
			nodeClaim := testNodeClaim("default-quota-live")
			rejectionsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_quota_rejections_total")

			done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, status)
			start := time.Now()
			_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
			elapsed := time.Since(start)
			<-done
			var quotaErr *nodegroup.ErrQuotaExceeded
			if !errors.As(err, &quotaErr) {
				t.Fatalf("expected *ErrQuotaExceeded, got %T: %v", err, err)
			}
			if elapsed >= window/2 {
				t.Errorf("Create took %s: it must return on the rejection", elapsed)
			}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
				t.Errorf("expected the quota-rejected nodegroup to be deleted, got %v", err)
			}
			if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_quota_rejections_total") - rejectionsBefore; delta != 1 {
				t.Errorf("quota_rejections_total delta = %v, want 1", delta)
			}
			if !slices.Contains(recorder.reasons(), "NodeGroupQuotaExceeded") {
				t.Errorf("expected a NodeGroupQuotaExceeded event on the nodeclaim, got %v", recorder.reasons())
			}
		})
	}
}

// TestCreateTerminatingGroupIsVanishing covers a group seen with a deletion
// timestamp: whatever its status still says, it is going away, and
// cloudprovider.Get already reads it as gone. On the AlreadyExists adoption
// path such a group can still carry ReconcileInProgress (acknowledged) or
// Ready; accepting it would launch the claim on a group that will not exist
// and release the hold-out of a flavor nothing runs. It fails the launch as a
// vanish does.
func TestCreateTerminatingGroupIsVanishing(t *testing.T) {
	// Short, so a regression that waits the group out fails on the error
	// assertion below without costing the full window.
	prev := nodegroup.SetQuotaCheckTimeout(3 * time.Second)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	cases := map[string]ngv1.NodeGroupStatus{
		"acknowledged": acknowledgedStatus(),
		"ready":        syncedStatus(),
	}
	for name, status := range cases {
		t.Run(name, func(t *testing.T) {
			provider, kubeClient, recorder := newTestProviderWithRecorder(t)
			// Hold S out, so the test can tell whether Create released it.
			refused := testNodeClaim("default-refused")
			done := failOnceCreated(t, kubeClient, refused.Name, "FlavorNotAvailable", "nope")
			_, _ = provider.Create(context.Background(), refused, testNodeClass("default"), "S")
			<-done
			if _, held := provider.RejectedFlavors()["S"]; !held {
				t.Fatalf("expected S to be held out, got %v", provider.RejectedFlavors())
			}

			// The Clever Cloud finalizer keeps a deleted group around with a
			// deletion timestamp.
			terminating := ownedNodeGroup("default-adopt", "S", status)
			terminating.Finalizers = []string{"api.clever-cloud.com/finalizer"}
			if err := kubeClient.Create(context.Background(), terminating); err != nil {
				t.Fatalf("seeding the terminating nodegroup: %v", err)
			}
			if err := kubeClient.Delete(context.Background(), terminating); err != nil {
				t.Fatalf("deleting the seeded nodegroup: %v", err)
			}
			vanishedBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_vanished_total")
			timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

			_, err := provider.Create(context.Background(), testNodeClaim("default-adopt"), testNodeClass("default"), "2XS")
			if !errors.Is(err, nodegroup.ErrNodeGroupVanished) {
				t.Fatalf("expected ErrNodeGroupVanished for a terminating group, got %T: %v", err, err)
			}
			if _, held := provider.RejectedFlavors()["S"]; !held {
				t.Errorf("a terminating group must not release the hold-out of its flavor, got %v", provider.RejectedFlavors())
			}
			if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_vanished_total") - vanishedBefore; delta != 1 {
				t.Errorf("nodegroup_vanished_total delta = %v, want 1", delta)
			}
			if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 0 {
				t.Errorf("acceptance_timeouts_total delta = %v, want 0: the group was seen going away, not left unacknowledged", delta)
			}
			if !slices.Contains(recorder.reasons(), "NodeGroupVanished") {
				t.Errorf("expected a NodeGroupVanished event, got %v", recorder.reasons())
			}
		})
	}
}

// TestCreateReleasesTheLockOnAcknowledgement measures how long createMu is
// held. It serializes creations so that the upstream quota engine evaluates
// one new group at a time, and the operator's first status write, about 1 s
// after the creation, is that evaluation. Held until Ready instead, which
// takes 38-58 s live, it outlasted the 15 s window on every healthy launch:
// launches ran 15 s apart, and karpenter-core, whose cluster state stays
// unsynced while any NodeClaim has no provider ID, paused disruption
// cluster-wide for as long as the queue lasted. The lock must be held until
// the decision, and no longer.
func TestCreateReleasesTheLockOnAcknowledgement(t *testing.T) {
	// Far longer than the acknowledgement: a lock held for the window fails
	// the bound below.
	prev := nodegroup.SetQuotaCheckTimeout(20 * time.Second)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	const (
		first  = "default-first"
		second = "default-second"
		// The operator acknowledges the first group on this poll of it (the
		// poll runs at once, then every second), and the second at once.
		acknowledgedOnPoll = 3
	)
	var (
		mu            sync.Mutex
		polls         = map[string]int{}
		posted        = map[string]time.Time{}
		firstDecision time.Time
	)
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				mu.Lock()
				posted[obj.GetName()] = time.Now()
				mu.Unlock()
				return c.Create(ctx, obj, opts...)
			},
			// Plays the operator on the reads of the acceptance poll.
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				ng, ok := obj.(*ngv1.NodeGroup)
				if !ok {
					return nil
				}
				mu.Lock()
				defer mu.Unlock()
				polls[key.Name]++
				if key.Name == second || polls[key.Name] >= acknowledgedOnPoll {
					ng.Status = acknowledgedStatus()
					if key.Name == first && firstDecision.IsZero() {
						firstDecision = time.Now()
					}
				}
				return nil
			},
		}).
		Build()
	provider := nodegroup.NewProvider(kubeClient, &fakeRecorder{}, catalogue, clock.RealClock{})
	timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

	firstErr := make(chan error, 1)
	go func() {
		_, err := provider.Create(context.Background(), testNodeClaim(first), testNodeClass("default"), "2XS")
		firstErr <- err
	}()
	// The second launch queues on the lock while the first holds it.
	for {
		mu.Lock()
		_, created := posted[first]
		mu.Unlock()
		if created {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := provider.Create(context.Background(), testNodeClaim(second), testNodeClass("default"), "2XS"); err != nil {
		t.Fatalf("second Create: %v", err)
	}
	if err := <-firstErr; err != nil {
		t.Fatalf("first Create: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	// Still serialized: the quota engine must not evaluate both groups at once.
	if !posted[second].After(firstDecision) {
		t.Errorf("the second nodegroup was created before the operator decided on the first")
	}
	// Released on that decision: the first poll ends there, and the second
	// creation follows it instead of waiting for the window.
	if polls[first] != acknowledgedOnPoll {
		t.Errorf("the first nodegroup was polled %d times, want %d: the poll must end on the acknowledgement", polls[first], acknowledgedOnPoll)
	}
	if held := posted[second].Sub(posted[first]); held > 5*time.Second {
		t.Errorf("the second creation waited %s behind a group acknowledged on poll %d: createMu must be released on the acknowledgement", held, acknowledgedOnPoll)
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 0 {
		t.Errorf("acceptance_timeouts_total delta = %v, want 0: both groups were acknowledged", delta)
	}
}

func TestCreateParentCancellationIsNotAnAcceptanceTimeout(t *testing.T) {
	prev := nodegroup.SetQuotaCheckTimeout(5 * time.Second)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	provider, _, recorder := newTestProviderWithRecorder(t)
	timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

	// Controller shutdown mid-poll must surface as an error, not as the
	// operator-down signal: no counter tick, no Warning event.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	if _, err := provider.Create(ctx, testNodeClaim("default-cancel"), testNodeClass("default"), "2XS"); err == nil {
		t.Fatal("expected an error when the parent context is cancelled mid-poll")
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 0 {
		t.Errorf("acceptance_timeouts_total delta = %v, want 0 on parent cancellation", delta)
	}
	if slices.Contains(recorder.reasons(), "NodeGroupAcceptanceTimeout") {
		t.Error("parent cancellation must not publish a NodeGroupAcceptanceTimeout event")
	}
}

func TestQuotaBackoffFailsFastUntilDelete(t *testing.T) {
	existing := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "default-old01",
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true"},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1},
	}
	provider, kubeClient := newTestProvider(t, existing)
	nodeClass := testNodeClass("default")

	first := testNodeClaim("default-quot1")
	done := rejectOnceCreated(t, kubeClient, first.Name, "Quota exceeded: vCPU max")
	_, err := provider.Create(context.Background(), first, nodeClass, "2XS")
	<-done
	var quotaErr *nodegroup.ErrQuotaExceeded
	if !errors.As(err, &quotaErr) {
		t.Fatalf("expected *ErrQuotaExceeded, got %T: %v", err, err)
	}

	// Within the backoff window the next Create fails fast without touching
	// the API, so no operator-simulating goroutine is needed.
	second := testNodeClaim("default-quot2")
	if _, err := provider.Create(context.Background(), second, nodeClass, "2XS"); !errors.As(err, &quotaErr) {
		t.Fatalf("expected fast *ErrQuotaExceeded during backoff, got %T: %v", err, err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: second.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected no nodegroup to be created during backoff, got %v", err)
	}

	// Deleting a NodeGroup frees capacity and clears the backoff: every
	// flavor is available again.
	if err := provider.Delete(context.Background(), existing); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for _, flavor := range []string{"2XS", "XL"} {
		if provider.Unavailable(flavor) {
			t.Errorf("%s still unavailable after capacity was freed", flavor)
		}
	}
	third := testNodeClaim("default-quot3")
	done = acceptOnceCreated(t, kubeClient, third.Name)
	_, err = provider.Create(context.Background(), third, nodeClass, "2XS")
	<-done
	if err != nil {
		t.Fatalf("expected Create to succeed after capacity freed, got %v", err)
	}
}

// TestQuotaRejectionCoversOnlyFlavorsAtLeastAsLarge pins the size-aware
// backoff. The quota counts vCPUs and memory, so capacity it cannot fit S into
// cannot fit a flavor at least as large either, but may still fit XS or 2XS.
// The backoff used to be global: after one rejection it failed every launch,
// the smallest included, so pods that fit the remaining quota stayed Pending
// while karpenter-core re-planned the same over-quota claim on every pass.
func TestQuotaRejectionCoversOnlyFlavorsAtLeastAsLarge(t *testing.T) {
	provider, kubeClient := newTestProvider(t)
	nodeClass := testNodeClass("default")
	ctx := context.Background()

	rejected := testNodeClaim("default-quots")
	done := rejectOnceCreated(t, kubeClient, rejected.Name, "")
	_, err := provider.Create(ctx, rejected, nodeClass, "S")
	<-done
	var quotaErr *nodegroup.ErrQuotaExceeded
	if !errors.As(err, &quotaErr) || quotaErr.Flavor != "S" {
		t.Fatalf("expected *ErrQuotaExceeded for S, got %T: %v", err, err)
	}
	for flavor, want := range map[string]bool{"2XS": false, "XS": false, "S": true, "M": true, "L": true, "XL": true} {
		if got := provider.Unavailable(flavor); got != want {
			t.Errorf("Unavailable(%s) = %v after a quota rejection of S, want %v", flavor, got, want)
		}
	}

	// A larger flavor fails fast, naming the rejection that covers it,
	// without reaching the API.
	larger := testNodeClaim("default-quotl")
	_, err = provider.Create(ctx, larger, nodeClass, "XL")
	if !errors.As(err, &quotaErr) {
		t.Fatalf("expected a fast *ErrQuotaExceeded for XL, got %T: %v", err, err)
	}
	if quotaErr.Flavor != "XL" || !strings.Contains(quotaErr.Message, "rejected S, and it is at least as large") {
		t.Errorf("fast-fail does not name the covering rejection: %+v", quotaErr)
	}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: larger.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected no nodegroup created for a covered flavor, got %v", err)
	}

	// A smaller flavor is not failed by it: it goes to the operator. The
	// error is checked first: a fast-fail never creates the group the
	// operator goroutine waits for.
	smaller := testNodeClaim("default-quotx")
	done = acceptOnceCreated(t, kubeClient, smaller.Name)
	if _, err := provider.Create(ctx, smaller, nodeClass, "XS"); err != nil {
		t.Fatalf("a flavor smaller than the rejected one must reach the API, got %v", err)
	}
	<-done
}

// TestQuotaRejectionSizesFlavorsByTheirCatalogueEntries pins what "at least as
// large" means: at least as many vCPUs AND at least as much memory, as the
// catalogue entries (overrides included) advertise. A flavor with fewer vCPUs
// but more memory may fit a quota that refused the other on vCPUs, so it stays
// available; a flavor the catalogue cannot size is covered by its own
// rejection only, and so is every flavor when there is no catalogue at all —
// which the event reporting the rejection says instead of claiming larger
// flavors it does not cover.
func TestQuotaRejectionSizesFlavorsByTheirCatalogueEntries(t *testing.T) {
	cpu, memoryKi := int64(4), int64(30000000)
	highMem := instancetype.NewProvider("par", nil, []instancetype.FlavorOverride{{Name: "HIGHMEM", CPU: &cpu, MemoryKi: &memoryKi}})

	// The event reporting the rejection says what it does: to the flavors at
	// least as large when the rejected one is sized, to it alone otherwise.
	const (
		sizedConsequence   = "every other flavor at least as large for up to 2m0s"
		unsizedConsequence = "the catalogue cannot size it, so no other flavor is affected"
	)
	for _, tc := range []struct {
		name        string
		catalogue   nodegroup.Catalogue
		rejected    string
		want        map[string]bool
		consequence string
	}{
		{
			name:        "fewer vCPUs, more memory",
			catalogue:   highMem,
			rejected:    "S",
			want:        map[string]bool{"HIGHMEM": false, "XS": false, "M": true},
			consequence: sizedConsequence,
		},
		{
			name:        "an override flavor covers the flavors at least as large as its entry",
			catalogue:   highMem,
			rejected:    "HIGHMEM",
			want:        map[string]bool{"HIGHMEM": true, "XL": true, "L": false, "2XS": false},
			consequence: sizedConsequence,
		},
		{
			name:        "a flavor the catalogue cannot size",
			catalogue:   highMem,
			rejected:    "GPU",
			want:        map[string]bool{"GPU": true, "XL": false, "2XS": false},
			consequence: unsizedConsequence,
		},
		{
			name:        "no catalogue",
			rejected:    "M",
			want:        map[string]bool{"M": true, "L": false, "XL": false, "2XS": false},
			consequence: unsizedConsequence,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
			recorder := &fakeRecorder{}
			provider := nodegroup.NewProvider(kubeClient, recorder, tc.catalogue, clock.RealClock{})
			provider.RecordLateRefusal(testNodeClaim("default-sized"), ownedNodeGroup("default-sized", tc.rejected, quotaRejectedStatus()))
			for flavor, want := range tc.want {
				if got := provider.Unavailable(flavor); got != want {
					t.Errorf("Unavailable(%s) = %v after a quota rejection of %s, want %v", flavor, got, tc.rejected, want)
				}
			}
			if len(recorder.events) != 1 || !strings.Contains(recorder.events[0].Message, tc.consequence) {
				t.Errorf("want one event saying %q, got %+v", tc.consequence, recorder.events)
			}
		})
	}
}

// TestQuotaRejectionReopensTheRejectedFlavorFirst pins when a quota
// rejection stops covering each flavor, which bounds what a quota that stays
// exhausted costs. Pending pods that fit several sizes walk down the
// catalogue within one window: L, then XS, then 2XS rejected. Every flavor
// used to reopen at the same moment — the 2XS rejection covers every other
// flavor and expired with them — so core packed the pods back onto an L and
// walked down again, one real rejection per flavor size in every window. Now
// the rejected flavor reopens alone after QuotaBackoff, the cheapest probe of
// whether capacity came back, and the flavors at least as large a window
// later, unless that probe was rejected in turn.
func TestQuotaRejectionReopensTheRejectedFlavorFirst(t *testing.T) {
	provider, kubeClient, _, clk := newClockedTestProvider(t)
	nodeClass := testNodeClass("default")
	ctx := context.Background()
	reject := func(name, flavor string) {
		t.Helper()
		done := rejectOnceCreated(t, kubeClient, name, "")
		_, err := provider.Create(ctx, testNodeClaim(name), nodeClass, flavor)
		<-done
		var quotaErr *nodegroup.ErrQuotaExceeded
		if !errors.As(err, &quotaErr) {
			t.Fatalf("expected the quota to reject %s, got %T: %v", flavor, err, err)
		}
	}
	requireUnavailable := func(stage string, available ...string) {
		t.Helper()
		for _, f := range instancetype.DefaultFlavors {
			if got, want := provider.Unavailable(f.Name), !slices.Contains(available, f.Name); got != want {
				t.Errorf("%s: Unavailable(%s) = %v, want %v", stage, f.Name, got, want)
			}
		}
	}

	reject("default-walkl", "L")
	clk.Step(time.Second)
	reject("default-walkx", "XS")
	clk.Step(time.Second)
	reject("default-walk2", "2XS")
	requireUnavailable("after the walk down")

	// The own windows of L and XS have passed as well, but the 2XS rejection
	// still covers them: 2XS reopens alone, and a larger flavor still fails
	// fast, naming it.
	clk.Step(nodegroup.QuotaBackoff)
	requireUnavailable("one window after the walk down", "2XS")
	var quotaErr *nodegroup.ErrQuotaExceeded
	if _, err := provider.Create(ctx, testNodeClaim("default-walkb"), nodeClass, "L"); !errors.As(err, &quotaErr) ||
		!strings.Contains(quotaErr.Message, "rejected 2XS") {
		t.Fatalf("expected L to fail fast on the 2XS rejection, got %T: %v", err, err)
	}

	// The 2XS probe is rejected again: the larger flavors stay covered.
	reject("default-prob1", "2XS")
	requireUnavailable("after the probe was rejected")
	clk.Step(nodegroup.QuotaBackoff)
	requireUnavailable("one window after the probe", "2XS")

	// Nothing rejected since: everything reopens a window later, and a launch
	// of the largest flavor reaches the API again.
	clk.Step(nodegroup.QuotaBackoff)
	requireUnavailable("two windows after the probe", "2XS", "XS", "S", "M", "L", "XL")
	next := testNodeClaim("default-walkn")
	done := acceptOnceCreated(t, kubeClient, next.Name)
	if _, err := provider.Create(ctx, next, nodeClass, "XL"); err != nil {
		t.Fatalf("expected Create to reach the API once the backoff expired, got %v", err)
	}
	<-done
}

// TestRefusedFlavorIsUnavailableForItsHoldOut pins the hold-out as the
// scheduler sees it. A refusal is a verdict on the flavor, not on capacity:
// it makes that flavor unavailable, and no other, until the hold-out expires.
func TestRefusedFlavorIsUnavailableForItsHoldOut(t *testing.T) {
	provider, kubeClient, _, clk := newClockedTestProvider(t)

	refused := testNodeClaim("default-refm")
	done := failOnceCreated(t, kubeClient, refused.Name, "FlavorNotAvailable", "nope")
	_, err := provider.Create(context.Background(), refused, testNodeClass("default"), "M")
	<-done
	var rejectedErr *nodegroup.ErrFlavorRejected
	if !errors.As(err, &rejectedErr) {
		t.Fatalf("expected *ErrFlavorRejected, got %T: %v", err, err)
	}
	for flavor, want := range map[string]bool{"M": true, "L": false, "S": false} {
		if got := provider.Unavailable(flavor); got != want {
			t.Errorf("Unavailable(%s) = %v after a refusal of M, want %v", flavor, got, want)
		}
	}

	clk.Step(nodegroup.FlavorBackoff - time.Second)
	if !provider.Unavailable("M") {
		t.Error("expected M to stay unavailable until its hold-out expires")
	}
	clk.Step(time.Second)
	if provider.Unavailable("M") {
		t.Error("expected M available again once its hold-out expired")
	}
}

// TestQuotaBackoffExpires pins both windows of a quota rejection to the
// provider's clock, to the second: the rejected flavor is unavailable, and
// fails fast, for QuotaBackoff; every flavor at least as large for twice as
// long; a smaller one never. Nothing but freed capacity clears them earlier,
// and nothing may keep them longer: a backoff that never expired would keep
// those flavors out of every launch until a NodeGroup happened to be deleted.
func TestQuotaBackoffExpires(t *testing.T) {
	provider, _, _, clk := newClockedTestProvider(t)
	provider.RecordLateRefusal(testNodeClaim("default-quotx"), ownedNodeGroup("default-quotx", "S", quotaRejectedStatus()))
	requireAvailability := func(stage string, want map[string]bool) {
		t.Helper()
		for flavor, unavailable := range want {
			if got := provider.Unavailable(flavor); got != unavailable {
				t.Errorf("%s: Unavailable(%s) = %v, want %v", stage, flavor, got, unavailable)
			}
		}
	}

	clk.Step(nodegroup.QuotaBackoff - time.Second)
	requireAvailability("just before the first window ends", map[string]bool{"XS": false, "S": true, "XL": true})
	var quotaErr *nodegroup.ErrQuotaExceeded
	if _, err := provider.Create(context.Background(), testNodeClaim("default-quots"), testNodeClass("default"), "S"); !errors.As(err, &quotaErr) {
		t.Errorf("expected S to fail fast within its window, got %T: %v", err, err)
	}
	clk.Step(time.Second)
	requireAvailability("once the first window ended", map[string]bool{"XS": false, "S": false, "XL": true})
	clk.Step(nodegroup.QuotaBackoff - time.Second)
	requireAvailability("just before the second window ends", map[string]bool{"S": false, "M": true, "XL": true})
	clk.Step(time.Second)
	requireAvailability("once the second window ended", map[string]bool{"XS": false, "S": false, "M": false, "XL": false})
}

// quotaRejectedStatus is the live shape of a quota rejection: the operator's
// first status write, with an empty message.
func quotaRejectedStatus() ngv1.NodeGroupStatus {
	return ngv1.NodeGroupStatus{
		Phase:      ngv1.PhaseQuotaExceeded,
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonQuotaExceeded, "")},
	}
}

func TestDeleteKeepsTheQuotaBackoffForARefusedGroup(t *testing.T) {
	// A group the quota engine rejected held no capacity. Deleting it, as
	// karpenter-core's termination does once the nodegroupstatus controller
	// has failed its launch, must leave armed the backoff its rejection armed.
	rejected := ownedNodeGroup("default-late1", "2XS", quotaRejectedStatus())
	// Nor did a group the operator refused for any other reason before it
	// synced. Deleting it must not disarm a backoff another claim's quota
	// rejection armed: the next Create would go back into the exhausted quota.
	refused := ownedNodeGroup("default-late2", "M", ngv1.NodeGroupStatus{
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, "FlavorUnavailable", "flavor M is not available")},
	})
	// A running group whose later scale-up the quota refused is still Ready:
	// its VM holds capacity, and deleting it frees that capacity.
	scaled := ownedNodeGroup("default-run01", "2XS", ngv1.NodeGroupStatus{
		Phase: ngv1.PhaseQuotaExceeded,
		Conditions: []ngv1.NodeGroupCondition{
			condTrue(ngv1.ConditionTypeReady, "Synced", ""),
			condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonQuotaExceeded, ""),
		},
	})
	provider, kubeClient := newTestProvider(t, rejected, refused, scaled)
	nodeClass := testNodeClass("default")

	provider.RecordLateRefusal(testNodeClaim(rejected.Name), rejected)
	if err := provider.Delete(context.Background(), rejected); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: rejected.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected the rejected nodegroup deleted, got %v", err)
	}
	var quotaErr *nodegroup.ErrQuotaExceeded
	if _, err := provider.Create(context.Background(), testNodeClaim("default-next1"), nodeClass, "2XS"); !errors.As(err, &quotaErr) {
		t.Fatalf("expected the backoff to survive the deletion of the group the quota rejected, got %T: %v", err, err)
	}

	if err := provider.Delete(context.Background(), refused); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: refused.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected the refused nodegroup deleted, got %v", err)
	}
	if _, err := provider.Create(context.Background(), testNodeClaim("default-next3"), nodeClass, "2XS"); !errors.As(err, &quotaErr) {
		t.Fatalf("expected the backoff to survive the deletion of a group refused before it synced, got %T: %v", err, err)
	}

	if err := provider.Delete(context.Background(), scaled); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	next := testNodeClaim("default-next2")
	done := acceptOnceCreated(t, kubeClient, next.Name)
	if _, err := provider.Create(context.Background(), next, nodeClass, "2XS"); err != nil {
		t.Fatalf("expected deleting a Ready group to clear the backoff, got %v", err)
	}
	<-done
}

func TestRecordLateRefusal(t *testing.T) {
	quota := ownedNodeGroup("default-lateq", "XS", quotaRejectedStatus())
	refused := ownedNodeGroup("default-later", "M", ngv1.NodeGroupStatus{
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, "FlavorUnavailable", "flavor M is not available")},
	})
	transient := ownedNodeGroup("default-latet", "S", upstreamErrorStatus(false))
	provider, _, recorder := newTestProviderWithRecorder(t)
	quotaBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_quota_rejections_total")
	rejectionsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total")

	// Not a refusal: ignored.
	provider.RecordLateRefusal(testNodeClaim(transient.Name), transient)
	if len(recorder.reasons()) != 0 || len(provider.RejectedFlavors()) != 0 {
		t.Fatalf("a transient failure must record nothing, got events %v and hold-outs %v", recorder.reasons(), provider.RejectedFlavors())
	}

	provider.RecordLateRefusal(testNodeClaim(refused.Name), refused)
	if _, held := provider.RejectedFlavors()["M"]; !held || len(provider.RejectedFlavors()) != 1 {
		t.Errorf("expected exactly the refused flavor held out, got %v", provider.RejectedFlavors())
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total") - rejectionsBefore; delta != 1 {
		t.Errorf("rejections_total delta = %v, want 1", delta)
	}
	rejectedEvents := recorder.eventsWithReason("NodeGroupRejected")
	if len(rejectedEvents) != 1 || !strings.Contains(rejectedEvents[0].Message, "FlavorUnavailable: flavor M is not available") ||
		!strings.Contains(rejectedEvents[0].Message, "after its launch") {
		t.Errorf("unexpected NodeGroupRejected events %+v", rejectedEvents)
	}

	provider.RecordLateRefusal(testNodeClaim(quota.Name), quota)
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_quota_rejections_total") - quotaBefore; delta != 1 {
		t.Errorf("quota_rejections_total delta = %v, want 1", delta)
	}
	quotaEvents := recorder.eventsWithReason("NodeGroupQuotaExceeded")
	if len(quotaEvents) != 1 || quotaEvents[0].Type != corev1.EventTypeWarning {
		t.Errorf("unexpected NodeGroupQuotaExceeded events %+v", quotaEvents)
	}
	var quotaErr *nodegroup.ErrQuotaExceeded
	if _, err := provider.Create(context.Background(), testNodeClaim("default-next3"), testNodeClass("default"), "XS"); !errors.As(err, &quotaErr) {
		t.Errorf("expected the late quota rejection to arm the backoff, got %T: %v", err, err)
	}
	// It feeds the same state as a rejection seen within the poll: the
	// rejected flavor and every larger one are unavailable, a smaller one is
	// not.
	for flavor, want := range map[string]bool{"2XS": false, "XS": true, "S": true, "XL": true} {
		if got := provider.Unavailable(flavor); got != want {
			t.Errorf("Unavailable(%s) = %v after a late quota rejection of XS, want %v", flavor, got, want)
		}
	}
	if len(provider.RejectedFlavors()) != 1 {
		t.Errorf("a quota rejection must not hold its flavor out, got %v", provider.RejectedFlavors())
	}
}

func TestListReturnsOnlyManagedNodeGroups(t *testing.T) {
	managed := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "default-abc12",
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true"},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "XS", NodeCount: 1},
	}
	unmanaged := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "user-pool"},
		Spec:       ngv1.NodeGroupSpec{Flavor: "M", NodeCount: 3},
	}
	provider, _ := newTestProvider(t, managed, unmanaged)

	items, err := provider.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 1 || items[0].Name != "default-abc12" {
		t.Errorf("expected only the managed nodegroup, got %+v", items)
	}
	// Get is unfiltered by design: callers check IsManaged themselves.
	for _, name := range []string{"default-abc12", "user-pool"} {
		if _, err := provider.Get(context.Background(), name); err != nil {
			t.Errorf("Get(%q): %v", name, err)
		}
	}
}

func TestDeleteRemovesNodeGroup(t *testing.T) {
	ng := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "default-del01",
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true"},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "XS", NodeCount: 1},
	}
	provider, kubeClient := newTestProvider(t, ng)

	if err := provider.Delete(context.Background(), ng); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: ng.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected nodegroup deleted, got %v", err)
	}
	// NotFound is surfaced, not swallowed: the caller maps it to karpenter's
	// NodeClaimNotFoundError.
	if err := provider.Delete(context.Background(), ng); !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound on second delete, got %v", err)
	}
}

func TestCreateFailsWhenNodeGroupVanishesDuringAcceptance(t *testing.T) {
	prev := nodegroup.SetQuotaCheckTimeout(5 * time.Second)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	provider, kubeClient, recorder := newTestProviderWithRecorder(t)
	vanishedBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_vanished_total")

	// Simulate the documented quota race: the group is created, observed by
	// the poll, then reclaimed upstream before it ever syncs.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: "default-vanish"}, ng); err != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			// Give the poll a beat to observe the group before deleting it.
			time.Sleep(1500 * time.Millisecond)
			_ = kubeClient.Delete(context.Background(), ng)
			return
		}
	}()
	_, err := provider.Create(context.Background(), testNodeClaim("default-vanish"), testNodeClass("default"), "2XS")
	<-done
	if !errors.Is(err, nodegroup.ErrNodeGroupVanished) {
		t.Fatalf("expected ErrNodeGroupVanished, got %v", err)
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_vanished_total") - vanishedBefore; delta != 1 {
		t.Errorf("nodegroup_vanished_total delta = %v, want 1", delta)
	}
	if !slices.Contains(recorder.reasons(), "NodeGroupVanished") {
		t.Errorf("expected a NodeGroupVanished event, got %v", recorder.reasons())
	}

	// A vanish arms the quota backoff (the documented cause is the quota
	// engine reclaiming capacity): the next create fails fast.
	var quotaErr *nodegroup.ErrQuotaExceeded
	if _, err := provider.Create(context.Background(), testNodeClaim("default-after"), testNodeClass("default"), "2XS"); !errors.As(err, &quotaErr) {
		t.Errorf("expected a fast quota-backoff failure after a vanish, got %v", err)
	}
}

// TestCreateWaitsOutInformerLag pins the other half of the vanish check. The
// acceptance poll reads through the informer cache, which can miss a group
// Create has just posted: a NotFound before the group was ever seen is that
// lag, not a vanish. Read as a vanish, it would fail healthy launches as
// ErrNodeGroupVanished — counted, announced, and recorded as a quota rejection
// of their flavor that makes it unavailable to the scheduler.
func TestCreateWaitsOutInformerLag(t *testing.T) {
	const (
		name = "default-lagged"
		// The cache misses the group on the poll right after the create and
		// on the next one, a second later.
		laggingPolls = 2
	)
	var (
		mu    sync.Mutex
		polls int
	)
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				ng, ok := obj.(*ngv1.NodeGroup)
				if !ok || key.Name != name {
					return c.Get(ctx, key, obj, opts...)
				}
				mu.Lock()
				polls++
				poll := polls
				mu.Unlock()
				if poll <= laggingPolls {
					return apierrors.NewNotFound(schema.GroupResource{Group: "api.clever-cloud.com", Resource: "nodegroups"}, name)
				}
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				// Once the cache has it, the operator's acknowledgement is there.
				ng.Status = acknowledgedStatus()
				return nil
			},
		}).
		Build()
	recorder := &fakeRecorder{}
	provider := nodegroup.NewProvider(kubeClient, recorder, catalogue, clock.RealClock{})
	vanishedBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_vanished_total")

	if _, err := provider.Create(context.Background(), testNodeClaim(name), testNodeClass("default"), "2XS"); err != nil {
		t.Fatalf("a group the cache has not caught up with is not a vanished one: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if polls != laggingPolls+1 {
		t.Errorf("the group was polled %d times, want %d: the poll must keep waiting through the lag", polls, laggingPolls+1)
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_vanished_total") - vanishedBefore; delta != 0 {
		t.Errorf("nodegroup_vanished_total delta = %v, want 0", delta)
	}
	if slices.Contains(recorder.reasons(), "NodeGroupVanished") {
		t.Errorf("unexpected NodeGroupVanished event, got %v", recorder.reasons())
	}
	if provider.Unavailable("2XS") {
		t.Error("informer lag made the launched flavor unavailable")
	}
}

// TestCreateNonQuotaRejectionIsTerminal covers every upstream refusal that is
// not the organisation quota: a flavor the cluster cannot provision, a spec the
// operator will not accept. Previously such a NodeGroup was indistinguishable
// from "still reconciling" — the poll timed out, Create reported optimistic
// success, and the launch burned karpenter's full 15-minute registration TTL
// before re-planning onto the very same flavor, with the operator's own
// explanation surfaced nowhere.
func TestCreateNonQuotaRejectionIsTerminal(t *testing.T) {
	provider, kubeClient, recorder := newTestProviderWithRecorder(t)
	nodeClaim := testNodeClaim("default-refused")
	const (
		reason  = "FlavorNotAvailable"
		message = `flavor "2XS" is not available for this cluster`
	)
	before := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total")

	done := failOnceCreated(t, kubeClient, nodeClaim.Name, reason, message)
	_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
	<-done

	var rejected *nodegroup.ErrFlavorRejected
	if !errors.As(err, &rejected) {
		t.Fatalf("expected *ErrFlavorRejected, got %T: %v", err, err)
	}
	if rejected.Flavor != "2XS" || rejected.Reason != reason || rejected.Message != message {
		t.Errorf("refusal not carried through: %+v", rejected)
	}
	// The refused reservation must be freed, exactly as a quota rejection is.
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected the refused nodegroup to be deleted, got %v", err)
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total") - before; delta != 1 {
		t.Errorf("rejections_total delta = %v, want 1", delta)
	}
	if !slices.Contains(recorder.reasons(), "NodeGroupRejected") {
		t.Errorf("expected a NodeGroupRejected event on the nodeclaim, got %v", recorder.reasons())
	}
	// The flavor is held out so the scheduler relaxes to another one instead
	// of looping: karpenter-core keeps no per-offering memory of an ICE.
	if _, held := provider.RejectedFlavors()["2XS"]; !held {
		t.Errorf("expected 2XS to be held out after the refusal, got %v", provider.RejectedFlavors())
	}
}

// invalidNodeGroup is the error the API server returns when the NodeGroup CRD's
// schema refuses a create: 422 Invalid, one cause per failing field.
func invalidNodeGroup(name string, errs ...*field.Error) error {
	return apierrors.NewInvalid(schema.GroupKind{Group: apis.CleverCloudGroup, Kind: "NodeGroup"}, name, errs)
}

// refuseCreateWith returns a client whose NodeGroup creates all fail with
// err, as the API server fails them at admission: nothing is stored.
func refuseCreateWith(err error) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*ngv1.NodeGroup); ok {
					return err
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
}

// TestCreateAdmissionRefusalOfTheFlavorIsTerminal covers a flavor the
// NodeGroup API does not accept at all: spec.flavor is an enum (2XS..XL), and
// a settings.flavors override can add a name outside it. The API server
// refuses such a create with 422 Invalid before the operator ever sees it.
// Returned as a plain error, karpenter-core retried the same claim until its
// 5-minute launch timeout and re-planned onto the same cheapest flavor,
// forever. It must be the same typed refusal as the operator's, with the same
// hold-out, count and event.
func TestCreateAdmissionRefusalOfTheFlavorIsTerminal(t *testing.T) {
	nodeClaim := testNodeClaim("default-2xl")
	// The error measured against a kube-apiserver serving the live CRD: the
	// enum refusal, plus a cause without a field for the CEL rules skipped.
	kubeClient := refuseCreateWith(invalidNodeGroup(nodeClaim.Name,
		field.NotSupported(field.NewPath("spec", "flavor"), "2XL", []string{"2XS", "XS", "S", "M", "L", "XL"}),
		field.Invalid(nil, nil, "some validation rules were not checked because the object was invalid; correct the existing errors to complete validation"),
	))
	recorder := &fakeRecorder{}
	provider := nodegroup.NewProvider(kubeClient, recorder, catalogue, clock.RealClock{})
	rejectionsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total")
	timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

	_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XL")

	var rejected *nodegroup.ErrFlavorRejected
	if !errors.As(err, &rejected) {
		t.Fatalf("an admission refusal of the flavor must be an *ErrFlavorRejected, got %T: %v", err, err)
	}
	if rejected.Flavor != "2XL" || rejected.Reason != string(metav1.StatusReasonInvalid) {
		t.Errorf("refusal = %+v, want flavor 2XL and reason Invalid", rejected)
	}
	// The API server's own explanation travels with the refusal.
	if want := `Unsupported value: "2XL": supported values: "2XS", "XS", "S", "M", "L", "XL"`; rejected.Message != want {
		t.Errorf("refusal message = %q, want the API server's %q", rejected.Message, want)
	}
	if _, held := provider.RejectedFlavors()["2XL"]; !held {
		t.Errorf("expected 2XL to be held out after the admission refusal, got %v", provider.RejectedFlavors())
	}
	if !provider.Unavailable("2XL") {
		t.Error("the refused 2XL must be reported unavailable, so that the scheduler plans around it")
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total") - rejectionsBefore; delta != 1 {
		t.Errorf("rejections_total delta = %v, want 1", delta)
	}
	// Nothing was created, so there is nothing to wait for.
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 0 {
		t.Errorf("acceptance_timeouts_total delta = %v, want 0: no NodeGroup exists to poll", delta)
	}
	published := recorder.eventsWithReason("NodeGroupRejected")
	if len(published) != 1 {
		t.Fatalf("expected one NodeGroupRejected event on the nodeclaim, got %v", recorder.reasons())
	}
	for _, want := range []string{"2XL", "at admission", "settings.flavors"} {
		if !strings.Contains(published[0].Message, want) {
			t.Errorf("event message must mention %q: %s", want, published[0].Message)
		}
	}
}

// TestCreateOtherAdmissionErrorsAreNotFlavorRefusals keeps the mapping narrow.
// A payload the CRD refuses for another field fails whatever the flavor, so
// holding the flavor out would only walk the catalogue one flavor per launch,
// and the launch is no capacity problem another flavor solves. That holds when
// the flavor is refused too: the API server reports every schema error at
// once, and the re-plan onto an accepted flavor would fail on the other field
// all the same. An error that is not Invalid says nothing about the flavor at
// all.
func TestCreateOtherAdmissionErrorsAreNotFlavorRefusals(t *testing.T) {
	for name, apiErr := range map[string]error{
		"invalid labels": invalidNodeGroup("default-other",
			field.Invalid(field.NewPath("spec", "labels"), nil, "Label keys with reserved prefixes (kubernetes.io/, node.kubernetes.io/, clever-cloud.com/) are not allowed")),
		// The error a kube-apiserver serving the live CRD returns for a
		// flavor outside the enum and a label value its pattern refuses.
		"invalid flavor and label value": invalidNodeGroup("default-other",
			field.NotSupported(field.NewPath("spec", "flavor"), "2XL", []string{"2XS", "XS", "S", "M", "L", "XL"}),
			field.Invalid(field.NewPath("spec", "labels", "a"), "bad value!", "spec.labels.a in body should match '^([a-zA-Z0-9]([-_.a-zA-Z0-9]{0,61}[a-zA-Z0-9])?)?$'"),
			field.Invalid(nil, nil, "some validation rules were not checked because the object was invalid; correct the existing errors to complete validation"),
		),
		"forbidden": apierrors.NewForbidden(schema.GroupResource{Group: apis.CleverCloudGroup, Resource: "nodegroups"}, "default-other", errors.New("denied")),
	} {
		t.Run(name, func(t *testing.T) {
			provider := nodegroup.NewProvider(refuseCreateWith(apiErr), &fakeRecorder{}, catalogue, clock.RealClock{})
			rejectionsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total")

			_, err := provider.Create(context.Background(), testNodeClaim("default-other"), testNodeClass("default"), "M")

			var rejected *nodegroup.ErrFlavorRejected
			if err == nil || errors.As(err, &rejected) {
				t.Fatalf("expected a plain error, got %T: %v", err, err)
			}
			if !errors.Is(err, apiErr) {
				t.Errorf("the API server's error must stay wrapped for karpenter-core to log, got %v", err)
			}
			if held := provider.RejectedFlavors(); len(held) != 0 {
				t.Errorf("nothing may be held out, got %v", held)
			}
			if provider.Unavailable("M") {
				t.Error("M must stay available: the error is no refusal of the flavor")
			}
			if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total") - rejectionsBefore; delta != 0 {
				t.Errorf("rejections_total delta = %v, want 0", delta)
			}
		})
	}
}

// TestRejectedFlavorIsReleasedOnSuccess proves the hold is not sticky: a flavor
// the operator accepts again is immediately usable. The release happens on the
// acknowledgement, the operator's first status write, not once the group is
// Ready: while the poll waited for Ready, every healthy launch timed out first
// and the hold was never released.
func TestRejectedFlavorIsReleasedOnSuccess(t *testing.T) {
	provider, kubeClient, _ := newTestProviderWithRecorder(t)

	refused := testNodeClaim("default-refused")
	done := failOnceCreated(t, kubeClient, refused.Name, "FlavorNotAvailable", "nope")
	_, _ = provider.Create(context.Background(), refused, testNodeClass("default"), "2XS")
	<-done
	if _, held := provider.RejectedFlavors()["2XS"]; !held {
		t.Fatalf("expected 2XS to be held out, got %v", provider.RejectedFlavors())
	}

	accepted := testNodeClaim("default-ok")
	ackDone := acceptOnceCreated(t, kubeClient, accepted.Name)
	if _, err := provider.Create(context.Background(), accepted, testNodeClass("default"), "2XS"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	<-ackDone
	if _, held := provider.RejectedFlavors()["2XS"]; held {
		t.Errorf("a flavor the operator accepted must be released, got %v", provider.RejectedFlavors())
	}
}

// TestAcceptanceKeepsAHoldOutRecordedDuringItsPoll pins which hold-outs an
// acceptance releases: only those recorded before its launch's create call.
// Group A of XS was acknowledged earlier, and the operator refuses it late:
// the nodegroupstatus controller records that refusal (RecordLateRefusal,
// which does not take createMu) while launch B of XS, resolved before it, is
// being polled. B is then accepted. An acknowledgement is exactly what A had
// before its refusal, so it proves nothing about XS; releasing A's hold-out on
// it let the next launch pick XS straight back, and a flavor refused late on
// every group never stayed held out while launches of it were acknowledged.
// TestRejectedFlavorIsReleasedOnSuccess covers a hold-out recorded before the
// create call, which the acceptance does release.
func TestAcceptanceKeepsAHoldOutRecordedDuringItsPoll(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status ngv1.NodeGroupStatus
	}{
		{"acknowledged", acknowledgedStatus()},
		{"ready", syncedStatus()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refusedA := ownedNodeGroup("default-a", "XS", ngv1.NodeGroupStatus{
				Phase:      "Failed",
				Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, "ImageUnavailable", "boot image missing")},
			})
			provider, kubeClient := newTestProvider(t, refusedA)
			claimB := testNodeClaim("default-b")

			done := make(chan struct{})
			go func() {
				defer close(done)
				// B exists: its launch took its mark and is now polling.
				ng := &ngv1.NodeGroup{}
				for kubeClient.Get(context.Background(), types.NamespacedName{Name: claimB.Name}, ng) != nil {
					time.Sleep(time.Millisecond)
				}
				provider.RecordLateRefusal(testNodeClaim(refusedA.Name), refusedA)
				if _, held := provider.RejectedFlavors()["XS"]; !held || !provider.Unavailable("XS") {
					t.Errorf("A's late refusal must hold XS out, got %v", provider.RejectedFlavors())
				}
				// Then the operator accepts B.
				ng.Status = tc.status
				if err := kubeClient.Update(context.Background(), ng); err != nil {
					t.Errorf("accepting B: %v", err)
				}
			}()
			if _, err := provider.Create(context.Background(), claimB, testNodeClass("default"), "XS"); err != nil {
				t.Fatalf("Create: %v", err)
			}
			<-done
			if _, held := provider.RejectedFlavors()["XS"]; !held || !provider.Unavailable("XS") {
				t.Errorf("B's acceptance must not release a hold-out recorded while it was polled: XS must stay unavailable, got %v", provider.RejectedFlavors())
			}
		})
	}
}

// TestRefusalStaysTerminalWhenCleanupFails guards a context-conflation trap.
// The cleanup Delete runs on the poll's own 15s context, so a refusal observed
// late enough fails it with a wrapped context.DeadlineExceeded. Returning that
// error would make wait.Interrupted match, waitForAcceptance return
// (false, nil), and Create report optimistic success for a group the operator
// has already refused — the exact 15-minute registration-TTL burn this branch
// exists to prevent. Freeing the reservation is best-effort; the refusal is not.
func TestRefusalStaysTerminalWhenCleanupFails(t *testing.T) {
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
				return fmt.Errorf("deleting nodegroup: %w", context.DeadlineExceeded)
			},
		}).
		Build()
	provider := nodegroup.NewProvider(kubeClient, &fakeRecorder{}, catalogue, clock.RealClock{})
	nodeClaim := testNodeClaim("default-refused")

	done := failOnceCreated(t, kubeClient, nodeClaim.Name, "FlavorNotAvailable", "nope")
	_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
	<-done

	var rejected *nodegroup.ErrFlavorRejected
	if !errors.As(err, &rejected) {
		t.Fatalf("a failed cleanup must not downgrade the refusal to success; got %T: %v", err, err)
	}
	if _, held := provider.RejectedFlavors()["2XS"]; !held {
		t.Errorf("the refused flavor must still be held out, got %v", provider.RejectedFlavors())
	}
}

// TestQuotaRefusalStaysTerminalWhenCleanupFails is the quota twin of
// TestRefusalStaysTerminalWhenCleanupFails: the cleanup Delete freeing the
// rejected reservation runs on the poll's own 15s context, so a rejection
// observed late enough fails it with a wrapped context.DeadlineExceeded.
// Returning that error would make wait.Interrupted match, waitForAcceptance
// return (false, nil), and Create report optimistic success for a group the
// quota engine has already rejected — burning the 15-minute registration TTL
// with the reservation never freed. Any other Delete error would replace the
// typed ErrQuotaExceeded with a plain one, so cloudprovider.Create would no
// longer map it to an InsufficientCapacityError.
func TestQuotaRefusalStaysTerminalWhenCleanupFails(t *testing.T) {
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
				return fmt.Errorf("deleting nodegroup: %w", context.DeadlineExceeded)
			},
		}).
		Build()
	provider := nodegroup.NewProvider(kubeClient, &fakeRecorder{}, catalogue, clock.RealClock{})
	nodeClaim := testNodeClaim("default-quota")
	rejectionsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_quota_rejections_total")

	done := rejectOnceCreated(t, kubeClient, nodeClaim.Name, "Quota exceeded: RAM max reached")
	_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
	<-done

	var quotaErr *nodegroup.ErrQuotaExceeded
	if !errors.As(err, &quotaErr) {
		t.Fatalf("a failed cleanup must not downgrade the quota rejection to success; got %T: %v", err, err)
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_quota_rejections_total") - rejectionsBefore; delta != 1 {
		t.Errorf("quota_rejections_total delta = %v, want 1", delta)
	}
	// The backoff must still be armed: the next create fails fast without
	// touching the API.
	if _, err := provider.Create(context.Background(), testNodeClaim("default-after"), testNodeClass("default"), "2XS"); !errors.As(err, &quotaErr) {
		t.Errorf("expected a fast quota-backoff failure after the rejection, got %T: %v", err, err)
	}
}

// TestAdoptionReleasesTheAdoptedGroupsFlavor pins which flavor the hold is
// released on. Create is idempotent: on AlreadyExists it adopts the existing
// group, whose spec.flavor is immutable upstream and can differ from the one
// just resolved. Releasing the requested flavor instead of the adopted one
// would keep a usable flavor out of provisioning and quietly let a refused one
// back in.
func TestAdoptionReleasesTheAdoptedGroupsFlavor(t *testing.T) {
	provider, kubeClient, _ := newTestProviderWithRecorder(t)

	// Get XS refused so it is held out.
	refused := testNodeClaim("default-refused")
	done := failOnceCreated(t, kubeClient, refused.Name, "FlavorNotAvailable", "nope")
	_, _ = provider.Create(context.Background(), refused, testNodeClass("default"), "XS")
	<-done
	if _, held := provider.RejectedFlavors()["XS"]; !held {
		t.Fatalf("expected XS to be held out, got %v", provider.RejectedFlavors())
	}

	// A NodeGroup for the next claim already exists, carrying XS, and is Synced.
	claim := testNodeClaim("default-adopt")
	existing := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: claim.Name,
			Labels: map[string]string{
				v1alpha1.ManagedLabelKey:   "true",
				v1alpha1.NodeClaimLabelKey: claim.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: claim.Name},
			},
		},
		Spec:   ngv1.NodeGroupSpec{Flavor: "XS", NodeCount: 1},
		Status: ngv1.NodeGroupStatus{Conditions: syncedConditions(), Phase: ngv1.PhaseSynced},
	}
	if err := kubeClient.Create(context.Background(), existing); err != nil {
		t.Fatalf("seeding the existing nodegroup: %v", err)
	}

	// Create resolves 2XS but adopts the XS group: XS is what became usable.
	if _, err := provider.Create(context.Background(), claim, testNodeClass("default"), "2XS"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, held := provider.RejectedFlavors()["XS"]; held {
		t.Errorf("adopting a Synced XS group must release the hold on XS, got %v", provider.RejectedFlavors())
	}
}

// TestCreateTransientFailureIsNotARefusal covers a fresh group whose first
// reconcile hits a Clever Cloud API error the operator retries on its own
// (ReconcileFailed=True(UpstreamError)). It was read as a refusal: the group
// was deleted mid-first-reconcile, its flavor held out and the launch failed
// with ICE — and since the same incident hits every flavor, successive claims
// walked and held out the whole catalogue. Next to ReconcileInProgress=True the
// operator has acknowledged the group, so the launch is accepted there and
// then, and the nodegroupstatus controller follows it; alone, the failure is
// no decision, so the poll keeps waiting and, here, times out into the
// optimistic launch. Either way it is surfaced as a Warning.
func TestCreateTransientFailureIsNotARefusal(t *testing.T) {
	// Long enough for the second poll (t=1s) to observe the status.
	prev := nodegroup.SetQuotaCheckTimeout(statusStep)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	for name, tc := range map[string]struct {
		status      ngv1.NodeGroupStatus
		wantTimeout bool
		wantOutcome string
	}{
		"acknowledged: in progress next to the failure": {
			status:      upstreamErrorStatus(false),
			wantOutcome: "the operator acknowledged the NodeGroup",
		},
		"not acknowledged: the failure alone": {
			status:      unacknowledgedUpstreamErrorStatus(),
			wantTimeout: true,
			wantOutcome: "neither acknowledged nor refused",
		},
	} {
		t.Run(name, func(t *testing.T) {
			provider, kubeClient, recorder := newTestProviderWithRecorder(t)
			nodeClaim := testNodeClaim("default-upstream")
			rejectionsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total")
			timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

			done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, tc.status)
			ng, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
			<-done
			if err != nil {
				t.Fatalf("a transient upstream failure must not fail the launch, got %T: %v", err, err)
			}
			if ng.Name != nodeClaim.Name {
				t.Errorf("nodegroup name = %q, want %q", ng.Name, nodeClaim.Name)
			}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, &ngv1.NodeGroup{}); err != nil {
				t.Errorf("the group must not be deleted while the operator retries it: %v", err)
			}
			if held := provider.RejectedFlavors(); len(held) != 0 {
				t.Errorf("a platform-side failure must not hold a flavor out, held %v", held)
			}
			if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total") - rejectionsBefore; delta != 0 {
				t.Errorf("rejections_total delta = %v, want 0", delta)
			}
			wantTimeouts := 0.0
			if tc.wantTimeout {
				wantTimeouts = 1
			}
			if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != wantTimeouts {
				t.Errorf("acceptance_timeouts_total delta = %v, want %v", delta, wantTimeouts)
			}
			if got := slices.Contains(recorder.reasons(), "NodeGroupAcceptanceTimeout"); got != tc.wantTimeout {
				t.Errorf("NodeGroupAcceptanceTimeout published = %v, want %v (events %v)", got, tc.wantTimeout, recorder.reasons())
			}
			if slices.Contains(recorder.reasons(), "NodeGroupRejected") {
				t.Errorf("a transient failure must not publish NodeGroupRejected, got %v", recorder.reasons())
			}
			evts := recorder.eventsWithReason("NodeGroupTransientFailure")
			if len(evts) != 1 {
				t.Fatalf("expected one NodeGroupTransientFailure event, got %v", recorder.reasons())
			}
			// Warning even when acknowledged: the machine is not up yet, and
			// the operator is still retrying.
			if evts[0].Type != corev1.EventTypeWarning || !strings.Contains(evts[0].Message, ngv1.ReasonUpstreamError) ||
				!strings.Contains(evts[0].Message, tc.wantOutcome) {
				t.Errorf("expected a Warning carrying the operator's reason and %q, got %+v", tc.wantOutcome, evts[0])
			}
		})
	}
}

// TestCreateTransientFailureThenReadyIsAccepted proves the poll keeps waiting
// through a transient failure that comes with no acknowledgement instead of
// ending on it: the operator's retry succeeds within the window and the launch
// is accepted like any other.
func TestCreateTransientFailureThenReadyIsAccepted(t *testing.T) {
	prev := nodegroup.SetQuotaCheckTimeout(5 * time.Second)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	provider, kubeClient, recorder := newTestProviderWithRecorder(t)
	nodeClaim := testNodeClaim("default-retried")
	timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

	done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, unacknowledgedUpstreamErrorStatus(), syncedStatus())
	_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
	<-done
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 0 {
		t.Errorf("acceptance_timeouts_total delta = %v, want 0: the group was accepted within the window", delta)
	}
	evts := recorder.eventsWithReason("NodeGroupTransientFailure")
	if len(evts) != 1 || evts[0].Type != corev1.EventTypeNormal {
		t.Errorf("expected one Normal NodeGroupTransientFailure event once the retry succeeded, got %+v", evts)
	}
}

// TestCreateTransientFailureThenVanishIsSurfaced covers a poll that ends in an
// error after seeing a transient failure: the group vanishes while the
// operator, which has not acknowledged it, retries a failed Clever Cloud API
// call. The launch fails as for any vanish, but the transient failure must
// still be surfaced, so the vanish can be tied back to the platform incident.
func TestCreateTransientFailureThenVanishIsSurfaced(t *testing.T) {
	prev := nodegroup.SetQuotaCheckTimeout(5 * time.Second)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	provider, kubeClient, recorder := newTestProviderWithRecorder(t)
	nodeClaim := testNodeClaim("default-upstream-vanish")

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-setStatusOnceCreated(t, kubeClient, nodeClaim.Name, unacknowledgedUpstreamErrorStatus())
		// Long enough for the poll (t=1s) to observe the transient failure.
		time.Sleep(statusStep)
		_ = kubeClient.Delete(context.Background(), &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: nodeClaim.Name}})
	}()
	_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
	<-done
	if !errors.Is(err, nodegroup.ErrNodeGroupVanished) {
		t.Fatalf("expected ErrNodeGroupVanished, got %T: %v", err, err)
	}
	if !slices.Contains(recorder.reasons(), "NodeGroupVanished") {
		t.Errorf("expected a NodeGroupVanished event, got %v", recorder.reasons())
	}
	evts := recorder.eventsWithReason("NodeGroupTransientFailure")
	if len(evts) != 1 {
		t.Fatalf("expected one NodeGroupTransientFailure event on the failed launch, got %v", recorder.reasons())
	}
	if evts[0].Type != corev1.EventTypeWarning ||
		!strings.Contains(evts[0].Message, ngv1.ReasonUpstreamError) ||
		!strings.Contains(evts[0].Message, nodegroup.ErrNodeGroupVanished.Error()) {
		t.Errorf("expected a Warning carrying the operator's reason and the vanish, got %+v", evts[0])
	}
}

// TestCreateParentCancellationPublishesNoTransientFailure covers a controller
// shutdown after the poll saw a transient failure, one without an
// acknowledgement so that the poll keeps waiting. A shutdown is not an
// outcome, as for the acceptance timeout: publishing the failure would report
// a failed launch that did not fail. The claim's next attempt adopts the group,
// and its own poll surfaces the failure if the operator still reports it.
func TestCreateParentCancellationPublishesNoTransientFailure(t *testing.T) {
	prev := nodegroup.SetQuotaCheckTimeout(5 * time.Second)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	provider, kubeClient, recorder := newTestProviderWithRecorder(t)
	nodeClaim := testNodeClaim("default-cancel-upstream")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, unacknowledgedUpstreamErrorStatus())
	go func() {
		// Between the polls at t=1s, which sees the transient failure, and
		// t=2s.
		time.Sleep(statusStep)
		cancel()
	}()
	_, err := provider.Create(ctx, nodeClaim, testNodeClass("default"), "2XS")
	<-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the parent's cancellation, got %T: %v", err, err)
	}
	if slices.Contains(recorder.reasons(), "NodeGroupTransientFailure") {
		t.Errorf("a shutdown must not publish the transient failure as a failed launch, got %v", recorder.reasons())
	}
	if slices.Contains(recorder.reasons(), "NodeGroupAcceptanceTimeout") {
		t.Errorf("a shutdown must not publish a NodeGroupAcceptanceTimeout event, got %v", recorder.reasons())
	}
}

// TestCreateReadyWinsOverFailureConditions pins the precedence one way: the
// operator reports several conditions at once, and a Ready group is a booted
// VM whatever else its status carries. On the AlreadyExists adoption path it
// is the VM this very claim launched on an earlier attempt; deleting it as
// "refused" would destroy a working machine.
func TestCreateReadyWinsOverFailureConditions(t *testing.T) {
	// Short, so a regression that waits a Ready group out as "in progress"
	// fails on the timeout assertions below without costing the full window.
	prev := nodegroup.SetQuotaCheckTimeout(5 * time.Second)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	cases := map[string]struct {
		status ngv1.NodeGroupStatus
		// transient: a transient failure is set next to Ready. Ready still
		// decides the outcome, but the poll saw the failure and must surface
		// it, as a Normal event.
		transient bool
	}{
		"live: ready + scaling + upstream error": {status: upstreamErrorStatus(true), transient: true},
		"ready + terminal refusal": {status: ngv1.NodeGroupStatus{
			Phase: ngv1.PhaseSynced,
			Conditions: []ngv1.NodeGroupCondition{
				condTrue(ngv1.ConditionTypeReady, "Synced", ""),
				condTrue(ngv1.ConditionTypeReconcileFailed, "FlavorNotAvailable", "nope"),
			},
		}},
		"ready + quota exceeded": {status: ngv1.NodeGroupStatus{
			Phase: ngv1.PhaseQuotaExceeded,
			Conditions: []ngv1.NodeGroupCondition{
				condTrue(ngv1.ConditionTypeReady, "Synced", ""),
				condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonQuotaExceeded, ""),
			},
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			provider, kubeClient, recorder := newTestProviderWithRecorder(t, ownedNodeGroup("default-adopt", "S", tc.status))
			rejectionsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total")
			quotaBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_quota_rejections_total")
			timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

			ng, err := provider.Create(context.Background(), testNodeClaim("default-adopt"), testNodeClass("default"), "2XS")
			if err != nil {
				t.Fatalf("adopting a Ready group must succeed, got %T: %v", err, err)
			}
			if ng.Spec.Flavor != "S" {
				t.Errorf("flavor = %q, want the adopted group's %q", ng.Spec.Flavor, "S")
			}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: "default-adopt"}, &ngv1.NodeGroup{}); err != nil {
				t.Errorf("a Ready group must never be deleted: %v", err)
			}
			if held := provider.RejectedFlavors(); len(held) != 0 {
				t.Errorf("no flavor may be held out for a Ready group, held %v", held)
			}
			if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total") - rejectionsBefore; delta != 0 {
				t.Errorf("rejections_total delta = %v, want 0", delta)
			}
			// Accepted on the first poll, not waited out as "in progress".
			if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 0 {
				t.Errorf("acceptance_timeouts_total delta = %v, want 0", delta)
			}
			// No quota rejection recorded either, hence no backoff armed.
			if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_quota_rejections_total") - quotaBefore; delta != 0 {
				t.Errorf("quota_rejections_total delta = %v, want 0", delta)
			}
			if slices.Contains(recorder.reasons(), "NodeGroupRejected") || slices.Contains(recorder.reasons(), "NodeGroupQuotaExceeded") {
				t.Errorf("a Ready group must not be reported as refused, got %v", recorder.reasons())
			}
			if tc.transient {
				assertOneNormalTransientFailure(t, recorder)
			} else if slices.Contains(recorder.reasons(), "NodeGroupTransientFailure") {
				t.Errorf("no transient failure was reported, got %v", recorder.reasons())
			}
		})
	}

	// Same live shape on a fresh create: the operator's first status write
	// already carries Ready.
	t.Run("fresh create: ready + scaling + upstream error", func(t *testing.T) {
		provider, kubeClient, recorder := newTestProviderWithRecorder(t)
		nodeClaim := testNodeClaim("default-fresh")
		timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")
		done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, upstreamErrorStatus(true))
		_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
		<-done
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, &ngv1.NodeGroup{}); err != nil {
			t.Errorf("a Ready group must never be deleted: %v", err)
		}
		if held := provider.RejectedFlavors(); len(held) != 0 {
			t.Errorf("no flavor may be held out for a Ready group, held %v", held)
		}
		// Accepted as soon as the poll sees Ready, not waited out as "in
		// progress": a timeout would still return nil, so err alone proves
		// nothing here.
		if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 0 {
			t.Errorf("acceptance_timeouts_total delta = %v, want 0", delta)
		}
		if slices.Contains(recorder.reasons(), "NodeGroupAcceptanceTimeout") {
			t.Errorf("a Ready group must not be reported as an acceptance timeout, got %v", recorder.reasons())
		}
		assertOneNormalTransientFailure(t, recorder)
	})

	// Not only on the first status: the poll saw the group with no decision
	// yet (no status written), then Ready and the transient failure together
	// in one later status. An acknowledgement in between would end the poll
	// before it.
	t.Run("no status yet, then ready + scaling + upstream error", func(t *testing.T) {
		provider, kubeClient, recorder := newTestProviderWithRecorder(t)
		nodeClaim := testNodeClaim("default-later")
		timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")
		done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, ngv1.NodeGroupStatus{}, upstreamErrorStatus(true))
		_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
		<-done
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 0 {
			t.Errorf("acceptance_timeouts_total delta = %v, want 0", delta)
		}
		assertOneNormalTransientFailure(t, recorder)
	})
}

// TestCreateUnknownReasonStaysTerminal pins the precedence the other way: only
// the allowlisted transient reasons are retried. A reason this provider has
// never seen is still a refusal, even while ReconcileInProgress is also True —
// a real refusal read as "in progress" would burn the 15-minute registration
// TTL.
func TestCreateUnknownReasonStaysTerminal(t *testing.T) {
	provider, kubeClient, recorder := newTestProviderWithRecorder(t)
	nodeClaim := testNodeClaim("default-unknown")

	done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, ngv1.NodeGroupStatus{
		Phase: "SomethingNew",
		Conditions: []ngv1.NodeGroupCondition{
			condTrue(ngv1.ConditionTypeReconcileInProgress, "Creating", ""),
			condTrue(ngv1.ConditionTypeReconcileFailed, "SomethingNew", "the operator said no"),
		},
	})
	_, err := provider.Create(context.Background(), nodeClaim, testNodeClass("default"), "2XS")
	<-done

	var rejected *nodegroup.ErrFlavorRejected
	if !errors.As(err, &rejected) {
		t.Fatalf("expected *ErrFlavorRejected for an unknown reason, got %T: %v", err, err)
	}
	if rejected.Reason != "SomethingNew" {
		t.Errorf("refusal reason = %q, want %q", rejected.Reason, "SomethingNew")
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected the refused nodegroup to be deleted, got %v", err)
	}
	if _, held := provider.RejectedFlavors()["2XS"]; !held {
		t.Errorf("expected 2XS to be held out, got %v", provider.RejectedFlavors())
	}
	if slices.Contains(recorder.reasons(), "NodeGroupTransientFailure") {
		t.Errorf("a refusal must not be reported as transient, got %v", recorder.reasons())
	}
}
