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
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
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

func newTestProvider(t *testing.T, objs ...client.Object) (*nodegroup.Provider, client.Client) {
	provider, kubeClient, _ := newTestProviderWithRecorder(t, objs...)
	return provider, kubeClient
}

func newTestProviderWithRecorder(t *testing.T, objs ...client.Object) (*nodegroup.Provider, client.Client, *fakeRecorder) {
	t.Helper()
	// No WithStatusSubresource for NodeGroup: status must stay writable via
	// plain Update so the tests can play the Clever Cloud operator, and so
	// that seeded objects keep their status.
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		Build()
	recorder := &fakeRecorder{}
	return nodegroup.NewProvider(kubeClient, recorder), kubeClient, recorder
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

// statusStep separates successive statuses written by setStatusOnceCreated:
// longer than the 1s acceptance-poll interval, so the poll observes each one.
const statusStep = 1500 * time.Millisecond

// setStatusOnceCreated plays the Clever Cloud operator: once the NodeGroup
// appears in the fake client, it writes each status in turn, statusStep apart.
// A status may carry several conditions at once, as the live operator reports
// them. It stops early if the group is deleted in between, so a Create that
// wrongly deletes the group fails its assertions instead of hanging the test.
func setStatusOnceCreated(t *testing.T, kubeClient client.Client, name string, statuses ...ngv1.NodeGroupStatus) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
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
// NodeGroup: once it appears in the fake client, its status is flipped to
// Synced so Create's acceptance poll returns well before the 15s timeout.
func acceptOnceCreated(t *testing.T, kubeClient client.Client, name string) <-chan struct{} {
	t.Helper()
	return setStatusOnceCreated(t, kubeClient, name, syncedStatus())
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

	// Nothing plays the operator: the group never turns Synced, the poll
	// times out, and Create must still succeed (optimistic launch) while
	// surfacing the timeout through the counter and a NodeClaim event.
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

	// Deleting a NodeGroup frees capacity and clears the backoff.
	if err := provider.Delete(context.Background(), existing); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	third := testNodeClaim("default-quot3")
	done = acceptOnceCreated(t, kubeClient, third.Name)
	_, err = provider.Create(context.Background(), third, nodeClass, "2XS")
	<-done
	if err != nil {
		t.Fatalf("expected Create to succeed after capacity freed, got %v", err)
	}
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
	if _, err := provider.Create(context.Background(), testNodeClaim("default-next3"), testNodeClass("default"), "2XS"); !errors.As(err, &quotaErr) {
		t.Errorf("expected the late quota rejection to arm the backoff, got %T: %v", err, err)
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

// TestRejectedFlavorIsReleasedOnSuccess proves the hold is not sticky: a flavor
// the operator accepts again is immediately usable.
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
	syncDone := acceptOnceCreated(t, kubeClient, accepted.Name)
	if _, err := provider.Create(context.Background(), accepted, testNodeClass("default"), "2XS"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	<-syncDone
	if _, held := provider.RejectedFlavors()["2XS"]; held {
		t.Errorf("a flavor the operator accepted must be released, got %v", provider.RejectedFlavors())
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
	provider := nodegroup.NewProvider(kubeClient, &fakeRecorder{})
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
	provider := nodegroup.NewProvider(kubeClient, &fakeRecorder{})
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
// (ReconcileFailed=True(UpstreamError) next to ReconcileInProgress=True). It
// was read as a refusal: the group was deleted mid-first-reconcile, its flavor
// held out and the launch failed with ICE — and since the same incident hits
// every flavor, successive claims walked and held out the whole catalogue. It
// must behave like any group still in progress: the poll keeps waiting and,
// here, times out into the optimistic launch.
func TestCreateTransientFailureIsNotARefusal(t *testing.T) {
	// Long enough for the second poll (t=1s) to observe the status.
	prev := nodegroup.SetQuotaCheckTimeout(statusStep)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	provider, kubeClient, recorder := newTestProviderWithRecorder(t)
	nodeClaim := testNodeClaim("default-upstream")
	rejectionsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_rejections_total")
	timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

	done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, upstreamErrorStatus(false))
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
	// The window still closed without acceptance: that signal is unchanged.
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 1 {
		t.Errorf("acceptance_timeouts_total delta = %v, want 1", delta)
	}
	if slices.Contains(recorder.reasons(), "NodeGroupRejected") {
		t.Errorf("a transient failure must not publish NodeGroupRejected, got %v", recorder.reasons())
	}
	evts := recorder.eventsWithReason("NodeGroupTransientFailure")
	if len(evts) != 1 {
		t.Fatalf("expected one NodeGroupTransientFailure event, got %v", recorder.reasons())
	}
	if evts[0].Type != corev1.EventTypeWarning || !strings.Contains(evts[0].Message, ngv1.ReasonUpstreamError) {
		t.Errorf("expected a Warning carrying the operator's reason, got %+v", evts[0])
	}
}

// TestCreateTransientFailureThenReadyIsAccepted proves the poll keeps waiting
// through a transient failure instead of ending on it: the operator's retry
// succeeds within the window and the launch is accepted like any other.
func TestCreateTransientFailureThenReadyIsAccepted(t *testing.T) {
	prev := nodegroup.SetQuotaCheckTimeout(5 * time.Second)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	provider, kubeClient, recorder := newTestProviderWithRecorder(t)
	nodeClaim := testNodeClaim("default-retried")
	timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

	done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, upstreamErrorStatus(false), syncedStatus())
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
// operator retries a failed Clever Cloud API call. The launch fails as for any
// vanish, but the transient failure must still be surfaced, so the vanish can
// be tied back to the platform incident.
func TestCreateTransientFailureThenVanishIsSurfaced(t *testing.T) {
	prev := nodegroup.SetQuotaCheckTimeout(5 * time.Second)
	t.Cleanup(func() { nodegroup.SetQuotaCheckTimeout(prev) })

	provider, kubeClient, recorder := newTestProviderWithRecorder(t)
	nodeClaim := testNodeClaim("default-upstream-vanish")

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-setStatusOnceCreated(t, kubeClient, nodeClaim.Name, upstreamErrorStatus(false))
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
// shutdown after the poll saw a transient failure. A shutdown is not an
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
	done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, upstreamErrorStatus(false))
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

	// Not only on the first status: the poll saw the group in progress, then
	// Ready and the transient failure together in one later status.
	t.Run("in progress, then ready + scaling + upstream error", func(t *testing.T) {
		provider, kubeClient, recorder := newTestProviderWithRecorder(t)
		nodeClaim := testNodeClaim("default-later")
		timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")
		inProgress := ngv1.NodeGroupStatus{
			Phase:      "Creating",
			Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileInProgress, "Creating", "")},
		}
		done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, inProgress, upstreamErrorStatus(true))
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
