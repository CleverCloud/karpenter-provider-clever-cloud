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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

// syncedConditions is the status the Clever Cloud operator reports once it
// has accepted a NodeGroup.
func syncedConditions() []ngv1.NodeGroupCondition {
	return []ngv1.NodeGroupCondition{{Type: ngv1.ConditionTypeReady, Status: corev1.ConditionTrue, Reason: "Synced"}}
}

// acceptOnceCreated simulates the Clever Cloud operator accepting the
// NodeGroup: once it appears in the fake client, its status is flipped to
// Synced so Create's acceptance poll returns well before the 15s timeout.
func acceptOnceCreated(t *testing.T, kubeClient client.Client, name string) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, ng); err != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			ng.Status.Conditions = syncedConditions()
			ng.Status.Phase = ngv1.PhaseSynced
			if err := kubeClient.Update(context.Background(), ng); err != nil {
				t.Errorf("updating nodegroup status: %v", err)
			}
			return
		}
	}()
	return done
}

// rejectOnceCreated simulates the Clever Cloud operator rejecting the
// NodeGroup on quota once it appears in the fake client.
func rejectOnceCreated(t *testing.T, kubeClient client.Client, name, message string) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, ng); err != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			ng.Status.Conditions = []ngv1.NodeGroupCondition{{
				Type: ngv1.ConditionTypeReconcileFailed, Status: corev1.ConditionTrue,
				Reason: ngv1.ReasonQuotaExceeded, Message: message,
			}}
			if err := kubeClient.Update(context.Background(), ng); err != nil {
				t.Errorf("updating nodegroup status: %v", err)
			}
			return
		}
	}()
	return done
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

	want := map[string]string{
		"team":                  "data",
		"app":                   "web",
		karpv1.NodePoolLabelKey: "default",
	}
	if len(ng.Spec.Labels) != len(want) {
		t.Errorf("expected exactly %d nodegroup labels, got %+v", len(want), ng.Spec.Labels)
	}
	for k, v := range want {
		if got := ng.Spec.Labels[k]; got != v {
			t.Errorf("nodegroup label %s = %q, want %q", k, got, v)
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
	if err := provider.Delete(context.Background(), existing.Name); err != nil {
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

	if err := provider.Delete(context.Background(), ng.Name); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: ng.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected nodegroup deleted, got %v", err)
	}
	// NotFound is surfaced, not swallowed: the caller maps it to karpenter's
	// NodeClaimNotFoundError.
	if err := provider.Delete(context.Background(), ng.Name); !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound on second delete, got %v", err)
	}
}
