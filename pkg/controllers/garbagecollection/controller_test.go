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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/garbagecollection"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

var errTest = errors.New("injected failure")

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

func (r *fakeRecorder) countReason(reason string) int {
	return len(r.eventsByReason(reason))
}

func (r *fakeRecorder) eventsByReason(reason string) []events.Event {
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

func newTestController(t *testing.T, objs ...client.Object) (*garbagecollection.Controller, client.Client) {
	ctrl, kubeClient, _ := newTestControllerWithRecorder(t, objs...)
	return ctrl, kubeClient
}

func newTestControllerWithRecorder(t *testing.T, objs ...client.Object) (*garbagecollection.Controller, client.Client, *fakeRecorder) {
	t.Helper()
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		Build()
	recorder := &fakeRecorder{}
	// The fake client serves as its own uncached reader: cache and API server
	// agree in these tests. Staleness scenarios build the controller by hand.
	return garbagecollection.NewController(kubeClient, kubeClient, nodegroup.NewProvider(kubeClient, recorder, nil, clock.RealClock{}), recorder), kubeClient, recorder
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
	ctrl, kubeClient, recorder := newTestControllerWithRecorder(t, ng, testNodeClaim("claim-alive"))

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !nodeGroupExists(t, kubeClient, "ng-mislabeled") {
		t.Error("expected nodegroup with a living NodeClaim owner to be retained despite its stale label")
	}
	// The uncached confirmation would keep the group too, since it also reads
	// the owner names; what the owner check adds is the refusal: a label that
	// names no living claim is an inconsistency an operator must see, on the
	// gauge and on the group.
	if got := metricstest.Value(t, "karpenter_clevercloud_gc_refused_nodegroups"); got != 1 {
		t.Errorf("gc_refused_nodegroups gauge = %v, want 1", got)
	}
	refusals := recorder.eventsByReason("GarbageCollectionRefused")
	if len(refusals) != 1 || !strings.Contains(refusals[0].Message, "owner is alive") {
		t.Errorf("expected one GarbageCollectionRefused event naming the living owner, got %+v", refusals)
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

func TestReconcileSurfacesReapsAndRefusals(t *testing.T) {
	ctrl, kubeClient, recorder := newTestControllerWithRecorder(t,
		managedNodeGroup("ng-orphan", "claim-gone", 10*time.Minute, false),
		labeledNodeGroup("copied-pool", "", 10*time.Minute, false),
	)
	reapedBefore := metricstest.Value(t, "karpenter_clevercloud_gc_reaped_nodegroups_total")

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if nodeGroupExists(t, kubeClient, "ng-orphan") {
		t.Error("expected orphaned nodegroup to be reaped")
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_gc_reaped_nodegroups_total") - reapedBefore; delta != 1 {
		t.Errorf("gc_reaped_nodegroups_total delta = %v, want 1", delta)
	}
	if got := recorder.countReason("GarbageCollected"); got != 1 {
		t.Errorf("GarbageCollected events = %d, want 1", got)
	}
	if got := metricstest.Value(t, "karpenter_clevercloud_gc_refused_nodegroups"); got != 1 {
		t.Errorf("gc_refused_nodegroups gauge = %v, want 1", got)
	}
	if got := recorder.countReason("GarbageCollectionRefused"); got != 1 {
		t.Errorf("GarbageCollectionRefused events = %d, want 1", got)
	}

	// A second sweep keeps the gauge at the persisting refusal and
	// republishes the event — hourly dedupe is the real recorder's job (the
	// fake captures every publish), so each publish must carry the timeout.
	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if got := metricstest.Value(t, "karpenter_clevercloud_gc_refused_nodegroups"); got != 1 {
		t.Errorf("gc_refused_nodegroups gauge after second sweep = %v, want 1", got)
	}
	if got := recorder.countReason("GarbageCollectionRefused"); got != 2 {
		t.Errorf("GarbageCollectionRefused publishes after second sweep = %d, want 2 (recorder dedupes in production)", got)
	}
	for _, e := range recorder.eventsByReason("GarbageCollectionRefused") {
		if e.DedupeTimeout != time.Hour {
			t.Errorf("GarbageCollectionRefused DedupeTimeout = %v, want 1h", e.DedupeTimeout)
		}
	}
}

func TestReconcileSkipsAlreadyDeletingNodeGroup(t *testing.T) {
	// The Clever Cloud finalizer keeps a group it is tearing down visible for
	// about 40 s, across sweeps. Reaping it again would send a DELETE for
	// nothing, count and announce a reap that never happened, and — through
	// nodegroup.Provider.Delete — forget every quota rejection on every sweep
	// while the finalizer holds the group.
	var mu sync.Mutex
	deletes := 0
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(
		managedNodeGroup("ng-deleting", "claim-gone", 10*time.Minute, true),
	).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			mu.Lock()
			deletes++
			mu.Unlock()
			return cl.Delete(ctx, obj, opts...)
		},
	}).Build()
	recorder := &fakeRecorder{}
	provider := nodegroup.NewProvider(kubeClient, recorder, nil, clock.RealClock{})
	ctrl := garbagecollection.NewController(kubeClient, kubeClient, provider, recorder)
	rejected := managedNodeGroup("ng-rejected", "claim-rejected", 0, false)
	rejected.Status = ngv1.NodeGroupStatus{Phase: ngv1.PhaseQuotaExceeded}
	provider.RecordLateRefusal(testNodeClaim("claim-rejected"), rejected)
	reapedBefore := metricstest.Value(t, "karpenter_clevercloud_gc_reaped_nodegroups_total")

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !nodeGroupExists(t, kubeClient, "ng-deleting") {
		t.Error("expected already-deleting nodegroup to still be present")
	}
	mu.Lock()
	defer mu.Unlock()
	if deletes != 0 {
		t.Errorf("sent %d DELETE requests for a nodegroup already being deleted, want 0", deletes)
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_gc_reaped_nodegroups_total") - reapedBefore; delta != 0 {
		t.Errorf("gc_reaped_nodegroups_total delta = %v, want 0", delta)
	}
	if got := recorder.countReason("GarbageCollected"); got != 0 {
		t.Errorf("GarbageCollected events = %d, want 0", got)
	}
	if !provider.Unavailable("2XS") {
		t.Error("the sweep forgot a quota rejection by deleting a nodegroup already being deleted")
	}
}

func TestReconcileReportsOverdueDeletionsWithoutDependingOnClaimsOrStatus(t *testing.T) {
	ctx := context.Background()
	overdue := func(name string) *ngv1.NodeGroup {
		ng := managedNodeGroup(name, "", time.Hour, true)
		ng.UID = types.UID(name)
		deleted := metav1.NewTime(time.Now().Add(-5*time.Minute - time.Second))
		ng.DeletionTimestamp = &deleted
		return ng
	}
	missingClaim := overdue("deleted-claim")
	registered := overdue("registered")
	// Ready is a stale provisioning condition, not deletion confirmation.
	registered.Status.Conditions = []ngv1.NodeGroupCondition{{Type: ngv1.ConditionTypeReady, Status: corev1.ConditionTrue}}
	registeredClaim := testNodeClaim(registered.Name)
	registeredClaim.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
	terminating := overdue("terminating-claim")
	terminatingClaim := testNodeClaim(terminating.Name)
	terminatingClaim.DeletionTimestamp = terminating.DeletionTimestamp.DeepCopy()
	terminatingClaim.Finalizers = []string{karpv1.TerminationFinalizer}
	// Reporting does not authorize any destructive action, so the monitor
	// also covers a managed group whose owner reference was stripped.
	terminating.OwnerReferences = nil
	unmanaged := overdue("unmanaged")
	unmanaged.Labels = nil
	recentDelete := managedNodeGroup("recent-delete", "", time.Hour, true)
	notDeleting := managedNodeGroup("not-deleting", "", time.Hour, false)
	deletes := 0
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithObjects(missingClaim, registered, registeredClaim, terminating, terminatingClaim,
			unmanaged, recentDelete, notDeleting, testNodeClaim(notDeleting.Name)).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				deletes++
				return errTest
			},
		}).Build()
	recorder := &fakeRecorder{}
	provider := nodegroup.NewProvider(kubeClient, recorder, nil, clock.RealClock{})
	ctrl := garbagecollection.NewController(kubeClient, kubeClient, provider, recorder)
	for range 2 {
		result, err := ctrl.Reconcile(ctx)
		if err != nil || result.RequeueAfter != 2*time.Minute {
			t.Fatalf("Reconcile = %+v, %v; want next sweep in 2m", result, err)
		}
		if got := metricstest.Value(t, "karpenter_clevercloud_nodegroup_deletion_overdue"); got != 3 {
			t.Fatalf("deletion_overdue = %v, want 3", got)
		}
	}
	if deletes != 0 {
		t.Fatalf("deletion reporting sent %d DELETE requests", deletes)
	}
	reported := recorder.eventsByReason("NodeGroupDeletionOverdue")
	if len(reported) != 6 {
		t.Fatalf("events = %d, want 6 publishes (the recorder dedupes them)", len(reported))
	}
	for _, event := range reported {
		if event.Type != corev1.EventTypeWarning || event.DedupeTimeout != time.Hour {
			t.Errorf("expected hourly-deduped Warning, got %+v", event)
		}
		ng, ok := event.InvolvedObject.(*ngv1.NodeGroup)
		if !ok || ng.Name == unmanaged.Name || ng.Name == recentDelete.Name || ng.Name == notDeleting.Name {
			t.Fatalf("unexpected involved object: %+v", event.InvolvedObject)
		}
		if len(event.DedupeValues) != 2 || event.DedupeValues[1] != string(ng.UID) {
			t.Errorf("event must distinguish recreated groups: %+v", event.DedupeValues)
		}
		for _, detail := range []string{"test.finalizer/keep", "CKE cluster state", "VM deletion is not confirmed"} {
			if !strings.Contains(event.Message, detail) {
				t.Errorf("event lacks %q: %s", detail, event.Message)
			}
		}
		stored := &ngv1.NodeGroup{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(ng), stored); err != nil {
			t.Fatal(err)
		}
		if len(stored.Finalizers) != 1 || stored.Finalizers[0] != "test.finalizer/keep" {
			t.Fatalf("reporting altered finalizers: %v", stored.Finalizers)
		}
	}
}

func TestOverdueDeletionGaugeKeepsLastObservationOnListFailure(t *testing.T) {
	ctx := context.Background()
	ng := managedNodeGroup("overdue-list-failure", "", time.Hour, true)
	deleted := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	ng.DeletionTimestamp = &deleted
	var failGroups, failClaims bool
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(ng).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				switch list.(type) {
				case *ngv1.NodeGroupList:
					if failGroups {
						return errTest
					}
				case *karpv1.NodeClaimList:
					if failClaims {
						return errTest
					}
				}
				return cl.List(ctx, list, opts...)
			},
		}).Build()
	recorder := &fakeRecorder{}
	ctrl := garbagecollection.NewController(kubeClient, kubeClient, nodegroup.NewProvider(kubeClient, recorder, nil, clock.RealClock{}), recorder)
	// No successful NodeClaim list is needed to first report the incident.
	failClaims = true
	if _, err := ctrl.Reconcile(ctx); !errors.Is(err, errTest) {
		t.Fatalf("expected NodeClaim list failure, got %v", err)
	}
	if got := metricstest.Value(t, "karpenter_clevercloud_nodegroup_deletion_overdue"); got != 1 {
		t.Fatalf("deletion_overdue despite NodeClaim list failure = %v, want 1", got)
	}
	if recorder.countReason("NodeGroupDeletionOverdue") != 1 {
		t.Fatal("NodeClaim list failure hid the overdue-deletion event")
	}
	// The operator finishes the Kubernetes deletion, but the next failed
	// group list must not claim that recovery has been observed yet.
	stored := &ngv1.NodeGroup{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(ng), stored); err != nil {
		t.Fatal(err)
	}
	stored.Finalizers = nil
	if err := kubeClient.Update(ctx, stored); err != nil {
		t.Fatal(err)
	}
	failGroups = true
	if _, err := ctrl.Reconcile(ctx); !errors.Is(err, errTest) {
		t.Fatalf("expected NodeGroup list failure, got %v", err)
	}
	if got := metricstest.Value(t, "karpenter_clevercloud_nodegroup_deletion_overdue"); got != 1 {
		t.Fatalf("failed NodeGroup list reset gauge to %v", got)
	}
	failGroups = false
	if _, err := ctrl.Reconcile(ctx); !errors.Is(err, errTest) {
		t.Fatalf("expected NodeClaim list failure, got %v", err)
	}
	if got := metricstest.Value(t, "karpenter_clevercloud_nodegroup_deletion_overdue"); got != 0 {
		t.Fatalf("successful empty NodeGroup list did not clear gauge: %v", got)
	}
}

func TestOverdueDeletionLogsForgetGroupsNoLongerMonitored(t *testing.T) {
	ng := managedNodeGroup("overdue-logs", "", time.Hour, true)
	ng.UID = "original"
	deleted := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	ng.DeletionTimestamp = &deleted
	ctrl, kubeClient, _ := newTestControllerWithRecorder(t, ng)
	logs := 0
	ctx := log.IntoContext(context.Background(), funcr.New(func(_, line string) {
		if strings.Contains(line, "VM deletion is not confirmed") {
			logs++
		}
	}, funcr.Options{}))
	reconcile := func(wantGauge float64, wantLogs int) {
		t.Helper()
		if _, err := ctrl.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if got := metricstest.Value(t, "karpenter_clevercloud_nodegroup_deletion_overdue"); got != wantGauge || logs != wantLogs {
			t.Fatalf("gauge/logs = %v/%d, want %v/%d", got, logs, wantGauge, wantLogs)
		}
	}
	reconcile(1, 1)
	reconcile(1, 1)
	stored := &ngv1.NodeGroup{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(ng), stored); err != nil {
		t.Fatal(err)
	}
	// A group that loses its managed label leaves both the gauge and the
	// bounded log state. Reintroducing it must produce a fresh diagnostic.
	delete(stored.Labels, v1alpha1.ManagedLabelKey)
	if err := kubeClient.Update(ctx, stored); err != nil {
		t.Fatal(err)
	}
	reconcile(0, 1)
	stored.Labels[v1alpha1.ManagedLabelKey] = "true"
	if err := kubeClient.Update(ctx, stored); err != nil {
		t.Fatal(err)
	}
	reconcile(1, 2)
}

// vanishedClaim builds a NodeClaim in the launched-but-never-registered
// window, backdated by age, whose NodeGroup does not exist.
func vanishedClaim(name string, age time.Duration) *karpv1.NodeClaim {
	claim := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-age)),
		},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{
				Group: "karpenter.clever-cloud.com",
				Kind:  "CleverNodeClass",
				Name:  "default",
			},
		},
		Status: karpv1.NodeClaimStatus{ProviderID: "clevercloud://" + name},
	}
	claim.StatusConditions().SetTrue(karpv1.ConditionTypeLaunched)
	return claim
}

func nodeClaimExists(t *testing.T, kubeClient client.Client, name string) bool {
	t.Helper()
	err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, &karpv1.NodeClaim{})
	if client.IgnoreNotFound(err) != nil {
		t.Fatalf("getting nodeclaim %s: %v", name, err)
	}
	return err == nil
}

func TestReconcileFastFailsVanishedNodeClaims(t *testing.T) {
	launchedGone := vanishedClaim("claim-vanished", 10*time.Minute)

	young := vanishedClaim("claim-young", 0)
	young.CreationTimestamp = metav1.NewTime(time.Now())

	registered := vanishedClaim("claim-registered", 10*time.Minute)
	registered.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)

	neverLaunched := &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:              "claim-unlaunched",
		CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
	}}

	// Another provider's claim: same vanished shape, foreign NodeClassRef —
	// never ours to touch.
	foreign := vanishedClaim("claim-foreign", 10*time.Minute)
	foreign.Spec.NodeClassRef = &karpv1.NodeClassReference{Group: "karpenter.k8s.aws", Kind: "EC2NodeClass", Name: "default"}

	// Launched, unregistered, old — but its NodeGroup exists (label-stripped
	// by hand, so invisible to the managed List): must be kept.
	groupAlive := vanishedClaim("claim-grouped", 10*time.Minute)
	unmanagedGroup := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-grouped"},
		Spec:       ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1},
	}

	ctrl, kubeClient, recorder := newTestControllerWithRecorder(t,
		launchedGone, young, registered, neverLaunched, foreign, groupAlive, unmanagedGroup)
	vanishedBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_vanished_total")

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if nodeClaimExists(t, kubeClient, "claim-vanished") {
		t.Error("expected the launched-but-vanished nodeclaim to be deleted")
	}
	for _, kept := range []string{"claim-young", "claim-registered", "claim-unlaunched", "claim-foreign", "claim-grouped"} {
		if !nodeClaimExists(t, kubeClient, kept) {
			t.Errorf("expected nodeclaim %s to be retained", kept)
		}
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_vanished_total") - vanishedBefore; delta != 1 {
		t.Errorf("nodegroup_vanished_total delta = %v, want 1", delta)
	}
	if got := recorder.countReason("NodeGroupVanished"); got != 1 {
		t.Errorf("NodeGroupVanished events = %d, want 1", got)
	}
}

func TestReconcileConfirmsAbsenceUncachedBeforeDestroying(t *testing.T) {
	// Simulate informer staleness: the cached client is missing the NodeClaim
	// and the NodeGroup that the uncached reader (the API server) still sees.
	recorder := &fakeRecorder{}
	cached := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(
		managedNodeGroup("ng-stale", "claim-stale", 10*time.Minute, false),
		vanishedClaim("claim-nogroup", 10*time.Minute),
	).Build()
	uncached := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(
		testNodeClaim("claim-stale"),
		&ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: "claim-nogroup"}, Spec: ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1}},
	).Build()
	ctrl := garbagecollection.NewController(cached, uncached, nodegroup.NewProvider(cached, recorder, nil, clock.RealClock{}), recorder)

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// The NodeGroup survives: its claim exists on the API server.
	if !nodeGroupExists(t, cached, "ng-stale") {
		t.Error("expected the nodegroup to be retained when the uncached read finds its nodeclaim")
	}
	// The NodeClaim survives: its group exists on the API server.
	if !nodeClaimExists(t, cached, "claim-nogroup") {
		t.Error("expected the nodeclaim to be retained when the uncached read finds its nodegroup")
	}
}

func TestReconcileConfirmsOwnerNamesUncached(t *testing.T) {
	// The owner reference is the identity: a group whose label matches
	// nothing but whose recorded owner is alive on the API server (stale
	// cache) must be kept — the uncached confirm covers the owner names too.
	recorder := &fakeRecorder{}
	ng := labeledNodeGroup("ng-owner-stale", "claim-nowhere", 10*time.Minute, false)
	ng.OwnerReferences = []metav1.OwnerReference{
		{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: "claim-owner", UID: types.UID("uid-claim-owner")},
	}
	cached := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(ng).Build()
	uncached := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(testNodeClaim("claim-owner")).Build()
	ctrl := garbagecollection.NewController(cached, uncached, nodegroup.NewProvider(cached, recorder, nil, clock.RealClock{}), recorder)

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !nodeGroupExists(t, cached, "ng-owner-stale") {
		t.Error("expected the nodegroup to be retained when its owner is alive on the API server")
	}
}

// TestReconcileFailsClosedWhenTheUncachedConfirmationFails pins what an error
// on a confirming read means: nothing. Each destructive decision here rests on
// an object being ABSENT, and an error proves no absence — an apiserver under
// load, a throttled request, a lost connection. Reading it as absence would
// destroy the VM of a claim that is alive, or write off the launch of a group
// that exists, on any transient API error. The sweep keeps the object and
// reports the error, so the next sweep retries. Each confirmation runs alone:
// with both in one sweep, either one's error would fail the sweep and hide the
// other being dropped, leaving a persistent failure on that path unreported
// (no retry, no error log).
func TestReconcileFailsClosedWhenTheUncachedConfirmationFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		// object's counterpart is missing from the cache: the claim of a
		// group, or the group of a launched claim.
		object  client.Object
		wantErr string
		kept    func(*testing.T, client.Client) bool
	}{
		{
			name:    "nodeclaim confirmation before reaping a group",
			object:  managedNodeGroup("ng-unconfirmed", "claim-unconfirmed", 10*time.Minute, false),
			wantErr: `confirming nodeclaim "claim-unconfirmed" absence`,
			kept: func(t *testing.T, c client.Client) bool {
				return nodeGroupExists(t, c, "ng-unconfirmed")
			},
		},
		{
			name:    "nodegroup confirmation before writing off a launch",
			object:  vanishedClaim("claim-nogroup", 10*time.Minute),
			wantErr: `confirming nodegroup "claim-nogroup" absence`,
			kept: func(t *testing.T, c client.Client) bool {
				return nodeClaimExists(t, c, "claim-nogroup")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &fakeRecorder{}
			cached := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(tc.object).Build()
			uncached := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithInterceptorFuncs(interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return apierrors.NewInternalError(errTest)
				},
			}).Build()
			ctrl := garbagecollection.NewController(cached, uncached, nodegroup.NewProvider(cached, recorder, nil, clock.RealClock{}), recorder)
			reapedBefore := metricstest.Value(t, "karpenter_clevercloud_gc_reaped_nodegroups_total")
			vanishedBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_vanished_total")

			_, err := ctrl.Reconcile(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Reconcile error = %v, want one reporting %s", err, tc.wantErr)
			}
			if !tc.kept(t, cached) {
				t.Error("expected the object to be kept when its counterpart's absence could not be confirmed")
			}
			if delta := metricstest.Value(t, "karpenter_clevercloud_gc_reaped_nodegroups_total") - reapedBefore; delta != 0 {
				t.Errorf("gc_reaped_nodegroups_total delta = %v, want 0", delta)
			}
			if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_vanished_total") - vanishedBefore; delta != 0 {
				t.Errorf("nodegroup_vanished_total delta = %v, want 0", delta)
			}
		})
	}
}

func TestReconcileContinuesPastDeleteFailures(t *testing.T) {
	// One stuck NodeGroup must not shield the others until the next sweep.
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(
		managedNodeGroup("ng-stuck", "claim-gone", 10*time.Minute, false),
		managedNodeGroup("ng-reapable", "claim-gone", 10*time.Minute, false),
	).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == "ng-stuck" {
				return apierrors.NewInternalError(errTest)
			}
			return cl.Delete(ctx, obj, opts...)
		},
	}).Build()
	recorder := &fakeRecorder{}
	ctrl := garbagecollection.NewController(kubeClient, kubeClient, nodegroup.NewProvider(kubeClient, recorder, nil, clock.RealClock{}), recorder)

	if _, err := ctrl.Reconcile(context.Background()); err == nil {
		t.Fatal("expected the sweep to report the stuck deletion")
	}
	if nodeGroupExists(t, kubeClient, "ng-reapable") {
		t.Error("expected the second orphan to be reaped despite the first failing")
	}
	if !nodeGroupExists(t, kubeClient, "ng-stuck") {
		t.Error("expected the stuck nodegroup to still exist")
	}
}

func TestReconcileFlagsExternallyResizedNodeGroups(t *testing.T) {
	resizedNG := managedNodeGroup("ng-resized", "claim-resized", 10*time.Minute, false)
	resizedNG.Spec.NodeCount = 3
	ctrl, kubeClient, recorder := newTestControllerWithRecorder(t,
		resizedNG,
		testNodeClaim("claim-resized"),
		managedNodeGroup("ng-normal", "claim-normal", 10*time.Minute, false),
		testNodeClaim("claim-normal"),
	)

	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Surfaced, never fought: the group must not be deleted or mutated.
	if !nodeGroupExists(t, kubeClient, "ng-resized") {
		t.Error("expected the resized nodegroup to be retained")
	}
	if got := metricstest.Value(t, "karpenter_clevercloud_nodegroup_external_resizes"); got != 1 {
		t.Errorf("nodegroup_external_resizes gauge = %v, want 1", got)
	}
	if got := recorder.countReason("NodeGroupExternallyResized"); got != 1 {
		t.Errorf("NodeGroupExternallyResized events = %d, want 1", got)
	}

	// The operator fixes it back to 1: the gauge clears on the next sweep.
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: "ng-resized"}, ng); err != nil {
		t.Fatal(err)
	}
	ng.Spec.NodeCount = 1
	if err := kubeClient.Update(context.Background(), ng); err != nil {
		t.Fatal(err)
	}
	if _, err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if got := metricstest.Value(t, "karpenter_clevercloud_nodegroup_external_resizes"); got != 0 {
		t.Errorf("nodegroup_external_resizes gauge after fix = %v, want 0", got)
	}
}
