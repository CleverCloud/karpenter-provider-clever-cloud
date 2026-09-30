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

package nodegroupstatus_test

import (
	"context"
	"errors"
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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/nodegroupstatus"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

const (
	quotaRejections = "karpenter_clevercloud_nodegroup_quota_rejections_total"
	rejections      = "karpenter_clevercloud_nodegroup_rejections_total"
	syncOverdue     = "karpenter_clevercloud_nodegroup_sync_overdue"
)

// errNoLaunch is what the probe client answers to any NodeGroup creation: the
// controller never creates one, and quotaBackoffArmed uses it to tell a
// Create that reached the API from one the quota backoff failed fast.
var errNoLaunch = errors.New("nodegroup creation refused by the test client")

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

func (r *fakeRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

type testEnv struct {
	ctrl       *nodegroupstatus.Controller
	kubeClient client.WithWatch
	provider   *nodegroup.Provider
	recorder   *fakeRecorder
}

// newTestEnv builds the controller on a fake client that serves as its own
// uncached reader: cache and API server agree unless a test says otherwise.
// No status subresource, so plain updates can play the operator and core.
func newTestEnv(t *testing.T, objs ...client.Object) *testEnv {
	t.Helper()
	kubeClient := probeClient(fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...))
	return newTestEnvWith(kubeClient, kubeClient)
}

func newTestEnvWith(kubeClient client.WithWatch, uncached client.Reader) *testEnv {
	recorder := &fakeRecorder{}
	provider := nodegroup.NewProvider(kubeClient, recorder)
	return &testEnv{
		ctrl:       nodegroupstatus.NewController(kubeClient, uncached, provider, recorder),
		kubeClient: kubeClient,
		provider:   provider,
		recorder:   recorder,
	}
}

// probeClient builds a fake client that refuses NodeGroup creations with
// errNoLaunch, so that quotaBackoffArmed can call Create without launching
// anything or waiting out an acceptance poll.
func probeClient(builder *fake.ClientBuilder) client.WithWatch {
	return builder.WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*ngv1.NodeGroup); ok {
				return errNoLaunch
			}
			return cl.Create(ctx, obj, opts...)
		},
	}).Build()
}

func (e *testEnv) reconcile(t *testing.T, name string) reconcile.Result {
	t.Helper()
	result, err := e.ctrl.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		t.Fatalf("Reconcile(%s): %v", name, err)
	}
	return result
}

// quotaBackoffArmed reports whether the provider's quota backoff fails the
// next Create fast, which is what karpenter's re-plan after a failed launch
// runs into.
func (e *testEnv) quotaBackoffArmed(t *testing.T) bool {
	t.Helper()
	nodeClass := &v1alpha1.CleverNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	_, err := e.provider.Create(context.Background(), launchedClaim("probe"), nodeClass, "2XS")
	var quotaErr *nodegroup.ErrQuotaExceeded
	switch {
	case errors.As(err, &quotaErr):
		return true
	case errors.Is(err, errNoLaunch):
		return false
	default:
		t.Fatalf("probing the quota backoff: unexpected Create result %v", err)
		return false
	}
}

func (e *testEnv) nodeClaimExists(t *testing.T, name string) bool {
	t.Helper()
	err := e.kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, &karpv1.NodeClaim{})
	if client.IgnoreNotFound(err) != nil {
		t.Fatalf("getting nodeclaim %s: %v", name, err)
	}
	return err == nil
}

// update applies mutate to the stored object, playing the operator or core.
func update[T client.Object](t *testing.T, kubeClient client.Client, obj T, mutate func(T)) {
	t.Helper()
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatalf("getting %s: %v", obj.GetName(), err)
	}
	mutate(obj)
	if err := kubeClient.Update(context.Background(), obj); err != nil {
		t.Fatalf("updating %s: %v", obj.GetName(), err)
	}
}

// launchedClaim is a NodeClaim of this provider as core leaves it once Create
// has returned: Launched, carrying the provider ID of the group named after
// it, not yet Registered.
func launchedClaim(name string) *karpv1.NodeClaim {
	nodeClaim := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			UID:    types.UID("uid-" + name),
			Labels: map[string]string{karpv1.NodePoolLabelKey: "default"},
		},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{Group: "karpenter.clever-cloud.com", Kind: "CleverNodeClass", Name: "default"},
		},
		Status: karpv1.NodeClaimStatus{ProviderID: nodegroup.ProviderID(name)},
	}
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeLaunched)
	return nodeClaim
}

func registeredClaim(name string) *karpv1.NodeClaim {
	nodeClaim := launchedClaim(name)
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
	return nodeClaim
}

// launchedGroup is a NodeGroup exactly as Create stamps it for the claim of
// the same name, backdated by age, with the status the operator wrote.
func launchedGroup(name string, age time.Duration, status ngv1.NodeGroupStatus) *ngv1.NodeGroup {
	return &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-age)),
			Labels: map[string]string{
				v1alpha1.ManagedLabelKey:   "true",
				v1alpha1.NodeClaimLabelKey: name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: name, UID: types.UID("uid-" + name)},
			},
		},
		Spec:   ngv1.NodeGroupSpec{Flavor: "XS", NodeCount: 1},
		Status: status,
	}
}

func condTrue(condType, reason, message string) ngv1.NodeGroupCondition {
	return ngv1.NodeGroupCondition{Type: condType, Status: corev1.ConditionTrue, Reason: reason, Message: message}
}

// quotaStatus is the live shape of a quota rejection: written directly as the
// operator's first status, never preceded by Creating, and with an empty
// message since the platform stopped filling it in.
func quotaStatus() ngv1.NodeGroupStatus {
	return ngv1.NodeGroupStatus{
		Phase:      ngv1.PhaseQuotaExceeded,
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonQuotaExceeded, "")},
	}
}

// refusedStatus is a terminal refusal for a reason other than the quota. No
// such reason has been seen live; any reason outside the transient allowlist
// is terminal.
func refusedStatus() ngv1.NodeGroupStatus {
	return ngv1.NodeGroupStatus{
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, "FlavorUnavailable", "flavor XS is not available in this zone")},
	}
}

// creatingStatus is the operator's acknowledgement of a group it is building:
// its first status write on a healthy launch.
func creatingStatus() ngv1.NodeGroupStatus {
	return ngv1.NodeGroupStatus{
		Phase:      "Creating",
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileInProgress, "Creating", "")},
	}
}

// upstreamErrorStatus is a group in its first reconcile while the operator
// retries a failing Clever Cloud API call (live reason and message).
func upstreamErrorStatus() ngv1.NodeGroupStatus {
	return ngv1.NodeGroupStatus{
		Phase: ngv1.PhaseUpstreamError,
		Conditions: []ngv1.NodeGroupCondition{
			condTrue(ngv1.ConditionTypeReconcileInProgress, "Creating", ""),
			condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonUpstreamError, "API error: RequestDidntReturnSuccess"),
		},
	}
}

func syncedStatus() ngv1.NodeGroupStatus {
	return ngv1.NodeGroupStatus{Phase: ngv1.PhaseSynced, Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReady, "Synced", "")}}
}

func TestReconcileFailsLaunchRejectedByQuotaAfterThePoll(t *testing.T) {
	env := newTestEnv(t, launchedClaim("claim-quota"), launchedGroup("claim-quota", 30*time.Second, quotaStatus()))
	quotaBefore, rejectionsBefore := metricstest.Value(t, quotaRejections), metricstest.Value(t, rejections)

	if result := env.reconcile(t, "claim-quota"); result != (reconcile.Result{}) {
		t.Errorf("result = %+v, want no requeue", result)
	}
	if env.nodeClaimExists(t, "claim-quota") {
		t.Error("expected the nodeclaim of the quota-rejected group to be deleted so karpenter re-plans")
	}
	// The group is core's to delete, through the claim's termination
	// finalizer: that path frees the reservation (see the cloudprovider test
	// pinning that it keeps the backoff armed).
	if err := env.kubeClient.Get(context.Background(), types.NamespacedName{Name: "claim-quota"}, &ngv1.NodeGroup{}); err != nil {
		t.Errorf("expected the nodegroup to be left to karpenter-core's termination, got %v", err)
	}
	if !env.quotaBackoffArmed(t) {
		t.Error("expected the late quota rejection to arm the quota backoff, like one seen within the poll")
	}
	if len(env.provider.RejectedFlavors()) != 0 {
		t.Errorf("a quota rejection must not hold a flavor out, got %v", env.provider.RejectedFlavors())
	}
	if delta := metricstest.Value(t, quotaRejections) - quotaBefore; delta != 1 {
		t.Errorf("quota_rejections_total delta = %v, want 1", delta)
	}
	if delta := metricstest.Value(t, rejections) - rejectionsBefore; delta != 0 {
		t.Errorf("rejections_total delta = %v, want 0", delta)
	}
	evts := env.recorder.withReason("NodeGroupQuotaExceeded")
	if len(evts) != 1 {
		t.Fatalf("NodeGroupQuotaExceeded events = %d, want 1", len(evts))
	}
	if evts[0].Type != corev1.EventTypeWarning || evts[0].InvolvedObject.(*karpv1.NodeClaim).Name != "claim-quota" ||
		!strings.Contains(evts[0].Message, "after its launch") {
		t.Errorf("unexpected event %+v", evts[0])
	}
}

func TestReconcileFailsLaunchRefusedAfterThePoll(t *testing.T) {
	env := newTestEnv(t, launchedClaim("claim-refused"), launchedGroup("claim-refused", 30*time.Second, refusedStatus()))
	quotaBefore, rejectionsBefore := metricstest.Value(t, quotaRejections), metricstest.Value(t, rejections)

	env.reconcile(t, "claim-refused")

	if env.nodeClaimExists(t, "claim-refused") {
		t.Error("expected the nodeclaim of the refused group to be deleted so karpenter re-plans")
	}
	if _, held := env.provider.RejectedFlavors()["XS"]; !held {
		t.Errorf("expected the refused flavor to be held out, got %v", env.provider.RejectedFlavors())
	}
	if env.quotaBackoffArmed(t) {
		t.Error("a refusal that is not the quota must not arm the quota backoff")
	}
	if delta := metricstest.Value(t, rejections) - rejectionsBefore; delta != 1 {
		t.Errorf("rejections_total delta = %v, want 1", delta)
	}
	if delta := metricstest.Value(t, quotaRejections) - quotaBefore; delta != 0 {
		t.Errorf("quota_rejections_total delta = %v, want 0", delta)
	}
	evts := env.recorder.withReason("NodeGroupRejected")
	if len(evts) != 1 {
		t.Fatalf("NodeGroupRejected events = %d, want 1", len(evts))
	}
	if !strings.Contains(evts[0].Message, "FlavorUnavailable: flavor XS is not available in this zone") {
		t.Errorf("expected the operator's reason and message in the event, got %q", evts[0].Message)
	}
}

// TestReconcileLeavesUnfollowedLaunchesAlone pins every guard in front of the
// destructive branch: each case carries a refusal that would fail the launch
// if the guard were missing.
func TestReconcileLeavesUnfollowedLaunchesAlone(t *testing.T) {
	deleting := func(nodeClaim *karpv1.NodeClaim) *karpv1.NodeClaim {
		now := metav1.Now()
		nodeClaim.DeletionTimestamp = &now
		nodeClaim.Finalizers = []string{karpv1.TerminationFinalizer}
		return nodeClaim
	}
	cases := []struct {
		name  string
		claim *karpv1.NodeClaim
		group *ngv1.NodeGroup
	}{
		{
			// The invariant: a Registered claim is never deleted, whatever
			// the group's status says.
			name:  "registered claim",
			claim: registeredClaim("claim"),
			group: launchedGroup("claim", 10*time.Minute, quotaStatus()),
		},
		{
			// Create is still polling, or failed: the poll owns the verdict.
			name: "claim not launched",
			claim: func() *karpv1.NodeClaim {
				c := launchedClaim("claim")
				c.StatusConditions().SetUnknown(karpv1.ConditionTypeLaunched)
				return c
			}(),
			group: launchedGroup("claim", time.Minute, quotaStatus()),
		},
		{
			name:  "claim terminating",
			claim: deleting(launchedClaim("claim")),
			group: launchedGroup("claim", time.Minute, quotaStatus()),
		},
		{
			name: "claim of another provider",
			claim: func() *karpv1.NodeClaim {
				c := launchedClaim("claim")
				c.Spec.NodeClassRef = &karpv1.NodeClassReference{Group: "karpenter.k8s.aws", Kind: "EC2NodeClass", Name: "default"}
				return c
			}(),
			group: launchedGroup("claim", time.Minute, quotaStatus()),
		},
		{
			name: "provider id naming another group",
			claim: func() *karpv1.NodeClaim {
				c := launchedClaim("claim")
				c.Status.ProviderID = nodegroup.ProviderID("other")
				return c
			}(),
			group: launchedGroup("claim", time.Minute, quotaStatus()),
		},
		{
			name:  "unmanaged group",
			claim: launchedClaim("claim"),
			group: func() *ngv1.NodeGroup {
				ng := launchedGroup("claim", time.Minute, quotaStatus())
				delete(ng.Labels, v1alpha1.ManagedLabelKey)
				return ng
			}(),
		},
		{
			// The managed label alone is forgeable: without the owner
			// reference Create stamps, the group is not proven ours.
			name:  "group without owner reference",
			claim: launchedClaim("claim"),
			group: func() *ngv1.NodeGroup {
				ng := launchedGroup("claim", time.Minute, quotaStatus())
				ng.OwnerReferences = nil
				return ng
			}(),
		},
		{
			name:  "group owned by another claim",
			claim: launchedClaim("claim"),
			group: func() *ngv1.NodeGroup {
				ng := launchedGroup("claim", time.Minute, quotaStatus())
				ng.OwnerReferences[0].Name = "other"
				return ng
			}(),
		},
		{
			name:  "group terminating",
			claim: launchedClaim("claim"),
			group: func() *ngv1.NodeGroup {
				ng := launchedGroup("claim", time.Minute, quotaStatus())
				now := metav1.Now()
				ng.DeletionTimestamp = &now
				ng.Finalizers = []string{"test.finalizer/keep"}
				return ng
			}(),
		},
		{
			// Ready wins, as in the acceptance poll: a quota rejection of a
			// later scale-up leaves the booted VM Ready.
			name:  "ready group",
			claim: launchedClaim("claim"),
			group: launchedGroup("claim", 10*time.Minute, ngv1.NodeGroupStatus{
				Phase: ngv1.PhaseQuotaExceeded,
				Conditions: []ngv1.NodeGroupCondition{
					condTrue(ngv1.ConditionTypeReady, "Synced", ""),
					condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonQuotaExceeded, ""),
				},
			}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv(t, tc.claim, tc.group)
			quotaBefore, rejectionsBefore := metricstest.Value(t, quotaRejections), metricstest.Value(t, rejections)

			if result := env.reconcile(t, "claim"); result != (reconcile.Result{}) {
				t.Errorf("result = %+v, want no requeue", result)
			}
			// A claim without finalizers disappears on delete; for the one
			// already terminating, the counters and events below catch it.
			if !env.nodeClaimExists(t, "claim") {
				t.Fatal("expected the nodeclaim to be kept")
			}
			if env.quotaBackoffArmed(t) || len(env.provider.RejectedFlavors()) != 0 {
				t.Error("expected no quota backoff and no flavor hold-out")
			}
			if metricstest.Value(t, quotaRejections) != quotaBefore || metricstest.Value(t, rejections) != rejectionsBefore {
				t.Error("expected no rejection counted")
			}
			if n := env.recorder.count(); n != 0 {
				t.Errorf("expected no event, got %d", n)
			}
		})
	}
}

func TestReconcileConfirmsTheRefusalUncached(t *testing.T) {
	// The cache still shows a launched, unregistered claim and a refused
	// group; the API server has moved on. Each case must keep the claim.
	cases := []struct {
		name     string
		uncached []client.Object
	}{
		{"claim registered", []client.Object{registeredClaim("claim"), launchedGroup("claim", time.Minute, quotaStatus())}},
		{"claim gone", []client.Object{launchedGroup("claim", time.Minute, quotaStatus())}},
		{"group ready", []client.Object{launchedClaim("claim"), launchedGroup("claim", time.Minute, ngv1.NodeGroupStatus{
			Phase: ngv1.PhaseSynced,
			Conditions: []ngv1.NodeGroupCondition{
				condTrue(ngv1.ConditionTypeReady, "Synced", ""),
				condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonQuotaExceeded, ""),
			},
		})}},
		{"group no longer refused", []client.Object{launchedClaim("claim"), launchedGroup("claim", time.Minute, creatingStatus())}},
		{"group gone", []client.Object{launchedClaim("claim")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cached := probeClient(fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(
				launchedClaim("claim"), launchedGroup("claim", time.Minute, quotaStatus())))
			uncached := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(tc.uncached...).Build()
			env := newTestEnvWith(cached, uncached)
			quotaBefore := metricstest.Value(t, quotaRejections)

			env.reconcile(t, "claim")

			if !env.nodeClaimExists(t, "claim") {
				t.Error("expected the nodeclaim to be kept when the API server contradicts the cache")
			}
			if env.quotaBackoffArmed(t) {
				t.Error("expected no quota backoff for a refusal the API server does not confirm")
			}
			if metricstest.Value(t, quotaRejections) != quotaBefore {
				t.Error("expected no rejection counted")
			}
		})
	}
}

func TestReconcileLeavesAClaimThatRegisteredAfterTheConfirmingRead(t *testing.T) {
	// Core registers the claim right after the uncached read confirmed it
	// unregistered: the delete's resourceVersion precondition must stop it.
	base := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(
		launchedClaim("claim-racy"), launchedGroup("claim-racy", time.Minute, quotaStatus())).Build()
	registered := false
	uncached := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if _, ok := obj.(*karpv1.NodeClaim); ok && !registered {
				registered = true
				update(t, cl, &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: key.Name}}, func(nc *karpv1.NodeClaim) {
					nc.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
				})
			}
			return nil
		},
	})
	env := newTestEnvWith(base, uncached)
	quotaBefore := metricstest.Value(t, quotaRejections)

	result := env.reconcile(t, "claim-racy")

	if !registered {
		t.Fatal("the test never exercised the race: the uncached read did not happen")
	}
	if !env.nodeClaimExists(t, "claim-racy") {
		t.Fatal("expected the claim that registered after the confirming read to be kept")
	}
	if result.RequeueAfter == 0 {
		t.Error("expected a requeue to re-examine the changed claim")
	}
	if metricstest.Value(t, quotaRejections) != quotaBefore || len(env.recorder.withReason("NodeGroupQuotaExceeded")) != 0 {
		t.Error("expected nothing recorded for a launch that was not failed")
	}
	// The re-examination sees the claim registered and leaves it.
	env.reconcile(t, "claim-racy")
	if !env.nodeClaimExists(t, "claim-racy") {
		t.Error("expected the registered claim to be kept on re-examination")
	}
}

func TestReconcileSurfacesATransientFailureWithoutFailingTheLaunch(t *testing.T) {
	env := newTestEnv(t, launchedClaim("claim-upstream"), launchedGroup("claim-upstream", time.Minute, upstreamErrorStatus()))
	quotaBefore, rejectionsBefore := metricstest.Value(t, quotaRejections), metricstest.Value(t, rejections)

	result := env.reconcile(t, "claim-upstream")

	if !env.nodeClaimExists(t, "claim-upstream") {
		t.Fatal("a transient failure must not fail the launch")
	}
	if len(env.provider.RejectedFlavors()) != 0 || env.quotaBackoffArmed(t) {
		t.Error("a transient failure must not hold a flavor out or arm the quota backoff")
	}
	if metricstest.Value(t, quotaRejections) != quotaBefore || metricstest.Value(t, rejections) != rejectionsBefore {
		t.Error("a transient failure must not be counted as a rejection")
	}
	evts := env.recorder.withReason("NodeGroupTransientFailure")
	if len(evts) != 1 {
		t.Fatalf("NodeGroupTransientFailure events = %d, want 1", len(evts))
	}
	if evts[0].Type != corev1.EventTypeWarning || !strings.Contains(evts[0].Message, "UpstreamError: API error: RequestDidntReturnSuccess") {
		t.Errorf("unexpected event %+v", evts[0])
	}
	if len(evts[0].DedupeValues) != 1 || evts[0].DedupeValues[0] != "claim-upstream" {
		t.Errorf("expected the event deduplicated per nodeclaim, got %v", evts[0].DedupeValues)
	}
	// Still followed: the requeue lands when the group becomes overdue.
	if result.RequeueAfter <= 3*time.Minute || result.RequeueAfter > 4*time.Minute {
		t.Errorf("RequeueAfter = %v, want the ~4m left before the sync is overdue", result.RequeueAfter)
	}
}

func TestReconcileReportsASyncOverdueLaunch(t *testing.T) {
	young := launchedGroup("claim-young", time.Minute, creatingStatus())
	overdue := launchedGroup("claim-stuck", 6*time.Minute, creatingStatus())
	silent := launchedGroup("claim-silent", 6*time.Minute, ngv1.NodeGroupStatus{})
	env := newTestEnv(t,
		launchedClaim("claim-young"), young,
		launchedClaim("claim-stuck"), overdue,
		launchedClaim("claim-silent"), silent,
	)

	// A healthy launch in progress is only requeued for the threshold.
	result := env.reconcile(t, "claim-young")
	if result.RequeueAfter <= 3*time.Minute || result.RequeueAfter > 4*time.Minute {
		t.Errorf("RequeueAfter = %v, want the ~4m left before the sync is overdue", result.RequeueAfter)
	}
	if n := env.recorder.count(); n != 0 {
		t.Errorf("expected no event for a launch within the threshold, got %d", n)
	}

	for _, name := range []string{"claim-stuck", "claim-silent"} {
		if result := env.reconcile(t, name); result != (reconcile.Result{}) {
			t.Errorf("%s: result = %+v, want no requeue once reported", name, result)
		}
	}
	if got := metricstest.Value(t, syncOverdue); got != 2 {
		t.Errorf("nodegroup_sync_overdue = %v, want 2", got)
	}
	evts := env.recorder.withReason("NodeGroupSyncOverdue")
	if len(evts) != 2 {
		t.Fatalf("NodeGroupSyncOverdue events = %d, want 2", len(evts))
	}
	for _, e := range evts {
		if e.Type != corev1.EventTypeWarning {
			t.Errorf("expected a Warning, got %+v", e)
		}
	}
	if !strings.Contains(evts[0].Message, `phase "Creating", ReconcileInProgress (Creating)`) {
		t.Errorf("expected the operator's state in the event, got %q", evts[0].Message)
	}
	if !strings.Contains(evts[1].Message, "has not picked it up") {
		t.Errorf("expected a silent operator to be named as such, got %q", evts[1].Message)
	}
	for _, name := range []string{"claim-young", "claim-stuck", "claim-silent"} {
		if !env.nodeClaimExists(t, name) {
			t.Errorf("an overdue launch must not be failed: %s was deleted", name)
		}
	}

	// Reconciling again neither double-counts nor loses the group.
	env.reconcile(t, "claim-stuck")
	if got := metricstest.Value(t, syncOverdue); got != 2 {
		t.Errorf("nodegroup_sync_overdue after a repeat reconcile = %v, want 2", got)
	}

	// The claim registers: the launch is no longer followed.
	update(t, env.kubeClient, &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-stuck"}}, func(nc *karpv1.NodeClaim) {
		nc.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
	})
	env.reconcile(t, "claim-stuck")
	if got := metricstest.Value(t, syncOverdue); got != 1 {
		t.Errorf("nodegroup_sync_overdue after registration = %v, want 1", got)
	}

	// The operator finally syncs the silent group: counted no more.
	update(t, env.kubeClient, &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: "claim-silent"}}, func(ng *ngv1.NodeGroup) {
		ng.Status = syncedStatus()
	})
	env.reconcile(t, "claim-silent")
	if got := metricstest.Value(t, syncOverdue); got != 0 {
		t.Errorf("nodegroup_sync_overdue after the sync = %v, want 0", got)
	}
}

func TestReconcileForgetsAnOverdueGroupThatIsGone(t *testing.T) {
	env := newTestEnv(t, launchedClaim("claim-gone"), launchedGroup("claim-gone", 6*time.Minute, creatingStatus()))
	env.reconcile(t, "claim-gone")
	if got := metricstest.Value(t, syncOverdue); got != 1 {
		t.Fatalf("nodegroup_sync_overdue = %v, want 1", got)
	}

	// Core's registration TTL deletes the claim, and its termination the
	// group: the delete event must take it out of the gauge.
	if err := env.kubeClient.Delete(context.Background(), &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: "claim-gone"}}); err != nil {
		t.Fatal(err)
	}
	env.reconcile(t, "claim-gone")
	if got := metricstest.Value(t, syncOverdue); got != 0 {
		t.Errorf("nodegroup_sync_overdue after the group is gone = %v, want 0", got)
	}
}

func TestReconcileRetriesAFailedClaimDelete(t *testing.T) {
	// A delete that fails for another reason than a conflict surfaces the
	// error (rate-limited retry) and records nothing yet: the count is of
	// failed launches, not of attempts.
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(
		launchedClaim("claim-stuck-delete"), launchedGroup("claim-stuck-delete", time.Minute, quotaStatus()),
	).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return apierrors.NewInternalError(errors.New("injected failure"))
		},
	}).Build()
	env := newTestEnvWith(kubeClient, kubeClient)
	quotaBefore := metricstest.Value(t, quotaRejections)

	if _, err := env.ctrl.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "claim-stuck-delete"}}); err == nil {
		t.Fatal("expected the failed delete to be returned for a retry")
	}
	if metricstest.Value(t, quotaRejections) != quotaBefore || env.recorder.count() != 0 {
		t.Error("expected nothing recorded before the launch is actually failed")
	}
}
