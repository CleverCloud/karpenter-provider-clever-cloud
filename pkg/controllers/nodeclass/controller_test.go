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

package nodeclass_test

import (
	"context"
	"errors"
	"github.com/awslabs/operatorpkg/status"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/nodeclass"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

func newTestController(t *testing.T, objs ...client.Object) (*nodeclass.Controller, client.Client) {
	t.Helper()
	ctrl, kubeClient, _ := newTestControllerWithRecorder(t, objs...)
	return ctrl, kubeClient
}

func newTestControllerWithRecorder(t *testing.T, objs ...client.Object) (*nodeclass.Controller, client.Client, *fakeRecorder) {
	t.Helper()
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		Build()
	recorder := &fakeRecorder{}
	return nodeclass.NewController(kubeClient, recorder), kubeClient, recorder
}

// fakeRecorder captures published events for assertions.
type fakeRecorder struct {
	events []events.Event
}

func (r *fakeRecorder) Publish(evts ...events.Event) {
	r.events = append(r.events, evts...)
}

func testNodeClass(name string, labels map[string]string) *v1alpha1.CleverNodeClass {
	return &v1alpha1.CleverNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.CleverNodeClassSpec{Labels: labels},
	}
}

func testNodeClaim(name, nodeClassName string) *karpv1.NodeClaim {
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{
				Group: "karpenter.clever-cloud.com",
				Kind:  "CleverNodeClass",
				Name:  nodeClassName,
			},
		},
	}
}

func reconcileNodeClass(t *testing.T, c *nodeclass.Controller, name string) reconcile.Result {
	t.Helper()
	result, err := c.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return result
}

func getNodeClass(t *testing.T, kubeClient client.Client, name string) *v1alpha1.CleverNodeClass {
	t.Helper()
	nodeClass := &v1alpha1.CleverNodeClass{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, nodeClass); err != nil {
		t.Fatalf("getting nodeclass: %v", err)
	}
	return nodeClass
}

func TestReconcileAddsFinalizerAndMarksReady(t *testing.T) {
	c, kubeClient := newTestController(t, testNodeClass("default", map[string]string{"team": "data"}))

	if result := reconcileNodeClass(t, c, "default"); result != (reconcile.Result{}) {
		t.Errorf("unexpected result %+v", result)
	}

	nodeClass := getNodeClass(t, kubeClient, "default")
	if !controllerutil.ContainsFinalizer(nodeClass, v1alpha1.TerminationFinalizer) {
		t.Errorf("expected termination finalizer, got %v", nodeClass.Finalizers)
	}
	if !nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded).IsTrue() {
		t.Errorf("expected ValidationSucceeded true, got %+v", nodeClass.Status.Conditions)
	}
	if !nodeClass.StatusConditions().Root().IsTrue() {
		t.Errorf("expected Ready true, got %+v", nodeClass.Status.Conditions)
	}
}

func TestReconcileRejectsReservedLabelPrefixes(t *testing.T) {
	for _, prefix := range []string{"kubernetes.io/", "node.kubernetes.io/", "clever-cloud.com/"} {
		t.Run(prefix, func(t *testing.T) {
			c, kubeClient := newTestController(t, testNodeClass("default", map[string]string{prefix + "role": "worker"}))

			reconcileNodeClass(t, c, "default")

			cond := getNodeClass(t, kubeClient, "default").StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded)
			if !cond.IsFalse() {
				t.Fatalf("expected ValidationSucceeded false, got %+v", cond)
			}
			if cond.Reason != "ValidationFailed" {
				t.Errorf("unexpected reason %q", cond.Reason)
			}
		})
	}
}

func TestReconcileRejectsLongLabelValues(t *testing.T) {
	c, kubeClient := newTestController(t, testNodeClass("default", map[string]string{"team": strings.Repeat("a", 64)}))

	reconcileNodeClass(t, c, "default")

	cond := getNodeClass(t, kubeClient, "default").StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded)
	if !cond.IsFalse() {
		t.Fatalf("expected ValidationSucceeded false, got %+v", cond)
	}
	if cond.Reason != "ValidationFailed" {
		t.Errorf("unexpected reason %q", cond.Reason)
	}
}

// TestReconcileRejectsUndeliverableLabels pins the classes of labels that used
// to validate cleanly and then fail downstream: a malformed value on a key the
// NodeGroup payload carries is refused by the NodeGroup CRD, and a malformed
// key by the apiserver when the label lands on the Node — the NodeClass must
// not go Ready, and the condition must say why. In the domains an upgrade
// otherwise tolerates (TestReconcileKeepsLegacyLabelsReady), what v0.12.0
// refused itself stays fatal too: it cannot have provisioned.
func TestReconcileRejectsUndeliverableLabels(t *testing.T) {
	for _, tc := range []struct {
		name        string
		labels      map[string]string
		wantMessage string
	}{
		{
			name:        "key with invalid syntax",
			labels:      map[string]string{"bad key": "x"},
			wantMessage: "not a valid label key",
		},
		{
			name:        "value with a space",
			labels:      map[string]string{"team": "not valid"},
			wantMessage: "not a valid label value",
		},
		{
			name:        "subdomained kubernetes.io key with a value over 63 characters",
			labels:      map[string]string{"app.kubernetes.io/name": strings.Repeat("a", 64)},
			wantMessage: "kubernetes.io/ domain",
		},
		{
			name:        "karpenter.sh key with a value over 63 characters",
			labels:      map[string]string{"karpenter.sh/capacity-type": strings.Repeat("a", 64)},
			wantMessage: "karpenter.sh domain",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, kubeClient := newTestController(t, testNodeClass("default", tc.labels))

			reconcileNodeClass(t, c, "default")

			cond := getNodeClass(t, kubeClient, "default").StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded)
			if !cond.IsFalse() {
				t.Fatalf("expected ValidationSucceeded false, got %+v", cond)
			}
			if cond.Reason != "ValidationFailed" {
				t.Errorf("unexpected reason %q", cond.Reason)
			}
			if !strings.Contains(cond.Message, tc.wantMessage) {
				t.Errorf("condition message %q does not contain %q", cond.Message, tc.wantMessage)
			}
		})
	}
}

func TestReconcileRecoversAfterFix(t *testing.T) {
	c, kubeClient := newTestController(t, testNodeClass("default", map[string]string{"kubernetes.io/role": "worker"}))

	reconcileNodeClass(t, c, "default")
	nodeClass := getNodeClass(t, kubeClient, "default")
	if !nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded).IsFalse() {
		t.Fatalf("expected ValidationSucceeded false before fix, got %+v", nodeClass.Status.Conditions)
	}

	nodeClass.Spec.Labels = map[string]string{"team": "data"}
	if err := kubeClient.Update(context.Background(), nodeClass); err != nil {
		t.Fatalf("updating nodeclass: %v", err)
	}

	reconcileNodeClass(t, c, "default")
	nodeClass = getNodeClass(t, kubeClient, "default")
	if !nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded).IsTrue() {
		t.Errorf("expected ValidationSucceeded true after fix, got %+v", nodeClass.Status.Conditions)
	}
}

// TestReconcileKeepsLegacyLabelsReady is the upgrade guard for keys v0.12.0
// accepted and the shared rule now rejects: subdomained kubernetes.io/ keys
// (v0.12.0 refused the kubernetes.io/ and node.kubernetes.io/ prefixes only)
// and the karpenter.sh domain. A NodeClass carrying one went
// ValidationSucceeded=False on upgrade, then NotReady, and Create refused
// every launch from it: provisioning stopped for every NodePool using it, for
// a key that had never reached a node (or, for karpenter.sh, no longer does).
// It must stay Ready, and say what it ignores — in a condition that does not
// gate readiness, and in a Warning event on the NodeClass. Label syntax does
// not matter there: v0.12.0 never checked it, and dropped a subdomained
// kubernetes.io/ key before its value could fail anything, so
// app.kubernetes.io/part-of: "My Platform" provisioned under v0.12.0.
func TestReconcileKeepsLegacyLabelsReady(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"app.kubernetes.io/part-of", "true"},
		{"topology.kubernetes.io/zone", "true"},
		{"karpenter.sh/initialized", "true"},
		{"compatibility.karpenter.sh/x", "true"},
		{"app.kubernetes.io/part-of", "My Platform"},
		{"app.kubernetes.io/name", "a/b"},
		{"karpenter.sh/bad key", "x"},
		{"karpenter.sh/capacity-type", "not valid"},
	} {
		key := tc.key
		t.Run(key+"="+tc.value, func(t *testing.T) {
			ctrl, kubeClient, recorder := newTestControllerWithRecorder(t,
				testNodeClass("default", map[string]string{"team": "data", key: tc.value}))

			reconcileNodeClass(t, ctrl, "default")

			nodeClass := getNodeClass(t, kubeClient, "default")
			if !nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded).IsTrue() {
				t.Errorf("expected ValidationSucceeded true, got %+v", nodeClass.Status.Conditions)
			}
			if !nodeClass.StatusConditions().Root().IsTrue() {
				t.Errorf("a NodeClass carrying legacy key %s must stay Ready, got %+v", key, nodeClass.Status.Conditions)
			}
			ignored := nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeLabelsIgnored)
			if !ignored.IsTrue() {
				t.Fatalf("expected LabelsIgnored true, got %+v", nodeClass.Status.Conditions)
			}
			if !strings.Contains(ignored.Message, key) || strings.Contains(ignored.Message, "team") {
				t.Errorf("LabelsIgnored message %q must name %s and only the ignored keys", ignored.Message, key)
			}
			// No NodeGroup carries an older hash generation: removing the key
			// drifts nothing, and the condition may say so.
			if ignored.Reason != "LegacyLabelKeys" || !strings.Contains(ignored.Message, "Remove them: no node drifts for it") {
				t.Errorf("LabelsIgnored = %s %q, want reason LegacyLabelKeys advising drift-free removal", ignored.Reason, ignored.Message)
			}
			if len(recorder.events) != 1 {
				t.Fatalf("expected one event, got %+v", recorder.events)
			}
			event := recorder.events[0]
			if event.Type != corev1.EventTypeWarning || event.Reason != "LabelsIgnored" || !strings.Contains(event.Message, key) {
				t.Errorf("expected a Warning LabelsIgnored event naming %s, got %+v", key, event)
			}
			if event.InvolvedObject.(*v1alpha1.CleverNodeClass).Name != "default" {
				t.Errorf("the event must be on the NodeClass, got %+v", event.InvolvedObject)
			}
			// Deduplicated per NodeClass, reason and key set: the status write
			// this reconcile makes re-triggers it, and must not repeat the
			// event.
			if !slices.Equal(event.DedupeValues, []string{"default", "LegacyLabelKeys", key}) || event.DedupeTimeout == 0 {
				t.Errorf("event must be deduplicated per NodeClass, reason and key set, got values %v, timeout %v",
					event.DedupeValues, event.DedupeTimeout)
			}
		})
	}
}

// TestReconcileReportsLegacyLabelsNextToFatalOnes: tolerating a legacy key
// must not hide a label that stays fatal, nor the other way round.
func TestReconcileReportsLegacyLabelsNextToFatalOnes(t *testing.T) {
	ctrl, kubeClient := newTestController(t, testNodeClass("default", map[string]string{
		"app.kubernetes.io/part-of": "shop",
		"team":                      "not valid",
	}))

	reconcileNodeClass(t, ctrl, "default")

	nodeClass := getNodeClass(t, kubeClient, "default")
	validation := nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded)
	if !validation.IsFalse() || !strings.Contains(validation.Message, "not a valid label value") {
		t.Errorf("expected ValidationSucceeded false on the invalid value, got %+v", validation)
	}
	if nodeClass.StatusConditions().Root().IsTrue() {
		t.Errorf("expected the NodeClass not to be Ready, got %+v", nodeClass.Status.Conditions)
	}
	if ignored := nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeLabelsIgnored); !ignored.IsTrue() ||
		!strings.Contains(ignored.Message, "app.kubernetes.io/part-of") {
		t.Errorf("expected LabelsIgnored true naming the legacy key, got %+v", ignored)
	}
}

// TestReconcileClearsLabelsIgnoredOnceRemoved: removing the legacy keys is the
// documented way out, and must leave no stale warning behind.
func TestReconcileClearsLabelsIgnoredOnceRemoved(t *testing.T) {
	ctrl, kubeClient, recorder := newTestControllerWithRecorder(t,
		testNodeClass("default", map[string]string{"team": "data", "karpenter.sh/capacity-type": "on-demand"}))
	reconcileNodeClass(t, ctrl, "default")

	nodeClass := getNodeClass(t, kubeClient, "default")
	nodeClass.Spec.Labels = map[string]string{"team": "data"}
	if err := kubeClient.Update(context.Background(), nodeClass); err != nil {
		t.Fatalf("updating nodeclass: %v", err)
	}
	reconcileNodeClass(t, ctrl, "default")

	nodeClass = getNodeClass(t, kubeClient, "default")
	if cond := nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeLabelsIgnored); cond != nil {
		t.Errorf("LabelsIgnored must be removed with the keys, got %+v", cond)
	}
	if !nodeClass.StatusConditions().Root().IsTrue() {
		t.Errorf("expected Ready true, got %+v", nodeClass.Status.Conditions)
	}
	if len(recorder.events) != 1 {
		t.Errorf("expected no event once the keys are gone, got %+v", recorder.events)
	}
}

func TestFinalizeBlocksWhileNodeClaimReferences(t *testing.T) {
	nodeClass := testNodeClass("default", nil)
	nodeClass.Finalizers = []string{v1alpha1.TerminationFinalizer}
	c, kubeClient := newTestController(t, nodeClass, testNodeClaim("default-abc12", "default"))

	if err := kubeClient.Delete(context.Background(), nodeClass); err != nil {
		t.Fatalf("deleting nodeclass: %v", err)
	}

	result := reconcileNodeClass(t, c, "default")
	if result.RequeueAfter != 30*time.Second {
		t.Errorf("expected 30s requeue while nodeclaim references the nodeclass, got %+v", result)
	}

	// The finalizer must keep the terminating object around.
	nodeClass = getNodeClass(t, kubeClient, "default")
	if !controllerutil.ContainsFinalizer(nodeClass, v1alpha1.TerminationFinalizer) {
		t.Errorf("expected finalizer to remain, got %v", nodeClass.Finalizers)
	}
}

func TestFinalizeRemovesFinalizerWhenUnreferenced(t *testing.T) {
	nodeClass := testNodeClass("default", nil)
	nodeClass.Finalizers = []string{v1alpha1.TerminationFinalizer}
	c, kubeClient := newTestController(t, nodeClass, testNodeClaim("other-abc12", "other"))

	if err := kubeClient.Delete(context.Background(), nodeClass); err != nil {
		t.Fatalf("deleting nodeclass: %v", err)
	}

	if result := reconcileNodeClass(t, c, "default"); result != (reconcile.Result{}) {
		t.Errorf("unexpected result %+v", result)
	}

	// Removing the last finalizer lets the fake client drop the object.
	err := kubeClient.Get(context.Background(), types.NamespacedName{Name: "default"}, &v1alpha1.CleverNodeClass{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected nodeclass to be gone, got err=%v", err)
	}
}

func TestReconcileMissingNodeClassNoError(t *testing.T) {
	c, _ := newTestController(t)

	if result := reconcileNodeClass(t, c, "absent"); result != (reconcile.Result{}) {
		t.Errorf("unexpected result %+v", result)
	}
}

func TestReconcileNotReadyWhenNodeGroupAPIUnserved(t *testing.T) {
	// Simulate a non-CKE cluster: listing nodegroups fails with a no-match
	// discovery error. The NodeClass must not go Ready — provisioning has to
	// fail here, with a readable condition, not at the first Create.
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(testNodeClass("default", nil)).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*ngv1.NodeGroupList); ok {
					return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "api.clever-cloud.com", Kind: "NodeGroup"}}
				}
				return cl.List(ctx, list, opts...)
			},
		}).
		Build()
	ctrl := nodeclass.NewController(kubeClient, noopRecorder{})

	result, err := ctrl.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "default"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// A failed probe must re-check on a short cadence: the informer resync
	// (10h by default) must never be what unparks provisioning.
	if result.RequeueAfter != time.Minute {
		t.Errorf("RequeueAfter = %v, want 1m while the API is unserved", result.RequeueAfter)
	}
	nc := &v1alpha1.CleverNodeClass{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: "default"}, nc); err != nil {
		t.Fatal(err)
	}
	if nc.StatusConditions().Get(v1alpha1.ConditionTypeNodeGroupAPIServed).IsTrue() {
		t.Error("expected NodeGroupAPIServed to be false when the API is not served")
	}
	if nc.StatusConditions().Get(status.ConditionReady).IsTrue() {
		t.Error("expected the NodeClass not to be Ready when the NodeGroup API is not served")
	}
}

// TestReconcileRequeuesOnStatusConflict: a status write that lost an
// optimistic-lock race is routine — retried shortly, never surfaced as a
// reconcile error, and never dropped.
func TestReconcileRequeuesOnStatusConflict(t *testing.T) {
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(testNodeClass("default", nil)).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
				return apierrors.NewConflict(schema.GroupResource{Group: "karpenter.clever-cloud.com", Resource: "clevernodeclasses"},
					obj.GetName(), errors.New("the object has been modified"))
			},
		}).
		Build()

	result := reconcileNodeClass(t, nodeclass.NewController(kubeClient, noopRecorder{}), "default")
	if result.RequeueAfter != time.Second {
		t.Errorf("RequeueAfter = %v, want 1s after a status conflict", result.RequeueAfter)
	}
}

// Stamps written by controllers up to v0.11.x (generation v1, no version
// annotation), pinned by TestHashGenerationsMatchTheirStamps in
// pkg/apis/v1alpha1.
const (
	v1StampTeamData    = "3789529822245891689"  // labels: {team: data}
	v1StampEmptyLabels = "14514438007709706818" // labels: {}
)

// Stamps written by v0.12.0 (generation v2), pinned the same way. v2 hashed
// every label, delivered or not.
const (
	v2StampTeamData = "3789529822245891689" // labels: {team: data}
	// labels: {team: data, app.kubernetes.io/part-of: shop}
	v2StampTeamDataPartOf = "16335349422164111380"
	// labels: {team: data, karpenter.sh/capacity-type: on-demand}
	v2StampTeamDataCapacityType = "14653552794626866276"
)

// stampedNodeGroup is a managed NodeGroup of the given NodeClass as a
// controller of the given hash generation stamped it: a hash, and a
// hash-version annotation unless version is empty (v1 wrote none).
func stampedNodeGroup(name, nodeClassName, hash, version string) *ngv1.NodeGroup {
	annotations := map[string]string{v1alpha1.NodeClassHashLabelKey: hash}
	if version != "" {
		annotations[v1alpha1.NodeClassHashVersionAnnotationKey] = version
	}
	return &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				v1alpha1.ManagedLabelKey:   "true",
				v1alpha1.NodeClassLabelKey: nodeClassName,
				v1alpha1.NodeClaimLabelKey: name,
			},
			Annotations: annotations,
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1},
	}
}

func getNodeGroup(t *testing.T, kubeClient client.Client, name string) *ngv1.NodeGroup {
	t.Helper()
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, ng); err != nil {
		t.Fatalf("getting nodegroup: %v", err)
	}
	return ng
}

type noopRecorder struct{}

func (noopRecorder) Publish(...events.Event) {}

// isDrifted evaluates the NodeClaim backed by the named NodeGroup through the
// CloudProvider, as karpenter-core's drift controller does: the migration is
// only correct if what it leaves behind drifts exactly when the NodeClass
// changed.
func isDrifted(t *testing.T, kubeClient client.Client, nodeGroupName string) corecloudprovider.DriftReason {
	t.Helper()
	itp := instancetype.NewProvider("par", nil, nil)
	cp := cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{}))
	nodeClaim := testNodeClaim(nodeGroupName, "default")
	nodeClaim.Status.ProviderID = nodegroup.ProviderID(nodeGroupName)
	reason, err := cp.IsDrifted(context.Background(), nodeClaim)
	if err != nil {
		t.Fatalf("IsDrifted: %v", err)
	}
	return reason
}

// TestReconcileMigratesHashOnlyWhenNodeClassUnchanged is the upgrade guard in
// both directions. A NodeGroup stamped by an older generation of Hash() is
// re-stamped only when its stamp still describes the current spec: an
// unchanged NodeClass — including one that dropped a no-op `labels: {}` line,
// which v1 hashed differently — migrates without rolling the fleet, while a
// NodeClass edited while no leader evaluated drift (lease handover, controller
// down during the upgrade) keeps its stamp and drifts the node, instead of
// being recorded as the configuration the node was built from.
func TestReconcileMigratesHashOnlyWhenNodeClassUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name      string
		labels    map[string]string
		stamp     string
		version   string
		wantDrift bool
	}{
		{name: "unchanged, no version annotation", labels: map[string]string{"team": "data"}, stamp: v1StampTeamData},
		{name: "unchanged, explicit v1", labels: map[string]string{"team": "data"}, stamp: v1StampTeamData, version: "v1"},
		{name: "no-op labels: {} line dropped", labels: nil, stamp: v1StampEmptyLabels},
		{name: "labels: {} kept", labels: map[string]string{}, stamp: v1StampEmptyLabels},
		{name: "edited during the upgrade", labels: map[string]string{"team": "ml"}, stamp: v1StampTeamData, wantDrift: true},
		{name: "edited during the upgrade, explicit v1", labels: map[string]string{"team": "ml"}, stamp: v1StampTeamData, version: "v1", wantDrift: true},
		{name: "labels added to labels: {}", labels: map[string]string{"team": "data"}, stamp: v1StampEmptyLabels, wantDrift: true},
		{name: "v2, unchanged", labels: map[string]string{"team": "data"}, stamp: v2StampTeamData, version: "v2"},
		// v0.12.0 accepted these keys and v2 hashed them; v3 leaves them out.
		// The unchanged NodeClass must migrate without drift all the same.
		{name: "v2 with a legacy kubernetes.io key, unchanged", version: "v2",
			labels: map[string]string{"team": "data", "app.kubernetes.io/part-of": "shop"}, stamp: v2StampTeamDataPartOf},
		{name: "v2 with a legacy karpenter.sh key, unchanged", version: "v2",
			labels: map[string]string{"team": "data", "karpenter.sh/capacity-type": "on-demand"}, stamp: v2StampTeamDataCapacityType},
		{name: "v2, edited during the upgrade", labels: map[string]string{"team": "ml"}, stamp: v2StampTeamData, version: "v2", wantDrift: true},
		{name: "v2 with a legacy key, delivered label edited during the upgrade", version: "v2",
			labels: map[string]string{"team": "ml", "app.kubernetes.io/part-of": "shop"}, stamp: v2StampTeamDataPartOf, wantDrift: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeClass := testNodeClass("default", tc.labels)
			ng := stampedNodeGroup("default-abc12", "default", tc.stamp, tc.version)
			ctrl, kubeClient := newTestController(t, nodeClass, ng, testNodeClaim("default-abc12", "default"))

			reconcileNodeClass(t, ctrl, "default")

			got := getNodeGroup(t, kubeClient, ng.Name)
			reason := isDrifted(t, kubeClient, ng.Name)
			if tc.wantDrift {
				if !equality.Semantic.DeepEqual(got.Annotations, ng.Annotations) {
					t.Errorf("the stamp of a NodeGroup built from an older NodeClass was rewritten: %v, want %v — "+
						"the node would be recorded as built from a configuration it never received", got.Annotations, ng.Annotations)
				}
				if reason != cloudprovider.NodeClassDrifted {
					t.Errorf("IsDrifted = %q, want %q: the NodeClass edit would never reach the node", reason, cloudprovider.NodeClassDrifted)
				}
				return
			}
			if v := got.Annotations[v1alpha1.NodeClassHashVersionAnnotationKey]; v != v1alpha1.NodeClassHashVersion {
				t.Errorf("hash version not migrated: got %q, want %q", v, v1alpha1.NodeClassHashVersion)
			}
			if h := got.Annotations[v1alpha1.NodeClassHashLabelKey]; h != nodeClass.Hash() {
				t.Errorf("hash not re-stamped: got %q, want %q", h, nodeClass.Hash())
			}
			if reason != "" {
				t.Errorf("IsDrifted = %q after migrating an unchanged NodeClass: the upgrade would replace every node", reason)
			}
		})
	}
}

// TestRemovingALegacyLabelDriftsNoNode is the way out of the upgrade trap,
// end to end. A NodeClass admitted by v0.12.0 with a key the shared rule now
// rejects backs nodes stamped by v0.12.0 (v2, which hashed that key) and nodes
// launched since (v3, which does not). Removing the key — what the
// LabelsIgnored warning asks for — changes nothing any NodeGroup carries, so
// it must drift none of them: when the hash covered the raw labels, it
// replaced the whole fleet.
func TestRemovingALegacyLabelDriftsNoNode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		key     string
		value   string
		v2Stamp string
	}{
		{name: "kubernetes.io subdomain", key: "app.kubernetes.io/part-of", value: "shop", v2Stamp: v2StampTeamDataPartOf},
		{name: "karpenter.sh", key: "karpenter.sh/capacity-type", value: "on-demand", v2Stamp: v2StampTeamDataCapacityType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacy := testNodeClass("default", map[string]string{"team": "data", tc.key: tc.value})
			fromV012 := stampedNodeGroup("default-v0120", "default", tc.v2Stamp, "v2")
			sinceUpgrade := stampedNodeGroup("default-since", "default", legacy.Hash(), v1alpha1.NodeClassHashVersion)
			ctrl, kubeClient := newTestController(t, legacy, fromV012, sinceUpgrade,
				testNodeClaim(fromV012.Name, "default"), testNodeClaim(sinceUpgrade.Name, "default"))

			// The upgrade: the controller keeps the NodeClass Ready and
			// migrates the v0.12.0 group.
			reconcileNodeClass(t, ctrl, "default")
			nodeClass := getNodeClass(t, kubeClient, "default")
			if !nodeClass.StatusConditions().Root().IsTrue() {
				t.Fatalf("expected the NodeClass to stay Ready, got %+v", nodeClass.Status.Conditions)
			}
			if v := getNodeGroup(t, kubeClient, fromV012.Name).Annotations[v1alpha1.NodeClassHashVersionAnnotationKey]; v != v1alpha1.NodeClassHashVersion {
				t.Fatalf("the v0.12.0 group was not migrated: hash version %q", v)
			}
			// The advice the operator acts on, written after that migration.
			if cond := nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeLabelsIgnored); cond.Reason != "LegacyLabelKeys" ||
				!strings.Contains(cond.Message, "Remove them: no node drifts for it") {
				t.Fatalf("expected LabelsIgnored to advise drift-free removal once every group is migrated, got %+v", cond)
			}

			// The way out: remove the key.
			nodeClass.Spec.Labels = map[string]string{"team": "data"}
			if err := kubeClient.Update(context.Background(), nodeClass); err != nil {
				t.Fatalf("updating nodeclass: %v", err)
			}
			reconcileNodeClass(t, ctrl, "default")
			for _, ng := range []*ngv1.NodeGroup{fromV012, sinceUpgrade} {
				if reason := isDrifted(t, kubeClient, ng.Name); reason != "" {
					t.Errorf("nodegroup %s: IsDrifted = %q after removing %s, which the NodeGroup payload does not carry: "+
						"the whole fleet would be replaced for an identical NodeGroup spec", ng.Name, reason, tc.key)
				}
			}

			// A delivered label still drifts: the hash did not go blind.
			nodeClass = getNodeClass(t, kubeClient, "default")
			nodeClass.Spec.Labels = map[string]string{"team": "ml"}
			if err := kubeClient.Update(context.Background(), nodeClass); err != nil {
				t.Fatalf("updating nodeclass: %v", err)
			}
			for _, ng := range []*ngv1.NodeGroup{fromV012, sinceUpgrade} {
				if reason := isDrifted(t, kubeClient, ng.Name); reason != cloudprovider.NodeClassDrifted {
					t.Errorf("nodegroup %s: IsDrifted = %q after editing a delivered label, want %q", ng.Name, reason, cloudprovider.NodeClassDrifted)
				}
			}
		})
	}
}

// TestRemovingALegacyLabelBeforeTheMigrationDrifts pins the conservative side
// of the upgrade order. A key removed before the upgraded controller
// re-stamped the NodeGroups v0.12.0 built (before the upgrade, in the same
// change as the upgrade, or while v0.12.0 still runs) leaves those groups with
// a v2 stamp of labels that included the key. v3's equivalence cannot be
// enumerated backwards, so nothing proves that stamp describes the current
// spec: the group keeps it and its node drifts, a replacement rather than a
// hidden edit. That is why LabelsIgnored, the README and docs/observability.md
// say to remove the keys only once the migration is done.
func TestRemovingALegacyLabelBeforeTheMigrationDrifts(t *testing.T) {
	cleaned := testNodeClass("default", map[string]string{"team": "data"})
	fromV012 := stampedNodeGroup("default-v0120", "default", v2StampTeamDataPartOf, "v2")
	ctrl, kubeClient := newTestController(t, cleaned, fromV012, testNodeClaim(fromV012.Name, "default"))

	reconcileNodeClass(t, ctrl, "default")

	if got := getNodeGroup(t, kubeClient, fromV012.Name); !equality.Semantic.DeepEqual(got.Annotations, fromV012.Annotations) {
		t.Errorf("the v2 stamp of a NodeClass that lost a key since was rewritten: %v, want %v", got.Annotations, fromV012.Annotations)
	}
	if reason := isDrifted(t, kubeClient, fromV012.Name); reason != cloudprovider.NodeClassDrifted {
		t.Errorf("IsDrifted = %q, want %q", reason, cloudprovider.NodeClassDrifted)
	}
	if cond := getNodeClass(t, kubeClient, "default").StatusConditions().Get(v1alpha1.ConditionTypeLabelsIgnored); cond != nil {
		t.Errorf("no key is left to report, got %+v", cond)
	}
}

// TestLabelsIgnoredWaitsForTheHashMigration pins the advice LabelsIgnored gives
// against what removing the keys does. A NodeGroup still stamped by v0.12.0
// (v2) carries a hash of the ignored keys, so removing them drifts it. The
// migration runs before the condition is written, and the condition promises
// drift-free removal only when no such NodeGroup is left: not while one keeps
// its v2 stamp because its NodeClaim is already Drifted, its NodeClass changed
// since it was built, or the controller could not write it, and not when the
// NodeGroups could not even be listed.
func TestLabelsIgnoredWaitsForTheHashMigration(t *testing.T) {
	legacy := map[string]string{"team": "data", "app.kubernetes.io/part-of": "shop"}
	nodeGroupWriteFails := interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*ngv1.NodeGroup); ok {
				return errors.New("nodegroup write failed")
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}
	// The API probe (Limit 1, no selector) succeeds; the migration's list of
	// the NodeClass's groups fails.
	nodeGroupsUnlisted := interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			listOpts := (&client.ListOptions{}).ApplyOptions(opts)
			if _, ok := list.(*ngv1.NodeGroupList); ok && listOpts.LabelSelector != nil {
				return errors.New("nodegroup list failed")
			}
			return cl.List(ctx, list, opts...)
		},
	}
	for _, tc := range []struct {
		name             string
		labels           map[string]string
		nodeClaimDrifted bool
		funcs            interceptor.Funcs
		wantErr          bool
		wantAdvice       string
	}{
		{name: "nodeclaim already drifted", labels: legacy, nodeClaimDrifted: true, wantAdvice: "drifts the 1 NodeGroup(s)"},
		{name: "nodeclass edited since the group was built", labels: map[string]string{"team": "ml", "app.kubernetes.io/part-of": "shop"},
			wantAdvice: "drifts the 1 NodeGroup(s)"},
		{name: "nodegroup write fails", labels: legacy, funcs: nodeGroupWriteFails, wantErr: true, wantAdvice: "drifts the 1 NodeGroup(s)"},
		{name: "nodegroups cannot be listed", labels: legacy, funcs: nodeGroupsUnlisted, wantErr: true, wantAdvice: "could not be listed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ng := stampedNodeGroup("default-v0120", "default", v2StampTeamDataPartOf, "v2")
			nodeClaim := testNodeClaim(ng.Name, "default")
			if tc.nodeClaimDrifted {
				nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeDrifted)
			}
			kubeClient := fake.NewClientBuilder().
				WithScheme(scheme.Scheme).
				WithObjects(testNodeClass("default", tc.labels), ng, nodeClaim).
				WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
				WithInterceptorFuncs(tc.funcs).
				Build()
			recorder := &fakeRecorder{}

			result, err := nodeclass.NewController(kubeClient, recorder).Reconcile(context.Background(),
				reconcile.Request{NamespacedName: types.NamespacedName{Name: "default"}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("Reconcile error = %v, want error: %v", err, tc.wantErr)
			}
			// Nothing about the NodeClass changes when the node is replaced or
			// its NodeClaim condition clears: the controller rechecks itself.
			if !tc.wantErr && result.RequeueAfter != time.Minute {
				t.Errorf("RequeueAfter = %v, want 1m while a NodeGroup is not migrated", result.RequeueAfter)
			}
			cond := getNodeClass(t, kubeClient, "default").StatusConditions().Get(v1alpha1.ConditionTypeLabelsIgnored)
			if !cond.IsTrue() || cond.Reason != "HashMigrationPending" {
				t.Fatalf("expected LabelsIgnored true with reason HashMigrationPending, got %+v", cond)
			}
			if strings.Contains(cond.Message, "no node drifts") || !strings.Contains(cond.Message, tc.wantAdvice) {
				t.Errorf("LabelsIgnored message %q must not promise drift-free removal, and must say %q", cond.Message, tc.wantAdvice)
			}
			if len(recorder.events) != 1 || recorder.events[0].Message != cond.Message {
				t.Errorf("expected one event carrying the condition's advice, got %+v", recorder.events)
			}
			if v := getNodeGroup(t, kubeClient, ng.Name).Annotations[v1alpha1.NodeClassHashVersionAnnotationKey]; v != "v2" {
				t.Fatalf("the group was migrated (hash version %q): the case does not exercise a pending migration", v)
			}

			// The advice is right: removing the key now drifts the group.
			nodeClass := getNodeClass(t, kubeClient, "default")
			delete(nodeClass.Spec.Labels, "app.kubernetes.io/part-of")
			if err := kubeClient.Update(context.Background(), nodeClass); err != nil {
				t.Fatalf("updating nodeclass: %v", err)
			}
			if reason := isDrifted(t, kubeClient, ng.Name); reason != cloudprovider.NodeClassDrifted {
				t.Errorf("IsDrifted = %q after removing the key, want %q", reason, cloudprovider.NodeClassDrifted)
			}
		})
	}
}

// TestLabelsIgnoredAdvisesRemovalOnceMigrated: once the pending NodeGroup is
// re-stamped, the same condition advises drift-free removal, the event says
// so too (the new advice is not swallowed by the previous one's dedupe), and
// removing the key then drifts nothing.
func TestLabelsIgnoredAdvisesRemovalOnceMigrated(t *testing.T) {
	ng := stampedNodeGroup("default-v0120", "default", v2StampTeamDataPartOf, "v2")
	writeFails := true
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(testNodeClass("default", map[string]string{"team": "data", "app.kubernetes.io/part-of": "shop"}),
			ng, testNodeClaim(ng.Name, "default")).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*ngv1.NodeGroup); ok && writeFails {
					return errors.New("nodegroup write failed")
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	recorder := &fakeRecorder{}
	ctrl := nodeclass.NewController(kubeClient, recorder)
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "default"}}

	if _, err := ctrl.Reconcile(context.Background(), req); err == nil {
		t.Fatal("expected the failed nodegroup write to fail the reconcile, so that it is retried")
	}
	if cond := getNodeClass(t, kubeClient, "default").StatusConditions().Get(v1alpha1.ConditionTypeLabelsIgnored); cond.Reason != "HashMigrationPending" {
		t.Fatalf("expected reason HashMigrationPending while the write fails, got %+v", cond)
	}

	writeFails = false
	if result := reconcileNodeClass(t, ctrl, "default"); result != (reconcile.Result{}) {
		t.Errorf("unexpected result %+v once nothing is pending", result)
	}
	if v := getNodeGroup(t, kubeClient, ng.Name).Annotations[v1alpha1.NodeClassHashVersionAnnotationKey]; v != v1alpha1.NodeClassHashVersion {
		t.Fatalf("the group was not migrated: hash version %q", v)
	}
	cond := getNodeClass(t, kubeClient, "default").StatusConditions().Get(v1alpha1.ConditionTypeLabelsIgnored)
	if cond.Reason != "LegacyLabelKeys" || !strings.Contains(cond.Message, "Remove them: no node drifts for it") {
		t.Fatalf("expected LabelsIgnored to advise drift-free removal once migrated, got %+v", cond)
	}
	if len(recorder.events) != 2 || recorder.events[1].Message != cond.Message ||
		slices.Equal(recorder.events[0].DedupeValues, recorder.events[1].DedupeValues) {
		t.Errorf("expected a second event with the new advice and its own dedupe key, got %+v", recorder.events)
	}

	nodeClass := getNodeClass(t, kubeClient, "default")
	nodeClass.Spec.Labels = map[string]string{"team": "data"}
	if err := kubeClient.Update(context.Background(), nodeClass); err != nil {
		t.Fatalf("updating nodeclass: %v", err)
	}
	reconcileNodeClass(t, ctrl, "default")
	if reason := isDrifted(t, kubeClient, ng.Name); reason != "" {
		t.Errorf("IsDrifted = %q after removing the key as advised", reason)
	}
}

// TestReconcileKeepsTheStoredHashWhenAlreadyDrifted protects a replacement in
// flight: a NodeGroup whose NodeClaim is already Drifted is never re-stamped,
// even when its stamp matches — the migration must not be what cancels a
// roll. It keeps its generation too: an old-generation hash under the current
// version would be compared unlike with like. A later pass migrates it once
// the condition clears.
func TestReconcileKeepsTheStoredHashWhenAlreadyDrifted(t *testing.T) {
	nodeClass := testNodeClass("default", map[string]string{"team": "data"})
	ng := stampedNodeGroup("default-abc12", "default", v1StampTeamData, "v1")
	nodeClaim := testNodeClaim("default-abc12", "default")
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeDrifted)
	ctrl, kubeClient := newTestController(t, nodeClass, ng, nodeClaim)

	reconcileNodeClass(t, ctrl, "default")

	if got := getNodeGroup(t, kubeClient, ng.Name); !equality.Semantic.DeepEqual(got.Annotations, ng.Annotations) {
		t.Errorf("an already-drifted nodeclaim must keep its stamp and generation, got %v, want %v", got.Annotations, ng.Annotations)
	}
}

// TestReconcileLeavesCurrentGenerationGroupsAlone pins the migration's scope: a
// NodeGroup already stamped by the current generation is IsDrifted's business
// alone. Re-stamping it would erase a pending drift (a NodeClass edit core has
// not evaluated yet), and a steady-state reconcile must not even read its
// NodeClaim — a lookup failure there must not fail the reconcile of a fleet
// that has nothing to migrate.
func TestReconcileLeavesCurrentGenerationGroupsAlone(t *testing.T) {
	nodeClass := testNodeClass("default", map[string]string{"team": "ml"})
	upToDate := stampedNodeGroup("default-abc12", "default", nodeClass.Hash(), v1alpha1.NodeClassHashVersion)
	pendingDrift := stampedNodeGroup("default-def34", "default",
		testNodeClass("default", map[string]string{"team": "data"}).Hash(), v1alpha1.NodeClassHashVersion)
	var nodeClaimReads, nodeGroupWrites int
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(nodeClass, upToDate, pendingDrift,
			testNodeClaim(upToDate.Name, "default"), testNodeClaim(pendingDrift.Name, "default")).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*karpv1.NodeClaim); ok {
					nodeClaimReads++
					return errors.New("nodeclaim lookup failed")
				}
				return cl.Get(ctx, key, obj, opts...)
			},
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*ngv1.NodeGroup); ok {
					nodeGroupWrites++
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	reconcileNodeClass(t, nodeclass.NewController(kubeClient, noopRecorder{}), "default")

	if nodeClaimReads != 0 || nodeGroupWrites != 0 {
		t.Errorf("current-generation NodeGroups were migrated: %d nodeclaim reads, %d nodegroup writes, want none",
			nodeClaimReads, nodeGroupWrites)
	}
	for _, ng := range []*ngv1.NodeGroup{upToDate, pendingDrift} {
		if got := getNodeGroup(t, kubeClient, ng.Name); !equality.Semantic.DeepEqual(got.Annotations, ng.Annotations) {
			t.Errorf("nodegroup %s must not be touched: got %v, want %v", ng.Name, got.Annotations, ng.Annotations)
		}
	}
	if reason := isDrifted(t, kubeClient, pendingDrift.Name); reason != cloudprovider.NodeClassDrifted {
		t.Errorf("IsDrifted = %q, want %q: the pending NodeClass edit was absorbed", reason, cloudprovider.NodeClassDrifted)
	}
}

// TestReconcileLeavesUnprovableStampsAlone pins the rollback path: a stamp
// from a generation this controller cannot compute (a newer controller's) and
// a NodeGroup without any stamp prove nothing about what the node was built
// from. Neither is re-stamped — which would record a configuration the node
// may never have received — nor drifted, which would replace the fleet on a
// rollback; a controller that knows the generation evaluates it after a
// re-upgrade.
func TestReconcileLeavesUnprovableStampsAlone(t *testing.T) {
	unknown := stampedNodeGroup("default-abc12", "default", "stamp-from-a-newer-controller", "v99")
	unstamped := stampedNodeGroup("default-def34", "default", "", "")
	delete(unstamped.Annotations, v1alpha1.NodeClassHashLabelKey)
	ctrl, kubeClient := newTestController(t, testNodeClass("default", map[string]string{"team": "data"}), unknown, unstamped,
		testNodeClaim(unknown.Name, "default"), testNodeClaim(unstamped.Name, "default"))

	reconcileNodeClass(t, ctrl, "default")

	for _, ng := range []*ngv1.NodeGroup{unknown, unstamped} {
		if got := getNodeGroup(t, kubeClient, ng.Name); !equality.Semantic.DeepEqual(got.Annotations, ng.Annotations) {
			t.Errorf("nodegroup %s must not be touched: got %v, want %v", ng.Name, got.Annotations, ng.Annotations)
		}
		if reason := isDrifted(t, kubeClient, ng.Name); reason != "" {
			t.Errorf("nodegroup %s: IsDrifted = %q, want no drift from a stamp that proves nothing", ng.Name, reason)
		}
	}
}

// TestReconcileLeavesForeignNodeGroupsAlone pins the blast radius: the
// migration must not touch NodeGroups belonging to another NodeClass, nor any
// group this provider does not manage. Every fixture carries a v1 stamp of the
// reconciled NodeClass's unchanged spec, which the migration re-stamps
// whenever it sees one: only the List selector keeps the foreign groups out of
// it. The managed group of that NodeClass is the control proving it — if it
// is not migrated, the others staying untouched proves nothing.
func TestReconcileLeavesForeignNodeGroupsAlone(t *testing.T) {
	nodeClass := testNodeClass("default", map[string]string{"team": "data"})
	own := stampedNodeGroup("default-abc12", "default", v1StampTeamData, "v1")
	// Managed, of another NodeClass: guards the NodeClass selector.
	other := stampedNodeGroup("other-abc12", "other", v1StampTeamData, "v1")
	// Of this NodeClass, but not managed: guards the managed selector.
	unmanaged := stampedNodeGroup("manual-abc12", "default", v1StampTeamData, "v1")
	delete(unmanaged.Labels, v1alpha1.ManagedLabelKey)
	ctrl, kubeClient := newTestController(t, nodeClass, own, other, unmanaged, testNodeClaim(own.Name, "default"))

	reconcileNodeClass(t, ctrl, "default")

	if got := getNodeGroup(t, kubeClient, own.Name); got.Annotations[v1alpha1.NodeClassHashLabelKey] != nodeClass.Hash() ||
		got.Annotations[v1alpha1.NodeClassHashVersionAnnotationKey] != v1alpha1.NodeClassHashVersion {
		t.Fatalf("control: the managed group of this NodeClass was not migrated, got %v: "+
			"the foreign groups below are not proven to be filtered out", got.Annotations)
	}
	for _, ng := range []*ngv1.NodeGroup{other, unmanaged} {
		if got := getNodeGroup(t, kubeClient, ng.Name); !equality.Semantic.DeepEqual(got.Annotations, ng.Annotations) {
			t.Errorf("nodegroup %s must not be touched: got %v, want %v", ng.Name, got.Annotations, ng.Annotations)
		}
	}
}
