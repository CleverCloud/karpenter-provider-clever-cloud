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
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
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
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		Build()
	return nodeclass.NewController(kubeClient), kubeClient
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
// to validate cleanly and then go missing or fail downstream: subdomained
// kubernetes.io/ keys are dropped by the NodeGroup label filter and a NodeClass
// label has no other path to the node, while malformed keys or values are
// refused by the apiserver when the label lands on the Node — either way the
// NodeClass must not go Ready, and the condition must say why.
func TestReconcileRejectsUndeliverableLabels(t *testing.T) {
	for _, tc := range []struct {
		name        string
		labels      map[string]string
		wantMessage string
	}{
		{
			name:        "subdomained kubernetes.io key",
			labels:      map[string]string{"app.kubernetes.io/name": "web"},
			wantMessage: "kubernetes.io/ domain",
		},
		{
			name:        "topology.kubernetes.io key",
			labels:      map[string]string{"topology.kubernetes.io/region": "par"},
			wantMessage: "kubernetes.io/ domain",
		},
		{
			// Not undeliverable but worse: it reached the node at join and
			// told karpenter-core the node was initialized before it was.
			name:        "karpenter.sh key",
			labels:      map[string]string{"karpenter.sh/initialized": "true"},
			wantMessage: "karpenter.sh domain",
		},
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
	ctrl := nodeclass.NewController(kubeClient)

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

	result := reconcileNodeClass(t, nodeclass.NewController(kubeClient), "default")
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
	cp := cloudprovider.New(kubeClient, instancetype.NewProvider("par", nil, nil), nodegroup.NewProvider(kubeClient, noopRecorder{}))
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

	reconcileNodeClass(t, nodeclass.NewController(kubeClient), "default")

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
