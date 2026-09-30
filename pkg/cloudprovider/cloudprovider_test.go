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

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/nodeclass"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

func newTestProvider(t *testing.T, objs ...client.Object) (*cloudprovider.CloudProvider, client.Client) {
	cp, kubeClient, _ := newTestProviderWithCatalog(t, objs...)
	return cp, kubeClient
}

func newTestProviderWithCatalog(t *testing.T, objs ...client.Object) (*cloudprovider.CloudProvider, client.Client, *instancetype.Provider) {
	t.Helper()
	return newTestProviderWithBase(t, nil, objs...)
}

// newTestProviderWithBase serves base instead of the built-in catalogue, to
// model a flavor that left it while its NodeGroups still run.
func newTestProviderWithBase(t *testing.T, base []instancetype.Flavor, objs ...client.Object) (*cloudprovider.CloudProvider, client.Client, *instancetype.Provider) {
	t.Helper()
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		Build()
	itp := instancetype.NewProvider("par", base, nil)
	ngp := nodegroup.NewProvider(kubeClient, noopRecorder{})
	return cloudprovider.New(kubeClient, itp, ngp), kubeClient, itp
}

// managedNodeGroup seeds a NodeGroup as this provider would have created it.
func managedNodeGroup(name, flavor string) *ngv1.NodeGroup {
	return &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true"},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: flavor, NodeCount: 1},
	}
}

// noopRecorder discards events; these tests assert on errors, not events.
type noopRecorder struct{}

func (noopRecorder) Publish(...events.Event) {
}

func readyNodeClass(name string) *v1alpha1.CleverNodeClass {
	nc := &v1alpha1.CleverNodeClass{ObjectMeta: metav1.ObjectMeta{Name: name}}
	nc.StatusConditions().SetTrue(v1alpha1.ConditionTypeValidationSucceeded)
	nc.StatusConditions().SetTrue(v1alpha1.ConditionTypeNodeGroupAPIServed)
	return nc
}

func testNodeClaim(name string) *karpv1.NodeClaim {
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			UID:    types.UID("uid-" + name),
			Labels: map[string]string{karpv1.NodePoolLabelKey: "default"},
		},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{
				Group: "karpenter.clever-cloud.com",
				Kind:  "CleverNodeClass",
				Name:  "default",
			},
			Requirements: []karpv1.NodeSelectorRequirementWithMinValues{
				{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"2XS", "XS", "S", "M"}},
			},
			Resources: karpv1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
		},
	}
}

// condTrue builds a True condition of the given type, as the Clever Cloud
// operator writes it.
func condTrue(condType, reason, message string) ngv1.NodeGroupCondition {
	return ngv1.NodeGroupCondition{Type: condType, Status: corev1.ConditionTrue, Reason: reason, Message: message}
}

// syncedStatus is the status of a NodeGroup the operator has accepted.
func syncedStatus() ngv1.NodeGroupStatus {
	return ngv1.NodeGroupStatus{
		Phase:      ngv1.PhaseSynced,
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReady, "Synced", "")},
	}
}

// upstreamErrorStatus is the status the operator reports while it retries a
// failing Clever Cloud API call. Measured live: phase=UpstreamError with
// Ready=True(Synced) + ReconcileInProgress=True(Scaling) +
// ReconcileFailed=True(UpstreamError), on a group that was already up; the
// operator retried until it succeeded. Without ready, the same failure lands
// on a group still in its first reconcile (Creating).
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

// setStatus plays the Clever Cloud operator: it writes the given status on the
// NodeGroup verbatim. A status may carry several conditions at once, as the
// live operator reports them. It reports failures with Errorf, never Fatalf,
// because callers run it from operator-simulating goroutines.
func setStatus(t *testing.T, kubeClient client.Client, name string, status ngv1.NodeGroupStatus) {
	t.Helper()
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, ng); err != nil {
		t.Errorf("getting nodegroup: %v", err)
		return
	}
	ng.Status = status
	if err := kubeClient.Update(context.Background(), ng); err != nil {
		t.Errorf("updating nodegroup status: %v", err)
	}
}

// markSynced simulates the Clever Cloud operator accepting the NodeGroup.
func markSynced(t *testing.T, kubeClient client.Client, name string) {
	t.Helper()
	setStatus(t, kubeClient, name, syncedStatus())
}

// statusStep separates successive statuses written by setStatusOnceCreated:
// longer than the 1s acceptance-poll interval, so the poll observes each one.
const statusStep = 1500 * time.Millisecond

// setStatusOnceCreated waits for the NodeGroup to appear, then writes each
// status in turn, statusStep apart. It stops early if the group is deleted in
// between, so a Create that wrongly deletes it fails its assertions instead of
// hanging the test.
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
				err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, &ngv1.NodeGroup{})
				if err == nil {
					break
				}
				if i > 0 && apierrors.IsNotFound(err) {
					return
				}
				time.Sleep(time.Millisecond)
			}
			setStatus(t, kubeClient, name, status)
		}
	}()
	return done
}

func TestCreatePicksCheapestCompatibleFlavor(t *testing.T) {
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"))
	nodeClaim := testNodeClaim("default-abc12")

	done := make(chan struct{})
	go func() {
		defer close(done)
		// The fake client has no Clever Cloud operator; flip the NodeGroup to
		// Synced once it appears so Create's acceptance poll returns.
		for {
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, ng); err == nil {
				markSynced(t, kubeClient, nodeClaim.Name)
				return
			}
		}
	}()
	created, err := cp.Create(context.Background(), nodeClaim)
	<-done
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := created.Labels[corev1.LabelInstanceTypeStable]; got != "2XS" {
		t.Errorf("expected cheapest flavor 2XS, got %q", got)
	}
	if got := created.Status.ProviderID; got != "clevercloud://default-abc12" {
		t.Errorf("unexpected provider id %q", got)
	}
	if got := created.Labels[karpv1.CapacityTypeLabelKey]; got != karpv1.CapacityTypeOnDemand {
		t.Errorf("unexpected capacity type %q", got)
	}
	if got := created.Labels[corev1.LabelTopologyZone]; got != "par" {
		t.Errorf("unexpected zone %q", got)
	}
	// The returned labels are the only source of the node's topology labels:
	// karpenter-core copies them onto it at registration.
	if got := created.Labels[corev1.LabelTopologyRegion]; got != "par" {
		t.Errorf("unexpected region %q", got)
	}

	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, ng); err != nil {
		t.Fatalf("expected nodegroup to exist: %v", err)
	}
	if ng.Spec.Flavor != "2XS" || ng.Spec.NodeCount != 1 {
		t.Errorf("unexpected nodegroup spec: %+v", ng.Spec)
	}
	if len(ng.Spec.Taints) != 1 || ng.Spec.Taints[0].Key != karpv1.UnregisteredTaintKey {
		t.Errorf("expected unregistered taint, got %+v", ng.Spec.Taints)
	}
	// The platform applies spec.labels to every node of the group; the
	// nodepool label reaches the registered node through karpenter-core's
	// registration sync instead (see clusterstate_test.go).
	if v, ok := ng.Spec.Labels[karpv1.NodePoolLabelKey]; ok {
		t.Errorf("nodegroup spec.labels must not carry %s (got %q)", karpv1.NodePoolLabelKey, v)
	}
}

func TestCreateRespectsMemoryRequests(t *testing.T) {
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"))
	nodeClaim := testNodeClaim("default-big01")
	// 10Gi memory cannot fit on 2XS (4GB), XS (8GB); S (12GB) is the cheapest fit.
	nodeClaim.Spec.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("10Gi")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, ng); err == nil {
				markSynced(t, kubeClient, nodeClaim.Name)
				return
			}
		}
	}()
	created, err := cp.Create(context.Background(), nodeClaim)
	<-done
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := created.Labels[corev1.LabelInstanceTypeStable]; got != "S" {
		t.Errorf("expected flavor S for 10Gi request, got %q", got)
	}
}

func TestCreateQuotaExceededReturnsInsufficientCapacity(t *testing.T) {
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"))
	nodeClaim := testNodeClaim("default-quota")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, ng); err == nil {
				ng.Status.Conditions = []ngv1.NodeGroupCondition{{
					Type: ngv1.ConditionTypeReconcileFailed, Status: corev1.ConditionTrue,
					Reason: ngv1.ReasonQuotaExceeded, Message: "Quota exceeded: RAM max",
				}}
				ng.Status.Phase = ngv1.PhaseQuotaExceeded
				_ = kubeClient.Update(context.Background(), ng)
				return
			}
		}
	}()
	_, err := cp.Create(context.Background(), nodeClaim)
	<-done
	if err == nil {
		t.Fatal("expected error")
	}
	if !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected InsufficientCapacityError, got %T: %v", err, err)
	}
	// The rejected NodeGroup must have been cleaned up.
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, ng); err == nil {
		t.Errorf("expected quota-rejected nodegroup to be deleted")
	}
}

func TestQuotaBackoffFailsFastUntilCapacityFreed(t *testing.T) {
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"))
	nodeClaim := testNodeClaim("default-quota")

	go func() {
		for {
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, ng); err == nil {
				ng.Status.Phase = ngv1.PhaseQuotaExceeded
				_ = kubeClient.Update(context.Background(), ng)
				return
			}
		}
	}()
	if _, err := cp.Create(context.Background(), nodeClaim); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected InsufficientCapacityError, got %v", err)
	}

	// Within the backoff window the next Create must fail fast without
	// creating a NodeGroup.
	second := testNodeClaim("default-quotb")
	if _, err := cp.Create(context.Background(), second); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected fast InsufficientCapacityError, got %v", err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: second.Name}, &ngv1.NodeGroup{}); err == nil {
		t.Fatal("expected no nodegroup to be created during quota backoff")
	}

	// A deletion (freed capacity) clears the backoff: Create reaches the API
	// again (the NodeGroup is created and synced).
	existing := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "default-old01",
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true"},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1},
	}
	if err := kubeClient.Create(context.Background(), existing); err != nil {
		t.Fatal(err)
	}
	oldClaim := testNodeClaim("default-old01")
	oldClaim.Status.ProviderID = "clevercloud://default-old01"
	if err := cp.Delete(context.Background(), oldClaim); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	third := testNodeClaim("default-quotc")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: third.Name}, ng); err == nil {
				markSynced(t, kubeClient, third.Name)
				return
			}
		}
	}()
	if _, err := cp.Create(context.Background(), third); err != nil {
		t.Fatalf("expected Create to succeed after capacity freed, got %v", err)
	}
	<-done
}

func TestDeleteOfALateQuotaRejectionKeepsTheBackoff(t *testing.T) {
	// The nodegroupstatus controller fails a launch the quota engine rejected
	// after the acceptance poll by deleting its NodeClaim; karpenter-core's
	// termination then calls Delete on the rejected group. That deletion
	// frees no capacity, so the re-plan's next Create must still fail fast
	// instead of hitting the exhausted quota again.
	rejected := managedNodeGroup("default-late1", "XS")
	rejected.Status = ngv1.NodeGroupStatus{
		Phase:      ngv1.PhaseQuotaExceeded,
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonQuotaExceeded, "")},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(readyNodeClass("default"), rejected).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		Build()
	ngp := nodegroup.NewProvider(kubeClient, noopRecorder{})
	cp := cloudprovider.New(kubeClient, instancetype.NewProvider("par", nil, nil), ngp)

	claim := testNodeClaim(rejected.Name)
	claim.Status.ProviderID = nodegroup.ProviderID(rejected.Name)
	ngp.RecordLateRefusal(claim, rejected)
	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: rejected.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected the rejected nodegroup deleted, got %v", err)
	}

	next := testNodeClaim("default-next1")
	if _, err := cp.Create(context.Background(), next); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected a fast InsufficientCapacityError, got %v", err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: next.Name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected no nodegroup created during the quota backoff, got %v", err)
	}
}

func TestDeleteAndGetLifecycle(t *testing.T) {
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"))
	ng := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-xyz99",
			Labels: map[string]string{
				v1alpha1.ManagedLabelKey:   "true",
				v1alpha1.NodeClaimLabelKey: "default-xyz99",
				v1alpha1.NodePoolLabelKey:  "default",
			},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "XS", NodeCount: 1},
	}
	if err := kubeClient.Create(context.Background(), ng); err != nil {
		t.Fatal(err)
	}

	claim, err := cp.Get(context.Background(), "clevercloud://default-xyz99")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if claim.Labels[corev1.LabelInstanceTypeStable] != "XS" {
		t.Errorf("unexpected instance type %q", claim.Labels[corev1.LabelInstanceTypeStable])
	}

	claims, err := cp.List(context.Background())
	if err != nil || len(claims) != 1 {
		t.Fatalf("List: %v (len=%d)", err, len(claims))
	}

	nodeClaim := testNodeClaim("default-xyz99")
	nodeClaim.Status.ProviderID = "clevercloud://default-xyz99"
	if err := cp.Delete(context.Background(), nodeClaim); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: ng.Name}, &ngv1.NodeGroup{}); err == nil {
		t.Error("expected nodegroup deleted")
	}
	if err := cp.Delete(context.Background(), nodeClaim); !corecloudprovider.IsNodeClaimNotFoundError(err) {
		t.Errorf("expected NodeClaimNotFoundError on second delete, got %v", err)
	}
	if _, err := cp.Get(context.Background(), "clevercloud://default-xyz99"); !corecloudprovider.IsNodeClaimNotFoundError(err) {
		t.Errorf("expected NodeClaimNotFoundError on Get after delete, got %v", err)
	}
}

func TestListIgnoresUnmanagedNodeGroups(t *testing.T) {
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"))
	userNG := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "user-pool"},
		Spec:       ngv1.NodeGroupSpec{Flavor: "M", NodeCount: 3},
	}
	if err := kubeClient.Create(context.Background(), userNG); err != nil {
		t.Fatal(err)
	}
	claims, err := cp.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(claims) != 0 {
		t.Errorf("expected user nodegroups to be ignored, got %d claims", len(claims))
	}
	// Deleting through the cloudprovider must refuse to touch it.
	nodeClaim := testNodeClaim("user-pool")
	nodeClaim.Status.ProviderID = "clevercloud://user-pool"
	if err := cp.Delete(context.Background(), nodeClaim); err == nil {
		t.Error("expected refusal to delete unmanaged nodegroup")
	}
}

func TestIsDriftedOnNodeClassChange(t *testing.T) {
	nodeClass := readyNodeClass("default")
	cp, kubeClient := newTestProvider(t, nodeClass)
	ng := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "default-drift",
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true"},
			Annotations: map[string]string{
				v1alpha1.NodeClassHashLabelKey:             nodeClass.Hash(),
				v1alpha1.NodeClassHashVersionAnnotationKey: v1alpha1.NodeClassHashVersion,
			},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "XS", NodeCount: 1},
	}
	if err := kubeClient.Create(context.Background(), ng); err != nil {
		t.Fatal(err)
	}
	nodeClaim := testNodeClaim("default-drift")
	nodeClaim.Status.ProviderID = "clevercloud://default-drift"

	reason, err := cp.IsDrifted(context.Background(), nodeClaim)
	if err != nil || reason != "" {
		t.Fatalf("expected no drift, got %q err=%v", reason, err)
	}

	nodeClass.Spec.Labels = map[string]string{"team": "data"}
	if err := kubeClient.Update(context.Background(), nodeClass); err != nil {
		t.Fatal(err)
	}
	reason, err = cp.IsDrifted(context.Background(), nodeClaim)
	if err != nil {
		t.Fatalf("IsDrifted: %v", err)
	}
	if reason == "" {
		t.Error("expected drift after nodeclass label change")
	}
}

func TestGetInstanceTypesCatalog(t *testing.T) {
	cp, _ := newTestProvider(t, readyNodeClass("default"))
	its, err := cp.GetInstanceTypes(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetInstanceTypes: %v", err)
	}
	if len(its) != 6 {
		t.Fatalf("expected 6 flavors, got %d", len(its))
	}
	for _, it := range its {
		if len(it.Offerings) != 1 || it.Offerings[0].Price <= 0 {
			t.Errorf("flavor %s: missing or invalid offering", it.Name)
		}
		allocatable := it.Allocatable()
		if allocatable.Memory().Value() >= it.Capacity.Memory().Value() {
			t.Errorf("flavor %s: allocatable not reduced by overhead", it.Name)
		}
	}
}

// The tests below pin the unknown-flavor degradation contract: a running
// NodeGroup whose flavor left the served catalogue — a removed override, or a
// release whose built-in seed dropped it — must keep Get and List working
// (core GC and node termination depend on them), while the synthesized type
// never reaches the provisioning catalog.

// onlyM is a catalogue that lost every flavor but M.
var onlyM = []instancetype.Flavor{{Name: "M", CPU: 10, MemoryKi: 15988992}}

// requireNotServed fails the test unless flavor is really absent from the
// served catalogue. Without it, a provider that ignored the narrowed base and
// served the full seed would pass every degradation assertion below: seed
// sizing and a normal lookup look the same from the outside.
func requireNotServed(t *testing.T, itp *instancetype.Provider, flavor string) {
	t.Helper()
	if _, err := itp.Get(flavor); !errors.Is(err, instancetype.ErrUnknownFlavor) {
		t.Fatalf("precondition: %s must be absent from the served catalogue, Get returned err=%v", flavor, err)
	}
}

func TestGetSynthesizesWhenFlavorLeftTheCatalogue(t *testing.T) {
	// The catalogue serves M only while a 2XS node still runs.
	cp, _, itp := newTestProviderWithBase(t, onlyM, managedNodeGroup("default-old2xs", "2XS"))
	requireNotServed(t, itp, "2XS")

	claim, err := cp.Get(context.Background(), "clevercloud://default-old2xs")
	if err != nil {
		t.Fatalf("Get must degrade, not fail: %v", err)
	}
	if claim.Status.ProviderID != "clevercloud://default-old2xs" {
		t.Errorf("provider id = %q", claim.Status.ProviderID)
	}
	if got := claim.Labels[corev1.LabelInstanceTypeStable]; got != "2XS" {
		t.Errorf("instance-type label = %q, want 2XS", got)
	}
	// Seed sizing survives for seed-known flavors.
	if cpu := claim.Status.Capacity[corev1.ResourceCPU]; cpu.Value() != 4 {
		t.Errorf("capacity cpu = %v, want the 2XS seed value 4", cpu.Value())
	}
}

func TestListIncludesNodeGroupWithUnknownFlavor(t *testing.T) {
	// 2XS left the catalogue while its node runs; CUSTOM was only ever there
	// through a since-removed override.
	cp, _, itp := newTestProviderWithBase(t, onlyM,
		managedNodeGroup("default-known", "M"),
		managedNodeGroup("default-old2xs", "2XS"),
		managedNodeGroup("default-custom", "CUSTOM"),
	)
	requireNotServed(t, itp, "2XS")

	claims, err := cp.List(context.Background())
	if err != nil {
		t.Fatalf("List must degrade per entry, not fail: %v", err)
	}
	if len(claims) != 3 {
		t.Fatalf("expected all nodegroups listed, got %d", len(claims))
	}
	// Skipping instead of synthesizing would make karpenter-core's GC read
	// the missing provider ID as an orphaned claim and delete it as soon as
	// its node is NotReady (a kubelet restart suffices).
	ids := map[string]struct{}{}
	for _, claim := range claims {
		ids[claim.Status.ProviderID] = struct{}{}
	}
	for _, id := range []string{"clevercloud://default-old2xs", "clevercloud://default-custom"} {
		if _, ok := ids[id]; !ok {
			t.Errorf("degraded nodegroup %s missing from List, got %v", id, ids)
		}
	}
}

func TestGetEnrichesSynthesizedTypeWithObservedCapacity(t *testing.T) {
	cp, _, itp := newTestProviderWithCatalog(t, managedNodeGroup("default-cust1", "CUSTOM"))

	// Name-only floor: nothing is known about CUSTOM yet.
	claim, err := cp.Get(context.Background(), "clevercloud://default-cust1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if cpu := claim.Status.Capacity[corev1.ResourceCPU]; !cpu.IsZero() {
		t.Errorf("expected zero cpu before any observation, got %v", cpu.Value())
	}

	// A live node reports real capacity; the synthesized type picks it up.
	itp.RecordObservedCapacity("CUSTOM",
		corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("8Gi")},
		corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("7Gi")},
	)
	claim, err = cp.Get(context.Background(), "clevercloud://default-cust1")
	if err != nil {
		t.Fatalf("Get after observation: %v", err)
	}
	if cpu := claim.Status.Capacity[corev1.ResourceCPU]; cpu.Value() != 6 {
		t.Errorf("capacity cpu = %v, want observed 6", cpu.Value())
	}
}

func TestSynthesizedFlavorStaysOutOfTheProvisioningCatalog(t *testing.T) {
	cp, _, _ := newTestProviderWithCatalog(t, managedNodeGroup("default-cust2", "CUSTOM"))

	if _, err := cp.Get(context.Background(), "clevercloud://default-cust2"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	// Serving CUSTOM for provisioning would let a zero-priced synthetic win
	// every cheapest-first decision and create NodeGroups the platform
	// rejects.
	its, err := cp.GetInstanceTypes(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetInstanceTypes: %v", err)
	}
	for _, it := range its {
		if it.Name == "CUSTOM" {
			t.Fatal("synthesized flavor leaked into the provisioning catalog")
		}
	}
}

func TestDeleteWorksWhenFlavorUnknown(t *testing.T) {
	cp, kubeClient, _ := newTestProviderWithCatalog(t, managedNodeGroup("default-cust3", "CUSTOM"))
	claim := testNodeClaim("default-cust3")
	claim.Status.ProviderID = "clevercloud://default-cust3"

	// Delete must never depend on an instance-type lookup: it is the path
	// that releases the actual VM.
	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: "default-cust3"}, &ngv1.NodeGroup{}); err == nil {
		t.Error("expected the nodegroup to be deleted")
	}
}

func TestCreateVanishedNodeGroupReturnsInsufficientCapacity(t *testing.T) {
	// A vanish must reach karpenter-core as InsufficientCapacityError: a
	// plain error would retry the SAME claim in a create→vanish loop holding
	// the creation mutex; ICE deletes the claim and re-plans.
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"))
	nodeClaim := testNodeClaim("default-vanish")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, ng); err == nil {
				// Let the poll observe the group once, then reclaim it.
				time.Sleep(1500 * time.Millisecond)
				_ = kubeClient.Delete(context.Background(), ng)
				return
			}
		}
	}()
	_, err := cp.Create(context.Background(), nodeClaim)
	<-done
	if !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected InsufficientCapacityError on vanish, got %T: %v", err, err)
	}
}

// TestLegacyNodeClassLabelKeepsProvisioningWithoutDelivery drives the upgrade
// trap of a key v0.12.0 accepted and the shared label rule now rejects,
// through the real nodeclass controller and Create. Failing its validation
// made the NodeClass NotReady and Create refused every launch from it, so
// provisioning stopped for every NodePool using it; and the only way out,
// removing the key, drifted every node although no NodeGroup payload changed.
// Now the NodeClass launches, the key stays out of the payload and of the hash
// stamped next to it, and removing it drifts nothing — whatever the syntax of
// its value, which v0.12.0 never checked for a key it did not deliver.
func TestLegacyNodeClassLabelKeepsProvisioningWithoutDelivery(t *testing.T) {
	for key, value := range map[string]string{
		"app.kubernetes.io/part-of":  "shop",
		"app.kubernetes.io/name":     "My Platform",
		"karpenter.sh/capacity-type": "on-demand",
	} {
		t.Run(key, func(t *testing.T) {
			ctx := context.Background()
			// As v0.12.0 left it: admitted, and Ready.
			nodeClass := readyNodeClass("default")
			nodeClass.Spec.Labels = map[string]string{"team": "data", key: value}
			cp, kubeClient := newTestProvider(t, nodeClass)
			reconcileNodeClass := func() {
				t.Helper()
				if _, err := nodeclass.NewController(kubeClient, noopRecorder{}).Reconcile(ctx,
					reconcile.Request{NamespacedName: types.NamespacedName{Name: "default"}}); err != nil {
					t.Fatalf("reconciling nodeclass: %v", err)
				}
			}
			reconcileNodeClass()

			nodeClaim := testNodeClaim("default-legacy")
			done := setStatusOnceCreated(t, kubeClient, nodeClaim.Name, syncedStatus())
			created, err := cp.Create(ctx, nodeClaim)
			if err != nil {
				// Before waiting on done: a refused launch creates no group,
				// and the status writer would wait for one forever.
				t.Fatalf("Create from a NodeClass carrying legacy key %s: %v — provisioning stops on upgrade", key, err)
			}
			<-done
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: nodeClaim.Name}, ng); err != nil {
				t.Fatalf("getting nodegroup: %v", err)
			}
			if _, ok := ng.Spec.Labels[key]; ok {
				t.Errorf("legacy key %s reached the NodeGroup payload: %v", key, ng.Spec.Labels)
			}
			if ng.Spec.Labels["team"] != "data" {
				t.Errorf("delivered label team lost from the NodeGroup payload: %v", ng.Spec.Labels)
			}
			delivered := readyNodeClass("default")
			delivered.Spec.Labels = map[string]string{"team": "data"}
			if got := ng.Annotations[v1alpha1.NodeClassHashLabelKey]; got != delivered.Hash() {
				t.Errorf("the stamped hash %q does not describe the payload (want %q, the hash without %s)", got, delivered.Hash(), key)
			}

			// The way out: remove the key.
			current := &v1alpha1.CleverNodeClass{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: "default"}, current); err != nil {
				t.Fatal(err)
			}
			current.Spec.Labels = map[string]string{"team": "data"}
			if err := kubeClient.Update(ctx, current); err != nil {
				t.Fatal(err)
			}
			reconcileNodeClass()
			nodeClaim.Status.ProviderID = created.Status.ProviderID
			reason, err := cp.IsDrifted(ctx, nodeClaim)
			if err != nil {
				t.Fatalf("IsDrifted: %v", err)
			}
			if reason != "" {
				t.Errorf("IsDrifted = %q after removing %s, which never reached the node", reason, key)
			}
		})
	}
}

func TestCreateRefusesUnknownReadiness(t *testing.T) {
	// A NodeClass whose readiness was never computed (no conditions) must not
	// launch machines: Ready has to be affirmatively True, not merely
	// not-False.
	unknown := &v1alpha1.CleverNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	cp, _ := newTestProvider(t, unknown)

	_, err := cp.Create(context.Background(), testNodeClaim("default-unrdy"))
	if err == nil {
		t.Fatal("expected an error for a NodeClass with unknown readiness")
	}
	if !corecloudprovider.IsNodeClassNotReadyError(err) {
		t.Fatalf("expected NodeClassNotReadyError, got %T: %v", err, err)
	}
}

// TestCreateRefusesTerminatingNodeClass pins the deletion-convergence guard.
// Deleting a CleverNodeClass never changes its readiness conditions, so a
// terminating NodeClass still reads Ready=True; launching from it creates a
// real hourly-billed VM whose NodeClaim then makes the nodeclass finalizer
// requeue forever — deletion never converges.
func TestCreateRefusesTerminatingNodeClass(t *testing.T) {
	nodeClass := readyNodeClass("default")
	nodeClass.Finalizers = []string{v1alpha1.TerminationFinalizer}
	cp, kubeClient := newTestProvider(t, nodeClass)
	// The finalizer keeps the object around with a DeletionTimestamp, exactly
	// the state the nodeclass controller holds while NodeClaims still exist.
	if err := kubeClient.Delete(context.Background(), nodeClass); err != nil {
		t.Fatalf("deleting nodeclass: %v", err)
	}

	nodeClaim := testNodeClaim("default-term1")
	_, err := cp.Create(context.Background(), nodeClaim)
	if err == nil {
		t.Fatal("expected an error for a terminating NodeClass")
	}
	if !corecloudprovider.IsNodeClassNotReadyError(err) {
		t.Fatalf("expected NodeClassNotReadyError, got %T: %v", err, err)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, &ngv1.NodeGroup{}); err == nil {
		t.Error("expected no nodegroup to be created from a terminating nodeclass")
	}
}

// TestIsDriftedComparesStampsWithTheirOwnGeneration pins drift across hash
// generations. A NodeGroup stamped by an older generation of Hash() is
// compared with what THAT generation computes for the current spec: never with
// today's Hash(), which would report drift on every NodeGroup at once and
// replace every node — real, hourly-billed VMs — on a controller upgrade; and
// never skipped, which would hide a NodeClass edit made before the nodeclass
// controller migrated the stamp. The stamps are what controllers up to
// v0.11.x (generation v1, no version annotation) wrote, pinned by
// TestHashGenerationsMatchTheirStamps in pkg/apis/v1alpha1.
func TestIsDriftedComparesStampsWithTheirOwnGeneration(t *testing.T) {
	const (
		v1StampTeamData    = "3789529822245891689"  // labels: {team: data}
		v1StampEmptyLabels = "14514438007709706818" // labels: {}
	)
	for _, tc := range []struct {
		name        string
		labels      map[string]string
		annotations map[string]string
		want        corecloudprovider.DriftReason
	}{
		{
			name:        "v1 stamp of the current spec",
			labels:      map[string]string{"team": "data"},
			annotations: map[string]string{v1alpha1.NodeClassHashLabelKey: v1StampTeamData},
		},
		{
			name:   "explicit v1 stamp of the current spec",
			labels: map[string]string{"team": "data"},
			annotations: map[string]string{
				v1alpha1.NodeClassHashLabelKey:             v1StampTeamData,
				v1alpha1.NodeClassHashVersionAnnotationKey: "v1",
			},
		},
		{
			name:        "v1 stamp of labels: {}, line dropped since",
			labels:      nil,
			annotations: map[string]string{v1alpha1.NodeClassHashLabelKey: v1StampEmptyLabels},
		},
		{
			name:        "v1 stamp of an edited NodeClass",
			labels:      map[string]string{"team": "ml"},
			annotations: map[string]string{v1alpha1.NodeClassHashLabelKey: v1StampTeamData},
			want:        cloudprovider.NodeClassDrifted,
		},
		{
			name:   "explicit v1 stamp of an edited NodeClass",
			labels: map[string]string{"team": "ml"},
			annotations: map[string]string{
				v1alpha1.NodeClassHashLabelKey:             v1StampTeamData,
				v1alpha1.NodeClassHashVersionAnnotationKey: "v1",
			},
			want: cloudprovider.NodeClassDrifted,
		},
		{
			name:   "stamp from a generation this controller does not know",
			labels: map[string]string{"team": "data"},
			annotations: map[string]string{
				v1alpha1.NodeClassHashLabelKey:             "stamp-from-a-newer-controller",
				v1alpha1.NodeClassHashVersionAnnotationKey: "v99",
			},
		},
		{
			name:        "no hash at all",
			labels:      map[string]string{"team": "data"},
			annotations: map[string]string{v1alpha1.NodeClassHashVersionAnnotationKey: v1alpha1.NodeClassHashVersion},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeClass := readyNodeClass("default")
			nodeClass.Spec.Labels = tc.labels
			cp, kubeClient := newTestProvider(t, nodeClass)
			ng := &ngv1.NodeGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "default-stamped",
					Labels:      map[string]string{v1alpha1.ManagedLabelKey: "true"},
					Annotations: tc.annotations,
				},
				Spec: ngv1.NodeGroupSpec{Flavor: "XS", NodeCount: 1},
			}
			if err := kubeClient.Create(context.Background(), ng); err != nil {
				t.Fatal(err)
			}
			nodeClaim := testNodeClaim("default-stamped")
			nodeClaim.Status.ProviderID = "clevercloud://default-stamped"

			reason, err := cp.IsDrifted(context.Background(), nodeClaim)
			if err != nil {
				t.Fatalf("IsDrifted: %v", err)
			}
			if reason != tc.want {
				t.Errorf("IsDrifted = %q, want %q", reason, tc.want)
			}
		})
	}
}

// TestCreateAdoptionDescribesTheExistingFlavor pins the adoption truth
// contract: nodegroup.Provider.Create is idempotent, and on AlreadyExists it
// adopts a group owned by the same NodeClaim whose immutable flavor can differ
// from the one resolved by THIS attempt (retry after a transient error, with
// the original flavor since refused or re-priced). The returned claim must
// describe the machine that exists — building it from the freshly resolved
// flavor would hand the scheduler a capacity and price no running node has,
// and nothing downstream ever corrects it.
func TestCreateAdoptionDescribesTheExistingFlavor(t *testing.T) {
	nodeClaim := testNodeClaim("default-adopt")
	// The existing group carries the full ownership proof Create's adoption
	// path requires (managed label, nodeclaim label, NodeClaim owner
	// reference), a flavor resolveInstanceType would NOT pick for this claim
	// (2XS is the cheapest fit), and is already accepted upstream so the
	// acceptance poll returns immediately.
	existing := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeClaim.Name,
			Labels: map[string]string{
				v1alpha1.ManagedLabelKey:   "true",
				v1alpha1.NodeClaimLabelKey: nodeClaim.Name,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "karpenter.sh/v1",
				Kind:       "NodeClaim",
				Name:       nodeClaim.Name,
				UID:        nodeClaim.UID,
			}},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "S", NodeCount: 1},
		Status: ngv1.NodeGroupStatus{
			Conditions: []ngv1.NodeGroupCondition{{Type: ngv1.ConditionTypeReady, Status: corev1.ConditionTrue, Reason: "Synced"}},
			Phase:      ngv1.PhaseSynced,
		},
	}
	cp, _, itp := newTestProviderWithCatalog(t, readyNodeClass("default"), existing)

	created, err := cp.Create(context.Background(), nodeClaim)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := created.Labels[corev1.LabelInstanceTypeStable]; got != "S" {
		t.Errorf("instance-type label = %q, want the existing group's flavor S, not the freshly resolved one", got)
	}
	want, err := itp.Get("S")
	if err != nil {
		t.Fatalf("Get(S): %v", err)
	}
	for name, wantQty := range want.Capacity {
		if gotQty := created.Status.Capacity[name]; gotQty.Cmp(wantQty) != 0 {
			t.Errorf("capacity[%s] = %v, want S's %v", name, gotQty.String(), wantQty.String())
		}
	}
	for name, wantQty := range want.Allocatable() {
		if gotQty := created.Status.Allocatable[name]; gotQty.Cmp(wantQty) != 0 {
			t.Errorf("allocatable[%s] = %v, want S's %v", name, gotQty.String(), wantQty.String())
		}
	}
}

// markRefused simulates the Clever Cloud operator refusing the NodeGroup for a
// reason that is not the organisation quota.
func markRefused(t *testing.T, kubeClient client.Client, name, reason string) <-chan struct{} {
	t.Helper()
	return setStatusOnceCreated(t, kubeClient, name, ngv1.NodeGroupStatus{
		Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, reason, "flavor is not available on this cluster")},
	})
}

// TestCreateFlavorRefusalReturnsInsufficientCapacity covers the cloudprovider
// half of the terminal-refusal path. Anything other than an
// InsufficientCapacityError makes karpenter-core retry the same claim with the
// same flavor instead of re-planning, so the refusal would still cost a full
// registration TTL.
func TestCreateFlavorRefusalReturnsInsufficientCapacity(t *testing.T) {
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"))
	nodeClaim := testNodeClaim("default-refused")

	done := markRefused(t, kubeClient, nodeClaim.Name, "FlavorNotAvailable")
	_, err := cp.Create(context.Background(), nodeClaim)
	<-done

	if err == nil {
		t.Fatal("expected the refusal to fail the launch")
	}
	if !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Errorf("expected an InsufficientCapacityError so the scheduler re-plans, got %T: %v", err, err)
	}
}

// TestCreateAvoidsARefusedFlavor covers the other half of the coupling. The
// catalogue is deliberately permissive — it offers every built-in flavor on
// every topology — so an upstream refusal is the only signal that one of them
// is unusable here. karpenter-core keeps no per-offering memory of an
// InsufficientCapacityError, so without the hold the scheduler would re-pick
// the same cheapest flavor forever.
func TestCreateAvoidsARefusedFlavor(t *testing.T) {
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"))

	// 2XS is the cheapest flavor satisfying the claim; get it refused.
	refused := testNodeClaim("default-refused")
	done := markRefused(t, kubeClient, refused.Name, "FlavorNotAvailable")
	if _, err := cp.Create(context.Background(), refused); err == nil {
		t.Fatal("expected the refusal to fail the launch")
	}
	<-done

	// The next launch must land on the next-cheapest flavor instead.
	next := testNodeClaim("default-next")
	syncDone := make(chan struct{})
	go func() {
		defer close(syncDone)
		for {
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: next.Name}, ng); err == nil {
				markSynced(t, kubeClient, next.Name)
				return
			}
		}
	}()
	created, err := cp.Create(context.Background(), next)
	<-syncDone
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := created.Labels[corev1.LabelInstanceTypeStable]; got == "2XS" {
		t.Errorf("expected the refused flavor to be skipped, got %q again", got)
	} else if got != "XS" {
		t.Errorf("expected the next-cheapest flavor XS, got %q", got)
	}
}

// TestCreateUpstreamErrorDoesNotWalkTheCatalogue covers the cloudprovider half
// of a Clever Cloud API incident. The operator reports
// ReconcileFailed=True(UpstreamError) on whatever group it is reconciling and
// retries on its own. Read as a flavor refusal, each launch failed with ICE
// and held its flavor out, so successive claims walked the catalogue until
// every flavor was held out and provisioning stopped. The launch must succeed
// once the retry does, and the flavor must stay available.
func TestCreateUpstreamErrorDoesNotWalkTheCatalogue(t *testing.T) {
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"))

	incident := testNodeClaim("default-incident")
	done := setStatusOnceCreated(t, kubeClient, incident.Name, upstreamErrorStatus(false), syncedStatus())
	created, err := cp.Create(context.Background(), incident)
	<-done
	if err != nil {
		t.Fatalf("a transient upstream failure must not fail the launch, got %T: %v (ICE=%v)",
			err, err, corecloudprovider.IsInsufficientCapacityError(err))
	}
	if got := created.Labels[corev1.LabelInstanceTypeStable]; got != "2XS" {
		t.Errorf("expected the cheapest flavor 2XS, got %q", got)
	}

	// The next launch still gets the cheapest flavor: nothing was held out.
	next := testNodeClaim("default-next")
	syncDone := setStatusOnceCreated(t, kubeClient, next.Name, syncedStatus())
	created, err = cp.Create(context.Background(), next)
	<-syncDone
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := created.Labels[corev1.LabelInstanceTypeStable]; got != "2XS" {
		t.Errorf("a platform-side failure must not hold 2XS out, next launch got %q", got)
	}
}

// TestCreateAdoptsReadyGroupDuringUpstreamError covers the AlreadyExists
// adoption path with the exact status measured live: a Ready group whose
// operator is retrying a failed Clever Cloud API call. The group is the VM this
// very claim launched on an earlier attempt; it was deleted as "refused" and
// the launch failed with ICE.
func TestCreateAdoptsReadyGroupDuringUpstreamError(t *testing.T) {
	nodeClaim := testNodeClaim("default-adopt")
	existing := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeClaim.Name,
			Labels: map[string]string{
				v1alpha1.ManagedLabelKey:   "true",
				v1alpha1.NodeClaimLabelKey: nodeClaim.Name,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "karpenter.sh/v1",
				Kind:       "NodeClaim",
				Name:       nodeClaim.Name,
				UID:        nodeClaim.UID,
			}},
		},
		Spec:   ngv1.NodeGroupSpec{Flavor: "S", NodeCount: 1},
		Status: upstreamErrorStatus(true),
	}
	cp, kubeClient := newTestProvider(t, readyNodeClass("default"), existing)
	timeoutsBefore := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total")

	created, err := cp.Create(context.Background(), nodeClaim)
	if err != nil {
		t.Fatalf("adopting a Ready group must succeed, got %T: %v (ICE=%v)",
			err, err, corecloudprovider.IsInsufficientCapacityError(err))
	}
	if got := created.Labels[corev1.LabelInstanceTypeStable]; got != "S" {
		t.Errorf("instance-type label = %q, want the adopted group's S", got)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, &ngv1.NodeGroup{}); err != nil {
		t.Errorf("a Ready group must never be deleted: %v", err)
	}
	// Accepted on the first poll, not waited out as "in progress": a timeout
	// is optimistic success too, so err alone proves nothing here.
	if delta := metricstest.Value(t, "karpenter_clevercloud_nodegroup_acceptance_timeouts_total") - timeoutsBefore; delta != 0 {
		t.Errorf("acceptance_timeouts_total delta = %v, want 0: a Ready group is accepted on the first poll", delta)
	}
}
