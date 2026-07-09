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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

func newTestProvider(t *testing.T, objs ...client.Object) (*cloudprovider.CloudProvider, client.Client) {
	cp, kubeClient, _ := newTestProviderWithCatalog(t, objs...)
	return cp, kubeClient
}

func newTestProviderWithCatalog(t *testing.T, objs ...client.Object) (*cloudprovider.CloudProvider, client.Client, *instancetype.Provider) {
	t.Helper()
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		Build()
	itp := instancetype.NewProvider("par", nil, nil)
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

// markSynced simulates the Clever Cloud operator accepting the NodeGroup.
func markSynced(t *testing.T, kubeClient client.Client, name string) {
	t.Helper()
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, ng); err != nil {
		t.Fatalf("getting nodegroup: %v", err)
	}
	ng.Status.Conditions = []ngv1.NodeGroupCondition{{Type: ngv1.ConditionTypeReady, Status: corev1.ConditionTrue, Reason: "Synced"}}
	ng.Status.Phase = ngv1.PhaseSynced
	if err := kubeClient.Update(context.Background(), ng); err != nil {
		t.Fatalf("updating nodegroup status: %v", err)
	}
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
	if ng.Spec.Labels[karpv1.NodePoolLabelKey] != "default" {
		t.Errorf("expected nodepool label on nodegroup, got %+v", ng.Spec.Labels)
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
			Name:        "default-drift",
			Labels:      map[string]string{v1alpha1.ManagedLabelKey: "true"},
			Annotations: map[string]string{v1alpha1.NodeClassHashLabelKey: nodeClass.Hash()},
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
// NodeGroup whose flavor left the served catalogue — refresher shrink or
// removed override — must keep Get and List working (core GC and node
// termination depend on them), while the synthesized type never reaches the
// provisioning catalog.

func TestGetSynthesizesWhenFlavorLeftTheCatalogue(t *testing.T) {
	cp, _, itp := newTestProviderWithCatalog(t, managedNodeGroup("default-old2xs", "2XS"))
	// The refresher shrinks the catalogue to M only (upstream removal or a
	// topology misread) while a 2XS node still runs.
	itp.SetBaseFlavors([]instancetype.Flavor{{Name: "M", CPU: 10, MemoryKi: 15988992, PriceHourly: 0.2}})

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
	cp, _, itp := newTestProviderWithCatalog(t,
		managedNodeGroup("default-known", "M"),
		managedNodeGroup("default-old2xs", "2XS"),
		managedNodeGroup("default-custom", "CUSTOM"),
	)
	// The refresher shrinks the catalogue (topology misread is the likeliest
	// trigger): 2XS leaves while its node runs, CUSTOM was never in it.
	itp.SetBaseFlavors([]instancetype.Flavor{{Name: "M", CPU: 10, MemoryKi: 15988992, PriceHourly: 0.2}})

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
