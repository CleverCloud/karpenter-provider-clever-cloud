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

// These tests pin what karpenter-core's scheduler learns when the Clever Cloud
// operator turns a launch down. Core keeps no memory of an
// InsufficientCapacityError: it deletes the claim and re-plans over the
// offerings GetInstanceTypes returns, so Offering.Available is the only way a
// quota rejection or a flavor refusal reaches it. Before these offerings
// carried it, core rebuilt the rejected claim on every pass: ten 2Gi pods with
// 10 GB of quota left stayed Pending for good, and a weighted NodePool never
// fell back from a refused flavor.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	coresched "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// nominalGB is the memory each flavor is sold with, which is what the
// organisation quota counts (its default is 40 vCPU / 40 GB).
var nominalGB = map[string]int64{"2XS": 4, "XS": 8, "S": 12, "M": 16, "L": 24, "XL": 32}

// fakeOperator plays the Clever Cloud node-group operator and quota engine on
// the NodeGroups created through the fake client, deciding at creation, as the
// live operator's first status write does. A flavor in refused is refused for
// a reason other than the quota. A group whose nominal memory exceeds the
// organisation's remaining headroom is rejected with the live shape (phase and
// reason QuotaExceeded, no message). Any other is accepted (Ready) and
// consumes its share of the headroom.
type fakeOperator struct {
	mu         sync.Mutex
	headroomGB int64
	refused    map[string]bool
	// flavors lists the flavor of every NodeGroup that reached the API.
	flavors []string
}

func (o *fakeOperator) create(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	if ng, ok := obj.(*ngv1.NodeGroup); ok {
		o.mu.Lock()
		o.flavors = append(o.flavors, ng.Spec.Flavor)
		switch gb := nominalGB[ng.Spec.Flavor]; {
		case o.refused[ng.Spec.Flavor]:
			ng.Status = ngv1.NodeGroupStatus{Conditions: []ngv1.NodeGroupCondition{
				condTrue(ngv1.ConditionTypeReconcileFailed, "FlavorNotAvailable", "flavor is not available on this cluster"),
			}}
		case gb > o.headroomGB:
			ng.Status = ngv1.NodeGroupStatus{
				Phase:      ngv1.PhaseQuotaExceeded,
				Conditions: []ngv1.NodeGroupCondition{condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonQuotaExceeded, "")},
			}
		default:
			o.headroomGB -= gb
			ng.Status = syncedStatus()
		}
		o.mu.Unlock()
	}
	return c.Create(ctx, obj, opts...)
}

func (o *fakeOperator) createCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.flavors)
}

// createdFlavors returns the flavors of the NodeGroups that reached the API,
// in order.
func (o *fakeOperator) createdFlavors() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.flavors)
}

// newOperatorProvider builds the provider over a fake client whose NodeGroups
// the fake operator decides on, carrying the pod index karpenter-core's
// scheduler and cluster state read through.
func newOperatorProvider(t *testing.T, operator *fakeOperator, objs ...client.Object) (*cloudprovider.CloudProvider, client.WithWatch) {
	t.Helper()
	return newClockedOperatorProvider(t, operator, clock.RealClock{}, objs...)
}

// newClockedOperatorProvider is newOperatorProvider with the clock that times
// the quota backoff and the refusal hold-out.
func newClockedOperatorProvider(t *testing.T, operator *fakeOperator, clk clock.PassiveClock, objs ...client.Object) (*cloudprovider.CloudProvider, client.WithWatch) {
	t.Helper()
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(append([]client.Object{readyNodeClass("default")}, objs...)...).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithInterceptorFuncs(interceptor.Funcs{Create: operator.create}).
		Build()
	itp := instancetype.NewProvider("par", nil, nil)
	return cloudprovider.New(kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clk)), kubeClient
}

// claimFor is a NodeClaim restricted to the given flavors, requesting memory.
func claimFor(name, memory string, flavors ...string) *karpv1.NodeClaim {
	nodeClaim := testNodeClaim(name)
	nodeClaim.Spec.Requirements = []karpv1.NodeSelectorRequirementWithMinValues{
		{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: flavors},
	}
	nodeClaim.Spec.Resources.Requests = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("100m"),
		corev1.ResourceMemory: resource.MustParse(memory),
	}
	return nodeClaim
}

// sizedPod is an unschedulable pod requesting memory.
func sizedPod(name, memory string) *corev1.Pod {
	pod := pendingPod(name)
	pod.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse(memory)
	return pod
}

// requireAvailability fails the test unless GetInstanceTypes serves every
// catalogue flavor — an unavailable one included — with an available offering
// exactly for the flavors want marks true.
func requireAvailability(t *testing.T, cp *cloudprovider.CloudProvider, want map[string]bool) {
	t.Helper()
	its, err := cp.GetInstanceTypes(context.Background(), topologyNodePool("default"))
	if err != nil {
		t.Fatalf("GetInstanceTypes: %v", err)
	}
	if len(its) != len(instancetype.DefaultFlavors) {
		t.Errorf("GetInstanceTypes served %d flavors, want all %d: an unavailable flavor must stay in the catalogue", len(its), len(instancetype.DefaultFlavors))
	}
	for _, it := range its {
		if got := len(it.Offerings.Available()) > 0; got != want[it.Name] {
			t.Errorf("flavor %s available = %v, want %v", it.Name, got, want[it.Name])
		}
	}
}

// launchAll does what karpenter-core's launch controller does with each claim
// the scheduler planned: Create, and on an InsufficientCapacityError move on
// (core deletes the claim). It returns how many pods landed on an accepted
// NodeGroup and the flavors launched.
func launchAll(t *testing.T, cp *cloudprovider.CloudProvider, results coresched.Results, prefix string) (int, []string) {
	t.Helper()
	placed, flavors := 0, []string{}
	for i, planned := range results.NewNodeClaims {
		nodeClaim := planned.ToNodeClaim()
		nodeClaim.Name = fmt.Sprintf("%s-c%d", prefix, i)
		nodeClaim.UID = types.UID("uid-" + nodeClaim.Name)
		created, err := cp.Create(context.Background(), nodeClaim)
		if err != nil {
			if !corecloudprovider.IsInsufficientCapacityError(err) {
				t.Fatalf("Create %s: want success or an InsufficientCapacityError, got %T: %v", nodeClaim.Name, err, err)
			}
			continue
		}
		placed += len(planned.Pods)
		flavors = append(flavors, created.Labels[corev1.LabelInstanceTypeStable])
	}
	return placed, flavors
}

// TestQuotaRejectionMarksLargerFlavorsUnavailable covers what the scheduler is
// served after the quota rejects an L: L and every flavor at least as large
// unavailable, every smaller one still available, and none dropped. A claim
// only covered flavors can hold then fails fast without reaching the API; a
// claim the remaining quota can hold is launched instead of being failed by
// the backoff, which used to be global.
func TestQuotaRejectionMarksLargerFlavorsUnavailable(t *testing.T) {
	operator := &fakeOperator{headroomGB: 10}
	cp, _ := newOperatorProvider(t, operator)
	ctx := context.Background()

	// The claim karpenter-core builds for ten 2Gi pods: only L and XL hold it.
	if _, err := cp.Create(ctx, claimFor("default-big01", "20Gi", "L", "XL")); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected the quota to reject the L, got %T: %v", err, err)
	}
	requireAvailability(t, cp, map[string]bool{"2XS": true, "XS": true, "S": true, "M": true, "L": false, "XL": false})

	if _, err := cp.Create(ctx, claimFor("default-big02", "20Gi", "L", "XL")); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected a claim only covered flavors hold to fail fast, got %T: %v", err, err)
	}
	if got := operator.createCount(); got != 1 {
		t.Errorf("NodeGroups created = %d, want 1: a covered flavor must not reach the API", got)
	}

	created, err := cp.Create(ctx, claimFor("default-small", "1Gi", "2XS", "XS", "S", "M", "L", "XL"))
	if err != nil {
		t.Fatalf("a claim the remaining quota holds must launch, got %T: %v", err, err)
	}
	if got := created.Labels[corev1.LabelInstanceTypeStable]; got != "2XS" {
		t.Errorf("launched flavor = %q, want 2XS", got)
	}
}

// TestRefusedFlavorIsReportedUnavailable covers the hold-out as the scheduler
// sees it. It used to be applied in resolveInstanceType only, while
// GetInstanceTypes kept advertising the refused flavor: core kept planning
// claims on it, and a claim restricted to it failed on every pass. A refusal
// is a verdict on the flavor, not on capacity, so the larger flavors stay
// available.
func TestRefusedFlavorIsReportedUnavailable(t *testing.T) {
	operator := &fakeOperator{headroomGB: 100, refused: map[string]bool{"S": true}}
	cp, _ := newOperatorProvider(t, operator)

	if _, err := cp.Create(context.Background(), claimFor("default-refs1", "1Gi", "S")); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected the refusal to fail the launch, got %T: %v", err, err)
	}
	requireAvailability(t, cp, map[string]bool{"2XS": true, "XS": true, "S": false, "M": true, "L": true, "XL": true})
}

// TestFreedCapacityRestoresAvailability pins that the quota rejection is
// forgotten as soon as a NodeGroup holding capacity is deleted: every flavor is
// available to the scheduler again, without waiting out the backoff.
func TestFreedCapacityRestoresAvailability(t *testing.T) {
	running := managedNodeGroup("default-run01", "XS")
	running.Status = syncedStatus()
	operator := &fakeOperator{headroomGB: 10}
	cp, _ := newOperatorProvider(t, operator, running)

	if _, err := cp.Create(context.Background(), claimFor("default-big01", "10Gi", "S")); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected the quota to reject the S, got %T: %v", err, err)
	}
	requireAvailability(t, cp, map[string]bool{"2XS": true, "XS": true, "S": false, "M": false, "L": false, "XL": false})

	claim := testNodeClaim(running.Name)
	claim.Status.ProviderID = nodegroup.ProviderID(running.Name)
	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	requireAvailability(t, cp, map[string]bool{"2XS": true, "XS": true, "S": true, "M": true, "L": true, "XL": true})
}

// TestRunningNodeOfAnUnavailableFlavorKeepsItsLabels pins why Get and List
// describe running NodeGroups from the plain catalogue: the claim they build
// takes its zone, region and capacity-type labels from the first available
// offering, and a node running a flavor the quota currently rules out for new
// launches is no less in the region.
func TestRunningNodeOfAnUnavailableFlavorKeepsItsLabels(t *testing.T) {
	running := managedNodeGroup("default-run01", "L")
	running.Status = syncedStatus()
	operator := &fakeOperator{headroomGB: 10}
	cp, _ := newOperatorProvider(t, operator, running)

	if _, err := cp.Create(context.Background(), claimFor("default-big01", "20Gi", "L")); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected the quota to reject the L, got %T: %v", err, err)
	}
	got, err := cp.Get(context.Background(), nodegroup.ProviderID(running.Name))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	listed, err := cp.List(context.Background())
	if err != nil || len(listed) != 1 {
		t.Fatalf("List = %d claims, %v; want the running one", len(listed), err)
	}
	for source, nodeClaim := range map[string]*karpv1.NodeClaim{"Get": got, "List": listed[0]} {
		for key, want := range map[string]string{
			corev1.LabelTopologyZone:    "par",
			corev1.LabelTopologyRegion:  "par",
			karpv1.CapacityTypeLabelKey: karpv1.CapacityTypeOnDemand,
		} {
			if nodeClaim.Labels[key] != want {
				t.Errorf("%s: %s = %q, want %q", source, key, nodeClaim.Labels[key], want)
			}
		}
	}
}

// TestUnavailableFlavorDoesNotDriftRunningNodes runs karpenter-core's nodeclaim
// drift controller over a node launched as an L, while a later quota rejection
// makes L unavailable. Its InstanceTypeNotFound check fails a claim whose
// flavor is missing from GetInstanceTypes but ignores availability, which is
// why an unavailable flavor is marked, never dropped: dropping it would
// replace every node running it each time the quota turns a launch down.
func TestUnavailableFlavorDoesNotDriftRunningNodes(t *testing.T) {
	operator := &fakeOperator{headroomGB: 30}
	cp, kubeClient := newOperatorProvider(t, operator)
	np := topologyNodePool("default")
	seed(t, kubeClient, np)

	// Launched while the quota still had room for an L.
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	results := provisioningPass(t, cp, kubeClient, cluster, np, sizedPod("big", "20Gi"))
	if len(results.NewNodeClaims) != 1 {
		t.Fatalf("want one planned NodeClaim, got %d", len(results.NewNodeClaims))
	}
	launched := launchPlanned(t, cp, kubeClient, results.NewNodeClaims[0], "default-l4rge")
	if got := launched.Labels[corev1.LabelInstanceTypeStable]; got != "L" {
		t.Fatalf("launched flavor = %q, want L", got)
	}
	// The next L does not fit what is left.
	if _, err := cp.Create(context.Background(), claimFor("default-l4rg2", "20Gi", "L")); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected the quota to reject the second L, got %T: %v", err, err)
	}
	requireAvailability(t, cp, map[string]bool{"2XS": true, "XS": true, "S": true, "M": true, "L": false, "XL": false})

	for i, tc := range []struct {
		name   string
		flavor string
		want   corecloudprovider.DriftReason
	}{
		{name: "flavor made unavailable by the quota", flavor: "L"},
		// Harness check: the InstanceTypeNotFound check ran, so the absence
		// of drift above is not a check that never happened.
		{name: "flavor missing from the catalogue", flavor: "GONE", want: disruption.InstanceTypeNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeClaim := launched.DeepCopy()
			nodeClaim.Name = fmt.Sprintf("default-drift%d", i)
			nodeClaim.UID = types.UID("uid-" + nodeClaim.Name)
			nodeClaim.ResourceVersion = ""
			nodeClaim.Labels[corev1.LabelInstanceTypeStable] = tc.flavor
			seed(t, kubeClient, nodeClaim)
			// Old enough for core to run its InstanceTypeNotFound check.
			nodeClaim.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))

			if _, err := disruption.NewController(clock.RealClock{}, kubeClient, cp).Reconcile(coreContext(), nodeClaim); err != nil {
				t.Fatalf("drift reconcile: %v", err)
			}
			got := corecloudprovider.DriftReason("")
			if cond := nodeClaim.StatusConditions().Get(karpv1.ConditionTypeDrifted); cond.IsTrue() {
				got = corecloudprovider.DriftReason(cond.Reason)
			}
			if got != tc.want {
				t.Errorf("drift = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestSchedulerFallsBackToASmallerFlavorAfterAQuotaRejection runs
// karpenter-core's scheduler over ten 2Gi pods with 10 GB of organisation quota
// left, room for one XS (8 GB). Core packs the pods onto one claim only an L
// or an XL holds, and the quota rejects its L. Every later pass used to rebuild
// that same claim, and the global backoff failed it fast along with anything
// else: no pod was ever placed, and each expiry of the backoff cost one more
// real create, reject and delete upstream. Now the next pass plans around L and
// XL, and the claim the remaining quota holds is launched.
func TestSchedulerFallsBackToASmallerFlavorAfterAQuotaRejection(t *testing.T) {
	operator := &fakeOperator{headroomGB: 10}
	cp, kubeClient := newOperatorProvider(t, operator)
	np := topologyNodePool("default")
	var pods []*corev1.Pod
	for i := range 10 {
		pods = append(pods, sizedPod(fmt.Sprintf("app-%d", i), "2Gi"))
	}
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)

	first := provisioningPass(t, cp, kubeClient, cluster, np, pods...)
	if len(first.NewNodeClaims) != 1 {
		t.Fatalf("first pass: want the ten pods on one claim, got %d claims", len(first.NewNodeClaims))
	}
	if placed, _ := launchAll(t, cp, first, "pass1"); placed != 0 {
		t.Fatalf("first pass: %d pods placed, want the quota to reject the claim", placed)
	}

	second := provisioningPass(t, cp, kubeClient, cluster, np, pods...)
	for _, planned := range second.NewNodeClaims {
		for _, it := range planned.InstanceTypeOptions {
			if it.Name == "L" || it.Name == "XL" {
				t.Errorf("second pass planned a claim with option %s, which the quota just ruled out", it.Name)
			}
		}
	}
	placed, flavors := launchAll(t, cp, second, "pass2")
	if placed == 0 {
		t.Fatalf("second pass: no pod placed with 10 GB of quota left (claims launched: %v)", flavors)
	}
	for _, flavor := range flavors {
		if nominalGB[flavor] > 10 {
			t.Errorf("launched %s, which the remaining quota cannot hold", flavor)
		}
	}
	t.Logf("second pass: %d/%d pods placed on %v; NodeGroups created upstream: %d", placed, len(pods), flavors, operator.createCount())
}

// TestWeightedNodePoolFallsBackFromARefusedFlavor runs karpenter-core's
// scheduler over the weighted NodePools of examples/v1/weighted-nodepools.yaml
// after the operator refused the preferred pool's only flavor. The hold-out
// used to live in resolveInstanceType alone: core kept planning the pod onto
// the preferred pool, whose claim then failed, so the fallback pool was never
// tried while the refusal lasted. Now the preferred pool has no available
// flavor and core plans the pod onto the fallback pool.
func TestWeightedNodePoolFallsBackFromARefusedFlavor(t *testing.T) {
	operator := &fakeOperator{headroomGB: 100, refused: map[string]bool{"2XS": true}}
	cp, kubeClient := newOperatorProvider(t, operator)
	preferredWeight, fallbackWeight := int32(100), int32(10)
	preferred := topologyNodePool("preferred-small", corev1.NodeSelectorRequirement{
		Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"2XS"},
	})
	preferred.Spec.Weight = &preferredWeight
	fallback := topologyNodePool("fallback-any")
	fallback.Spec.Weight = &fallbackWeight
	nodePools := []*karpv1.NodePool{preferred, fallback}
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	pod := pendingPod("app")

	first := weightedProvisioningPass(t, cp, kubeClient, cluster, nodePools, pod)
	if len(first.NewNodeClaims) != 1 || first.NewNodeClaims[0].NodePoolName != preferred.Name {
		t.Fatalf("first pass: want one claim on %s, got %d", preferred.Name, len(first.NewNodeClaims))
	}
	if placed, _ := launchAll(t, cp, first, "pass1"); placed != 0 {
		t.Fatal("first pass: expected the operator to refuse 2XS")
	}

	second := weightedProvisioningPass(t, cp, kubeClient, cluster, nodePools, pod)
	if len(second.NewNodeClaims) != 1 || second.NewNodeClaims[0].NodePoolName != fallback.Name {
		pools := []string{}
		for _, planned := range second.NewNodeClaims {
			pools = append(pools, planned.NodePoolName)
		}
		t.Fatalf("second pass: want one claim on %s, got claims on %v (pod error: %v)", fallback.Name, pools, second.PodErrors[pod])
	}
	placed, flavors := launchAll(t, cp, second, "pass2")
	if placed != 1 || len(flavors) != 1 || flavors[0] != "XS" {
		t.Errorf("second pass: placed %d pod(s) on %v, want the pod on the next-cheapest flavor XS", placed, flavors)
	}
}

// TestExhaustedQuotaCostsOneRejectionAWindow runs karpenter-core's scheduler
// over ten 2Gi pods for several quota windows while the organisation quota has
// no room left at all. The first window walks down the catalogue, one real
// rejection per flavor size core plans, until the smallest one is rejected too.
// Every flavor used to reopen at the same moment after that — the smallest
// flavor's rejection covered the others and expired with them — so core packed
// the pods back onto an L and every window walked down again: three real
// create, reject and delete cycles upstream a window here, against one for the
// global backoff this replaced. Now the smallest rejected flavor is retried
// alone, once a window, and the larger ones stay covered while it is rejected.
func TestExhaustedQuotaCostsOneRejectionAWindow(t *testing.T) {
	operator := &fakeOperator{headroomGB: 0}
	clk := clocktesting.NewFakeClock(time.Now())
	cp, kubeClient := newClockedOperatorProvider(t, operator, clk)
	np := topologyNodePool("default")
	var pods []*corev1.Pod
	for i := range 10 {
		pods = append(pods, sizedPod(fmt.Sprintf("app-%d", i), "2Gi"))
	}
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)

	const windows = 5
	perWindow := make([][]string, windows)
	for w := range windows {
		before := operator.createCount()
		// Provisioning passes until one reaches the API no more: within a
		// window, only a create that reaches the API changes what the next
		// pass is served.
		for pass := 0; ; pass++ {
			if pass == 2*len(instancetype.DefaultFlavors) {
				t.Fatalf("window %d: still reaching the API after %d passes: %v", w, pass, operator.createdFlavors()[before:])
			}
			n := operator.createCount()
			results := provisioningPass(t, cp, kubeClient, cluster, np, pods...)
			if placed, flavors := launchAll(t, cp, results, fmt.Sprintf("w%dp%d", w, pass)); placed != 0 {
				t.Fatalf("window %d: %d pods placed on %v with no quota left", w, placed, flavors)
			}
			if operator.createCount() == n {
				break
			}
		}
		perWindow[w] = operator.createdFlavors()[before:]
		clk.Step(nodegroup.QuotaBackoff)
	}
	t.Logf("NodeGroups created upstream per window: %v", perWindow)

	walk := perWindow[0]
	if len(walk) == 0 || walk[len(walk)-1] != "2XS" || len(walk) > len(instancetype.DefaultFlavors) {
		t.Errorf("first window reached the API with %v, want a walk down the catalogue ending on 2XS", walk)
	}
	distinct := slices.Clone(walk)
	slices.Sort(distinct)
	if len(slices.Compact(distinct)) != len(walk) {
		t.Errorf("first window reached the API with %v, want each flavor size at most once", walk)
	}
	for w := 1; w < windows; w++ {
		if !slices.Equal(perWindow[w], []string{"2XS"}) {
			t.Errorf("window %d reached the API with %v, want the one 2XS probe", w, perWindow[w])
		}
	}
}

// TestResolveFailureNamesUnavailabilityOnlyWhenItIsTheCause pins the error of a
// claim no available flavor serves. It names the flavors unavailable after a
// rejection only when one of them would have served the claim: a claim whose
// own requirements rule out every flavor must not send operators after a quota
// rejection that has nothing to do with it.
func TestResolveFailureNamesUnavailabilityOnlyWhenItIsTheCause(t *testing.T) {
	operator := &fakeOperator{headroomGB: 10}
	cp, _ := newOperatorProvider(t, operator)
	ctx := context.Background()
	if _, err := cp.Create(ctx, claimFor("default-big01", "20Gi", "L")); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("expected the quota to reject the L, got %T: %v", err, err)
	}

	for _, tc := range []struct {
		name     string
		claim    *karpv1.NodeClaim
		blamesIt bool
	}{
		{name: "only the unavailable flavor holds the claim", claim: claimFor("default-big02", "20Gi", "L"), blamesIt: true},
		{name: "the claim's own requirements rule every flavor out", claim: claimFor("default-tiny1", "20Gi", "2XS")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cp.Create(ctx, tc.claim)
			if !corecloudprovider.IsInsufficientCapacityError(err) {
				t.Fatalf("expected an InsufficientCapacityError, got %T: %v", err, err)
			}
			if got := strings.Contains(err.Error(), "currently unavailable"); got != tc.blamesIt {
				t.Errorf("error %q names unavailable flavors = %v, want %v", err, got, tc.blamesIt)
			}
		})
	}
	if got := operator.createCount(); got != 1 {
		t.Errorf("NodeGroups created = %d, want 1: neither claim may reach the API", got)
	}
}
