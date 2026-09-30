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

package instancetypecapacity_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/instancetypecapacity"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

const rejectionsMetric = "karpenter_clevercloud_instancetype_observed_capacity_rejections_total"

func newTestController(t *testing.T, objs ...client.Object) (*instancetypecapacity.Controller, *instancetype.Provider, client.Client) {
	t.Helper()
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		Build()
	itp := instancetype.NewProvider("par", nil, nil)
	return instancetypecapacity.NewController(kubeClient, itp), itp, kubeClient
}

// setStatus rewrites a node's reported capacity, as its kubelet would.
func setStatus(t *testing.T, kubeClient client.Client, node *corev1.Node, capacity, allocatable corev1.ResourceList) {
	t.Helper()
	node.Status.Capacity = capacity
	node.Status.Allocatable = allocatable
	if err := kubeClient.Status().Update(context.Background(), node); err != nil {
		t.Fatalf("updating the status of node %s: %v", node.Name, err)
	}
}

// relabel rewrites a node's nodegroup and flavor labels, as its kubelet can.
func relabel(t *testing.T, kubeClient client.Client, node *corev1.Node, nodeGroup, flavor string) {
	t.Helper()
	stored := node.DeepCopy()
	node.Labels[v1alpha1.NodeGroupNodeLabelKey] = nodeGroup
	node.Labels[v1alpha1.FlavorLabelKey] = flavor
	if err := kubeClient.Patch(context.Background(), node, client.MergeFrom(stored)); err != nil {
		t.Fatalf("relabeling the node %s to %s/%s: %v", node.Name, nodeGroup, flavor, err)
	}
}

// managedNodeGroup is a NodeGroup this provider created with the given flavor.
func managedNodeGroup(name, flavor string) *ngv1.NodeGroup {
	return &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true"},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: flavor, NodeCount: 1},
	}
}

// workerNode is node0 of nodeGroup, labeled the way the platform labels it.
func workerNode(nodeGroup, flavor string, capacity, allocatable corev1.ResourceList) *corev1.Node {
	return namedWorkerNode(nodeGroup+"-node0", nodeGroup, flavor, capacity, allocatable)
}

// namedWorkerNode is a worker node whose labels claim nodeGroup and flavor,
// whatever its name says.
func namedWorkerNode(name, nodeGroup, flavor string, capacity, allocatable corev1.ResourceList) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				v1alpha1.FlavorLabelKey:        flavor,
				v1alpha1.NodeRoleLabelKey:      v1alpha1.NodeRoleWorker,
				v1alpha1.NodeGroupNodeLabelKey: nodeGroup,
			},
		},
		Status: corev1.NodeStatus{
			Capacity:    capacity,
			Allocatable: allocatable,
		},
	}
}

// report is a node's capacity/allocatable pair: cpu, memory with the measured
// 100Mi kubelet reservation, and the disk and pod figures every flavor shares.
func report(cpu, memoryKi int64) (corev1.ResourceList, corev1.ResourceList) {
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:              *resource.NewQuantity(cpu, resource.DecimalSI),
		corev1.ResourceMemory:           resource.MustParse(fmt.Sprintf("%dKi", memoryKi)),
		corev1.ResourceEphemeralStorage: resource.MustParse("40971488Ki"),
		corev1.ResourcePods:             resource.MustParse("110"),
	}
	allocatable := corev1.ResourceList{
		corev1.ResourceCPU:              *resource.NewQuantity(cpu, resource.DecimalSI),
		corev1.ResourceMemory:           resource.MustParse(fmt.Sprintf("%dKi", memoryKi-102400)),
		corev1.ResourceEphemeralStorage: resource.MustParse("37759323279"),
		corev1.ResourcePods:             resource.MustParse("110"),
	}
	return capacity, allocatable
}

// observedL returns a plausible report of an L node that differs from the
// catalogue entry (memory 22896304Ki, 100Mi reserved).
func observedL() (corev1.ResourceList, corev1.ResourceList) {
	return report(12, 23500000)
}

func nodeRequest(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name}}
}

func reconcileNode(t *testing.T, ctx context.Context, ctrl *instancetypecapacity.Controller, name string) {
	t.Helper()
	result, err := ctrl.Reconcile(ctx, nodeRequest(name))
	if err != nil {
		t.Fatalf("Reconcile(%s): %v", name, err)
	}
	if result != (reconcile.Result{}) {
		t.Errorf("expected no requeue, got %+v", result)
	}
}

// assertMemory verifies the memory capacity the catalogue serves for a flavor.
func assertMemory(t *testing.T, itp *instancetype.Provider, flavor, want string) {
	t.Helper()
	it, err := itp.Get(flavor)
	if err != nil {
		t.Fatalf("Get(%s): %v", flavor, err)
	}
	if q := resource.MustParse(want); it.Capacity.Memory().Cmp(q) != 0 {
		t.Errorf("%s: served memory %s, want %s", flavor, it.Capacity.Memory(), q.String())
	}
}

// assertCatalogueUntouched verifies every flavor still serves its seed.
func assertCatalogueUntouched(t *testing.T, itp *instancetype.Provider) {
	t.Helper()
	for _, it := range itp.List() {
		s := instancetype.SizingByName[it.Name]
		if it.Capacity.Cpu().Value() != s.CPU || it.Capacity.Memory().Value() != s.MemoryKi*1024 {
			t.Errorf("%s: served cpu %s memory %s, want the seed's %d / %dKi", it.Name, it.Capacity.Cpu(), it.Capacity.Memory(), s.CPU, s.MemoryKi)
		}
		wantReserved := resource.MustParse("100Mi")
		if got := it.Overhead.KubeReserved[corev1.ResourceMemory]; got.Cmp(wantReserved) != 0 {
			t.Errorf("%s: KubeReserved %s, want the static %s", it.Name, got.String(), wantReserved.String())
		}
	}
}

// logSink captures the controller's log lines.
type logSink struct{ lines []string }

func (s *logSink) context() context.Context {
	return log.IntoContext(context.Background(), funcr.New(func(prefix, args string) {
		s.lines = append(s.lines, args)
	}, funcr.Options{}))
}

func (s *logSink) count(substr string) int {
	n := 0
	for _, line := range s.lines {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func TestReconcileFeedsObservedCapacityIntoCatalog(t *testing.T) {
	capacity, allocatable := observedL()
	node := workerNode("default-abc12", "L", capacity, allocatable)
	ctrl, itp, _ := newTestController(t, managedNodeGroup("default-abc12", "L"), node)
	rejectionsBefore := metricstest.Value(t, rejectionsMetric)

	reconcileNode(t, context.Background(), ctrl, node.Name)

	it, err := itp.Get("L")
	if err != nil {
		t.Fatalf("Get(L): %v", err)
	}
	if it.Capacity.Cpu().Cmp(*capacity.Cpu()) != 0 {
		t.Errorf("expected observed cpu capacity %s, got %s", capacity.Cpu(), it.Capacity.Cpu())
	}
	if it.Capacity.Memory().Cmp(*capacity.Memory()) != 0 {
		t.Errorf("expected observed memory capacity %s, got %s", capacity.Memory(), it.Capacity.Memory())
	}
	alloc := it.Allocatable()
	if alloc.Memory().Cmp(*allocatable.Memory()) != 0 {
		t.Errorf("expected allocatable memory %s, got %s", allocatable.Memory(), alloc.Memory())
	}
	if alloc.Cpu().Cmp(*allocatable.Cpu()) != 0 {
		t.Errorf("expected allocatable cpu %s, got %s", allocatable.Cpu(), alloc.Cpu())
	}
	// List() must serve the corrected entry too.
	for _, li := range itp.List() {
		if li.Name == "L" && li.Capacity.Memory().Cmp(*capacity.Memory()) != 0 {
			t.Errorf("List(): expected observed memory capacity %s, got %s", capacity.Memory(), li.Capacity.Memory())
		}
	}
	if delta := metricstest.Value(t, rejectionsMetric) - rejectionsBefore; delta != 0 {
		t.Errorf("observed_capacity_rejections_total moved by %v for an accepted report", delta)
	}
}

func TestReconcileIgnoresNodesOutsideManagedNodeGroups(t *testing.T) {
	// Only a NodeGroup this provider created vouches for a node's flavor.
	// Everything else is skipped quietly — a fixed nodegroup or the control
	// plane is normal, not a rejection.
	capacity, allocatable := observedL()
	noRole := workerNode("no-role", "L", capacity, allocatable)
	delete(noRole.Labels, v1alpha1.NodeRoleLabelKey)
	controlPlane := workerNode("control-plane", "L", capacity, allocatable)
	controlPlane.Labels[v1alpha1.NodeRoleLabelKey] = "control-plane"
	flavorless := workerNode("flavorless", "L", capacity, allocatable)
	delete(flavorless.Labels, v1alpha1.FlavorLabelKey)
	groupless := workerNode("groupless", "L", capacity, allocatable)
	delete(groupless.Labels, v1alpha1.NodeGroupNodeLabelKey)
	orphan := workerNode("vanished-group", "L", capacity, allocatable)
	fixedGroup := managedNodeGroup("fixed-pool", "L")
	fixedGroup.Labels = nil
	fixed := workerNode("fixed-pool", "L", capacity, allocatable)

	nodes := []*corev1.Node{noRole, controlPlane, flavorless, groupless, orphan, fixed}
	objs := []client.Object{fixedGroup}
	for _, n := range nodes {
		objs = append(objs, n)
	}
	ctrl, itp, _ := newTestController(t, objs...)
	rejectionsBefore := metricstest.Value(t, rejectionsMetric)

	for _, n := range nodes {
		reconcileNode(t, context.Background(), ctrl, n.Name)
	}
	assertCatalogueUntouched(t, itp)
	if delta := metricstest.Value(t, rejectionsMetric) - rejectionsBefore; delta != 0 {
		t.Errorf("observed_capacity_rejections_total moved by %v for nodes that are simply not ours", delta)
	}
}

func TestReconcileMissingNodeNoError(t *testing.T) {
	ctrl, itp, _ := newTestController(t)
	reconcileNode(t, context.Background(), ctrl, "does-not-exist")
	assertCatalogueUntouched(t, itp)
}

func TestReconcileRefusesFlavorLabelContradictingTheNodeGroup(t *testing.T) {
	// A kubelet can rewrite its own node's clever-cloud.com/flavor label and
	// status. The NodeGroup's spec.flavor — immutable, written by this
	// provider — has the last word, and a node contradicting it is refused
	// outright, whichever flavor its figures fit.
	for name, fig := range map[string]struct{ cpu, memoryKi int64 }{
		// Used to drag the 2XS entry down for the whole cluster.
		"figures of the label's flavor (2XS, 8% under its entry)": {4, 3418116},
		// Genuine S figures: plausible for the group, but the node lies
		// about its own flavor, so nothing it reports is kept.
		"figures of the group's flavor (S, 5% under its entry)": {8, 10816540},
	} {
		t.Run(name, func(t *testing.T) {
			capacity, allocatable := report(fig.cpu, fig.memoryKi)
			node := workerNode("ng-s", "2XS", capacity, allocatable)
			ctrl, itp, _ := newTestController(t, managedNodeGroup("ng-s", "S"), node)
			rejectionsBefore := metricstest.Value(t, rejectionsMetric)

			reconcileNode(t, context.Background(), ctrl, node.Name)

			assertCatalogueUntouched(t, itp)
			if delta := metricstest.Value(t, rejectionsMetric) - rejectionsBefore; delta != 1 {
				t.Errorf("observed_capacity_rejections_total delta = %v, want 1", delta)
			}
		})
	}
}

func TestReconcileRefusesImplausibleCapacity(t *testing.T) {
	// The node of a genuine managed 2XS group reports what no 2XS VM has.
	capacity, allocatable := report(64, 268435456)
	node := workerNode("ng-2xs", "2XS", capacity, allocatable)
	ctrl, itp, _ := newTestController(t, managedNodeGroup("ng-2xs", "2XS"), node)
	rejectionsBefore := metricstest.Value(t, rejectionsMetric)

	reconcileNode(t, context.Background(), ctrl, node.Name)

	assertCatalogueUntouched(t, itp)
	if delta := metricstest.Value(t, rejectionsMetric) - rejectionsBefore; delta != 1 {
		t.Errorf("observed_capacity_rejections_total delta = %v, want 1", delta)
	}
}

func TestReconcileRefusesANodeClaimingAGroupItIsNotANodeOf(t *testing.T) {
	// The nodegroup label is as forgeable as the flavor label: a node of a
	// fixed nodegroup, or of another managed group, points both at a managed
	// group and reports figures plausible for that group's flavor. The node's
	// name — bound to its kubelet's credential — says which group it belongs
	// to, and the claim is refused whatever the figures.
	xl := instancetype.SizingByName["XL"]
	for name, tc := range map[string]struct {
		node     string
		memoryKi int64
	}{
		"a node of a fixed nodegroup, 9.5% under the entry":    {"fixed-pool-node3", xl.MemoryKi * 905 / 1000},
		"a node of a fixed nodegroup, 9.5% above the entry":    {"fixed-pool-node3", xl.MemoryKi * 1095 / 1000},
		"a node of another managed group":                      {"ng-2xs-node0", xl.MemoryKi * 95 / 100},
		"a node named like no nodegroup's node":                {"worker-1", xl.MemoryKi * 95 / 100},
		"a node of a group whose name extends the claimed one": {"default-xl123-b-node0", xl.MemoryKi * 95 / 100},
		"a node of a group the claimed one's name extends":     {"default-node0", xl.MemoryKi * 95 / 100},
	} {
		t.Run(name, func(t *testing.T) {
			capacity, allocatable := report(xl.CPU, tc.memoryKi)
			node := namedWorkerNode(tc.node, "default-xl123", "XL", capacity, allocatable)
			ctrl, itp, _ := newTestController(t, managedNodeGroup("default-xl123", "XL"), managedNodeGroup("ng-2xs", "2XS"), node)
			sink := &logSink{}
			rejectionsBefore := metricstest.Value(t, rejectionsMetric)

			reconcileNode(t, sink.context(), ctrl, node.Name)

			assertCatalogueUntouched(t, itp)
			if delta := metricstest.Value(t, rejectionsMetric) - rejectionsBefore; delta != 1 {
				t.Errorf("observed_capacity_rejections_total delta = %v, want 1", delta)
			}
			if n := sink.count("is not one of its nodes"); n != 1 {
				t.Errorf("the refusal must say the node is not one of the group's nodes; lines: %q", sink.lines)
			}
		})
	}
}

func TestReconcileAcceptsEveryNodeOfAResizedGroup(t *testing.T) {
	// A group resized outside karpenter has node1 onwards: genuine VMs of
	// the group's flavor, as trustworthy as node0.
	capacity, allocatable := observedL()
	node := namedWorkerNode("default-abc12-node1", "default-abc12", "L", capacity, allocatable)
	ctrl, itp, _ := newTestController(t, managedNodeGroup("default-abc12", "L"), node)
	rejectionsBefore := metricstest.Value(t, rejectionsMetric)

	reconcileNode(t, context.Background(), ctrl, node.Name)

	assertMemory(t, itp, "L", capacity.Memory().String())
	if delta := metricstest.Value(t, rejectionsMetric) - rejectionsBefore; delta != 0 {
		t.Errorf("observed_capacity_rejections_total moved by %v for a node of the group", delta)
	}
}

func TestReconcileOneNodeCannotRewriteEveryFlavor(t *testing.T) {
	// One node cycling its own labels through one managed group per flavor,
	// each time reporting figures plausible for the flavor it claims (9%
	// under the entry), used to rewrite every flavor one patch at a time.
	// Only the flavor of the group its name places it in can be reached —
	// and only within the tolerance, which is the bounded residual a
	// compromised node keeps. A node of a fixed nodegroup reaches none.
	var groups []client.Object
	for _, f := range instancetype.FlavorSizing {
		groups = append(groups, managedNodeGroup("ng-"+strings.ToLower(f.Name), f.Name))
	}
	for name, tc := range map[string]struct {
		node      string
		reachable string
	}{
		"a node of the managed 2XS group": {"ng-2xs-node0", "2XS"},
		"a node of a fixed nodegroup":     {"fixed-pool-node0", ""},
	} {
		t.Run(name, func(t *testing.T) {
			node := namedWorkerNode(tc.node, "fixed-pool", "2XS", nil, nil)
			ctrl, itp, kubeClient := newTestController(t, append([]client.Object{node}, groups...)...)
			rejectionsBefore := metricstest.Value(t, rejectionsMetric)

			for _, f := range instancetype.FlavorSizing {
				relabel(t, kubeClient, node, "ng-"+strings.ToLower(f.Name), f.Name)
				capacity, allocatable := report(f.CPU, f.MemoryKi*91/100)
				setStatus(t, kubeClient, node, capacity, allocatable)
				reconcileNode(t, context.Background(), ctrl, node.Name)
			}

			wantRejections := len(instancetype.FlavorSizing)
			for _, it := range itp.List() {
				s := instancetype.SizingByName[it.Name]
				served := it.Capacity.Memory().Value() / 1024
				switch {
				case it.Name == tc.reachable:
					wantRejections--
					if served < s.MemoryKi*9/10 {
						t.Errorf("%s: served memory %dKi, below the 10%% tolerance of its entry %dKi", it.Name, served, s.MemoryKi)
					}
				case served != s.MemoryKi:
					t.Errorf("%s: served memory %dKi, want its entry %dKi — node %s rewrote it", it.Name, served, s.MemoryKi, tc.node)
				}
			}
			if delta := metricstest.Value(t, rejectionsMetric) - rejectionsBefore; delta != float64(wantRejections) {
				t.Errorf("observed_capacity_rejections_total delta = %v, want %d", delta, wantRejections)
			}
		})
	}
}

func TestReconcileServesTheSmallestNodeOfAFlavor(t *testing.T) {
	// Two images in one fleet: the catalogue must settle on the smaller node
	// whatever order the reports arrive in, and stay there.
	bigCapacity, bigAllocatable := report(12, 23500000)
	smallCapacity, smallAllocatable := report(12, 22500000)
	big := workerNode("ng-l-big", "L", bigCapacity, bigAllocatable)
	small := workerNode("ng-l-small", "L", smallCapacity, smallAllocatable)
	ctrl, itp, _ := newTestController(t, managedNodeGroup("ng-l-big", "L"), managedNodeGroup("ng-l-small", "L"), big, small)

	reconcileNode(t, context.Background(), ctrl, big.Name)
	assertMemory(t, itp, "L", "23500000Ki")
	reconcileNode(t, context.Background(), ctrl, small.Name)
	assertMemory(t, itp, "L", "22500000Ki")
	reconcileNode(t, context.Background(), ctrl, big.Name)
	assertMemory(t, itp, "L", "22500000Ki")
}

func TestReconcileRateLimitsTheRefusalLog(t *testing.T) {
	// A refused node is reconciled on every status update: each report is
	// counted, but it is logged once per node per interval — and again as
	// soon as the node misreports after having been accepted.
	badCapacity, badAllocatable := report(64, 268435456)
	node := workerNode("ng-2xs", "2XS", badCapacity, badAllocatable)
	other := workerNode("ng-2xs-b", "2XS", badCapacity, badAllocatable)
	ctrl, _, kubeClient := newTestController(t, managedNodeGroup("ng-2xs", "2XS"), managedNodeGroup("ng-2xs-b", "2XS"), node, other)
	sink := &logSink{}
	ctx := sink.context()
	rejectionsBefore := metricstest.Value(t, rejectionsMetric)

	for i := 0; i < 3; i++ {
		reconcileNode(t, ctx, ctrl, node.Name)
	}
	if n := sink.count("refusing the node's capacity report"); n != 1 {
		t.Errorf("refusal logged %d times for one node, want 1", n)
	}
	if delta := metricstest.Value(t, rejectionsMetric) - rejectionsBefore; delta != 3 {
		t.Errorf("observed_capacity_rejections_total delta = %v, want 3 (every refused report counts)", delta)
	}

	reconcileNode(t, ctx, ctrl, other.Name)
	if n := sink.count("refusing the node's capacity report"); n != 2 {
		t.Errorf("refusal of a second node: %d lines in total, want 2", n)
	}

	goodCapacity, goodAllocatable := report(4, 3715344)
	setStatus(t, kubeClient, node, goodCapacity, goodAllocatable)
	reconcileNode(t, ctx, ctrl, node.Name)
	setStatus(t, kubeClient, node, badCapacity, badAllocatable)
	reconcileNode(t, ctx, ctrl, node.Name)
	if n := sink.count("refusing the node's capacity report"); n != 3 {
		t.Errorf("a node misreporting again after an accepted report must be logged again: %d lines, want 3", n)
	}
}

func TestReconcileLogsAStaleCatalogueEntryOncePerFlavor(t *testing.T) {
	// Nodes of the previous image expose 5.3% more memory than the 2XS
	// entry: accepted, and the drift is logged once for the flavor so the
	// next image change is visible. A node matching its entry logs nothing.
	oldCapacity, oldAllocatable := report(4, 3911884)
	currentCapacity, currentAllocatable := report(6, 7553664)
	first := workerNode("ng-2xs-a", "2XS", oldCapacity, oldAllocatable)
	second := workerNode("ng-2xs-b", "2XS", oldCapacity, oldAllocatable)
	current := workerNode("ng-xs", "XS", currentCapacity, currentAllocatable)
	ctrl, itp, _ := newTestController(t,
		managedNodeGroup("ng-2xs-a", "2XS"), managedNodeGroup("ng-2xs-b", "2XS"), managedNodeGroup("ng-xs", "XS"),
		first, second, current)
	sink := &logSink{}
	ctx := sink.context()

	for _, n := range []*corev1.Node{first, second, first, current} {
		reconcileNode(t, ctx, ctrl, n.Name)
	}
	if n := sink.count("differs from the flavor's catalogue entry"); n != 1 {
		t.Errorf("stale-entry log emitted %d times, want once for 2XS and never for XS; lines: %q", n, sink.lines)
	}
	if n := sink.count(`"flavor"="2XS"`); n != 1 {
		t.Errorf("the stale-entry log must name 2XS; lines: %q", sink.lines)
	}
	assertMemory(t, itp, "2XS", "3911884Ki")
}

func TestReconcileStaleCatalogueThresholdIsOnePercent(t *testing.T) {
	// The stale-entry log exists to surface the next node image change, so
	// its 1% threshold is pinned from both sides and in both directions.
	seed := instancetype.SizingByName["M"]
	for _, tc := range []struct {
		permille int64
		wantLog  bool
	}{
		{1005, false},
		{995, false},
		{1015, true},
		{985, true},
	} {
		t.Run(fmt.Sprintf("%+.1f%%", float64(tc.permille-1000)/10), func(t *testing.T) {
			capacity, allocatable := report(seed.CPU, seed.MemoryKi*tc.permille/1000)
			node := workerNode("ng-m", "M", capacity, allocatable)
			ctrl, itp, _ := newTestController(t, managedNodeGroup("ng-m", "M"), node)
			sink := &logSink{}

			reconcileNode(t, sink.context(), ctrl, node.Name)

			assertMemory(t, itp, "M", capacity.Memory().String())
			if got := sink.count("differs from the flavor's catalogue entry") == 1; got != tc.wantLog {
				t.Errorf("stale-entry logged = %v, want %v; lines: %q", got, tc.wantLog, sink.lines)
			}
		})
	}
}
