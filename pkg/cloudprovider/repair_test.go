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

// These tests run karpenter-core's node repair over the provider's
// RepairPolicies. Since core 1.15 repair is a method of the disruption
// controller, behind the NodeRepair feature gate: cluster state matches every
// Node against the policies as it changes (health.RepairPolicyMatcher, built
// once by main.go), the disruption loop picks an eligible node within the
// NodePool's budget, launches a replacement first when the node's pods need
// one, and the orchestration queue deletes the node once the replacement is
// initialized, stamping the policy's drain bound. LEGACY_NODE_REPAIR=true
// still runs the standalone controller of earlier releases instead.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	coredisruption "sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/controllers/node/legacyrepair"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/cost"

	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

const repairToleration = 10 * time.Minute

// repairContext is coreContext with the NodeRepair feature gate on, and
// --legacy-node-repair as given.
func repairContext(legacy bool) context.Context {
	ctx := coreContext()
	options.FromContext(ctx).FeatureGates.NodeRepair = true
	options.FromContext(ctx).LegacyNodeRepair = legacy
	return ctx
}

// TestRepairPoliciesCompileForCoreRepair pins what main.go's lo.Must relies on:
// with the gate on, core compiles the policy set or the controller crashloops
// at startup; with the gate off, or the legacy controller selected, it builds
// no matcher. Every policy replaces (Reboot is not implemented, and core does
// not check that at startup) and is forceful.
func TestRepairPoliciesCompileForCoreRepair(t *testing.T) {
	cp, _ := newTestProvider(t)
	if matcher, err := health.NewRepairPolicyMatcher(repairContext(false), cp); err != nil || matcher == nil {
		t.Fatalf("core refused the repair policy set: matcher=%v err=%v", matcher, err)
	}
	for name, ctx := range map[string]context.Context{"gate off": coreContext(), "legacy": repairContext(true)} {
		if matcher, err := health.NewRepairPolicyMatcher(ctx, cp); matcher != nil || err != nil {
			t.Errorf("%s: matcher=%v err=%v, want neither", name, matcher, err)
		}
	}
	for _, policy := range cp.RepairPolicies() {
		if policy.Action != corecloudprovider.ReplaceNode {
			t.Errorf("policy %+v: action %q, but Reboot is not implemented", policy, policy.Action)
		}
		if policy.TerminationGracePeriod == nil || *policy.TerminationGracePeriod != 0 {
			t.Errorf("policy %+v: repair must stay forceful (TerminationGracePeriod 0)", policy)
		}
	}
	if err := cp.Reboot(context.Background(), &karpv1.NodeClaim{}, "op"); !corecloudprovider.IsNodeRebootNotImplementedError(err) {
		t.Errorf("Reboot: %v, want NodeRebootNotImplementedError", err)
	}
}

// TestRepairPoliciesMatchOnlyANotReadyNode runs core's matcher over the
// conditions a CKE node reports. NodeReady False (kubelet NotReady) or Unknown
// (kubelet silent; the node lifecycle controller then also marks the pressure
// conditions Unknown) is repaired after exactly 10 minutes, whatever its
// reason; no other condition is.
func TestRepairPoliciesMatchOnlyANotReadyNode(t *testing.T) {
	cp, _ := newTestProvider(t)
	matcher, err := health.NewRepairPolicyMatcher(repairContext(false), cp)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)
	since := created.Add(time.Hour)
	condition := func(conditionType corev1.NodeConditionType, status corev1.ConditionStatus, reason string) corev1.NodeCondition {
		return corev1.NodeCondition{Type: conditionType, Status: status, Reason: reason, LastTransitionTime: metav1.NewTime(since)}
	}
	silent := []corev1.NodeCondition{
		condition(corev1.NodeMemoryPressure, corev1.ConditionUnknown, "NodeStatusUnknown"),
		condition(corev1.NodeDiskPressure, corev1.ConditionUnknown, "NodeStatusUnknown"),
		condition(corev1.NodePIDPressure, corev1.ConditionUnknown, "NodeStatusUnknown"),
	}
	for _, tc := range []struct {
		ready    corev1.NodeCondition
		repaired bool
	}{
		{condition(corev1.NodeReady, corev1.ConditionFalse, "KubeletNotReady"), true},
		{condition(corev1.NodeReady, corev1.ConditionFalse, ""), true},
		{condition(corev1.NodeReady, corev1.ConditionUnknown, "NodeStatusUnknown"), true},
		{condition(corev1.NodeReady, corev1.ConditionUnknown, ""), true},
		{condition(corev1.NodeReady, corev1.ConditionTrue, "KubeletReady"), false},
	} {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "default-heal1-node0", CreationTimestamp: metav1.NewTime(created)},
			Status: corev1.NodeStatus{Conditions: append([]corev1.NodeCondition{
				tc.ready, condition(corev1.NodeNetworkUnavailable, corev1.ConditionTrue, "NoRouteCreated"),
			}, silent...)},
		}
		name := string(tc.ready.Status) + "/" + tc.ready.Reason
		matches := matcher.Match(node)
		if early := health.Resolve(matches, since.Add(repairToleration-time.Nanosecond), time.Time{}); early.Action != "" {
			t.Errorf("Ready=%s: eligible before %s", name, repairToleration)
		}
		got := health.Resolve(matches, since.Add(repairToleration), time.Time{})
		if !tc.repaired {
			if got.Action != "" || len(matches) != 0 {
				t.Errorf("Ready=%s: matched %d policies, resolved %q; want none", name, len(matches), got.Action)
			}
			continue
		}
		if len(matches) != 1 || got.Action != corecloudprovider.ReplaceNode || got.Condition != corev1.NodeReady ||
			got.TerminationGracePeriod == nil || *got.TerminationGracePeriod != 0 {
			t.Errorf("Ready=%s: %d matches, resolved %+v; want one forceful NodeReady replacement", name, len(matches), got)
		}
	}
}

// repairFixture is a NodePool, its nodes and core's repair machinery over the
// fake client: cluster state carrying the provider's repair matcher, the
// provisioner whose scheduler sizes replacements, the disruption controller
// and its orchestration queue, all on one fake clock.
type repairFixture struct {
	t          *testing.T
	ctx        context.Context
	clk        *clocktesting.FakeClock
	kubeClient client.Client
	cp         *cloudprovider.CloudProvider
	cluster    *state.Cluster
	queue      *coredisruption.Queue
	controller *coredisruption.Controller
}

// repairNode is a launched NodeGroup's NodeClaim and node, registered and
// initialized unless said otherwise, whose NodeReady condition has had status
// ready since the given time.
type repairNode struct {
	name           string
	flavor         string
	ready          corev1.ConditionStatus
	since          time.Time
	unregistered   bool
	uninitialized  bool
	consolidatable bool
}

func newRepairFixture(t *testing.T, operator *fakeOperator, budget string, nodes []repairNode, objs ...client.Object) *repairFixture {
	t.Helper()
	ctx := repairContext(false)
	clk := clocktesting.NewFakeClock(time.Now().UTC().Truncate(time.Second))
	np := topologyNodePool("default")
	np.Spec.Disruption.Budgets = []karpv1.Budget{{Nodes: budget}}
	np.StatusConditions().SetTrue(karpv1.ConditionTypeValidationSucceeded)
	np.StatusConditions().SetTrue(karpv1.ConditionTypeNodeClassReady)
	itp := instancetype.NewProvider("par", nil, nil)
	byFlavor := map[string]*corecloudprovider.InstanceType{}
	for _, it := range itp.List() {
		byFlavor[it.Name] = it
	}
	objs = append(objs, readyNodeClass("default"), np)
	var claims []*karpv1.NodeClaim
	var stateNodes []*corev1.Node
	for _, n := range nodes {
		it := byFlavor[n.flavor]
		group := managedNodeGroup(n.name, n.flavor)
		group.Status = syncedStatus()
		claim := launchedNodeClaim(n.name)
		claim.Labels[corev1.LabelInstanceTypeStable] = n.flavor
		claim.Labels[karpv1.CapacityTypeLabelKey] = karpv1.CapacityTypeOnDemand
		claim.Labels[corev1.LabelTopologyRegion], claim.Labels[corev1.LabelTopologyZone] = "par", "par"
		claim.Status.Capacity, claim.Status.Allocatable = it.Capacity, it.Allocatable()
		claim.StatusConditions().SetTrue(karpv1.ConditionTypeLaunched)
		node := clusterNode(claim, n.ready, n.since)
		node.CreationTimestamp = metav1.NewTime(clk.Now().Add(-time.Hour))
		node.Labels[corev1.LabelHostname] = node.Name
		node.Status.Capacity, node.Status.Allocatable = it.Capacity, it.Allocatable()
		if !n.unregistered {
			claim.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
			node.Labels[karpv1.NodeRegisteredLabelKey] = "true"
			if !n.uninitialized {
				claim.StatusConditions().SetTrue(karpv1.ConditionTypeInitialized)
				node.Labels[karpv1.NodeInitializedLabelKey] = "true"
			}
		}
		if n.consolidatable {
			claim.StatusConditions().SetTrue(karpv1.ConditionTypeConsolidatable)
		}
		claims, stateNodes = append(claims, claim), append(stateNodes, node)
		objs = append(objs, group, claim, node)
	}
	kubeClient := newCoreIndexedClient(objs...)
	if operator != nil {
		kubeClient = interceptor.NewClient(kubeClient.(client.WithWatch), interceptor.Funcs{Create: operator.create})
	}
	cp := cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clk))
	matcher, err := health.NewRepairPolicyMatcher(ctx, cp)
	if err != nil {
		t.Fatal(err)
	}
	cluster := state.NewCluster(clk, kubeClient, cp, state.WithRepairPolicyMatcher(matcher))
	for _, claim := range claims {
		cluster.UpdateNodeClaim(claim)
	}
	for _, node := range stateNodes {
		if err := cluster.UpdateNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	for _, obj := range objs {
		if pod, ok := obj.(*corev1.Pod); ok {
			if err := cluster.UpdatePod(ctx, pod); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !cluster.Synced(ctx) {
		t.Fatal("cluster state did not sync")
	}
	provisioner := provisioning.NewProvisioner(kubeClient, noopRecorder{}, cp, cluster, clk, nil, nil)
	queue := coredisruption.NewQueue(kubeClient, noopRecorder{}, cluster, clk, provisioner)
	controller := coredisruption.NewController(ctx, clk, kubeClient, provisioner, cp, noopRecorder{}, cluster, queue, cost.NewClusterCost(ctx, cp, kubeClient))
	return &repairFixture{t: t, ctx: ctx, clk: clk, kubeClient: kubeClient, cp: cp, cluster: cluster, queue: queue, controller: controller}
}

// pass runs one disruption loop and returns the commands in flight.
func (f *repairFixture) pass() []*coredisruption.Command {
	f.t.Helper()
	if _, err := f.controller.Reconcile(f.ctx); err != nil {
		f.t.Fatalf("disruption loop: %v", err)
	}
	return f.queue.GetCommands()
}

// complete brings the command's replacements up and runs the orchestration
// queue, which deletes the candidate. It returns the candidate's NodeClaim.
func (f *repairFixture) complete(cmd *coredisruption.Command) *karpv1.NodeClaim {
	f.t.Helper()
	for _, replacement := range cmd.Replacements {
		nodeClaim := &karpv1.NodeClaim{}
		if err := f.kubeClient.Get(f.ctx, types.NamespacedName{Name: replacement.Name}, nodeClaim); err != nil {
			f.t.Fatalf("replacement %s: %v", replacement.Name, err)
		}
		nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeInitialized)
		if err := f.kubeClient.Status().Update(f.ctx, nodeClaim); err != nil {
			f.t.Fatal(err)
		}
	}
	if _, err := f.queue.Reconcile(f.ctx, cmd.Candidates[0].NodeClaim); err != nil {
		f.t.Fatal(err)
	}
	nodeClaim := &karpv1.NodeClaim{}
	if err := f.kubeClient.Get(f.ctx, client.ObjectKeyFromObject(cmd.Candidates[0].NodeClaim), nodeClaim); err != nil {
		f.t.Fatalf("candidate: %v", err)
	}
	return nodeClaim
}

// shieldedPod is a ReplicaSet pod of the broken node that tolerates the
// node-lifecycle NoExecute taints, so taint-based eviction never removed it.
func shieldedPod(name, nodeName string) *corev1.Pod {
	pod := pendingPod(name)
	pod.Labels = map[string]string{"app": name}
	pod.Spec.NodeName = nodeName
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: name, UID: types.UID("rs-" + name), Controller: lo.ToPtr(true)}}
	pod.Spec.Tolerations = []corev1.Toleration{
		{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
		{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
	}
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}}
	return pod
}

// TestCoreRepairReplacesANodeNotReadyForTenMinutes runs core's repair over a
// NotReady node of a managed group whose pods would block a graceful drain: one
// covered by a PodDisruptionBudget at its limit (its replica on the broken node
// is what holds the budget at zero), one marked do-not-disrupt. A node
// NotReady (False) or silent (Unknown) for 10 minutes is deleted with a
// termination deadline of the moment of deletion, so its pods are deleted
// rather than evicted — the forceful repair of earlier releases. A nil drain
// bound would leave such a node unrepaired forever. A Ready node is left alone.
func TestCoreRepairReplacesANodeNotReadyForTenMinutes(t *testing.T) {
	for _, status := range []corev1.ConditionStatus{corev1.ConditionFalse, corev1.ConditionUnknown, corev1.ConditionTrue} {
		t.Run(string(status), func(t *testing.T) {
			start := time.Now().UTC().Truncate(time.Second)
			guarded, pinned := shieldedPod("guarded", "default-heal1-node0"), shieldedPod("pinned", "default-heal1-node0")
			pinned.Annotations = map[string]string{karpv1.DoNotDisruptAnnotationKey: "true"}
			pdb := &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: "guarded", Namespace: "default"},
				Spec: policyv1.PodDisruptionBudgetSpec{
					MinAvailable: lo.ToPtr(intstr.FromInt32(1)),
					Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "guarded"}},
				},
				Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, CurrentHealthy: 1, DesiredHealthy: 1, ExpectedPods: 2},
			}
			f := newRepairFixture(t, nil, "10%", []repairNode{
				{name: "default-ok001", flavor: "M", ready: corev1.ConditionTrue, since: start.Add(-time.Hour)},
				{name: "default-heal1", flavor: "XS", ready: status, since: start},
			}, guarded, pinned, pdb)
			f.clk.SetTime(start.Add(repairToleration - time.Second))
			if cmds := f.pass(); len(cmds) != 0 {
				t.Fatalf("repaired before its toleration elapsed: %v", cmds[0])
			}
			f.clk.SetTime(start.Add(repairToleration))
			cmds := f.pass()
			if status == corev1.ConditionTrue {
				if len(cmds) != 0 {
					t.Fatalf("Ready node repaired: %v", cmds[0])
				}
				return
			}
			if len(cmds) != 1 || cmds[0].Reason() != karpv1.DisruptionReasonUnhealthy || cmds[0].Candidates[0].NodeClaim.Name != "default-heal1" {
				t.Fatalf("node Ready=%s for %s: want one repair of default-heal1, got %v", status, repairToleration, cmds)
			}
			deletedAt := f.clk.Now()
			nodeClaim := f.complete(cmds[0])
			if nodeClaim.DeletionTimestamp.IsZero() {
				t.Fatal("repair did not delete the NodeClaim")
			}
			if got, want := nodeClaim.Annotations[karpv1.NodeClaimTerminationTimestampAnnotationKey], deletedAt.Format(time.RFC3339); got != want {
				t.Errorf("termination deadline %q, want %q (forceful: no drain past deletion)", got, want)
			}
		})
	}
}

// TestCoreRepairOfANodeThatNeverInitialized covers the hand-off between the
// provider's launch watchers and core's repair. A NodeClaim whose node never
// registered stays with core's registration timeout and the nodegroupstatus
// controller (Launched, not Registered); once registered, a node NotReady for
// 10 minutes without ever initializing is repaired, outside the NodePool's
// budget since it runs no workload.
func TestCoreRepairOfANodeThatNeverInitialized(t *testing.T) {
	for _, tc := range []struct {
		name     string
		node     repairNode
		repaired bool
	}{
		{"registered", repairNode{name: "default-heal1", flavor: "XS", ready: corev1.ConditionFalse, uninitialized: true}, true},
		{"unregistered", repairNode{name: "default-heal1", flavor: "XS", ready: corev1.ConditionFalse, unregistered: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now().UTC().Truncate(time.Second)
			tc.node.since = start
			f := newRepairFixture(t, nil, "0", []repairNode{
				{name: "default-ok001", flavor: "M", ready: corev1.ConditionTrue, since: start.Add(-time.Hour)},
				tc.node,
			})
			f.clk.SetTime(start.Add(repairToleration))
			if got := len(f.pass()) == 1; got != tc.repaired {
				t.Errorf("repaired = %v, want %v", got, tc.repaired)
			}
		})
	}
}

// TestCoreRepairIsPacedByNodePoolBudgets pins what an operator pausing
// disruption relies on: a budget of zero nodes listing no reasons covers
// repair (reason Unhealthy) as well, and so does one listing Unhealthy.
func TestCoreRepairIsPacedByNodePoolBudgets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		budget  karpv1.Budget
		repairs int
	}{
		{"zero, no reasons", karpv1.Budget{Nodes: "0"}, 0},
		{"10%, no reasons", karpv1.Budget{Nodes: "10%"}, 1},
		{"zero for Unhealthy", karpv1.Budget{Nodes: "0", Reasons: []karpv1.DisruptionReason{karpv1.DisruptionReasonUnhealthy}}, 0},
		{"zero for Drifted only", karpv1.Budget{Nodes: "0", Reasons: []karpv1.DisruptionReason{karpv1.DisruptionReasonDrifted}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now().UTC().Truncate(time.Second)
			f := newRepairFixture(t, nil, "10%", []repairNode{
				{name: "default-ok001", flavor: "M", ready: corev1.ConditionTrue, since: start.Add(-time.Hour)},
				{name: "default-heal1", flavor: "XS", ready: corev1.ConditionUnknown, since: start},
			})
			nodePool := &karpv1.NodePool{}
			if err := f.kubeClient.Get(f.ctx, types.NamespacedName{Name: "default"}, nodePool); err != nil {
				t.Fatal(err)
			}
			nodePool.Spec.Disruption.Budgets = []karpv1.Budget{tc.budget}
			if err := f.kubeClient.Update(f.ctx, nodePool); err != nil {
				t.Fatal(err)
			}
			f.clk.SetTime(start.Add(repairToleration))
			if got := len(f.pass()); got != tc.repairs {
				t.Errorf("%d repairs, want %d", got, tc.repairs)
			}
		})
	}
}

// TestCoreRepairHonoursDoNotRepair pins the operator's per-node veto:
// karpenter.sh/do-not-repair on a node NotReady past its toleration keeps
// core from repairing it.
func TestCoreRepairHonoursDoNotRepair(t *testing.T) {
	for _, vetoed := range []bool{true, false} {
		t.Run(lo.Ternary(vetoed, "annotated", "not annotated"), func(t *testing.T) {
			start := time.Now().UTC().Truncate(time.Second)
			f := newRepairFixture(t, nil, "10%", []repairNode{
				{name: "default-ok001", flavor: "M", ready: corev1.ConditionTrue, since: start.Add(-time.Hour)},
				{name: "default-heal1", flavor: "XS", ready: corev1.ConditionUnknown, since: start},
			})
			if vetoed {
				node := &corev1.Node{}
				if err := f.kubeClient.Get(f.ctx, types.NamespacedName{Name: "default-heal1-node0"}, node); err != nil {
					t.Fatal(err)
				}
				node.Annotations = map[string]string{karpv1.DoNotRepairAnnotationKey: "true"}
				if err := f.kubeClient.Update(f.ctx, node); err != nil {
					t.Fatal(err)
				}
				if err := f.cluster.UpdateNode(f.ctx, node); err != nil {
					t.Fatal(err)
				}
			}
			f.clk.SetTime(start.Add(repairToleration))
			if got, want := len(f.pass()), lo.Ternary(vetoed, 0, 1); got != want {
				t.Errorf("%d repairs, want %d", got, want)
			}
		})
	}
}

// TestCoreRepairWaitsForClusterStateSync pins that repair, as a disruption
// method, waits for core's cluster state to sync: a NodeClaim without a
// provider ID — a launch queued on the create lock, a launch retried after a
// plain error — holds every repair until it is launched or gone.
func TestCoreRepairWaitsForClusterStateSync(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	f := newRepairFixture(t, nil, "10%", []repairNode{
		{name: "default-ok001", flavor: "M", ready: corev1.ConditionTrue, since: start.Add(-time.Hour)},
		{name: "default-heal1", flavor: "XS", ready: corev1.ConditionUnknown, since: start},
	})
	f.cluster.UpdateNodeClaim(testNodeClaim("default-queue"))
	f.clk.SetTime(start.Add(repairToleration))
	if f.cluster.Synced(f.ctx) {
		t.Fatal("a NodeClaim without a provider ID must hold cluster state unsynced")
	}
	if cmds := f.pass(); len(cmds) != 0 {
		t.Fatalf("repaired while cluster state was unsynced: %v", cmds[0])
	}
	f.cluster.DeleteNodeClaim("default-queue")
	if cmds := f.pass(); len(cmds) != 1 {
		t.Fatalf("want the repair once cluster state syncs, got %v", cmds)
	}
}

// TestCoreRepairWaitsForQuotaToReplaceAStatefulPod pins a limit of repair as
// a voluntary disruption: it launches the replacement before deleting the
// node, and the organisation quota counts the broken node's VM until it is
// deleted. A dead node whose StatefulSet replica (terminating, which its
// controller waits on) needs capacity is therefore not repaired while the
// quota is exhausted: each attempt costs one upstream rejection, the backoff
// then serves no flavor and core reports the pod unschedulable. A dead node
// whose pods were already evicted needs no replacement and is repaired.
// LEGACY_NODE_REPAIR=true deletes first instead.
func TestCoreRepairWaitsForQuotaToReplaceAStatefulPod(t *testing.T) {
	for _, stateful := range []bool{true, false} {
		t.Run(lo.Ternary(stateful, "statefulset replica", "evicted deployment replica"), func(t *testing.T) {
			start := time.Now().UTC().Truncate(time.Second)
			pod := shieldedPod("db", "default-heal1-node0")
			evicted := metav1.NewTime(start.Add(5 * time.Minute))
			pod.DeletionTimestamp, pod.Finalizers = &evicted, []string{"test/keep"}
			if stateful {
				pod.OwnerReferences[0].Kind = "StatefulSet"
			}
			operator := &fakeOperator{headroomGB: 0}
			f := newRepairFixture(t, operator, "10%", []repairNode{{name: "default-heal1", flavor: "XS", ready: corev1.ConditionUnknown, since: start}}, pod)
			f.clk.SetTime(start.Add(repairToleration))
			repaired := false
			for range 6 {
				cmds := f.pass()
				if len(cmds) == 0 {
					f.clk.Step(20 * time.Second)
					continue
				}
				if len(cmds[0].Replacements) == 0 {
					repaired = !f.complete(cmds[0]).DeletionTimestamp.IsZero()
					break
				}
				// Core's launch of the replacement, refused by the quota engine.
				replacement := &karpv1.NodeClaim{}
				if err := f.kubeClient.Get(f.ctx, types.NamespacedName{Name: cmds[0].Replacements[0].Name}, replacement); err != nil {
					t.Fatal(err)
				}
				if _, err := f.cp.Create(f.ctx, replacement); !corecloudprovider.IsInsufficientCapacityError(err) {
					t.Fatalf("replacement launch at exhausted quota: %v", err)
				}
				if err := f.kubeClient.Delete(f.ctx, replacement); err != nil {
					t.Fatal(err)
				}
				f.cluster.DeleteNodeClaim(replacement.Name)
				if _, err := f.queue.Reconcile(f.ctx, cmds[0].Candidates[0].NodeClaim); err != nil {
					t.Fatal(err)
				}
				f.clk.Step(20 * time.Second)
			}
			if repaired == stateful {
				t.Errorf("repaired = %v after %s at exhausted quota (rejections %v)", repaired, f.clk.Since(start), operator.createdFlavors())
			}
			if stateful && operator.createCount() != 2 {
				t.Errorf("%d replacement launches in two minutes of backoff, want one a window", operator.createCount())
			}
		})
	}
}

// TestCoreRepairWithoutAReadyNodePoolStallsDisruption pins an upstream limit
// the runbook documents. Repair runs first in core's disruption loop and
// simulates where a broken node's pods would go; with no NodePool Ready — its
// CleverNodeClass deleted (the provider's finalizer keeps it until its nodes
// are gone), invalid, or not yet reconciled — the simulation fails, and the
// error ends the whole pass. The broken node is not repaired, and the methods
// after repair do not run either: an empty node stays. karpenter.sh/do-not-repair
// on the broken node lets them run again.
func TestCoreRepairWithoutAReadyNodePoolStallsDisruption(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	// A budget with room for the empty node: core counts a NotReady node
	// against every reason's budget but Unhealthy, so 10% of two nodes is
	// already spent for Empty.
	f := newRepairFixture(t, nil, "100%", []repairNode{
		{name: "default-empty", flavor: "XS", ready: corev1.ConditionTrue, since: start.Add(-time.Hour), consolidatable: true},
		{name: "default-heal1", flavor: "XS", ready: corev1.ConditionUnknown, since: start},
	})
	nodePool := &karpv1.NodePool{}
	if err := f.kubeClient.Get(f.ctx, types.NamespacedName{Name: "default"}, nodePool); err != nil {
		t.Fatal(err)
	}
	nodePool.Spec.Disruption.ConsolidationPolicy = karpv1.ConsolidationPolicyWhenEmptyOrUnderutilized
	nodePool.Spec.Disruption.ConsolidateAfter = karpv1.MustParseNillableDuration("30s")
	nodePool.StatusConditions().SetFalse(karpv1.ConditionTypeNodeClassReady, "NodeClassTerminating", "CleverNodeClass is terminating")
	if err := f.kubeClient.Update(f.ctx, nodePool); err != nil {
		t.Fatal(err)
	}
	f.clk.SetTime(start.Add(repairToleration))
	// Bounded: were core to stop ending the pass at repair, emptiness would
	// wait out its validation on the fake clock, and the test must fail, not
	// hang.
	firstPass, stopFirstPass := context.WithTimeout(f.ctx, 10*time.Second)
	defer stopFirstPass()
	if _, err := f.controller.Reconcile(firstPass); err == nil || !strings.Contains(err.Error(), "no nodepools found") {
		t.Fatalf("disruption pass with no NodePool Ready: %v, want the repair simulation to fail it", err)
	}
	if cmds := f.queue.GetCommands(); len(cmds) != 0 {
		t.Fatalf("a pass that failed at repair started %v", cmds[0])
	}

	node := &corev1.Node{}
	if err := f.kubeClient.Get(f.ctx, types.NamespacedName{Name: "default-heal1-node0"}, node); err != nil {
		t.Fatal(err)
	}
	node.Annotations = map[string]string{karpv1.DoNotRepairAnnotationKey: "true"}
	if err := f.kubeClient.Update(f.ctx, node); err != nil {
		t.Fatal(err)
	}
	if err := f.cluster.UpdateNode(f.ctx, node); err != nil {
		t.Fatal(err)
	}
	// Emptiness waits out its validation period on the clock: step the fake
	// one while the pass waits.
	secondPass, stopSecondPass := context.WithCancel(f.ctx)
	defer stopSecondPass()
	passed := make(chan error, 1)
	go func() {
		_, err := f.controller.Reconcile(secondPass)
		passed <- err
	}()
	for deadline := time.After(30 * time.Second); ; {
		select {
		case err := <-passed:
			if err != nil {
				t.Fatalf("disruption pass with the broken node exempted from repair: %v", err)
			}
			cmds := f.queue.GetCommands()
			if len(cmds) != 1 || cmds[0].Reason() != karpv1.DisruptionReasonEmpty || cmds[0].Candidates[0].NodeClaim.Name != "default-empty" {
				t.Fatalf("with the broken node exempted from repair: want the empty node removed, got %v", cmds)
			}
			return
		case <-deadline:
			t.Fatal("the disruption pass did not return")
		case <-time.After(10 * time.Millisecond):
			if f.clk.HasWaiters() {
				f.clk.Step(time.Second)
			}
		}
	}
}

// TestLegacyRepairReplacesANodeNotReadyForTenMinutes runs the controller
// LEGACY_NODE_REPAIR=true selects: it reads the same policies (and ignores
// their drain bound), deletes the NodeClaim of a node NotReady for 10 minutes
// with a termination deadline of now, without a replacement or a budget.
func TestLegacyRepairReplacesANodeNotReadyForTenMinutes(t *testing.T) {
	for _, status := range []corev1.ConditionStatus{corev1.ConditionFalse, corev1.ConditionUnknown, corev1.ConditionTrue} {
		t.Run(string(status), func(t *testing.T) {
			ctx := repairContext(true)
			clk := clocktesting.NewFakeClock(time.Now())
			group := managedNodeGroup("default-heal1", "XS")
			nodeClaim := launchedNodeClaim(group.Name)
			// Core's termination finalizer, so the deleted claim stays readable.
			nodeClaim.Finalizers = []string{karpv1.TerminationFinalizer}
			node := clusterNode(nodeClaim, status, clk.Now())
			kubeClient := newCoreIndexedClient(readyNodeClass("default"), group, nodeClaim, node)
			itp := instancetype.NewProvider("par", nil, nil)
			cp := cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{}))
			controller := legacyrepair.NewController(kubeClient, cp, clk, noopRecorder{})
			repaired := func() bool {
				t.Helper()
				if _, err := controller.Reconcile(ctx, node); err != nil {
					t.Fatalf("legacy node repair: %v", err)
				}
				current := &karpv1.NodeClaim{}
				err := kubeClient.Get(ctx, types.NamespacedName{Name: nodeClaim.Name}, current)
				if apierrors.IsNotFound(err) {
					return true
				}
				if err != nil {
					t.Fatalf("getting nodeclaim: %v", err)
				}
				return !current.DeletionTimestamp.IsZero()
			}
			clk.Step(repairToleration - time.Second)
			if repaired() {
				t.Fatalf("node %s: repaired before its toleration elapsed", status)
			}
			clk.Step(time.Second)
			if got, want := repaired(), status != corev1.ConditionTrue; got != want {
				t.Fatalf("node Ready=%s for %s: repaired = %v, want %v", status, repairToleration, got, want)
			}
			if status == corev1.ConditionTrue {
				return
			}
			current := &karpv1.NodeClaim{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: nodeClaim.Name}, current); err != nil {
				t.Fatal(err)
			}
			if got, want := current.Annotations[karpv1.NodeClaimTerminationTimestampAnnotationKey], clk.Now().Format(time.RFC3339); got != want {
				t.Errorf("termination deadline %q, want %q (forceful: no drain past deletion)", got, want)
			}
		})
	}
}
