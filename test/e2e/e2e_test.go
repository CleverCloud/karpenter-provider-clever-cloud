//go:build e2e

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

// Package e2e runs the provider end-to-end against a real Clever Kubernetes
// Engine cluster: real VMs, the real quota engine, the real node-group
// operator. It automates the manual validation runs recorded in
// docs/E2E-RESULTS.md; the wait bounds below are that document's measured
// envelope (provision 40-185s, deletion ~40s, drift detection <10s) with
// generous headroom for platform variance.
//
// The suite is deliberately serial: every scenario shares one org quota.
// It requires E2E_CONTEXT (see docs/e2e.md) and creates real, hourly-billed
// VMs — cleanup runs even when the suite context expires, and
// hack/e2e-cleanup.sh is the out-of-band fallback.
package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

func TestE2E(t *testing.T) {
	timeout := 40 * time.Minute
	if v := os.Getenv("E2E_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("parsing E2E_TIMEOUT: %v", err)
		}
		timeout = d
	}
	// The suite context must expire long enough before go test's own
	// -timeout for the cleanup (fresh 20-minute context) and the controller
	// stop to finish — a go test timeout kills the process without running
	// cleanups, which would leak billing VMs. Enforce it instead of hoping.
	const cleanupReserve = 22 * time.Minute
	if deadline, ok := t.Deadline(); ok {
		budget := time.Until(deadline) - cleanupReserve
		if budget < 5*time.Minute {
			t.Fatalf("go test -timeout leaves only %s for the suite after the %s cleanup reserve; raise -timeout or lower E2E_TIMEOUT", budget.Round(time.Minute), cleanupReserve)
		}
		if timeout > budget {
			t.Logf("clamping E2E_TIMEOUT %s -> %s to preserve the cleanup reserve inside go test -timeout", timeout, budget.Round(time.Minute))
			timeout = budget
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	f := newFramework(t)
	f.checkCluster(ctx)
	f.applyCRDs(ctx)

	// t.Cleanup, not defer: cleanups also run when a subtest goroutine
	// panics, where plain defers on this goroutine would not. LIFO order:
	// cleanupAll first (needs the live controller to drain claims), then
	// the controller stop.
	stop := f.startController(ctx)
	t.Cleanup(stop)
	t.Cleanup(f.cleanupAll)

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.namespace, Labels: f.labels()}}
	if err := f.client.Create(ctx, ns); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}

	// The scenarios build on each other (consolidation drains what provision
	// created, drift rolls a provisioned node, ...): a failure invalidates
	// everything after it, so abort the chain instead of piling misleading
	// failures on a broken cluster state. Cleanup still runs via t.Cleanup.
	for _, step := range []struct {
		name string
		fn   func(*testing.T, context.Context, *framework)
	}{
		{"Provision", testProvision},
		{"ConsolidationScaleToZero", testConsolidationScaleToZero},
		{"Drift", testDrift},
		{"GarbageCollection", testGarbageCollection},
		{"QuotaFastFail", testQuotaFastFail},
	} {
		if ok := t.Run(step.name, func(t *testing.T) { step.fn(t, ctx, f.withT(t)) }); !ok {
			t.Errorf("aborting the remaining scenarios after %s failed", step.name)
			break
		}
	}
}

// testProvision covers the core promise: a pending pod becomes a running pod
// on a dedicated, correctly-labeled nodeCount:1 NodeGroup, with the claim
// registered and the node carrying the stamped provider ID.
func testProvision(t *testing.T, ctx context.Context, f *framework) {
	poolName := f.prefix + "-main"
	if err := f.client.Create(ctx, f.nodeClass(poolName, nil)); err != nil {
		t.Fatalf("creating nodeclass: %v", err)
	}
	if err := f.client.Create(ctx, f.nodePool(poolName, poolName, []string{"2XS", "XS"}, "8")); err != nil {
		t.Fatalf("creating nodepool: %v", err)
	}
	f.eventually(ctx, 2*time.Minute, "nodeclass Ready", func(ctx context.Context) (bool, string) {
		nc := &v1alpha1.CleverNodeClass{}
		if err := f.client.Get(ctx, types.NamespacedName{Name: poolName}, nc); err != nil {
			return false, err.Error()
		}
		if !nc.StatusConditions().Root().IsTrue() {
			return false, fmt.Sprintf("conditions: %+v", nc.Status.Conditions)
		}
		return true, ""
	})

	if err := f.client.Create(ctx, f.deployment("inflate", 2, "1", "1500Mi", false)); err != nil {
		t.Fatalf("creating deployment: %v", err)
	}

	start := time.Now()
	f.eventually(ctx, 8*time.Minute, "2 inflate replicas Running on provisioned nodes", func(ctx context.Context) (bool, string) {
		pods := &corev1.PodList{}
		if err := f.client.List(ctx, pods, client.InNamespace(f.namespace), client.MatchingLabels{"app": "inflate"}); err != nil {
			return false, err.Error()
		}
		running := 0
		for _, p := range pods.Items {
			if p.Status.Phase == corev1.PodRunning {
				running++
			}
		}
		return running == 2, fmt.Sprintf("%d/2 pods Running", running)
	})
	t.Logf("pod-pending -> pods-Running: %s (measured envelope: 40-185s)", time.Since(start).Round(time.Second))

	claims := f.claimsOfPool(ctx, poolName)
	if len(claims) == 0 {
		t.Fatal("no nodeclaim for the main pool despite running pods")
	}
	for _, claim := range claims {
		if !claim.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue() {
			t.Errorf("nodeclaim %s is not Registered", claim.Name)
		}
		ng := &ngv1.NodeGroup{}
		if err := f.client.Get(ctx, types.NamespacedName{Name: claim.Name}, ng); err != nil {
			t.Errorf("nodegroup %s (1 claim = 1 group invariant): %v", claim.Name, err)
			continue
		}
		if !nodegroup.IsManaged(ng) {
			t.Errorf("nodegroup %s misses the managed marker label", ng.Name)
		}
		if ng.Spec.NodeCount != 1 {
			t.Errorf("nodegroup %s nodeCount = %d, want 1", ng.Name, ng.Spec.NodeCount)
		}
		if owners := nodegroup.NodeClaimOwners(ng); len(owners) != 1 || owners[0] != claim.Name {
			t.Errorf("nodegroup %s owner references = %v, want [%s]", ng.Name, owners, claim.Name)
		}
		node := &corev1.Node{}
		if err := f.client.Get(ctx, types.NamespacedName{Name: claim.Status.NodeName}, node); err != nil {
			t.Errorf("node of claim %s: %v", claim.Name, err)
			continue
		}
		if want := nodegroup.ProviderID(claim.Name); node.Spec.ProviderID != want {
			t.Errorf("node %s providerID = %q, want %q", node.Name, node.Spec.ProviderID, want)
		}
	}
}

// testConsolidationScaleToZero proves billing stops when the workload goes
// away: claims drain, NodeGroups (and their VMs) are deleted, node objects
// leave the cluster.
func testConsolidationScaleToZero(t *testing.T, ctx context.Context, f *framework) {
	f.scaleDeployment(ctx, "inflate", 0)
	f.eventually(ctx, 12*time.Minute, "main pool consolidated to zero", func(ctx context.Context) (bool, string) {
		claims := f.claimsOfPool(ctx, f.prefix+"-main")
		groups := f.runNodeGroups(ctx)
		return len(claims) == 0 && len(groups) == 0,
			fmt.Sprintf("%d claims, %d nodegroups remaining", len(claims), len(groups))
	})
}

// testDrift proves a NodeClass change rolls the node: the claim goes Drifted
// and is replaced by one whose node carries the new NodeGroup labels.
func testDrift(t *testing.T, ctx context.Context, f *framework) {
	poolName := f.prefix + "-main"
	f.scaleDeployment(ctx, "inflate", 1)
	f.eventually(ctx, 8*time.Minute, "one registered claim before drift", func(ctx context.Context) (bool, string) {
		claims := f.claimsOfPool(ctx, poolName)
		if len(claims) != 1 {
			return false, fmt.Sprintf("%d claims", len(claims))
		}
		return claims[0].StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue(), "claim not Registered yet"
	})
	preDrift := f.claimsOfPool(ctx, poolName)
	if len(preDrift) != 1 {
		t.Fatalf("claim count changed right after settling: %d", len(preDrift))
	}
	oldClaim := preDrift[0].Name

	nc := &v1alpha1.CleverNodeClass{}
	if err := f.client.Get(ctx, types.NamespacedName{Name: poolName}, nc); err != nil {
		t.Fatalf("getting nodeclass: %v", err)
	}
	stored := nc.DeepCopy()
	nc.Spec.Labels = map[string]string{"e2e-drift": f.runID}
	if err := f.client.Patch(ctx, nc, client.MergeFrom(stored)); err != nil {
		t.Fatalf("patching nodeclass: %v", err)
	}

	start := time.Now()
	f.eventually(ctx, 15*time.Minute, "drifted node replaced", func(ctx context.Context) (bool, string) {
		claims := f.claimsOfPool(ctx, poolName)
		if len(claims) != 1 {
			return false, fmt.Sprintf("%d claims (rolling)", len(claims))
		}
		claim := claims[0]
		if claim.Name == oldClaim {
			return false, "old claim still in place"
		}
		if !claim.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue() {
			return false, "replacement claim not Registered yet"
		}
		node := &corev1.Node{}
		if err := f.client.Get(ctx, types.NamespacedName{Name: claim.Status.NodeName}, node); err != nil {
			return false, err.Error()
		}
		if node.Labels["e2e-drift"] != f.runID {
			return false, fmt.Sprintf("replacement node misses the e2e-drift label: %v", node.Labels)
		}
		return true, ""
	})
	t.Logf("nodeclass patch -> replacement registered: %s", time.Since(start).Round(time.Second))
}

// testGarbageCollection covers both faces of the safety net on a live
// cluster: it must refuse a managed-labeled group it cannot prove it owns,
// and it must reap the group of a force-deleted NodeClaim.
func testGarbageCollection(t *testing.T, ctx context.Context, f *framework) {
	// A hand-made group wearing the managed label but no NodeClaim owner
	// reference: the GC must refuse it (the label is forgeable, ownership is
	// not) and say so via event + gauge. This intentionally starts a real VM.
	// The taint keeps that VM out of the scheduler's hands: untainted, its
	// node counts as reschedulable capacity and consolidation moves the
	// inflate pod onto it, deleting the claim the reap step needs (observed
	// on the first live run).
	decoyName := f.prefix + "-decoy"
	decoy := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   decoyName,
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true", suiteLabelKey: "true", runLabelKey: f.runID},
		},
		Spec: ngv1.NodeGroupSpec{
			Flavor:    "2XS",
			NodeCount: 1,
			Taints:    []ngv1.NodeGroupTaint{{Key: "e2e-decoy", Value: "true", Effect: corev1.TaintEffectNoSchedule}},
		},
	}
	if err := f.client.Create(ctx, decoy); err != nil {
		t.Fatalf("creating decoy nodegroup: %v", err)
	}

	// minAge is 2 min and the sweep runs every 2 min: the refusal must be
	// visible within two sweeps.
	f.eventually(ctx, 8*time.Minute, "GC refuses the ownerless decoy", func(ctx context.Context) (bool, string) {
		if f.hasEvent(ctx, decoyName, "GarbageCollectionRefused") {
			return true, ""
		}
		return false, fmt.Sprintf("no GarbageCollectionRefused event yet (gc_refused_nodegroups=%v)",
			f.metric("karpenter_clevercloud_gc_refused_nodegroups"))
	})
	if got := f.metric("karpenter_clevercloud_gc_refused_nodegroups"); got < 1 {
		t.Errorf("gc_refused_nodegroups = %v, want >= 1", got)
	}
	if err := f.client.Get(ctx, types.NamespacedName{Name: decoyName}, &ngv1.NodeGroup{}); err != nil {
		t.Errorf("the decoy must survive the GC (managed label alone is not ownership): %v", err)
	}
	if err := f.client.Delete(ctx, decoy); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("deleting decoy: %v", err)
	}

	// Force-delete the running claim. Two reapers then race for the orphaned
	// NodeGroup: kube-controller-manager's owner-reference cascade (the
	// ownerRef stamped at creation names the claim) usually wins within
	// seconds, and the provider's 2-minute sweep is the backstop for when
	// the cascade misses. Either is a correct outcome on a live cluster —
	// what this scenario proves is that the orphan cannot survive; the
	// deterministic proof of the provider sweep itself lives in the unit
	// tests. The drift scenario normally leaves exactly one claim, but
	// consolidation may have churned it while the refusal wait ran — settle
	// on one registered claim rather than assuming.
	poolName := f.prefix + "-main"
	f.eventually(ctx, 8*time.Minute, "one registered claim before the reap step", func(ctx context.Context) (bool, string) {
		claims := f.claimsOfPool(ctx, poolName)
		if len(claims) != 1 {
			return false, fmt.Sprintf("%d claims", len(claims))
		}
		return claims[0].StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue(), "claim not Registered yet"
	})
	settled := f.claimsOfPool(ctx, poolName)
	if len(settled) != 1 {
		t.Fatalf("claim count changed right after settling: %d", len(settled))
	}
	claim := settled[0]

	reapedBefore := f.metric("karpenter_clevercloud_gc_reaped_nodegroups_total")
	stored := claim.DeepCopy()
	claim.Finalizers = nil
	if err := f.client.Patch(ctx, &claim, client.MergeFrom(stored)); err != nil {
		t.Fatalf("stripping claim finalizers: %v", err)
	}
	if err := f.client.Delete(ctx, &claim); err != nil {
		t.Fatalf("force-deleting claim: %v", err)
	}

	f.eventually(ctx, 8*time.Minute, "orphaned nodegroup gone after claim force-delete", func(ctx context.Context) (bool, string) {
		err := f.client.Get(ctx, types.NamespacedName{Name: claim.Name}, &ngv1.NodeGroup{})
		if err == nil {
			return false, fmt.Sprintf("nodegroup %s still present (gc_reaped_total=%v)",
				claim.Name, f.metric("karpenter_clevercloud_gc_reaped_nodegroups_total"))
		}
		return apierrors.IsNotFound(err), err.Error()
	})
	if got := f.metric("karpenter_clevercloud_gc_reaped_nodegroups_total"); got >= reapedBefore+1 {
		t.Logf("the provider GC sweep won the reap race (gc_reaped_nodegroups_total moved)")
	} else {
		t.Logf("the kubernetes owner-reference cascade reaped the group before the provider sweep (expected on most runs)")
	}

	// The inflate pod lost its node; karpenter must provision a replacement —
	// the cluster self-heals from the force-delete. Then drain everything.
	f.eventually(ctx, 8*time.Minute, "replacement claim after force-delete", func(ctx context.Context) (bool, string) {
		claims := f.claimsOfPool(ctx, poolName)
		if len(claims) != 1 {
			return false, fmt.Sprintf("%d claims", len(claims))
		}
		return claims[0].StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue(), "not Registered yet"
	})
	f.scaleDeployment(ctx, "inflate", 0)
	f.eventually(ctx, 12*time.Minute, "main pool drained after GC scenario", func(ctx context.Context) (bool, string) {
		claims := f.claimsOfPool(ctx, poolName)
		groups := f.runNodeGroups(ctx)
		return len(claims) == 0 && len(groups) == 0,
			fmt.Sprintf("%d claims, %d nodegroups remaining", len(claims), len(groups))
	})
}

// testQuotaFastFail drives provisioning into the organisation quota and
// requires the fast, visible failure shape: quota rejections surface in the
// provider metric and event, pods stay Pending, and nothing leaks once the
// workload is gone.
func testQuotaFastFail(t *testing.T, ctx context.Context, f *framework) {
	poolName := f.prefix + "-quota"
	if err := f.client.Create(ctx, f.nodeClass(poolName, nil)); err != nil {
		t.Fatalf("creating nodeclass: %v", err)
	}
	// XL-only pool with a limit far above the org quota: the quota engine,
	// not the NodePool limit, must be what stops provisioning. One pod per
	// node forces one XL VM per replica; the default 40 vCPU / 40 GB org
	// quota rejects at the first or second XL (16 vCPU / 32 GB each).
	if err := f.client.Create(ctx, f.nodePool(poolName, poolName, []string{"XL"}, "400")); err != nil {
		t.Fatalf("creating nodepool: %v", err)
	}
	if err := f.client.Create(ctx, f.deployment("quota-inflate", 4, "1", "1Gi", true)); err != nil {
		t.Fatalf("creating deployment: %v", err)
	}

	f.eventually(ctx, 10*time.Minute, "quota rejection surfaced", func(ctx context.Context) (bool, string) {
		if f.metric("karpenter_clevercloud_nodegroup_quota_rejections_total") >= 1 {
			return true, ""
		}
		return false, "quota_rejections_total still 0"
	})

	pods := &corev1.PodList{}
	if err := f.client.List(ctx, pods, client.InNamespace(f.namespace), client.MatchingLabels{"app": "quota-inflate"}); err != nil {
		t.Fatalf("listing quota pods: %v", err)
	}
	pending := 0
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodPending {
			pending++
		}
	}
	if pending == 0 {
		t.Error("expected at least one pod parked Pending by the quota")
	}
	t.Logf("quota scenario: %d/%d pods Pending after rejection", pending, len(pods.Items))

	// The whole point of the fast-fail path: once the pressure is gone, no
	// NodeGroup may survive the rejection churn.
	if err := f.client.Delete(ctx, f.deployment("quota-inflate", 0, "1", "1Gi", true)); err != nil {
		t.Fatalf("deleting quota deployment: %v", err)
	}
	f.eventually(ctx, 12*time.Minute, "no leaked nodegroup after quota churn", func(ctx context.Context) (bool, string) {
		claims := f.claimsOfPool(ctx, poolName)
		var leftover []string
		for _, ng := range f.runNodeGroups(ctx) {
			if strings.HasPrefix(ng.Name, poolName) {
				leftover = append(leftover, ng.Name)
			}
		}
		return len(claims) == 0 && len(leftover) == 0,
			fmt.Sprintf("%d claims, leftover groups: %v", len(claims), leftover)
	})
}
