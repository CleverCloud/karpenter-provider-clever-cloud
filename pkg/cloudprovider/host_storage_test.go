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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	coresched "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/utils/daemonset"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/instancetypecapacity"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// hostStorageProvider uses the same core scheduler/client harness as the
// topology tests, with the production requirement that disk capacity be learned
// before provisioning claims that request it. The fake operator counts POSTs.
func hostStorageProvider(t *testing.T) (*cloudprovider.CloudProvider, client.Client, *instancetype.Provider, *fakeOperator) {
	t.Helper()
	_, baseClient := newClusterStateProvider(t)
	withWatch, ok := baseClient.(client.WithWatch)
	if !ok {
		t.Fatalf("harness client %T does not support watches", baseClient)
	}
	operator := &fakeOperator{headroomGB: 100}
	kubeClient := interceptor.NewClient(withWatch, interceptor.Funcs{Create: operator.create})
	itp := instancetype.NewProvider("par", nil, nil)
	itp.RequireEphemeralStorageReference()
	cp := cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{}))
	return cp, kubeClient, itp, operator
}

func hostDisk(t *testing.T, itp *instancetype.Provider, capacity, allocatable string) {
	t.Helper()
	if _, err := itp.SetEphemeralStorageReference(resource.MustParse(capacity), resource.MustParse(allocatable)); err != nil {
		t.Fatalf("setting host disk: %v", err)
	}
}

func diskPod(name, disk string) *corev1.Pod {
	pod := pendingPod(name)
	pod.Spec.Containers[0].Resources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse(disk)
	return pod
}

// registerHostStorageWorker leaves a real core StateNode describing the disk
// that worker was launched with. Later host measurements must never rewrite
// its actual capacity, including after a decrease for future launches.
func registerHostStorageWorker(t *testing.T, cp *cloudprovider.CloudProvider, kubeClient client.Client, cluster *state.Cluster, np *karpv1.NodePool) (*karpv1.NodeClaim, string) {
	t.Helper()
	initial := provisioningPass(t, cp, kubeClient, cluster, np, pendingPod("initial"))
	if len(initial.NewNodeClaims) != 1 {
		t.Fatalf("expected one initial claim, got %d", len(initial.NewNodeClaims))
	}
	claim := launchPlanned(t, cp, kubeClient, initial.NewNodeClaims[0], "default-previous-disk")
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(coreContext(), types.NamespacedName{Name: claim.Name}, ng); err != nil {
		t.Fatal(err)
	}
	node := joinNode(t, kubeClient, ng, "0")
	stampNode(t, kubeClient, node.Name)
	registerNode(t, kubeClient, claim, node.Name)
	reportCapacity(t, kubeClient, node.Name, claim)
	observeNode(t, kubeClient, cluster, node.Name)
	cluster.UpdateNodeClaim(claim)
	return claim, node.Name
}

func TestHostStorageUnavailableMixedBatch(t *testing.T) {
	for _, stage := range []string{"startup", "invalidated reference"} {
		t.Run(stage, func(t *testing.T) {
			cp, kubeClient, itp, operator := hostStorageProvider(t)
			if stage == "invalidated reference" {
				hostDisk(t, itp, "160Gi", "144Gi")
				itp.InvalidateEphemeralStorageReference()
			}
			np := topologyNodePool("default")
			seed(t, kubeClient, np)
			cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
			plain, disk := pendingPod("without-disk-request"), diskPod("requires-disk", "1Gi")
			results := provisioningPass(t, cp, kubeClient, cluster, np, disk, plain)
			if len(results.NewNodeClaims) != 1 || results.PodErrors[plain] != nil || results.PodErrors[disk] == nil {
				t.Fatalf("mixed batch must provision only the pod without disk requests: claims=%d plain=%v disk=%v", len(results.NewNodeClaims), results.PodErrors[plain], results.PodErrors[disk])
			}
			if pods := results.NewNodeClaims[0].Pods; len(pods) != 1 || pods[0].UID != plain.UID {
				t.Fatalf("planned claim contains the wrong pods: %v", pods)
			}
			launched := launchPlanned(t, cp, kubeClient, results.NewNodeClaims[0], "default-unknown-disk")
			assertClaimDisk(t, launched, "0", "0")
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(coreContext(), types.NamespacedName{Name: launched.Name}, ng); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{snapshotCapacity, snapshotAllocatable} {
				quantity, err := resource.ParseQuantity(ng.Annotations[key])
				if err != nil || !quantity.IsZero() {
					t.Fatalf("unknown disk must persist a zero snapshot, %s=%q: %v", key, ng.Annotations[key], err)
				}
			}
			cluster.UpdateNodeClaim(launched)
			inflight := provisioningPass(t, cp, kubeClient, cluster, np, disk, plain)
			if len(inflight.NewNodeClaims) != 0 || inflight.PodErrors[plain] != nil || inflight.PodErrors[disk] == nil || len(inflight.ExistingNodeToPodMapping()[launched.Name]) != 1 {
				t.Fatalf("unknown in-flight disk must absorb only the plain pod: new=%d plain=%v disk=%v mapping=%v", len(inflight.NewNodeClaims), inflight.PodErrors[plain], inflight.PodErrors[disk], inflight.ExistingNodeToPodMapping())
			}
			// Recovery describes future workers, not the disk of a machine
			// already launched while its capacity was unknown.
			hostDisk(t, itp, "40Gi", "36Gi")
			got, err := cp.Get(coreContext(), launched.Status.ProviderID)
			if err != nil {
				t.Fatal(err)
			}
			assertClaimDisk(t, got, "0", "0")
			again, err := cp.Create(coreContext(), launched)
			if err != nil {
				t.Fatalf("adopting the unknown-disk launch after recovery: %v", err)
			}
			assertClaimDisk(t, again, "0", "0")
			recovered := provisioningPass(t, cp, kubeClient, cluster, np, disk)
			if len(recovered.NewNodeClaims) != 1 || recovered.PodErrors[disk] != nil {
				t.Fatalf("recovery must plan a known-disk worker: new=%d error=%v", len(recovered.NewNodeClaims), recovered.PodErrors[disk])
			}
			known := launchPlanned(t, cp, kubeClient, recovered.NewNodeClaims[0], "default-known-disk")
			assertClaimDisk(t, known, "40Gi", "36Gi")
			if operator.createCount() != 2 {
				t.Fatalf("expected one unknown-disk and one known-disk launch, got %d POSTs", operator.createCount())
			}
			// Once the initially unknown worker registers, core must use the
			// real Node status despite its immutable zero launch snapshot.
			node := joinNode(t, kubeClient, ng, "0")
			stampNode(t, kubeClient, node.Name)
			registerNode(t, kubeClient, launched, node.Name)
			actual := launched.DeepCopy()
			actual.Status.Capacity[corev1.ResourceEphemeralStorage] = resource.MustParse("40Gi")
			actual.Status.Allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse("36Gi")
			reportCapacity(t, kubeClient, node.Name, actual)
			observeNode(t, kubeClient, cluster, node.Name)
			cluster.UpdateNodeClaim(launched)
			itp.InvalidateEphemeralStorageReference()
			requireAbsorbed(t, provisioningPass(t, cp, kubeClient, cluster, np, disk), disk, "real node status supersedes unknown launch disk")
		})
	}
}

func TestHostStorageUnavailableCreateChecksRequestedDisk(t *testing.T) {
	for _, stage := range []string{"startup", "invalidated reference"} {
		t.Run(stage, func(t *testing.T) {
			cp, kubeClient, itp, operator := hostStorageProvider(t)
			if stage == "invalidated reference" {
				hostDisk(t, itp, "160Gi", "144Gi")
				itp.InvalidateEphemeralStorageReference()
			}
			ctx := coreContext()
			seed(t, kubeClient, topologyNodePool("default"))
			positive := claimFor("default-needs-disk", "128Mi", "2XS")
			positive.Spec.Resources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse("1Gi")
			stored(t, kubeClient, positive)
			if _, err := cp.Create(ctx, positive); err == nil || corecloudprovider.IsInsufficientCapacityError(err) || corecloudprovider.IsNodeClassNotReadyError(err) {
				t.Fatalf("positive disk request must wait with a retriable error: %T %v", err, err)
			}
			if operator.createCount() != 0 {
				t.Fatalf("positive disk request caused %d POSTs", operator.createCount())
			}
			for _, request := range []string{"absent", "zero"} {
				claim := claimFor("default-disk-"+request, "128Mi", "2XS")
				if request == "zero" {
					claim.Spec.Resources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse("0")
				}
				created, err := cp.Create(ctx, stored(t, kubeClient, claim))
				if err != nil {
					t.Fatalf("%s disk request must provision without a reference: %v", request, err)
				}
				assertClaimDisk(t, created, "0", "0")
			}
			if operator.createCount() != 2 {
				t.Fatalf("expected exactly the absent/zero disk launches, got %d", operator.createCount())
			}
			hostDisk(t, itp, "40Gi", "36Gi")
			created, err := cp.Create(ctx, positive)
			if err != nil {
				t.Fatalf("retrying positive disk request after recovery: %v", err)
			}
			assertClaimDisk(t, created, "40Gi", "36Gi")
		})
	}
}

func TestHostStorageUnavailablePreservesExistingWorker(t *testing.T) {
	cp, kubeClient, itp, _ := hostStorageProvider(t)
	hostDisk(t, itp, "160Gi", "144Gi")
	np := topologyNodePool("default")
	seed(t, kubeClient, np)
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	claim, _ := registerHostStorageWorker(t, cp, kubeClient, cluster, np)
	itp.InvalidateEphemeralStorageReference()
	pod := diskPod("uses-existing-disk", "100Gi")
	requireAbsorbed(t, provisioningPass(t, cp, kubeClient, cluster, np, pod), pod, "known existing disk during reference outage")
	got, err := cp.Get(coreContext(), claim.Status.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	assertClaimDisk(t, got, "160Gi", "144Gi")
	list, err := cp.List(coreContext())
	if err != nil || len(list) != 1 {
		t.Fatalf("reference loss hid an existing worker: claims=%d error=%v", len(list), err)
	}
	assertClaimDisk(t, list[0], "160Gi", "144Gi")
	if err := cp.Delete(coreContext(), claim); err != nil {
		t.Fatalf("reference loss blocked worker deletion: %v", err)
	}
}

// daemonStoragePass uses core's DaemonSet overhead calculation, passing its
// pod template exactly as Provisioner does before scheduler construction.
func daemonStoragePass(t *testing.T, cp *cloudprovider.CloudProvider, kubeClient client.Client, cluster *state.Cluster, np *karpv1.NodePool, ds *appsv1.DaemonSet, pod *corev1.Pod) coresched.Results {
	t.Helper()
	ctx := coreContext()
	types, err := cp.GetInstanceTypes(ctx, np)
	if err != nil {
		t.Fatal(err)
	}
	instanceTypes := map[string][]*corecloudprovider.InstanceType{np.Name: types}
	nodePools := []*karpv1.NodePool{np}
	nodes := cluster.DeepCopyNodes().Active()
	topology, err := coresched.NewTopology(ctx, kubeClient, cluster, nodes, nodePools, instanceTypes, []*corev1.Pod{pod})
	if err != nil {
		t.Fatal(err)
	}
	scheduler := coresched.NewScheduler(ctx, kubeClient, nodePools, cluster, nodes, topology, instanceTypes,
		[]*corev1.Pod{daemonset.PodForDaemonSet(ds)}, noopRecorder{}, clock.RealClock{}, nil, nil,
		coresched.DisableReservedCapacityFallback, coresched.MinValuesPolicy(options.MinValuesPolicyStrict))
	results, err := scheduler.Solve(ctx, []*corev1.Pod{pod})
	if err != nil {
		t.Fatal(err)
	}
	return results
}

func TestHostStorageUnavailableAccountsForDaemonSetRequests(t *testing.T) {
	cp, kubeClient, itp, operator := hostStorageProvider(t)
	np := topologyNodePool("default")
	seed(t, kubeClient, np)
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	pod := pendingPod("app-without-disk-request")
	daemonPod := diskPod("daemon", "256Mi")
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "disk-agent", Namespace: "default", UID: "uid-disk-agent"},
		Spec:       appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: daemonPod.Spec}},
	}
	results := daemonStoragePass(t, cp, kubeClient, cluster, np, ds, pod)
	if len(results.NewNodeClaims) != 0 || results.PodErrors[pod] == nil {
		t.Fatalf("DaemonSet disk request must block an otherwise plain pod: new=%d error=%v", len(results.NewNodeClaims), results.PodErrors[pod])
	}
	hostDisk(t, itp, "40Gi", "36Gi")
	results = daemonStoragePass(t, cp, kubeClient, cluster, np, ds, pod)
	if len(results.NewNodeClaims) != 1 || results.PodErrors[pod] != nil {
		t.Fatalf("known disk must fit the DaemonSet and app: new=%d error=%v", len(results.NewNodeClaims), results.PodErrors[pod])
	}
	claim := results.NewNodeClaims[0].ToNodeClaim()
	if requested := claim.Spec.Resources.Requests[corev1.ResourceEphemeralStorage]; requested.Cmp(resource.MustParse("256Mi")) != 0 {
		t.Fatalf("scheduler omitted DaemonSet disk overhead: %s", requested.String())
	}
	claim.Name, claim.UID = "default-daemon-disk", "uid-default-daemon-disk"
	stored(t, kubeClient, claim)
	itp.InvalidateEphemeralStorageReference()
	if _, err := cp.Create(coreContext(), claim); err == nil || corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("lost reference must hold the aggregate disk request for retry: %T %v", err, err)
	}
	if operator.createCount() != 0 {
		t.Fatalf("DaemonSet disk launch issued %d POSTs without a reference", operator.createCount())
	}
	hostDisk(t, itp, "40Gi", "36Gi")
	if _, err := cp.Create(coreContext(), claim); err != nil {
		t.Fatalf("aggregate request did not recover: %v", err)
	}
}

func TestHostStorageUnavailablePreservesNodePoolDiskLimit(t *testing.T) {
	cp, kubeClient, itp, operator := hostStorageProvider(t)
	limited := topologyNodePool("limited")
	limited.Spec.Limits = karpv1.Limits{corev1.ResourceEphemeralStorage: resource.MustParse("200Gi")}
	plain := topologyNodePool("default")
	seed(t, kubeClient, limited, plain)
	if _, err := cp.GetInstanceTypes(coreContext(), limited); err == nil {
		t.Fatal("an unknown per-worker disk must not bypass the NodePool's total disk limit")
	}
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	pod := pendingPod("without-local-request")
	results := provisioningPass(t, cp, kubeClient, cluster, plain, pod)
	if len(results.NewNodeClaims) != 1 || results.PodErrors[pod] != nil {
		t.Fatalf("unlimited NodePool must remain schedulable: new=%d error=%v", len(results.NewNodeClaims), results.PodErrors[pod])
	}
	hostDisk(t, itp, "40Gi", "36Gi")
	results = provisioningPass(t, cp, kubeClient, cluster, limited, pod)
	if len(results.NewNodeClaims) != 1 || results.PodErrors[pod] != nil {
		t.Fatalf("measured disk must permit the limited NodePool: new=%d error=%v", len(results.NewNodeClaims), results.PodErrors[pod])
	}
	claim := results.NewNodeClaims[0].ToNodeClaim()
	claim.Name, claim.UID = "limited-planned-without-disk-request", "uid-limited-planned-without-disk-request"
	if disk := claim.Spec.Resources.Requests[corev1.ResourceEphemeralStorage]; !disk.IsZero() {
		t.Fatalf("harness claim unexpectedly requests disk: %s", disk.String())
	}
	stored(t, kubeClient, claim)
	itp.InvalidateEphemeralStorageReference()
	if _, err := cp.Create(coreContext(), claim); err == nil || corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("launch must recheck the NodePool disk budget after invalidation: %T %v", err, err)
	}
	if operator.createCount() != 0 {
		t.Fatalf("unknown worker disk bypassed the NodePool limit with %d POSTs", operator.createCount())
	}
	hostDisk(t, itp, "40Gi", "36Gi")
	if _, err := cp.Create(coreContext(), claim); err != nil {
		t.Fatalf("limited NodePool launch did not recover: %v", err)
	}
}

func TestHostStorageIncreaseIgnoresOlderWorkerDisk(t *testing.T) {
	cp, kubeClient, itp, _ := hostStorageProvider(t)
	hostDisk(t, itp, "40Gi", "36Gi")
	np := topologyNodePool("default")
	seed(t, kubeClient, np)
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	_, oldNode := registerHostStorageWorker(t, cp, kubeClient, cluster, np)
	capacityController := instancetypecapacity.NewController(kubeClient, itp)
	observe := func() {
		t.Helper()
		if _, err := capacityController.Reconcile(coreContext(), reconcile.Request{NamespacedName: types.NamespacedName{Name: oldNode}}); err != nil {
			t.Fatal(err)
		}
	}
	// A previously accepted observation and a later report from the old
	// generation must both leave the updated new-launch profile intact.
	observe()
	hostDisk(t, itp, "160Gi", "144Gi")
	observe()
	small := diskPod("small-on-old-disk", "1Gi")
	requireAbsorbed(t, provisioningPass(t, cp, kubeClient, cluster, np, small), small, "old worker remains usable")
	large := diskPod("large-on-new-disk", "100Gi")
	results := provisioningPass(t, cp, kubeClient, cluster, np, large)
	if len(results.NewNodeClaims) != 1 || results.PodErrors[large] != nil {
		t.Fatalf("100Gi pod needs a new worker: claims=%d error=%v", len(results.NewNodeClaims), results.PodErrors[large])
	}
	for _, existing := range results.ExistingNodes {
		if len(existing.Pods) != 0 {
			t.Fatalf("100Gi pod was assigned to an old 40Gi worker: %v", results.ExistingNodeToPodMapping())
		}
	}
	launched := launchPlanned(t, cp, kubeClient, results.NewNodeClaims[0], "default-new-disk")
	if capacity, allocatable := launched.Status.Capacity[corev1.ResourceEphemeralStorage], launched.Status.Allocatable[corev1.ResourceEphemeralStorage]; capacity.Cmp(resource.MustParse("160Gi")) != 0 || allocatable.Cmp(resource.MustParse("144Gi")) != 0 {
		t.Fatalf("launched disk capacity=%s allocatable=%s, want 160Gi/144Gi", capacity.String(), allocatable.String())
	}
	cluster.UpdateNodeClaim(launched)
	inflight := provisioningPass(t, cp, kubeClient, cluster, np, large)
	if len(inflight.NewNodeClaims) != 0 || inflight.PodErrors[large] != nil || len(inflight.ExistingNodeToPodMapping()[launched.Name]) != 1 {
		t.Fatalf("100Gi pod must reuse the large in-flight claim: claims=%d placement=%v error=%v", len(inflight.NewNodeClaims), inflight.ExistingNodeToPodMapping(), inflight.PodErrors[large])
	}
}

func TestHostStorageDecreasePreservesExistingWorker(t *testing.T) {
	cp, kubeClient, itp, _ := hostStorageProvider(t)
	hostDisk(t, itp, "160Gi", "144Gi")
	np := topologyNodePool("default")
	seed(t, kubeClient, np)
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	claim, oldNode := registerHostStorageWorker(t, cp, kubeClient, cluster, np)
	hostDisk(t, itp, "40Gi", "36Gi")
	if _, err := instancetypecapacity.NewController(kubeClient, itp).Reconcile(coreContext(), reconcile.Request{NamespacedName: types.NamespacedName{Name: oldNode}}); err != nil {
		t.Fatal(err)
	}
	large := diskPod("large-after-decrease", "100Gi")
	emptyCluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	results := provisioningPass(t, cp, kubeClient, emptyCluster, np, large)
	if len(results.NewNodeClaims) != 0 || results.PodErrors[large] == nil {
		t.Fatalf("new 40Gi workers cannot satisfy 100Gi: claims=%d error=%v", len(results.NewNodeClaims), results.PodErrors[large])
	}
	requireAbsorbed(t, provisioningPass(t, cp, kubeClient, cluster, np, large), large, "existing 160Gi worker after decrease")
	claim.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	if _, err := disruption.NewController(clock.RealClock{}, kubeClient, cp).Reconcile(coreContext(), claim); err != nil {
		t.Fatalf("old worker drift reconcile: %v", err)
	}
	if cond := claim.StatusConditions().Get(karpv1.ConditionTypeDrifted); cond.IsTrue() {
		t.Fatalf("disk decrease drifted an existing worker: %s", cond.Reason)
	}
}

func TestHostStorageDecreaseRecheckedBeforeCreate(t *testing.T) {
	cp, kubeClient, itp, operator := hostStorageProvider(t)
	hostDisk(t, itp, "160Gi", "144Gi")
	np := topologyNodePool("default")
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	pod := diskPod("planned-before-decrease", "100Gi")
	results := provisioningPass(t, cp, kubeClient, cluster, np, pod)
	if len(results.NewNodeClaims) != 1 || results.PodErrors[pod] != nil {
		t.Fatalf("expected a claim before decrease: claims=%d error=%v", len(results.NewNodeClaims), results.PodErrors[pod])
	}
	claim := results.NewNodeClaims[0].ToNodeClaim()
	claim.Name = "default-planned-before-decrease"
	claim.UID = types.UID("uid-" + claim.Name)
	stored(t, kubeClient, claim)
	hostDisk(t, itp, "40Gi", "36Gi")
	if _, err := cp.Create(coreContext(), claim); !corecloudprovider.IsInsufficientCapacityError(err) {
		t.Fatalf("Create must reject the obsolete capacity plan for replanning, got %T: %v", err, err)
	}
	if got := operator.createCount(); got != 0 {
		t.Fatalf("created %d workers too small for the planned pod", got)
	}
}

func TestHostStorageDoesNotLimitRemotePersistentVolumes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		bound     bool
		reference string
	}{
		{name: "WaitForFirstConsumer/known disk", reference: "known"},
		{name: "bound CSI volume/known disk", bound: true, reference: "known"},
		{name: "WaitForFirstConsumer/startup", reference: "startup"},
		{name: "bound CSI volume/startup", bound: true, reference: "startup"},
		{name: "WaitForFirstConsumer/invalidated", reference: "invalidated"},
		{name: "bound CSI volume/invalidated", bound: true, reference: "invalidated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cp, kubeClient, itp, _ := hostStorageProvider(t)
			if tc.reference != "startup" {
				hostDisk(t, itp, "40Gi", "36Gi")
			}
			if tc.reference == "invalidated" {
				itp.InvalidateEphemeralStorageReference()
			}
			bindingMode := storagev1.VolumeBindingWaitForFirstConsumer
			sc := &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{Name: "remote"}, Provisioner: "csi.example.com", VolumeBindingMode: &bindingMode,
				AllowedTopologies: []corev1.TopologySelectorTerm{{MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{
					{Key: corev1.LabelTopologyRegion, Values: []string{"par"}},
				}}},
			}
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: &sc.Name,
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("200Gi")}},
				},
			}
			if tc.bound {
				pv := &corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{Name: "remote-data"},
					Spec: corev1.PersistentVolumeSpec{
						Capacity:    corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("200Gi")},
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: sc.Name,
						PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: sc.Provisioner, VolumeHandle: "vol-200gi"}},
						NodeAffinity:           &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{inRegion}}}}},
					},
				}
				pvc.Spec.VolumeName = pv.Name
				pvc.Status.Phase = corev1.ClaimBound
				seed(t, kubeClient, pv)
			}
			seed(t, kubeClient, sc, pvc)
			np := topologyNodePool("default")
			seed(t, kubeClient, np)
			cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
			pod := pendingPod("remote-data")
			if tc.reference == "known" {
				pod = diskPod("remote-data", "1Gi")
			}
			withClaim(pod, pvc.Name)
			results := provisioningPass(t, cp, kubeClient, cluster, np, pod)
			if len(results.NewNodeClaims) != 1 || results.PodErrors[pod] != nil {
				t.Fatalf("a 200Gi remote volume must not require 200Gi local storage: claims=%d error=%v", len(results.NewNodeClaims), results.PodErrors[pod])
			}
			claim := launchPlanned(t, cp, kubeClient, results.NewNodeClaims[0], "default-remote-volume")
			if tc.reference == "known" {
				assertClaimDisk(t, claim, "40Gi", "36Gi")
			} else {
				assertClaimDisk(t, claim, "0", "0")
			}
			cluster.UpdateNodeClaim(claim)
			requireAbsorbed(t, provisioningPass(t, cp, kubeClient, cluster, np, pod), pod, "remote volume on a smaller in-flight local disk")
		})
	}
}
