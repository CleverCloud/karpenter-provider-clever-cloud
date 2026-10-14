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
	"sigs.k8s.io/karpenter/pkg/controllers/state"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/instancetypecapacity"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// hostStorageProvider uses the same core scheduler/client harness as the
// topology tests, with the production requirement that disk capacity be
// learned before provisioning. The fake operator counts every NodeGroup POST.
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

func TestHostStorageReferenceGatesProvisioning(t *testing.T) {
	cp, kubeClient, itp, operator := hostStorageProvider(t)
	ctx := coreContext()
	if types, err := cp.GetInstanceTypes(ctx, topologyNodePool("default")); err == nil || len(types) != 0 {
		t.Fatalf("unmeasured host disk must not expose a schedulable catalogue: types=%d error=%v", len(types), err)
	}
	// Even a claim with no ephemeral-storage request must wait: allowing the
	// fallback seed here would silently promise an unmeasured disk at startup.
	claim := stored(t, kubeClient, claimFor("default-before-measurement", "128Mi", "2XS"))
	if _, err := cp.Create(ctx, claim); err == nil || corecloudprovider.IsInsufficientCapacityError(err) || corecloudprovider.IsNodeClassNotReadyError(err) {
		t.Fatalf("Create must return a retriable observation error, got %T: %v", err, err)
	}
	if got := operator.createCount(); got != 0 {
		t.Fatalf("created %d NodeGroups before the host was measured", got)
	}
	hostDisk(t, itp, "40Gi", "36Gi")
	if _, err := cp.GetInstanceTypes(ctx, topologyNodePool("default")); err != nil {
		t.Fatalf("catalogue remained blocked after measurement: %v", err)
	}
	created, err := cp.Create(ctx, claim)
	if err != nil {
		t.Fatalf("retrying the same claim after measurement: %v", err)
	}
	if got := operator.createCount(); got != 1 {
		t.Fatalf("retry created %d NodeGroups, want exactly one", got)
	}
	// Losing the source suspends new scheduling, but must not block the
	// reads and deletion core needs to clean up an already launched worker.
	itp.InvalidateEphemeralStorageReference()
	if _, err := cp.GetInstanceTypes(ctx, topologyNodePool("default")); err == nil {
		t.Fatal("invalidated reference still permits new scheduling")
	}
	if _, err := cp.Get(ctx, created.Status.ProviderID); err != nil {
		t.Fatalf("reference loss blocked Get of an existing worker: %v", err)
	}
	if claims, err := cp.List(ctx); err != nil || len(claims) != 1 {
		t.Fatalf("reference loss hid an existing worker from List: claims=%d error=%v", len(claims), err)
	}
	if err := cp.Delete(ctx, created); err != nil {
		t.Fatalf("reference loss blocked deletion of an existing worker: %v", err)
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
	for _, bound := range []bool{false, true} {
		name := "WaitForFirstConsumer"
		if bound {
			name = "bound CSI volume"
		}
		t.Run(name, func(t *testing.T) {
			cp, kubeClient, itp, _ := hostStorageProvider(t)
			hostDisk(t, itp, "40Gi", "36Gi")
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
			if bound {
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
			cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
			pod := diskPod("remote-data", "1Gi")
			withClaim(pod, pvc.Name)
			results := provisioningPass(t, cp, kubeClient, cluster, np, pod)
			if len(results.NewNodeClaims) != 1 || results.PodErrors[pod] != nil {
				t.Fatalf("a 200Gi remote volume must fit alongside 1Gi local storage: claims=%d error=%v", len(results.NewNodeClaims), results.PodErrors[pod])
			}
			claim := launchPlanned(t, cp, kubeClient, results.NewNodeClaims[0], "default-remote-volume")
			if disk := claim.Status.Capacity[corev1.ResourceEphemeralStorage]; disk.Cmp(resource.MustParse("40Gi")) != 0 {
				t.Fatalf("remote PVC changed local capacity: %s", disk.String())
			}
			cluster.UpdateNodeClaim(claim)
			requireAbsorbed(t, provisioningPass(t, cp, kubeClient, cluster, np, pod), pod, "remote volume on a smaller in-flight local disk")
		})
	}
}
