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

// These tests run karpenter-core's own requirement checks over this provider's
// catalogue and Create: its scheduler (a new claim, then the same claim in
// flight and registered), its volume topology, and its nodeclaim drift
// controller. The checks differ in how they treat a well-known label an
// instance type does not declare: a new claim tolerates it, an in-flight or
// existing node does not, and drift compares the labels a claim was launched
// with. Reproduced live: a single pod with nodeSelector
// topology.kubernetes.io/region=par launched three VMs in a minute and was
// never scheduled, because the region was not declared.

import (
	"context"
	"fmt"
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
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	coresched "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
)

// inRegion is what a pod, a volume or a NodePool asks for to stay in the
// cluster's region (the provider is configured with "par").
var inRegion = corev1.NodeSelectorRequirement{
	Key: corev1.LabelTopologyRegion, Operator: corev1.NodeSelectorOpIn, Values: []string{"par"},
}

// coreContext carries karpenter-core's options at their defaults, as its
// controllers read them.
func coreContext() context.Context {
	return options.ToContext(injection.WithControllerName(context.Background(), "provisioner"), &options.Options{
		PreferencePolicy:  options.PreferencePolicyRespect,
		MinValuesPolicy:   options.MinValuesPolicyStrict,
		IgnoreDRARequests: true,
		FeatureGates:      options.DefaultFeatureGates(),
	})
}

// topologyNodePool is a NodePool over the "default" CleverNodeClass with the
// given template requirements. Consolidation is off so the drift controller
// only runs drift.
func topologyNodePool(name string, requirements ...corev1.NodeSelectorRequirement) *karpv1.NodePool {
	np := &karpv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)},
		Spec: karpv1.NodePoolSpec{
			Template: karpv1.NodeClaimTemplate{Spec: karpv1.NodeClaimTemplateSpec{
				NodeClassRef: &karpv1.NodeClassReference{Group: "karpenter.clever-cloud.com", Kind: "CleverNodeClass", Name: "default"},
			}},
			Disruption: karpv1.Disruption{ConsolidateAfter: karpv1.MustParseNillableDuration("Never")},
		},
	}
	for _, r := range requirements {
		np.Spec.Template.Spec.Requirements = append(np.Spec.Template.Spec.Requirements,
			karpv1.NodeSelectorRequirementWithMinValues{Key: r.Key, Operator: r.Operator, Values: r.Values})
	}
	return np
}

// pendingPod is an unschedulable pod requesting a sliver of any flavor.
func pendingPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name)},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  "app",
			Image: "app",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			}},
		}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
			}},
		},
	}
}

// withClaim mounts a PersistentVolumeClaim into the pod.
func withClaim(pod *corev1.Pod, claim string) {
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name:         claim,
		VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}},
	})
}

// provisioningPass runs one pass of karpenter-core's provisioner over the pod:
// the provider's catalogue, core's volume topology, and core's scheduler over
// the active nodes of its cluster state — the inputs Provisioner.Schedule
// builds, minus the listing of pending pods.
func provisioningPass(t *testing.T, cp *cloudprovider.CloudProvider, kubeClient client.Client, cluster *state.Cluster, np *karpv1.NodePool, pod *corev1.Pod) coresched.Results {
	t.Helper()
	ctx := coreContext()
	kubeClient = asAPIServer(t, kubeClient)
	its, err := cp.GetInstanceTypes(ctx, np)
	if err != nil {
		t.Fatalf("GetInstanceTypes: %v", err)
	}
	instanceTypes := map[string][]*corecloudprovider.InstanceType{np.Name: its}
	pods := []*corev1.Pod{pod}
	volumeRequirements := map[types.UID][]scheduling.Requirements{}
	reqs, err := coresched.NewVolumeTopology(kubeClient).GetRequirements(ctx, pod)
	if err != nil {
		t.Fatalf("volume topology: %v", err)
	}
	if len(reqs) > 0 {
		volumeRequirements[pod.UID] = reqs
	}
	stateNodes := cluster.DeepCopyNodes().Active()
	nodePools := []*karpv1.NodePool{np}
	topology, err := coresched.NewTopology(ctx, kubeClient, cluster, stateNodes, nodePools, instanceTypes, pods)
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	scheduler := coresched.NewScheduler(ctx, kubeClient, nodePools, cluster, stateNodes, topology, instanceTypes, nil,
		noopRecorder{}, clock.RealClock{}, volumeRequirements, nil,
		coresched.DisableReservedCapacityFallback, coresched.MinValuesPolicy(options.MinValuesPolicyStrict))
	results, err := scheduler.Solve(ctx, pods)
	if err != nil {
		t.Fatalf("Solve: %v", err)
	}
	return results
}

// asAPIServer makes the fake client read PersistentVolumes the way the API
// server does. Core reads one both with the pod's namespace (volume topology)
// and without (volume limits); the API server ignores the namespace of a
// cluster-scoped kind, the fake client does not.
func asAPIServer(t *testing.T, kubeClient client.Client) client.Client {
	t.Helper()
	withWatch, ok := kubeClient.(client.WithWatch)
	if !ok {
		t.Fatalf("harness: %T is not the fake client", kubeClient)
	}
	return interceptor.NewClient(withWatch, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, clusterScoped := obj.(*corev1.PersistentVolume); clusterScoped {
				key.Namespace = ""
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
}

// launchPlanned does what core's launch controller does with a NodeClaim the
// scheduler planned: persists it, calls Create (the Clever Cloud operator
// accepting the group), merges the returned claim into it with core's own
// PopulateNodeClaimDetails, and marks it Launched.
func launchPlanned(t *testing.T, cp *cloudprovider.CloudProvider, kubeClient client.Client, planned *coresched.NodeClaim, name string) *karpv1.NodeClaim {
	t.Helper()
	ctx := context.Background()
	nodeClaim := planned.ToNodeClaim()
	nodeClaim.Name = name
	nodeClaim.UID = types.UID("uid-" + name)
	if err := kubeClient.Create(ctx, nodeClaim); err != nil {
		t.Fatalf("creating nodeclaim: %v", err)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop: // Create gave up before creating the group
				return
			default:
			}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: name}, &ngv1.NodeGroup{}); err == nil {
				markSynced(t, kubeClient, name)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	created, err := cp.Create(ctx, nodeClaim)
	close(stop)
	<-done
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	nodeClaim = lifecycle.PopulateNodeClaimDetails(nodeClaim, created)
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeLaunched)
	if err := kubeClient.Update(ctx, nodeClaim); err != nil {
		t.Fatalf("recording the launch on the nodeclaim: %v", err)
	}
	return nodeClaim
}

// requireAbsorbed fails unless the pass placed the pod on the one node the
// cluster state holds instead of planning another NodeClaim for it.
func requireAbsorbed(t *testing.T, results coresched.Results, pod *corev1.Pod, stage string) {
	t.Helper()
	if n := len(results.NewNodeClaims); n != 0 {
		t.Fatalf("%s: karpenter-core planned %d more NodeClaim(s) for a pod the node launched for it can run "+
			"(one more billed VM per provisioning pass)", stage, n)
	}
	if err := results.PodErrors[pod]; err != nil {
		t.Fatalf("%s: pod left unschedulable: %v", stage, err)
	}
	if len(results.ExistingNodes) != 1 || len(results.ExistingNodes[0].Pods) != 1 {
		t.Fatalf("%s: pod not placed on the node launched for it: %v", stage, results.ExistingNodeToPodMapping())
	}
}

// TestRegionConstrainedPodIsAbsorbedByTheNodeLaunchedForIt drives every way a
// region requirement reaches the scheduler — a pod nodeSelector, a NodePool
// requirement, a PersistentVolume's node affinity (written by a CSI driver, or
// carried by a static or restored volume), a StorageClass's allowedTopologies —
// through three provisioning passes: the pass that
// launches a node, then the same pod against that node in flight, then
// registered. Only the first pass tolerates an undeclared region: for a pod or
// volume requirement each later pass used to launch one more node. A NodePool
// requirement is not re-checked against existing nodes, so the pod was
// absorbed but the node drifted (TestRegionLabelDoesNotDriftExistingNodeClaims);
// here it pins that the node carries the region.
func TestRegionConstrainedPodIsAbsorbedByTheNodeLaunchedForIt(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup returns the NodePool and the pod for the scenario, after
		// seeding any volume objects the pod needs.
		setup func(t *testing.T, kubeClient client.Client) (*karpv1.NodePool, *corev1.Pod)
	}{
		{
			name: "pod nodeSelector",
			setup: func(t *testing.T, _ client.Client) (*karpv1.NodePool, *corev1.Pod) {
				pod := pendingPod("selector")
				pod.Spec.NodeSelector = map[string]string{corev1.LabelTopologyRegion: "par"}
				return topologyNodePool("default"), pod
			},
		},
		{
			name: "NodePool requirement",
			setup: func(t *testing.T, _ client.Client) (*karpv1.NodePool, *corev1.Pod) {
				return topologyNodePool("default", inRegion), pendingPod("plain")
			},
		},
		{
			name: "PersistentVolume node affinity",
			setup: func(t *testing.T, kubeClient client.Client) (*karpv1.NodePool, *corev1.Pod) {
				pv := &corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{Name: "regional-pv"},
					Spec: corev1.PersistentVolumeSpec{
						Capacity:    corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{Driver: "csi.example.com", VolumeHandle: "vol-1"},
						},
						NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{inRegion}}},
						}},
					},
				}
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						VolumeName:  pv.Name,
					},
					Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
				}
				seed(t, kubeClient, pv, pvc)
				pod := pendingPod("bound-volume")
				withClaim(pod, pvc.Name)
				return topologyNodePool("default"), pod
			},
		},
		{
			name: "StorageClass allowedTopologies",
			setup: func(t *testing.T, kubeClient client.Client) (*karpv1.NodePool, *corev1.Pod) {
				waitForFirstConsumer := storagev1.VolumeBindingWaitForFirstConsumer
				sc := &storagev1.StorageClass{
					ObjectMeta:        metav1.ObjectMeta{Name: "regional"},
					Provisioner:       "csi.example.com",
					VolumeBindingMode: &waitForFirstConsumer,
					AllowedTopologies: []corev1.TopologySelectorTerm{{MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{
						{Key: corev1.LabelTopologyRegion, Values: []string{"par"}},
					}}},
				}
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						StorageClassName: &sc.Name,
					},
				}
				seed(t, kubeClient, sc, pvc)
				pod := pendingPod("unbound-volume")
				withClaim(pod, pvc.Name)
				return topologyNodePool("default"), pod
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cp, kubeClient := newClusterStateProvider(t)
			np, pod := tc.setup(t, kubeClient)
			cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)

			results := provisioningPass(t, cp, kubeClient, cluster, np, pod)
			if len(results.NewNodeClaims) != 1 || results.PodErrors[pod] != nil {
				t.Fatalf("first pass: want one new NodeClaim for the pod, got %d (pod error: %v)",
					len(results.NewNodeClaims), results.PodErrors[pod])
			}
			nodeClaim := launchPlanned(t, cp, kubeClient, results.NewNodeClaims[0], "default-t0p0l")
			if got := nodeClaim.Labels[corev1.LabelTopologyRegion]; got != "par" {
				t.Errorf("launched NodeClaim carries %s=%q, want par: core's registration sync is the only "+
					"way the label reaches the node", corev1.LabelTopologyRegion, got)
			}

			// In flight: core knows the machine only through its NodeClaim.
			cluster.UpdateNodeClaim(nodeClaim)
			requireAbsorbed(t, provisioningPass(t, cp, kubeClient, cluster, np, pod), pod, "in-flight node")

			// Registered: the node joins as the platform registers it, then
			// core's registration sync copies the NodeClaim's labels onto it.
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClaim.Name}, ng); err != nil {
				t.Fatalf("getting nodegroup: %v", err)
			}
			node := joinNode(t, kubeClient, ng, "0")
			stampNode(t, kubeClient, node.Name)
			registerNode(t, kubeClient, nodeClaim, node.Name)
			reportCapacity(t, kubeClient, node.Name, nodeClaim)
			observeNode(t, kubeClient, cluster, node.Name)
			cluster.UpdateNodeClaim(nodeClaim)
			for n := range cluster.Nodes() {
				if !n.Registered() || n.Labels()[corev1.LabelTopologyRegion] != "par" {
					t.Fatalf("the state node must be the registered node, carrying the region: registered=%v labels=%v",
						n.Registered(), n.Labels())
				}
			}
			requireAbsorbed(t, provisioningPass(t, cp, kubeClient, cluster, np, pod), pod, "registered node")
		})
	}
}

// seed creates objects in the fake API server.
func seed(t *testing.T, kubeClient client.Client, objs ...client.Object) {
	t.Helper()
	for _, obj := range objs {
		if err := kubeClient.Create(context.Background(), obj); err != nil {
			t.Fatalf("creating %T %s: %v", obj, obj.GetName(), err)
		}
	}
}

// reportCapacity plays the kubelet: the node reports what its NodeClaim
// promised.
func reportCapacity(t *testing.T, kubeClient client.Client, nodeName string, nodeClaim *karpv1.NodeClaim) {
	t.Helper()
	node := &corev1.Node{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeName}, node); err != nil {
		t.Fatalf("getting node: %v", err)
	}
	node.Status.Capacity = nodeClaim.Status.Capacity
	node.Status.Allocatable = nodeClaim.Status.Allocatable
	if err := kubeClient.Status().Update(context.Background(), node); err != nil {
		t.Fatalf("reporting node capacity: %v", err)
	}
}

// observeNode runs core's node informer on a node, as its watch would.
func observeNode(t *testing.T, kubeClient client.Client, cluster *state.Cluster, nodeName string) {
	t.Helper()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: nodeName}}
	if _, err := informer.NewNodeController(kubeClient, cluster).Reconcile(context.Background(), req); err != nil {
		t.Fatalf("state node reconcile: %v", err)
	}
}

// TestRegionLabelDoesNotDriftExistingNodeClaims runs karpenter-core's nodeclaim
// drift controller over claims launched before and after the region label was
// declared. Claims launched by an earlier release lack the label, and nothing
// ever adds it to them (core copies the provider's labels onto a claim at
// launch only), so the upgrade must not read that absence as drift: that
// would replace every node of the fleet. The claims are two hours old so core
// also runs its InstanceTypeNotFound check, which only starts after an hour.
func TestRegionLabelDoesNotDriftExistingNodeClaims(t *testing.T) {
	cp, kubeClient := newClusterStateProvider(t)
	plain := topologyNodePool("plain")
	regional := topologyNodePool("regional", inRegion)
	seed(t, kubeClient, plain, regional)

	// A claim launched through the current Create for each NodePool: its
	// labels are exactly what an earlier release produced, plus the region.
	launched := map[string]*karpv1.NodeClaim{}
	for _, np := range []*karpv1.NodePool{plain, regional} {
		cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
		results := provisioningPass(t, cp, kubeClient, cluster, np, pendingPod("for-"+np.Name))
		if len(results.NewNodeClaims) != 1 {
			t.Fatalf("NodePool %s: want one planned NodeClaim, got %d", np.Name, len(results.NewNodeClaims))
		}
		launched[np.Name] = launchPlanned(t, cp, kubeClient, results.NewNodeClaims[0], np.Name+"-l4nch")
	}

	for i, tc := range []struct {
		name     string
		nodePool string
		// labels edits the launched claim's labels into the scenario's.
		labels func(map[string]string)
		want   corecloudprovider.DriftReason
	}{
		{
			name:     "claim from an earlier release, NodePool without a region requirement",
			nodePool: "plain",
			labels:   func(l map[string]string) { delete(l, corev1.LabelTopologyRegion) },
		},
		{
			name:     "claim from this release, NodePool without a region requirement",
			nodePool: "plain",
			labels:   func(map[string]string) {},
		},
		{
			// The drift loop a region-requiring NodePool used to run: its
			// nodes never carried the label it requires.
			name:     "claim from this release, NodePool requiring the region",
			nodePool: "regional",
			labels:   func(map[string]string) {},
		},
		{
			// Already the case before this release: such claims were launched
			// without the label their NodePool requires. They are replaced
			// once, by nodes that carry it and settle.
			name:     "claim from an earlier release, NodePool requiring the region",
			nodePool: "regional",
			labels:   func(l map[string]string) { delete(l, corev1.LabelTopologyRegion) },
			want:     disruption.RequirementsDrifted,
		},
		{
			// Harness check: the offering check ran and reads the region, so
			// the absence of drift above is not a check that never happened.
			name:     "claim in another region",
			nodePool: "plain",
			labels:   func(l map[string]string) { l[corev1.LabelTopologyRegion] = "elsewhere" },
			want:     disruption.InstanceTypeNotFound,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeClaim := launched[tc.nodePool].DeepCopy()
			nodeClaim.Name = fmt.Sprintf("%s-drift%d", tc.nodePool, i)
			nodeClaim.UID = types.UID("uid-" + nodeClaim.Name)
			nodeClaim.ResourceVersion = ""
			tc.labels(nodeClaim.Labels)
			seed(t, kubeClient, nodeClaim)
			nodeClaim.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))

			if _, err := disruption.NewController(clock.RealClock{}, kubeClient, cp).Reconcile(coreContext(), nodeClaim); err != nil {
				t.Fatalf("drift reconcile: %v", err)
			}
			got := corecloudprovider.DriftReason("")
			if cond := nodeClaim.StatusConditions().Get(karpv1.ConditionTypeDrifted); cond.IsTrue() {
				got = corecloudprovider.DriftReason(cond.Reason)
			}
			if got != tc.want {
				t.Errorf("drift = %q, want %q (labels %v)", got, tc.want, nodeClaim.Labels)
			}
		})
	}
}
