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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	coredisruption "sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/state/nodepoolhealth"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// A reference can disappear between scheduling and Create. Returning a plain
// error then keeps the NodeClaim without a provider ID, and core deliberately
// stops all provisioning while Cluster.Synced is false. Exercise its actual
// lifecycle, state informer and provisioner: the capacity failure must release
// that claim so a different pool can launch a pod without a disk request.
func TestHostStorageLostReferenceDoesNotFreezeCoreProvisioning(t *testing.T) {
	for _, reason := range []string{"positive disk request", "NodePool disk limit"} {
		t.Run(reason, func(t *testing.T) {
			ctx := coreContext()
			options.FromContext(ctx).CPURequests = 1000
			options.FromContext(ctx).BatchMaxDuration = time.Millisecond
			options.FromContext(ctx).BatchIdleDuration = time.Millisecond
			blockedPool, cpuPool := topologyNodePool("default"), topologyNodePool("cpu")
			for _, np := range []*karpv1.NodePool{blockedPool, cpuPool} {
				np.StatusConditions().SetTrue(karpv1.ConditionTypeValidationSucceeded)
				np.StatusConditions().SetTrue(karpv1.ConditionTypeNodeClassReady)
			}
			wanted := pendingPod("planned-before-reference-loss")
			if reason == "positive disk request" {
				wanted = diskPod(wanted.Name, "1Gi")
			} else {
				blockedPool.Spec.Limits = karpv1.Limits{corev1.ResourceEphemeralStorage: resource.MustParse("200Gi")}
			}
			base := newCoreIndexedClient(readyNodeClass("default"), blockedPool, cpuPool)
			operator := &fakeOperator{headroomGB: 100}
			kubeClient := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{Create: operator.create})
			itp := instancetype.NewProvider("par", nil, nil)
			itp.RequireEphemeralStorageReference()
			hostDisk(t, itp, "40Gi", "36Gi")
			cp := cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{}))
			cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
			if !cluster.Synced(ctx) {
				t.Fatal("empty cluster should start synced")
			}
			planned := provisioningPass(t, cp, kubeClient, cluster, blockedPool, wanted)
			if len(planned.NewNodeClaims) != 1 {
				t.Fatalf("expected a plan while the reference was valid, got %d", len(planned.NewNodeClaims))
			}
			claim := planned.NewNodeClaims[0].ToNodeClaim()
			claim.Name, claim.UID = "default-stale-disk-plan", "uid-default-stale-disk-plan"
			claim.StatusConditions().SetUnknownWithReason(karpv1.ConditionTypeLaunched, "LaunchPending", "Waiting to launch")
			stored(t, kubeClient, claim)
			cluster.UpdateNodeClaim(claim)
			itp.InvalidateEphemeralStorageReference()
			plain := pendingPod("cpu-during-reference-outage")
			plain.Spec.NodeSelector = map[string]string{karpv1.NodePoolLabelKey: cpuPool.Name}
			seed(t, kubeClient, plain)
			provisioner := provisioning.NewProvisioner(kubeClient, noopRecorder{}, cp, cluster, clock.RealClock{}, nil, nil)
			provisioner.Trigger(plain.UID)
			if _, err := provisioner.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			if cluster.Synced(ctx) {
				t.Fatal("harness did not reproduce the unlaunched-claim sync gate")
			}
			claims := &karpv1.NodeClaimList{}
			if err := kubeClient.List(ctx, claims); err != nil || len(claims.Items) != 1 {
				t.Fatalf("provisioner bypassed the sync gate: claims=%d error=%v", len(claims.Items), err)
			}
			controller := lifecycle.NewController(clock.RealClock{}, kubeClient, cp, noopRecorder{}, cluster, nodepoolhealth.NewState(), nil)
			if _, err := controller.Reconcile(ctx, claim); err != nil {
				t.Fatalf("core must handle the known capacity failure without retrying it: %v", err)
			}
			if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), claim); err != nil {
				t.Fatal(err)
			}
			if claim.DeletionTimestamp.IsZero() || claim.Status.ProviderID != "" {
				t.Fatalf("failed launch must be deleting without a machine: deletion=%v providerID=%q", claim.DeletionTimestamp, claim.Status.ProviderID)
			}
			if operator.createCount() != 0 {
				t.Fatalf("failed launch reached the NodeGroup API %d times", operator.createCount())
			}
			if _, err := controller.Reconcile(ctx, claim); err != nil {
				t.Fatalf("finalizing the failed launch: %v", err)
			}
			if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), &karpv1.NodeClaim{}); !apierrors.IsNotFound(err) {
				t.Fatalf("core did not release the failed claim: %v", err)
			}
			stateInformer := informer.NewNodeClaimController(kubeClient, cp, cluster, cost.NewClusterCost(ctx, cp, kubeClient))
			if _, err := stateInformer.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(claim)}); err != nil {
				t.Fatal(err)
			}
			if !cluster.Synced(ctx) {
				t.Fatal("failed launch left core unsynced")
			}
			provisioner.Trigger(plain.UID)
			if _, err := provisioner.Reconcile(ctx); err != nil {
				t.Fatalf("CPU provisioning did not resume during the reference outage: %v", err)
			}
			if err := kubeClient.List(ctx, claims); err != nil || len(claims.Items) != 1 {
				t.Fatalf("expected one CPU claim after sync recovered: claims=%d error=%v", len(claims.Items), err)
			}
			cpuClaim := &claims.Items[0]
			if cpuClaim.Labels[karpv1.NodePoolLabelKey] != cpuPool.Name {
				t.Fatalf("provisioner selected the wrong pool: %v", cpuClaim.Labels)
			}
			if _, err := controller.Reconcile(ctx, cpuClaim); err != nil {
				t.Fatalf("CPU claim failed to launch while disk reference remained absent: %v", err)
			}
			if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(cpuClaim), cpuClaim); err != nil {
				t.Fatal(err)
			}
			if cpuClaim.Status.ProviderID == "" || operator.createCount() != 1 {
				t.Fatalf("CPU launch missing: providerID=%q POSTs=%d", cpuClaim.Status.ProviderID, operator.createCount())
			}
			assertClaimDisk(t, cpuClaim, "0", "0")
		})
	}
}

func TestHostStorageUnavailableKeepsLimitedPoolDisruptable(t *testing.T) {
	cp, kubeClient, itp, _ := hostStorageProvider(t)
	hostDisk(t, itp, "40Gi", "36Gi")
	np := topologyNodePool("default")
	np.Spec.Limits = karpv1.Limits{corev1.ResourceEphemeralStorage: resource.MustParse("200Gi")}
	seed(t, kubeClient, np)
	cluster := state.NewCluster(clock.RealClock{}, kubeClient, cp)
	claim, _ := registerHostStorageWorker(t, cp, kubeClient, cluster, np)
	itp.InvalidateEphemeralStorageReference()
	pools, typesByPool, err := coredisruption.BuildNodePoolMap(coreContext(), kubeClient, cp)
	if err != nil || len(typesByPool[np.Name]) != len(instancetype.DefaultFlavors) {
		t.Fatalf("limited pool disappeared from core's disruption map: types=%d error=%v", len(typesByPool[np.Name]), err)
	}
	nodes := cluster.DeepCopyNodes().Active()
	if len(nodes) != 1 {
		t.Fatalf("expected one registered worker, got %d", len(nodes))
	}
	queue := coredisruption.NewQueue(kubeClient, noopRecorder{}, cluster, clock.RealClock{}, nil)
	candidate, err := coredisruption.NewCandidate(coreContext(), kubeClient, noopRecorder{}, clock.RealClock{}, nodes[0], nil,
		pools, typesByPool, queue, coredisruption.GracefulDisruptionClass)
	if err != nil || !candidate.IsEmpty() {
		t.Fatalf("empty worker must remain a disruption candidate: %v", err)
	}
	claim.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	if _, err := disruption.NewController(clock.RealClock{}, kubeClient, cp).Reconcile(coreContext(), claim); err != nil {
		t.Fatal(err)
	}
	if cond := claim.StatusConditions().Get(karpv1.ConditionTypeDrifted); cond.IsTrue() {
		t.Fatalf("unavailable disk offerings falsely drifted a worker: %s", cond.Reason)
	}
}

func TestHostStorageNodePoolLimitUsesAPIReader(t *testing.T) {
	for _, scenario := range []string{"limit only in API", "limit only in cache", "forbidden", "not found"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := coreContext()
			_, kubeClient, itp, operator := hostStorageProvider(t)
			cachedPool, livePool := topologyNodePool("default"), topologyNodePool("default")
			if scenario == "limit only in cache" {
				cachedPool.Spec.Limits = karpv1.Limits{corev1.ResourceEphemeralStorage: resource.MustParse("200Gi")}
			}
			if scenario == "limit only in API" {
				livePool.Spec.Limits = karpv1.Limits{corev1.ResourceEphemeralStorage: resource.MustParse("200Gi")}
			}
			seed(t, kubeClient, cachedPool)
			live := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
			if scenario != "not found" {
				seed(t, live, livePool)
			}
			var readError error
			if scenario == "forbidden" {
				readError = apierrors.NewForbidden(schema.GroupResource{Group: "karpenter.sh", Resource: "nodepools"}, livePool.Name, errors.New("denied"))
			}
			reads := 0
			reader := interceptor.NewClient(kubeClient.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, nodePool := obj.(*karpv1.NodePool); nodePool {
					reads++
					if readError != nil {
						return readError
					}
					return live.Get(ctx, key, obj, opts...)
				}
				return c.Get(ctx, key, obj, opts...)
			}})
			cp := cloudprovider.New(kubeClient, reader, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{}))
			claim := stored(t, kubeClient, claimFor("default-api-budget", "128Mi", "2XS"))
			_, err := cp.Create(ctx, claim)
			if reads == 0 {
				t.Fatal("launch never checked the API server's NodePool")
			}
			switch scenario {
			case "limit only in API":
				if !corecloudprovider.IsInsufficientCapacityError(err) {
					t.Fatalf("API's limit must reject the stale cache's launch: %T %v", err, err)
				}
			case "limit only in cache":
				if err != nil || operator.createCount() != 1 {
					t.Fatalf("removed limit must permit the launch: POSTs=%d error=%v", operator.createCount(), err)
				}
				return
			default:
				var createError *corecloudprovider.CreateError
				if corecloudprovider.IsInsufficientCapacityError(err) || !errors.As(err, &createError) || createError.ConditionReason != "NodePoolReadFailed" {
					t.Fatalf("API read failure must remain a diagnosable retriable error: %T %v", err, err)
				}
				if scenario == "not found" && !apierrors.IsNotFound(err) || scenario == "forbidden" && !apierrors.IsForbidden(err) {
					t.Fatalf("underlying API error was lost: %v", err)
				}
			}
			if operator.createCount() != 0 {
				t.Fatalf("unchecked or exceeded budget caused %d POSTs", operator.createCount())
			}
			if scenario == "limit only in API" {
				return
			}
			// Neither API uncertainty nor the retry may create a replacement
			// object before the pool's real budget can be read again.
			readError = nil
			if scenario == "not found" {
				seed(t, live, livePool)
			}
			if _, err := cp.Create(ctx, claim); err != nil || operator.createCount() != 1 {
				t.Fatalf("retry after API recovery: POSTs=%d error=%v", operator.createCount(), err)
			}
		})
	}
}
