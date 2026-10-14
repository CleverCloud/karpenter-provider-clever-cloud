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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

const (
	snapshotCapacity    = "karpenter.clever-cloud.com/ephemeral-storage-capacity"
	snapshotAllocatable = "karpenter.clever-cloud.com/ephemeral-storage-allocatable"
)

func assertClaimDisk(t *testing.T, claim *karpv1.NodeClaim, capacity, allocatable string) {
	t.Helper()
	gotCapacity := claim.Status.Capacity[corev1.ResourceEphemeralStorage]
	gotAllocatable := claim.Status.Allocatable[corev1.ResourceEphemeralStorage]
	if gotCapacity.Cmp(resource.MustParse(capacity)) != 0 || gotAllocatable.Cmp(resource.MustParse(allocatable)) != 0 {
		t.Fatalf("claim %s disk=%s/%s, want %s/%s", claim.Name, gotCapacity.String(), gotAllocatable.String(), capacity, allocatable)
	}
}

// Retrying Create models the response/status write lost before Launched was
// persisted. The original group's disk survives both a reference change and a
// host outage; finishing this adoption must not create another VM.
func TestHostStorageAdoptionPreservesLaunchSnapshot(t *testing.T) {
	for _, tt := range []struct {
		name, capacity, allocatable, nextCapacity, nextAllocatable, request string
		invalidate                                                          bool
	}{
		{name: "increase", capacity: "40Gi", allocatable: "36Gi", nextCapacity: "160Gi", nextAllocatable: "144Gi", request: "1Gi"},
		{name: "decrease", capacity: "160Gi", allocatable: "144Gi", nextCapacity: "40Gi", nextAllocatable: "36Gi", request: "100Gi"},
		{name: "reference unavailable", capacity: "160Gi", allocatable: "144Gi", nextCapacity: "40Gi", nextAllocatable: "36Gi", request: "100Gi", invalidate: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := coreContext()
			cp, kubeClient, itp, operator := hostStorageProvider(t)
			hostDisk(t, itp, tt.capacity, tt.allocatable)
			wanted := testNodeClaim("default-snapshot")
			wanted.Spec.Resources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse(tt.request)
			stored(t, kubeClient, wanted)
			first, err := cp.Create(ctx, wanted)
			if err != nil {
				t.Fatal(err)
			}
			assertClaimDisk(t, first, tt.capacity, tt.allocatable)
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: wanted.Name}, ng); err != nil {
				t.Fatal(err)
			}
			for key, want := range map[string]string{snapshotCapacity: tt.capacity, snapshotAllocatable: tt.allocatable} {
				got, err := resource.ParseQuantity(ng.Annotations[key])
				if err != nil || got.Cmp(resource.MustParse(want)) != 0 {
					t.Fatalf("NodeGroup POST did not persist %s=%s: got %q, %v", key, want, ng.Annotations[key], err)
				}
			}
			hostDisk(t, itp, tt.nextCapacity, tt.nextAllocatable)
			if tt.invalidate {
				itp.InvalidateEphemeralStorageReference()
			}
			again, err := cp.Create(ctx, wanted)
			if err != nil {
				t.Fatalf("adopting existing launch: %v", err)
			}
			assertClaimDisk(t, again, tt.capacity, tt.allocatable)
			got, err := cp.Get(ctx, first.Status.ProviderID)
			if err != nil {
				t.Fatal(err)
			}
			assertClaimDisk(t, got, tt.capacity, tt.allocatable)
			list, err := cp.List(ctx)
			if err != nil || len(list) != 1 {
				t.Fatalf("List: %d claims, %v", len(list), err)
			}
			assertClaimDisk(t, list[0], tt.capacity, tt.allocatable)
			if count := operator.createCount(); count != 1 {
				t.Fatalf("adoption issued another POST: %d total", count)
			}
		})
	}
}

// An upgrade can find an accepted v0.14.0 group whose launch has not yet been
// persisted on its NodeClaim. No snapshot exists to recover its historical disk,
// but this must not turn a retry into another provisioning decision or POST.
func TestPreFeatureAdoptionSurvivesReferenceChangesAndQuota(t *testing.T) {
	for _, scenario := range []string{"smaller reference", "unavailable reference", "quota backoff"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := coreContext()
			cp, kubeClient, itp, operator := hostStorageProvider(t)
			hostDisk(t, itp, "160Gi", "144Gi")
			wanted := testNodeClaim("default-pre-feature-launch")
			wanted.Spec.Resources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse("100Gi")
			stored(t, kubeClient, wanted)
			first, err := cp.Create(ctx, wanted)
			if err != nil {
				t.Fatal(err)
			}
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: wanted.Name}, ng); err != nil {
				t.Fatal(err)
			}
			delete(ng.Annotations, snapshotCapacity)
			delete(ng.Annotations, snapshotAllocatable)
			ng.Status = ngv1.NodeGroupStatus{Phase: "Creating", Conditions: []ngv1.NodeGroupCondition{
				condTrue(ngv1.ConditionTypeReconcileInProgress, "Creating", ""),
			}}
			if err := kubeClient.Update(ctx, ng); err != nil {
				t.Fatal(err)
			}
			nodeGroups := nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{})
			switch scenario {
			case "smaller reference", "unavailable reference":
				hostDisk(t, itp, "40Gi", "36Gi")
				if scenario == "unavailable reference" {
					itp.InvalidateEphemeralStorageReference()
				}
			case "quota backoff":
				refused := managedNodeGroup("default-other-refused", ng.Spec.Flavor)
				refused.Status = ngv1.NodeGroupStatus{Phase: ngv1.PhaseQuotaExceeded, Conditions: []ngv1.NodeGroupCondition{
					condTrue(ngv1.ConditionTypeReconcileFailed, ngv1.ReasonQuotaExceeded, "quota exhausted"),
				}}
				nodeGroups.RecordLateRefusal(testNodeClaim(refused.Name), refused)
				if !nodeGroups.Unavailable(ng.Spec.Flavor) {
					t.Fatal("test did not arm quota backoff")
				}
			}
			cp = cloudprovider.New(kubeClient, kubeClient, itp, nodeGroups)
			// Without a historical snapshot, Get/List already use the catalogue.
			// Adoption preserves that behavior instead of inventing a past size.
			described, err := cp.Get(ctx, first.Status.ProviderID)
			if err != nil {
				t.Fatal(err)
			}
			capacity := described.Status.Capacity[corev1.ResourceEphemeralStorage]
			allocatable := described.Status.Allocatable[corev1.ResourceEphemeralStorage]
			adopted, err := cp.Create(ctx, wanted)
			if err != nil {
				t.Fatalf("pre-feature launch must be adopted without re-planning: %v", err)
			}
			assertClaimDisk(t, adopted, capacity.String(), allocatable.String())
			if adopted.Status.ProviderID != first.Status.ProviderID || operator.createCount() != 1 {
				t.Fatalf("adoption changed identity or attempted another POST: providerID=%s POSTs=%d", adopted.Status.ProviderID, operator.createCount())
			}
			storedGroup := &ngv1.NodeGroup{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: ng.Name}, storedGroup); err != nil {
				t.Fatal(err)
			}
			if storedGroup.ResourceVersion != ng.ResourceVersion {
				t.Fatal("adoption changed the existing group")
			}
			if _, exists := storedGroup.Annotations[snapshotCapacity]; exists {
				t.Fatal("adoption invented a historical disk snapshot")
			}
		})
	}
}

func TestPreFeatureAdoptionRequiresCurrentClaimOwnership(t *testing.T) {
	for _, scenario := range []string{"unmanaged", "old owner UID", "missing owner", "wrong claim label"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := coreContext()
			cp, kubeClient, itp, operator := hostStorageProvider(t)
			hostDisk(t, itp, "160Gi", "144Gi")
			claim := stored(t, kubeClient, testNodeClaim("default-pre-feature-identity"))
			if _, err := cp.Create(ctx, claim); err != nil {
				t.Fatal(err)
			}
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: claim.Name}, ng); err != nil {
				t.Fatal(err)
			}
			delete(ng.Annotations, snapshotCapacity)
			delete(ng.Annotations, snapshotAllocatable)
			switch scenario {
			case "unmanaged":
				delete(ng.Labels, v1alpha1.ManagedLabelKey)
			case "old owner UID":
				ng.OwnerReferences[0].UID = "older-claim-with-same-name"
			case "missing owner":
				ng.OwnerReferences = nil
			case "wrong claim label":
				ng.Labels[v1alpha1.NodeClaimLabelKey] = "another-claim"
			}
			if err := kubeClient.Update(ctx, ng); err != nil {
				t.Fatal(err)
			}
			_, err := cp.Create(ctx, claim)
			if err == nil || !strings.Contains(err.Error(), "not managed by this nodeclaim") || corecloudprovider.IsInsufficientCapacityError(err) {
				t.Fatalf("foreign pre-feature group must be refused with a plain identity error: %v", err)
			}
			if operator.createCount() != 1 {
				t.Fatalf("unsafe adoption attempted another POST: %d", operator.createCount())
			}
		})
	}
}

func TestHostStorageInvalidSnapshotRetriesCreateButKeepsGetList(t *testing.T) {
	for _, corrupt := range []string{"missing capacity", "invalid quantity", "fractional", "allocatable exceeds capacity", "zero capacity", "zero allocatable", "negative capacity", "negative allocatable", "both negative", "overflow"} {
		t.Run(corrupt, func(t *testing.T) {
			ctx := coreContext()
			cp, kubeClient, itp, operator := hostStorageProvider(t)
			hostDisk(t, itp, "160Gi", "144Gi")
			claim := stored(t, kubeClient, testNodeClaim("default-corrupt-snapshot"))
			first, err := cp.Create(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: claim.Name}, ng); err != nil {
				t.Fatal(err)
			}
			switch corrupt {
			case "missing capacity":
				delete(ng.Annotations, snapshotCapacity)
			case "invalid quantity":
				ng.Annotations[snapshotCapacity] = "invalid"
			case "fractional":
				ng.Annotations[snapshotAllocatable] = "1500m"
			case "allocatable exceeds capacity":
				ng.Annotations[snapshotAllocatable] = "200Gi"
			case "zero capacity":
				ng.Annotations[snapshotCapacity] = "0"
			case "zero allocatable":
				ng.Annotations[snapshotAllocatable] = "0"
			case "negative capacity":
				ng.Annotations[snapshotCapacity] = "-160Gi"
			case "negative allocatable":
				ng.Annotations[snapshotAllocatable] = "-144Gi"
			case "both negative":
				ng.Annotations[snapshotCapacity] = "-160Gi"
				ng.Annotations[snapshotAllocatable] = "-144Gi"
			case "overflow":
				ng.Annotations[snapshotCapacity] = "9223372036854775808"
			}
			if err := kubeClient.Update(ctx, ng); err != nil {
				t.Fatal(err)
			}
			hostDisk(t, itp, "40Gi", "36Gi")
			_, err = cp.Create(ctx, claim)
			if err == nil || corecloudprovider.IsInsufficientCapacityError(err) {
				t.Fatalf("corruption must return a retriable error, got %v", err)
			}
			if count := operator.createCount(); count != 1 {
				t.Fatalf("corruption issued another POST: %d", count)
			}
			got, err := cp.Get(ctx, first.Status.ProviderID)
			if err != nil {
				t.Fatalf("Get must preserve termination: %v", err)
			}
			assertClaimDisk(t, got, "40Gi", "36Gi")
			list, err := cp.List(ctx)
			if err != nil || len(list) != 1 {
				t.Fatalf("List must preserve GC: %d claims, %v", len(list), err)
			}
			assertClaimDisk(t, list[0], "40Gi", "36Gi")
		})
	}
}

func TestHostStorageUnknownSnapshotSurvivesRecoveryAndRestart(t *testing.T) {
	ctx := coreContext()
	cp, kubeClient, itp, operator := hostStorageProvider(t)
	seed(t, kubeClient, topologyNodePool("default"))
	wanted := stored(t, kubeClient, testNodeClaim("default-unknown-snapshot"))
	first, err := cp.Create(ctx, wanted)
	if err != nil {
		t.Fatalf("launching without an ephemeral-storage request: %v", err)
	}
	assertClaimDisk(t, first, "0", "0")
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: wanted.Name}, ng); err != nil {
		t.Fatal(err)
	}
	if ng.Annotations[snapshotCapacity] != "0" || ng.Annotations[snapshotAllocatable] != "0" {
		t.Fatalf("unknown disk must be explicit in the initial POST: %v", ng.Annotations)
	}
	assertUnknown := func(t *testing.T, provider *cloudprovider.CloudProvider) {
		t.Helper()
		again, err := provider.Create(ctx, wanted)
		if err != nil {
			t.Fatalf("adopting an unknown disk snapshot: %v", err)
		}
		assertClaimDisk(t, again, "0", "0")
		got, err := provider.Get(ctx, first.Status.ProviderID)
		if err != nil {
			t.Fatal(err)
		}
		assertClaimDisk(t, got, "0", "0")
		list, err := provider.List(ctx)
		if err != nil || len(list) != 1 {
			t.Fatalf("List: %d claims, %v", len(list), err)
		}
		assertClaimDisk(t, list[0], "0", "0")
	}
	// A newly valid reference only describes new launches. The in-flight
	// unknown group must not gain disk capacity it was never promised.
	hostDisk(t, itp, "160Gi", "144Gi")
	assertUnknown(t, cp)
	// An empty process-local state still finds the persisted zero/zero pair
	// and adopts it, rather than treating it as a pre-feature missing snapshot.
	restarted := instancetype.NewProvider("par", nil, nil)
	restarted.RequireEphemeralStorageReference()
	cp = cloudprovider.New(kubeClient, kubeClient, restarted, nodegroup.NewProvider(kubeClient, noopRecorder{}, restarted, clock.RealClock{}))
	assertUnknown(t, cp)
	hostDisk(t, restarted, "40Gi", "36Gi")
	assertUnknown(t, cp)
	if count := operator.createCount(); count != 1 {
		t.Fatalf("unknown snapshot adoption recreated a VM: %d POSTs", count)
	}
}

func TestHostStorageSnapshotSurvivesUnknownFlavorFallback(t *testing.T) {
	ctx := coreContext()
	cp, kubeClient, itp, _ := hostStorageProvider(t)
	hostDisk(t, itp, "160Gi", "144Gi")
	claim := stored(t, kubeClient, testNodeClaim("default-retired-flavor"))
	first, err := cp.Create(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: claim.Name}, ng); err != nil {
		t.Fatal(err)
	}
	// Fake client models a formerly supported flavor outside today's seed.
	ng.Spec.Flavor = "3XL"
	if err := kubeClient.Update(ctx, ng); err != nil {
		t.Fatal(err)
	}
	hostDisk(t, itp, "40Gi", "36Gi")
	got, err := cp.Get(ctx, first.Status.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	assertClaimDisk(t, got, "160Gi", "144Gi")
	list, err := cp.List(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("List: %d, %v", len(list), err)
	}
	assertClaimDisk(t, list[0], "160Gi", "144Gi")
}

func TestHostStorageAdoptionUsesAuthoritativeReader(t *testing.T) {
	for _, scenario := range []string{"cache missing", "disappeared after read", "replaced after read"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := coreContext()
			cp, kubeClient, itp, operator := hostStorageProvider(t)
			hostDisk(t, itp, "160Gi", "144Gi")
			claim := stored(t, kubeClient, testNodeClaim("default-adoption-race"))
			if _, err := cp.Create(ctx, claim); err != nil {
				t.Fatal(err)
			}
			hostDisk(t, itp, "40Gi", "36Gi")
			underlying := kubeClient.(client.WithWatch)
			nodeGroupReads := 0
			reader := interceptor.NewClient(underlying, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				err := c.Get(ctx, key, obj, opts...)
				if _, ok := obj.(*ngv1.NodeGroup); !ok || err != nil {
					return err
				}
				nodeGroupReads++
				if nodeGroupReads == 1 && scenario != "cache missing" {
					old := obj.(*ngv1.NodeGroup).DeepCopy()
					if err := c.Delete(ctx, old); err != nil {
						return err
					}
					if scenario == "replaced after read" {
						old.ResourceVersion = ""
						old.UID = "replacement-nodegroup"
						if err := c.Create(ctx, old); err != nil {
							return err
						}
					}
				}
				return nil
			}})
			cached := interceptor.NewClient(underlying, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*ngv1.NodeGroup); ok {
					return apierrors.NewNotFound(schema.GroupResource{Group: "api.clever-cloud.com", Resource: "nodegroups"}, key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			}})
			// Create's identity/class reads and adoption poll use reader, never
			// the deliberately empty informer cache.
			cp = cloudprovider.New(cached, reader, itp, nodegroup.NewProvider(cached, noopRecorder{}, itp, clock.RealClock{}))
			got, err := cp.Create(ctx, claim)
			if scenario == "cache missing" {
				if err != nil {
					t.Fatalf("informer lag was mistaken for a vanish: %v", err)
				}
				assertClaimDisk(t, got, "160Gi", "144Gi")
			} else if !corecloudprovider.IsInsufficientCapacityError(err) {
				t.Fatalf("vanished/replaced adoption must fail without recreating, got %v", err)
			}
			wantCreates := 1
			if scenario == "replaced after read" {
				wantCreates++
			} // the simulated external replacement
			if count := operator.createCount(); count != wantCreates {
				t.Fatalf("adoption recreated the group: %d POSTs, want %d", count, wantCreates)
			}
		})
	}
}

func TestHostStorageAdoptionPreservesOwnershipAndWantedChecks(t *testing.T) {
	for _, scenario := range []string{"unmanaged", "wrong owner UID", "wrong owner name", "wrong claim label", "claim deleted"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := coreContext()
			cp, kubeClient, itp, operator := hostStorageProvider(t)
			hostDisk(t, itp, "160Gi", "144Gi")
			claim := stored(t, kubeClient, testNodeClaim("default-adoption-identity"))
			if _, err := cp.Create(ctx, claim); err != nil {
				t.Fatal(err)
			}
			ng := &ngv1.NodeGroup{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Name: claim.Name}, ng); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "unmanaged":
				delete(ng.Labels, v1alpha1.ManagedLabelKey)
			case "wrong owner UID":
				ng.OwnerReferences[0].UID = "another-claim"
			case "wrong owner name":
				ng.OwnerReferences[0].Name = "another-claim"
			case "wrong claim label":
				ng.Labels[v1alpha1.NodeClaimLabelKey] = "another-claim"
			case "claim deleted":
				if err := kubeClient.Delete(ctx, claim); err != nil {
					t.Fatal(err)
				}
			}
			if err := kubeClient.Update(ctx, ng); err != nil {
				t.Fatal(err)
			}
			_, err := cp.Create(ctx, claim)
			if err == nil || corecloudprovider.IsInsufficientCapacityError(err) {
				t.Fatalf("must refuse unsafe adoption with a plain error, got %v", err)
			}
			if scenario == "claim deleted" && !strings.Contains(err.Error(), "no longer exists") {
				t.Fatalf("wanted check lost: %v", err)
			}
			if count := operator.createCount(); count != 1 {
				t.Fatalf("unsafe adoption issued another POST: %d", count)
			}
		})
	}
}

// A reference update concurrent with the POST must not rewrite the plan that
// was resolved under the creation lock or what is persisted for later adoption.
func TestHostStorageSnapshotCapturesTheResolvedLaunch(t *testing.T) {
	ctx := coreContext()
	_, kubeClient, itp, _ := hostStorageProvider(t)
	hostDisk(t, itp, "160Gi", "144Gi")
	underlying := kubeClient.(client.WithWatch)
	posting := interceptor.NewClient(underlying, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if _, ok := obj.(*ngv1.NodeGroup); ok {
			hostDisk(t, itp, "40Gi", "36Gi")
		}
		return c.Create(ctx, obj, opts...)
	}})
	cp := cloudprovider.New(posting, posting, itp, nodegroup.NewProvider(posting, noopRecorder{}, itp, clock.RealClock{}))
	wanted := stored(t, posting, testNodeClaim("default-profile-switch-at-post"))
	got, err := cp.Create(ctx, wanted)
	if err != nil {
		t.Fatal(err)
	}
	assertClaimDisk(t, got, "160Gi", "144Gi")
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: wanted.Name}, ng); err != nil {
		t.Fatal(err)
	}
	if ng.Annotations[snapshotCapacity] != "160Gi" || ng.Annotations[snapshotAllocatable] != "144Gi" {
		t.Fatalf("POST persisted another profile than the resolved launch: %v", ng.Annotations)
	}
	next, err := cp.GetInstanceTypes(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range next {
		if got := it.Allocatable()[corev1.ResourceEphemeralStorage]; got.Cmp(resource.MustParse("36Gi")) != 0 {
			t.Fatalf("new launches did not follow the new profile: %s", got.String())
		}
	}
}
