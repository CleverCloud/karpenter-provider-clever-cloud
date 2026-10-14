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

package envtest_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

const hostStorageNodeName = "controller-disk-reference"

// TestHostStorageFollowsNodeWatch drives only API writes. Unlike unit tests
// calling Reconcile directly, it proves status updates, deletion and a new UID
// trigger the registered controller and update the scheduler's catalogue.
func TestHostStorageFollowsNodeWatch(t *testing.T) {
	ctx := context.Background()
	cp := cloudprovider.New(kubeClient, apiReader, hostStorageProvider,
		nodegroup.NewProvider(kubeClient, noopRecorder{}, hostStorageProvider, clock.RealClock{}))
	assertWaiting := func() {
		t.Helper()
		eventually(t, 10*time.Second, func(ctx context.Context) error {
			if hostStorageProvider.EphemeralStorageReady() {
				return fmt.Errorf("host disk is still considered ready")
			}
			instanceTypes, err := cp.GetInstanceTypes(ctx, nil)
			if err != nil || len(instanceTypes) == 0 {
				return fmt.Errorf("unknown disk blocks CPU/RAM provisioning: types=%d error=%v", len(instanceTypes), err)
			}
			for _, it := range instanceTypes {
				capacity, allocatable := it.Capacity[corev1.ResourceEphemeralStorage], it.Allocatable()[corev1.ResourceEphemeralStorage]
				if !capacity.IsZero() || !allocatable.IsZero() {
					return fmt.Errorf("unknown disk still guarantees storage for %s: %s/%s", it.Name, capacity.String(), allocatable.String())
				}
			}
			return nil
		})
	}
	assertDisk := func(capacity, allocatable string) {
		t.Helper()
		eventually(t, 10*time.Second, func(ctx context.Context) error {
			types, err := cp.GetInstanceTypes(ctx, nil)
			if err != nil {
				return err
			}
			if len(types) == 0 {
				return fmt.Errorf("empty catalogue")
			}
			for _, it := range types {
				gotCapacity := it.Capacity[corev1.ResourceEphemeralStorage]
				gotAllocatable := it.Allocatable()[corev1.ResourceEphemeralStorage]
				if gotCapacity.Cmp(resource.MustParse(capacity)) != 0 || gotAllocatable.Cmp(resource.MustParse(allocatable)) != 0 {
					return fmt.Errorf("flavor %s disk=%s/%s, want %s/%s", it.Name, gotCapacity.String(), gotAllocatable.String(), capacity, allocatable)
				}
			}
			return nil
		})
	}
	writeDisk := func(capacity, allocatable string) {
		t.Helper()
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			node := &corev1.Node{}
			if err := apiReader.Get(ctx, types.NamespacedName{Name: hostStorageNodeName}, node); err != nil {
				return err
			}
			node.Status.Capacity = corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse(capacity)}
			node.Status.Allocatable = corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse(allocatable)}
			return kubeClient.Status().Update(ctx, node)
		}); err != nil {
			t.Fatalf("writing host status: %v", err)
		}
	}
	assertWaiting() // the controller starts before its configured node exists
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: hostStorageNodeName}}
	if err := kubeClient.Create(ctx, node); err != nil {
		t.Fatalf("creating reference node: %v", err)
	}
	t.Cleanup(func() {
		_ = kubeClient.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: hostStorageNodeName}})
	})
	assertWaiting() // a created node is not a usable measurement without status
	writeDisk("160Gi", "144Gi")
	assertDisk("160Gi", "144Gi")
	writeDisk("40Gi", "36Gi")
	assertDisk("40Gi", "36Gi")
	writeDisk("40Gi", "41Gi")
	assertWaiting() // invalid status must not preserve readiness indefinitely
	writeDisk("40Gi", "36Gi")
	assertDisk("40Gi", "36Gi")
	if err := kubeClient.Delete(ctx, node); err != nil {
		t.Fatalf("deleting reference node: %v", err)
	}
	assertWaiting()
	replacement := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: hostStorageNodeName}}
	if err := kubeClient.Create(ctx, replacement); err != nil {
		t.Fatalf("recreating reference node: %v", err)
	}
	if replacement.UID == node.UID {
		t.Fatal("replacement unexpectedly reused the original node UID")
	}
	writeDisk("200Gi", "180Gi")
	assertDisk("200Gi", "180Gi")
}
