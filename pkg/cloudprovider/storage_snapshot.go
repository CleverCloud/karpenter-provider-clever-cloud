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

package cloudprovider

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
)

const (
	diskCapacityAnnotation    = "karpenter.clever-cloud.com/ephemeral-storage-capacity"
	diskAllocatableAnnotation = "karpenter.clever-cloud.com/ephemeral-storage-allocatable"
)

type diskSnapshot struct {
	capacity    resource.Quantity
	allocatable resource.Quantity
}

// Capture the resolved instance type, not a second provider lookup: the host
// reference can change while the NodeGroup POST or acceptance poll is running.
func diskSnapshotAnnotations(it *cloudprovider.InstanceType) map[string]string {
	capacity := it.Capacity[corev1.ResourceEphemeralStorage]
	allocatable := it.Allocatable()[corev1.ResourceEphemeralStorage]
	return map[string]string{diskCapacityAnnotation: capacity.String(), diskAllocatableAnnotation: allocatable.String()}
}

// An absent snapshot is a pre-feature group. A partial or malformed snapshot
// must not silently become a different disk during a retried Create.
func readDiskSnapshot(ng *ngv1.NodeGroup) (*diskSnapshot, error) {
	capacity, hasCapacity := ng.Annotations[diskCapacityAnnotation]
	allocatable, hasAllocatable := ng.Annotations[diskAllocatableAnnotation]
	if !hasCapacity && !hasAllocatable {
		return nil, nil
	}
	snapshot := &diskSnapshot{}
	for _, field := range []struct {
		name, value string
		target      *resource.Quantity
	}{
		{diskCapacityAnnotation, capacity, &snapshot.capacity},
		{diskAllocatableAnnotation, allocatable, &snapshot.allocatable},
	} {
		quantity, err := resource.ParseQuantity(field.value)
		if err != nil {
			return nil, fmt.Errorf("nodegroup %s has invalid %s=%q: %w", ng.Name, field.name, field.value, err)
		}
		bytes := quantity.Value()
		if bytes <= 0 || quantity.Cmp(*resource.NewQuantity(bytes, resource.DecimalSI)) != 0 {
			return nil, fmt.Errorf("nodegroup %s has invalid %s=%q: expected positive whole bytes representable by int64", ng.Name, field.name, field.value)
		}
		*field.target = quantity
	}
	if snapshot.allocatable.Cmp(snapshot.capacity) > 0 {
		return nil, fmt.Errorf("nodegroup %s disk snapshot allocatable %s exceeds capacity %s", ng.Name, allocatable, capacity)
	}
	return snapshot, nil
}

// Read uncached under createMu, before selecting a new launch profile. A known
// snapshot turns this attempt into adoption only; the provider must never POST
// a replacement if this group disappears after the read.
func (c *CloudProvider) existingDiskLaunch(ctx context.Context, claim *karpv1.NodeClaim) (*ngv1.NodeGroup, error) {
	if err := c.checkNodeClaimWanted(ctx, claim); err != nil {
		return nil, err
	}
	existing := &ngv1.NodeGroup{}
	if err := c.apiReader.Get(ctx, types.NamespacedName{Name: claim.Name}, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("checking for an existing nodegroup launch, %w", err)
	}
	snapshot, err := readDiskSnapshot(existing)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, nil
	}
	return existing, nil
}
