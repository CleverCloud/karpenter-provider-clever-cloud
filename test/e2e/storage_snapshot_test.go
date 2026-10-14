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

package e2e

import (
	"fmt"
	"math"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

const (
	launchDiskCapacityAnnotation    = "karpenter.clever-cloud.com/ephemeral-storage-capacity"
	launchDiskAllocatableAnnotation = "karpenter.clever-cloud.com/ephemeral-storage-allocatable"
)

// These launches request real disk, so 0/0 is not a valid result. NodeClaim
// status must retain the launch snapshot even after the actual worker reports.
// In dynamic mode the snapshot must be exactly the chosen reference, not the
// seed or a worker's observation. Static mode tolerates the same 1% measured
// variation as the existing worker-capacity check.
func validateProvisionDiskSnapshot(claim *karpv1.NodeClaim, ng *ngv1.NodeGroup, reference *corev1.Node) error {
	var expectedCapacity, expectedAllocatable resource.Quantity
	if reference != nil {
		expectedCapacity = reference.Status.Capacity[corev1.ResourceEphemeralStorage]
		expectedAllocatable = reference.Status.Allocatable[corev1.ResourceEphemeralStorage]
	} else {
		instanceType, err := instancetype.NewProvider("par", nil, nil).Get(ng.Spec.Flavor)
		if err != nil {
			return err
		}
		expectedCapacity = instanceType.Capacity[corev1.ResourceEphemeralStorage]
		expectedAllocatable = instanceType.Allocatable()[corev1.ResourceEphemeralStorage]
	}
	var capacity resource.Quantity
	for _, field := range []struct {
		name, annotation string
		claim, expected  resource.Quantity
	}{
		{"capacity", launchDiskCapacityAnnotation, claim.Status.Capacity[corev1.ResourceEphemeralStorage], expectedCapacity},
		{"allocatable", launchDiskAllocatableAnnotation, claim.Status.Allocatable[corev1.ResourceEphemeralStorage], expectedAllocatable},
	} {
		value, present := ng.Annotations[field.annotation]
		if !present {
			return fmt.Errorf("missing %s", field.annotation)
		}
		snapshot, err := resource.ParseQuantity(value)
		if err != nil {
			return fmt.Errorf("invalid %s=%q: %w", field.annotation, value, err)
		}
		if snapshot.Sign() <= 0 || snapshot.Cmp(*resource.NewQuantity(snapshot.Value(), resource.BinarySI)) != 0 {
			return fmt.Errorf("%s snapshot must contain positive whole bytes, got %s", field.name, value)
		}
		if field.name == "capacity" {
			capacity = snapshot
		} else if snapshot.Cmp(capacity) > 0 {
			return fmt.Errorf("snapshot allocatable %s exceeds capacity %s", snapshot.String(), capacity.String())
		}
		if snapshot.Cmp(field.claim) != 0 {
			return fmt.Errorf("%s snapshot %s differs from NodeClaim status %s", field.name, snapshot.String(), field.claim.String())
		}
		if reference != nil {
			if snapshot.Cmp(field.expected) != 0 {
				return fmt.Errorf("%s snapshot %s differs from reference %s: %s", field.name, snapshot.String(), reference.Name, field.expected.String())
			}
		} else if deviation := float64(snapshot.Value())/float64(field.expected.Value()) - 1; math.Abs(deviation) > 0.01 {
			return fmt.Errorf("%s snapshot %s differs from static catalogue %s by %+.2f%%", field.name, snapshot.String(), field.expected.String(), deviation*100)
		}
	}
	return nil
}

func TestHarnessProvisionDiskSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*karpv1.NodeClaim, *ngv1.NodeGroup)
		want   string
	}{
		{name: "reference distinct from static seed"},
		{name: "missing snapshot", mutate: func(_ *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
			delete(ng.Annotations, launchDiskCapacityAnnotation)
		}, want: "missing"},
		{name: "malformed snapshot", mutate: func(_ *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
			ng.Annotations[launchDiskCapacityAnnotation] = "invalid"
		}, want: "invalid"},
		{name: "unknown disk despite positive request", mutate: func(_ *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
			ng.Annotations[launchDiskCapacityAnnotation] = "0"
			ng.Annotations[launchDiskAllocatableAnnotation] = "0"
		}, want: "positive whole bytes"},
		{name: "fractional bytes", mutate: func(_ *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
			ng.Annotations[launchDiskCapacityAnnotation] = "1001m"
		}, want: "positive whole bytes"},
		{name: "claim capacity differs", mutate: func(claim *karpv1.NodeClaim, _ *ngv1.NodeGroup) {
			claim.Status.Capacity[corev1.ResourceEphemeralStorage] = resource.MustParse("22Gi")
		}, want: "differs from NodeClaim status"},
		{name: "claim allocatable differs", mutate: func(claim *karpv1.NodeClaim, _ *ngv1.NodeGroup) {
			claim.Status.Allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse("18Gi")
		}, want: "differs from NodeClaim status"},
		{name: "snapshot allocatable exceeds capacity", mutate: func(_ *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
			ng.Annotations[launchDiskAllocatableAnnotation] = "24Gi"
		}, want: "exceeds capacity"},
		{name: "snapshot and claim use wrong reference", mutate: func(claim *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
			ng.Annotations[launchDiskCapacityAnnotation] = "98Gi"
			claim.Status.Capacity[corev1.ResourceEphemeralStorage] = resource.MustParse("98Gi")
		}, want: "differs from reference"},
		{name: "snapshot and claim use wrong reference reserve", mutate: func(claim *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
			ng.Annotations[launchDiskAllocatableAnnotation] = "18Gi"
			claim.Status.Allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse("18Gi")
		}, want: "differs from reference"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reference := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "fixed-reference"}, Status: corev1.NodeStatus{
				Capacity:    corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("23Gi")},
				Allocatable: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("19Gi")},
			}}
			claim := &karpv1.NodeClaim{Status: karpv1.NodeClaimStatus{
				Capacity: reference.Status.Capacity.DeepCopy(), Allocatable: reference.Status.Allocatable.DeepCopy(),
			}}
			ng := &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
				launchDiskCapacityAnnotation: "23Gi", launchDiskAllocatableAnnotation: "19Gi",
			}}}
			if tc.mutate != nil {
				tc.mutate(claim, ng)
			}
			err := validateProvisionDiskSnapshot(claim, ng, reference)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("validateProvisionDiskSnapshot = %v, want %q", err, tc.want)
			}
		})
	}
	t.Run("static accepts observed rounding", func(t *testing.T) {
		instanceType, err := instancetype.NewProvider("par", nil, nil).Get("2XS")
		if err != nil {
			t.Fatal(err)
		}
		claim := &karpv1.NodeClaim{Status: karpv1.NodeClaimStatus{Capacity: instanceType.Capacity, Allocatable: instanceType.Allocatable()}}
		capacity := claim.Status.Capacity[corev1.ResourceEphemeralStorage]
		allocatable := claim.Status.Allocatable[corev1.ResourceEphemeralStorage]
		allocatable.Add(resource.MustParse("512"))
		claim.Status.Allocatable[corev1.ResourceEphemeralStorage] = allocatable
		ng := &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			launchDiskCapacityAnnotation: capacity.String(), launchDiskAllocatableAnnotation: allocatable.String(),
		}}, Spec: ngv1.NodeGroupSpec{Flavor: "2XS"}}
		if err := validateProvisionDiskSnapshot(claim, ng, nil); err != nil {
			t.Fatal(err)
		}
		ng.Annotations[launchDiskCapacityAnnotation] = "23Gi"
		claim.Status.Capacity[corev1.ResourceEphemeralStorage] = resource.MustParse("23Gi")
		if err := validateProvisionDiskSnapshot(claim, ng, nil); err == nil || !strings.Contains(err.Error(), "static catalogue") {
			t.Fatalf("expected the wrong static disk to fail, got %v", err)
		}
	})
}
