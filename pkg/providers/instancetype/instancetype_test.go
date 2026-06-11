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

package instancetype_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

func findInstanceType(t *testing.T, its []*corecloudprovider.InstanceType, name string) *corecloudprovider.InstanceType {
	t.Helper()
	for _, it := range its {
		if it.Name == name {
			return it
		}
	}
	t.Fatalf("instance type %q not found in catalog", name)
	return nil
}

// observedL returns a plausible measured capacity/allocatable pair for the L
// flavor that differs from the static estimate.
func observedL(t *testing.T) (corev1.ResourceList, corev1.ResourceList) {
	t.Helper()
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:              resource.MustParse("12"),
		corev1.ResourceMemory:           resource.MustParse("24117248Ki"),
		corev1.ResourceEphemeralStorage: resource.MustParse("40971488Ki"),
		corev1.ResourcePods:             resource.MustParse("110"),
	}
	allocatable := corev1.ResourceList{
		corev1.ResourceCPU:              resource.MustParse("12"),
		corev1.ResourceMemory:           resource.MustParse("24014848Ki"),
		corev1.ResourceEphemeralStorage: resource.MustParse("36874339Ki"),
		corev1.ResourcePods:             resource.MustParse("110"),
	}
	return capacity, allocatable
}

func TestListReturnsFreshObjectsPerCall(t *testing.T) {
	p := instancetype.NewProvider("par")

	first := p.List()
	second := p.List()
	if len(first) == 0 || len(second) == 0 {
		t.Fatalf("expected non-empty catalogs, got %d and %d", len(first), len(second))
	}
	for i := range first {
		if first[i] == second[i] {
			t.Errorf("List() returned the same *InstanceType for index %d on two calls", i)
		}
	}

	// Karpenter mutates returned objects; a mutation must not leak into the
	// catalog served by subsequent calls.
	mutated := findInstanceType(t, first, "2XS")
	mutated.Capacity[corev1.ResourceCPU] = resource.MustParse("999")

	fresh := findInstanceType(t, p.List(), "2XS")
	if fresh.Capacity.Cpu().Value() != 4 {
		t.Errorf("mutation leaked into a later List() call: cpu = %s", fresh.Capacity.Cpu())
	}
}

func TestListExposesExpectedRequirements(t *testing.T) {
	p := instancetype.NewProvider("par")

	its := p.List()
	if len(its) != 6 {
		t.Fatalf("expected 6 flavors, got %d", len(its))
	}
	for _, it := range its {
		if !it.Requirements.Has(corev1.LabelInstanceTypeStable) || !it.Requirements.Get(corev1.LabelInstanceTypeStable).Has(it.Name) {
			t.Errorf("flavor %s: missing instance-type requirement", it.Name)
		}
		if !it.Requirements.Has(v1alpha1.FlavorLabelKey) || !it.Requirements.Get(v1alpha1.FlavorLabelKey).Has(it.Name) {
			t.Errorf("flavor %s: missing flavor label requirement", it.Name)
		}
		if !it.Requirements.Has(corev1.LabelTopologyZone) || it.Requirements.Get(corev1.LabelTopologyZone).Any() != "par" {
			t.Errorf("flavor %s: expected zone par, got %q", it.Name, it.Requirements.Get(corev1.LabelTopologyZone).Any())
		}
		if !it.Requirements.Has(karpv1.CapacityTypeLabelKey) || it.Requirements.Get(karpv1.CapacityTypeLabelKey).Any() != karpv1.CapacityTypeOnDemand {
			t.Errorf("flavor %s: expected on-demand capacity type, got %q", it.Name, it.Requirements.Get(karpv1.CapacityTypeLabelKey).Any())
		}
	}
}

func TestGetKnownFlavor(t *testing.T) {
	p := instancetype.NewProvider("par")

	it, err := p.Get("M")
	if err != nil {
		t.Fatalf("Get(M): %v", err)
	}
	if it.Name != "M" {
		t.Errorf("unexpected name %q", it.Name)
	}
	if got := it.Capacity.Cpu().Value(); got != 10 {
		t.Errorf("expected 10 vCPU, got %d", got)
	}
	wantMemory := resource.MustParse("15988992Ki")
	if it.Capacity.Memory().Cmp(wantMemory) != 0 {
		t.Errorf("expected memory %s, got %s", wantMemory.String(), it.Capacity.Memory())
	}
}

func TestGetUnknownFlavorErrors(t *testing.T) {
	p := instancetype.NewProvider("par")

	if _, err := p.Get("3XL"); err == nil {
		t.Fatal("expected error for unknown flavor")
	}
}

func TestRecordObservedCapacityOverridesEstimate(t *testing.T) {
	p := instancetype.NewProvider("par")
	capacity, allocatable := observedL(t)

	p.RecordObservedCapacity("L", capacity, allocatable)

	it, err := p.Get("L")
	if err != nil {
		t.Fatalf("Get(L): %v", err)
	}
	if it.Capacity.Memory().Cmp(*capacity.Memory()) != 0 {
		t.Errorf("expected observed memory capacity %s, got %s", capacity.Memory(), it.Capacity.Memory())
	}
	if it.Capacity.Cpu().Cmp(*capacity.Cpu()) != 0 {
		t.Errorf("expected observed cpu capacity %s, got %s", capacity.Cpu(), it.Capacity.Cpu())
	}
	// KubeReserved must be the measured capacity-allocatable gap, replacing the
	// static 100Mi estimate.
	wantReserved := resource.MustParse("102400Ki")
	gotReserved := it.Overhead.KubeReserved[corev1.ResourceMemory]
	if gotReserved.Cmp(wantReserved) != 0 {
		t.Errorf("expected KubeReserved memory %s, got %s", wantReserved.String(), gotReserved.String())
	}
	alloc := it.Allocatable()
	if alloc.Memory().Cmp(*allocatable.Memory()) != 0 {
		t.Errorf("expected allocatable memory %s, got %s", allocatable.Memory(), alloc.Memory())
	}
	if alloc.Cpu().Cmp(*allocatable.Cpu()) != 0 {
		t.Errorf("expected allocatable cpu %s, got %s", allocatable.Cpu(), alloc.Cpu())
	}
}

func TestRecordObservedCapacityIgnoresZero(t *testing.T) {
	p := instancetype.NewProvider("par")

	zeroCPU := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("0"),
		corev1.ResourceMemory: resource.MustParse("24117248Ki"),
	}
	zeroMemory := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("12"),
		corev1.ResourceMemory: resource.MustParse("0"),
	}
	p.RecordObservedCapacity("L", zeroCPU, zeroCPU)
	p.RecordObservedCapacity("L", zeroMemory, zeroMemory)

	it, err := p.Get("L")
	if err != nil {
		t.Fatalf("Get(L): %v", err)
	}
	if got := it.Capacity.Cpu().Value(); got != 12 {
		t.Errorf("expected static cpu estimate 12, got %d", got)
	}
	wantMemory := resource.MustParse("23983488Ki")
	if it.Capacity.Memory().Cmp(wantMemory) != 0 {
		t.Errorf("expected static memory estimate %s, got %s", wantMemory.String(), it.Capacity.Memory())
	}
	wantReserved := resource.MustParse("100Mi")
	gotReserved := it.Overhead.KubeReserved[corev1.ResourceMemory]
	if gotReserved.Cmp(wantReserved) != 0 {
		t.Errorf("expected static KubeReserved %s, got %s", wantReserved.String(), gotReserved.String())
	}
}

func TestRecordObservedCapacityDeepCopies(t *testing.T) {
	p := instancetype.NewProvider("par")
	capacity, allocatable := observedL(t)

	p.RecordObservedCapacity("L", capacity, allocatable)

	// Mutating the caller's lists after recording must not affect the catalog.
	capacity[corev1.ResourceCPU] = resource.MustParse("999")
	capacity[corev1.ResourceMemory] = resource.MustParse("1Ki")
	allocatable[corev1.ResourceMemory] = resource.MustParse("1Ki")

	it, err := p.Get("L")
	if err != nil {
		t.Fatalf("Get(L): %v", err)
	}
	if got := it.Capacity.Cpu().Value(); got != 12 {
		t.Errorf("caller mutation leaked into recorded cpu: got %d", got)
	}
	wantMemory := resource.MustParse("24117248Ki")
	if it.Capacity.Memory().Cmp(wantMemory) != 0 {
		t.Errorf("caller mutation leaked into recorded memory: got %s", it.Capacity.Memory())
	}
	wantReserved := resource.MustParse("102400Ki")
	gotReserved := it.Overhead.KubeReserved[corev1.ResourceMemory]
	if gotReserved.Cmp(wantReserved) != 0 {
		t.Errorf("caller mutation leaked into KubeReserved: got %s, want %s", gotReserved.String(), wantReserved.String())
	}
}
