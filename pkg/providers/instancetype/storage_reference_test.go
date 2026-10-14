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
	"errors"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

func setDiskReference(t *testing.T, p *instancetype.Provider, capacity, allocatable string) bool {
	t.Helper()
	changed, err := p.SetEphemeralStorageReference(resource.MustParse(capacity), resource.MustParse(allocatable))
	if err != nil {
		t.Fatal(err)
	}
	return changed
}

func TestEphemeralStorageReferenceReadiness(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
	if !p.EphemeralStorageReady() {
		t.Fatal("static mode must be ready")
	}
	p.InvalidateEphemeralStorageReference()
	if !p.EphemeralStorageReady() {
		t.Fatal("invalidation must not disable static mode")
	}
	if _, err := p.SetEphemeralStorageReference(resource.MustParse("120Gi"), resource.MustParse("108Gi")); err == nil {
		t.Fatal("setting a reference without opting in must fail")
	}
	p.RequireEphemeralStorageReference()
	if p.EphemeralStorageReady() {
		t.Fatal("dynamic mode must wait for its reference")
	}
	if !setDiskReference(t, p, "120Gi", "108Gi") || !p.EphemeralStorageReady() {
		t.Fatal("the first reference must become ready")
	}
	p.RequireEphemeralStorageReference()
	if !p.EphemeralStorageReady() || setDiskReference(t, p, "120Gi", "108Gi") {
		t.Fatal("repeated opt-in and identical reference must preserve readiness without a change")
	}
	if setDiskReference(t, p, "128849018880", "115964116992") {
		t.Fatal("equivalent byte quantities must not count as a change")
	}
	p.InvalidateEphemeralStorageReference()
	if p.EphemeralStorageReady() {
		t.Fatal("an invalidated reference must stop provisioning")
	}
	assertDisk(t, p, "120Gi", "108Gi")
	if !setDiskReference(t, p, "120Gi", "108Gi") || !p.EphemeralStorageReady() {
		t.Fatal("the same valid reference must restore readiness")
	}
}

func TestEphemeralStorageReferenceReplacesOldMinimaInBothDirections(t *testing.T) {
	p := instancetype.NewProvider("par", nil, []instancetype.FlavorOverride{{Name: "2XL", CPU: ptr(int64(24)), MemoryKi: ptr(int64(40000000))}})
	capacity, allocatable := nodeReport(4, 3700000, 3597600)
	capacity[corev1.ResourceEphemeralStorage] = resource.MustParse("95Gi")
	allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse("85Gi")
	record(t, p, "2XS", capacity, allocatable)
	assertDisk(t, p, "95Gi", "85Gi")
	p.RequireEphemeralStorageReference()
	prices := map[string]float64{}
	for _, it := range p.List() {
		prices[it.Name] = it.Offerings[0].Price
	}
	for _, profile := range [][2]string{{"200Gi", "180Gi"}, {"20Gi", "15Gi"}, {"300Gi", "250Gi"}} {
		if !setDiskReference(t, p, profile[0], profile[1]) {
			t.Fatal("a different profile must report a change")
		}
		assertDisk(t, p, profile[0], profile[1])
		for _, it := range p.List() {
			gotCapacity, gotAllocatable := it.Capacity[corev1.ResourceEphemeralStorage], it.Allocatable()[corev1.ResourceEphemeralStorage]
			if gotCapacity.Cmp(resource.MustParse(profile[0])) != 0 || gotAllocatable.Cmp(resource.MustParse(profile[1])) != 0 {
				t.Errorf("flavor %s retained a different disk: capacity=%s allocatable=%s", it.Name, gotCapacity.String(), gotAllocatable.String())
			}
			if it.Offerings[0].Price != prices[it.Name] {
				t.Errorf("disk change repriced flavor %s", it.Name)
			}
		}
		assertServed(t, p, "2XS", 4, 3700000, 3597600)
	}
}

func TestEphemeralStorageReferenceIgnoresOtherNodeDisks(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
	p.RequireEphemeralStorageReference()
	setDiskReference(t, p, "150Gi", "135Gi")
	for _, disk := range [][2]string{{"1Ki", "512"}, {"60Gi", "54Gi"}, {"2Ti", "1Ti"}} {
		capacity, allocatable := nodeReport(4, 3700000, 3597600)
		capacity[corev1.ResourceEphemeralStorage] = resource.MustParse(disk[0])
		allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse(disk[1])
		record(t, p, "2XS", capacity, allocatable)
		assertDisk(t, p, "150Gi", "135Gi")
		assertServed(t, p, "2XS", 4, 3700000, 3597600)
	}
	capacity, allocatable := nodeReport(4, 3700000, 3597600)
	delete(capacity, corev1.ResourceEphemeralStorage)
	delete(allocatable, corev1.ResourceEphemeralStorage)
	record(t, p, "2XS", capacity, allocatable)
	assertDisk(t, p, "150Gi", "135Gi")
	// Opting into dynamic disk sizing does not relax CPU/RAM trust bounds.
	capacity[corev1.ResourceMemory] = resource.MustParse("100Gi")
	if _, err := p.RecordObservedCapacity("2XS", capacity, allocatable); !errors.Is(err, instancetype.ErrImplausibleCapacity) {
		t.Fatalf("untrusted memory report: expected ErrImplausibleCapacity, got %v", err)
	}
	capacity, allocatable = nodeReport(4, 3700000, 3597600)
	capacity[corev1.ResourceEphemeralStorage] = resource.MustParse("10Gi")
	allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse("20Gi")
	if _, err := p.RecordObservedCapacity("2XS", capacity, allocatable); !errors.Is(err, instancetype.ErrImplausibleCapacity) {
		t.Fatalf("inconsistent disk report: expected ErrImplausibleCapacity, got %v", err)
	}
}

func TestEphemeralStorageReferenceRejectsInvalidQuantities(t *testing.T) {
	for name, profile := range map[string][2]string{
		"zero capacity":          {"0", "1"},
		"zero allocatable":       {"10Gi", "0"},
		"negative capacity":      {"-10Gi", "1"},
		"negative allocatable":   {"10Gi", "-1"},
		"fractional capacity":    {"1500m", "1"},
		"fractional allocatable": {"10Gi", "1500m"},
		"overflow capacity":      {"9223372036854775808", "1"},
		"overflow allocatable":   {"10Gi", "9223372036854775808"},
		"allocatable above disk": {"10Gi", "11Gi"},
	} {
		t.Run(name, func(t *testing.T) {
			p := instancetype.NewProvider("par", nil, nil)
			p.RequireEphemeralStorageReference()
			capacity, allocatable := resource.MustParse(profile[0]), resource.MustParse(profile[1])
			if changed, err := p.SetEphemeralStorageReference(capacity, allocatable); err == nil || changed || p.EphemeralStorageReady() {
				t.Fatalf("invalid first reference: changed=%v error=%v ready=%v", changed, err, p.EphemeralStorageReady())
			}
			setDiskReference(t, p, "100Gi", "90Gi")
			if changed, err := p.SetEphemeralStorageReference(capacity, allocatable); err == nil || changed {
				t.Fatalf("invalid replacement: changed=%v error=%v", changed, err)
			}
			assertDisk(t, p, "100Gi", "90Gi")
		})
	}
	// Equal capacity and allocatable is consistent; small whole-byte values
	// remain representable without rounding up the available space.
	p := instancetype.NewProvider("par", nil, nil)
	p.RequireEphemeralStorageReference()
	setDiskReference(t, p, "1", "1")
	assertDisk(t, p, "1", "1")
}

func TestEphemeralStorageReferenceOwnsItsQuantities(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
	p.RequireEphemeralStorageReference()
	capacity, allocatable := resource.MustParse("120Gi"), resource.MustParse("108Gi")
	// Force the decimal representation, which shares a pointer when copied
	// without Quantity.DeepCopy even if simple int64-backed examples do not.
	capacity.AsDec()
	allocatable.AsDec()
	if _, err := p.SetEphemeralStorageReference(capacity, allocatable); err != nil {
		t.Fatal(err)
	}
	capacity.Add(resource.MustParse("1Gi"))
	allocatable.Sub(resource.MustParse("1Gi"))
	assertDisk(t, p, "120Gi", "108Gi")
	it, err := p.Get("2XS")
	if err != nil {
		t.Fatal(err)
	}
	disk := it.Capacity[corev1.ResourceEphemeralStorage]
	disk.Add(resource.MustParse("1Gi"))
	reserve := it.Overhead.KubeReserved[corev1.ResourceEphemeralStorage]
	reserve.Add(resource.MustParse("1Gi"))
	assertDisk(t, p, "120Gi", "108Gi")
}

func TestEphemeralStorageReferenceConcurrentUpdatesAndObservations(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
	p.RequireEphemeralStorageReference()
	setDiskReference(t, p, "100Gi", "90Gi")
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Go(func() {
			for i := range 50 {
				switch worker {
				case 0:
					capacity, allocatable := resource.MustParse("100Gi"), resource.MustParse("90Gi")
					if i%2 == 0 {
						capacity, allocatable = resource.MustParse("200Gi"), resource.MustParse("180Gi")
					}
					if _, err := p.SetEphemeralStorageReference(capacity, allocatable); err != nil {
						t.Error(err)
					}
				case 1:
					capacity, allocatable := nodeReport(4, 3700000, 3597600)
					if _, err := p.RecordObservedCapacity("2XS", capacity, allocatable); err != nil {
						t.Error(err)
					}
				case 2:
					p.InvalidateEphemeralStorageReference()
					_ = p.EphemeralStorageReady()
				case 3:
					var previous resource.Quantity
					for _, it := range p.List() {
						capacity, allocatable := it.Capacity[corev1.ResourceEphemeralStorage], it.Allocatable()[corev1.ResourceEphemeralStorage]
						if !previous.IsZero() && previous.Cmp(capacity) != 0 {
							t.Errorf("one List returned different disk profiles: %s and %s", previous.String(), capacity.String())
						}
						previous = capacity
						if capacity.Cmp(resource.MustParse("100Gi")) != 0 && capacity.Cmp(resource.MustParse("200Gi")) != 0 {
							t.Errorf("unexpected disk profile: %s", capacity.String())
						}
						if capacity.Cmp(resource.MustParse("100Gi")) == 0 && allocatable.Cmp(resource.MustParse("90Gi")) != 0 ||
							capacity.Cmp(resource.MustParse("200Gi")) == 0 && allocatable.Cmp(resource.MustParse("180Gi")) != 0 {
							t.Errorf("disk capacity/allocatable came from different profiles: %s/%s", capacity.String(), allocatable.String())
						}
					}
				}
			}
		})
	}
	workers.Wait()
	setDiskReference(t, p, "300Gi", "270Gi")
	assertDisk(t, p, "300Gi", "270Gi")
}
