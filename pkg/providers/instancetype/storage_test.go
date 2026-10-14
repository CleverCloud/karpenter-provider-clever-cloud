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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

// Current disks were measured on 2026-10-14 (Kubernetes 1.36.3, kernel
// 7.2.10). New workers on every cluster receive them; existing workers keep
// the smaller disk measured in September (nodeReport).
const (
	currentDiskCapacity    = "102624184Ki"
	currentDiskAllocatable = "94578447818"
	seedDiskAllocatable    = "92361765Ki" // 10% eviction reserve rounded up to KiB
)

func assertDisk(t *testing.T, p *instancetype.Provider, wantCapacity, wantAllocatable string) {
	t.Helper()
	it, err := p.Get("2XS")
	if err != nil {
		t.Fatal(err)
	}
	for name, pair := range map[string][2]resource.Quantity{
		"capacity":    {it.Capacity[corev1.ResourceEphemeralStorage], resource.MustParse(wantCapacity)},
		"allocatable": {it.Allocatable()[corev1.ResourceEphemeralStorage], resource.MustParse(wantAllocatable)},
	} {
		if pair[0].Cmp(pair[1]) != 0 {
			t.Errorf("disk %s = %s, want %s", name, pair[0].String(), pair[1].String())
		}
	}
}

func TestStorageSeedMatchesNewWorkers(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
	assertDisk(t, p, currentDiskCapacity, seedDiskAllocatable)
	live := resource.MustParse(currentDiskAllocatable)
	seed := resource.MustParse(seedDiskAllocatable)
	if gap := live.Value() - seed.Value(); gap < 0 || gap >= 1024 {
		t.Errorf("seed allocatable differs from the live worker by %d bytes, want a conservative rounding below 1Ki", gap)
	}
}

func TestStorageObservationsKeepLegacyDisksOutOfNewLaunches(t *testing.T) {
	for _, order := range [][]string{{"legacy"}, {"current", "legacy"}, {"legacy", "current"}} {
		t.Run(strings.Join(order, " then "), func(t *testing.T) {
			p := instancetype.NewProvider("par", nil, nil)
			wantAllocatable := seedDiskAllocatable
			for _, generation := range order {
				capacity, allocatable := nodeReport(4, 3700000, 3597600)
				if generation == "current" {
					capacity[corev1.ResourceEphemeralStorage] = resource.MustParse(currentDiskCapacity)
					allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse(currentDiskAllocatable)
					capacity[corev1.ResourceMemory] = resource.MustParse("3715344Ki")
					allocatable[corev1.ResourceMemory] = resource.MustParse("3612944Ki")
					wantAllocatable = currentDiskAllocatable
				}
				record(t, p, "2XS", capacity, allocatable)
			}
			assertDisk(t, p, currentDiskCapacity, wantAllocatable)
			// Legacy nodes still correct other resources; only their obsolete
			// disk profile must not lower what new launches can deliver.
			assertServed(t, p, "2XS", 4, 3700000, 3597600)
		})
	}
}

func TestObservationWithoutDiskPreservesItsEvictionReserve(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
	capacity, allocatable := nodeReport(4, 3700000, 3597600)
	delete(capacity, corev1.ResourceEphemeralStorage)
	delete(allocatable, corev1.ResourceEphemeralStorage)
	record(t, p, "2XS", capacity, allocatable)
	assertDisk(t, p, currentDiskCapacity, seedDiskAllocatable)
	assertServed(t, p, "2XS", 4, 3700000, 3597600)
}

func TestStorageObservationsKeepTheSmallestCurrentDisk(t *testing.T) {
	for _, order := range [][]string{{"small", "current"}, {"current", "small"}} {
		t.Run(order[0]+" first", func(t *testing.T) {
			p := instancetype.NewProvider("par", nil, nil)
			for _, size := range order {
				capacity, allocatable := nodeReport(4, 3715344, 3612944)
				capacity[corev1.ResourceEphemeralStorage] = resource.MustParse(currentDiskCapacity)
				allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse(currentDiskAllocatable)
				if size == "small" {
					capacity[corev1.ResourceEphemeralStorage] = resource.MustParse("99000000Ki")
					allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse("91238400000")
				}
				record(t, p, "2XS", capacity, allocatable)
			}
			assertDisk(t, p, "99000000Ki", "91238400000")
		})
	}
}

func TestStorageObservationsRejectUnknownProfiles(t *testing.T) {
	for name, disk := range map[string][2]string{
		"intermediate disk":       {"60Gi", "54Gi"},
		"inflated disk":           {"400Gi", "360Gi"},
		"undersized legacy disk":  {"30Gi", "27Gi"},
		"undersized current disk": {"85Gi", "76Gi"},
		"current with old alloc":  {currentDiskCapacity, "37759323279"},
		"legacy with new alloc":   {"40971488Ki", currentDiskAllocatable},
		// Each quantity is separately within 10% of its profile. The pair
		// still implies a reservation far from the platform's 10% threshold.
		"current wrong ratio": {"110000000Ki", "86000000000"},
		"legacy wrong ratio":  {"44000000Ki", "34500000000"},
	} {
		t.Run(name, func(t *testing.T) {
			p := instancetype.NewProvider("par", nil, nil)
			capacity, allocatable := nodeReport(4, 3700000, 3597600)
			capacity[corev1.ResourceEphemeralStorage] = resource.MustParse(disk[0])
			allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse(disk[1])
			if _, err := p.RecordObservedCapacity("2XS", capacity, allocatable); !errors.Is(err, instancetype.ErrImplausibleCapacity) {
				t.Fatalf("expected ErrImplausibleCapacity, got %v", err)
			}
			assertDisk(t, p, currentDiskCapacity, seedDiskAllocatable)
			assertServed(t, p, "2XS", 4, 3715344, 3612944)
		})
	}
}
