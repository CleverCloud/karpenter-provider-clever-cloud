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
	"fmt"
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

// measuredNodes is what one live CKE node of each flavor reported on
// 2026-09-30 (Kubernetes 1.37.0, kernel 7.2.8): status.capacity cpu and
// memory, and an allocatable memory of exactly capacity - 100Mi. Every flavor
// also reported ephemeral-storage 40971488Ki (allocatable 37759323279 bytes)
// and 110 pods.
var measuredNodes = []struct {
	flavor                  string
	cpu                     int64
	memoryKi, allocatableKi int64
}{
	{"2XS", 4, 3715344, 3612944},
	{"XS", 6, 7553664, 7451264},
	{"S", 8, 11385832, 11283432},
	{"M", 10, 15229256, 15126856},
	{"L", 12, 22896304, 22793904},
	{"XL", 16, 30584176, 30481776},
}

// nodeReport builds the status.capacity/allocatable pair of a node, with the
// disk and pod figures every measured flavor reported.
func nodeReport(cpu, memoryKi, allocatableKi int64) (corev1.ResourceList, corev1.ResourceList) {
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:              *resource.NewQuantity(cpu, resource.DecimalSI),
		corev1.ResourceMemory:           resource.MustParse(fmt.Sprintf("%dKi", memoryKi)),
		corev1.ResourceEphemeralStorage: resource.MustParse("40971488Ki"),
		corev1.ResourcePods:             resource.MustParse("110"),
	}
	allocatable := corev1.ResourceList{
		corev1.ResourceCPU:              *resource.NewQuantity(cpu, resource.DecimalSI),
		corev1.ResourceMemory:           resource.MustParse(fmt.Sprintf("%dKi", allocatableKi)),
		corev1.ResourceEphemeralStorage: resource.MustParse("37759323279"),
		corev1.ResourcePods:             resource.MustParse("110"),
	}
	return capacity, allocatable
}

// assertServed checks the cpu and memory the catalogue serves for a flavor.
func assertServed(t *testing.T, p *instancetype.Provider, flavor string, cpu, memoryKi, allocatableKi int64) {
	t.Helper()
	it, err := p.Get(flavor)
	if err != nil {
		t.Fatalf("Get(%s): %v", flavor, err)
	}
	if got := it.Capacity.Cpu().Value(); got != cpu {
		t.Errorf("%s: served cpu %d, want %d", flavor, got, cpu)
	}
	if want := resource.MustParse(fmt.Sprintf("%dKi", memoryKi)); it.Capacity.Memory().Cmp(want) != 0 {
		t.Errorf("%s: served memory capacity %s, want %s", flavor, it.Capacity.Memory(), want.String())
	}
	if want, got := resource.MustParse(fmt.Sprintf("%dKi", allocatableKi)), it.Allocatable()[corev1.ResourceMemory]; got.Cmp(want) != 0 {
		t.Errorf("%s: served memory allocatable %s, want %s", flavor, got.String(), want.String())
	}
}

func TestSeedMatchesMeasuredNodes(t *testing.T) {
	// Until a node of a flavor reports, karpenter packs pods against the
	// seed: it must promise exactly what a live node of the current image
	// offers. The previous seed claimed 4.4-5.0% more memory, and pods that
	// fit the catalogue did not fit the node launched for them.
	if len(measuredNodes) != len(instancetype.FlavorSizing) {
		t.Fatalf("measured %d flavors, the seed has %d", len(measuredNodes), len(instancetype.FlavorSizing))
	}
	p := instancetype.NewProvider("par", nil, nil)
	for _, m := range measuredNodes {
		s, ok := instancetype.SizingByName[m.flavor]
		if !ok {
			t.Errorf("measured flavor %s has no sizing seed", m.flavor)
			continue
		}
		if s.CPU != m.cpu || s.MemoryKi != m.memoryKi {
			t.Errorf("%s: seed cpu/memoryKi %d/%d, measured %d/%d", m.flavor, s.CPU, s.MemoryKi, m.cpu, m.memoryKi)
		}
		assertServed(t, p, m.flavor, m.cpu, m.memoryKi, m.allocatableKi)
		it, _ := p.Get(m.flavor)
		if pods := it.Allocatable()[corev1.ResourcePods]; pods.Value() != 110 {
			t.Errorf("%s: served pods %s, measured 110", m.flavor, pods.String())
		}
	}
}

func TestRecordObservedCapacityAcceptsLiveNodes(t *testing.T) {
	for _, m := range measuredNodes {
		p := instancetype.NewProvider("par", nil, nil)
		capacity, allocatable := nodeReport(m.cpu, m.memoryKi, m.allocatableKi)
		deviation, err := p.RecordObservedCapacity(m.flavor, capacity, allocatable)
		if err != nil {
			t.Fatalf("%s: a live node's own report was refused: %v", m.flavor, err)
		}
		if deviation != 0 {
			t.Errorf("%s: memory deviation %v from a seed measured on this very node, want 0", m.flavor, deviation)
		}
		assertServed(t, p, m.flavor, m.cpu, m.memoryKi, m.allocatableKi)
	}
}

func TestRecordObservedCapacityRefusesImplausibleReports(t *testing.T) {
	// A kubelet can rewrite its own Node's status, and whatever is recorded
	// becomes the flavor's capacity for the whole cluster. Each report below
	// cannot come from a 2XS VM; each used to rewrite the 2XS entry.
	setMemory := func(list corev1.ResourceList, ki int64) {
		list[corev1.ResourceMemory] = resource.MustParse(fmt.Sprintf("%dKi", ki))
	}
	cases := map[string]func(capacity, allocatable corev1.ResourceList){
		"forged 64 vCPU and 256Gi": func(capacity, allocatable corev1.ResourceList) {
			for _, list := range []corev1.ResourceList{capacity, allocatable} {
				list[corev1.ResourceCPU] = resource.MustParse("64")
				list[corev1.ResourceMemory] = resource.MustParse("256Gi")
			}
		},
		"cpu of another flavor, 2XS memory": func(capacity, allocatable corev1.ResourceList) {
			capacity[corev1.ResourceCPU] = resource.MustParse("8")
			allocatable[corev1.ResourceCPU] = resource.MustParse("8")
		},
		// The vCPU count is exact: one more core is within 10% of a larger
		// flavor, and here the allocatable alone looks like a 2XS.
		"one vCPU more, 2XS allocatable": func(capacity, _ corev1.ResourceList) {
			capacity[corev1.ResourceCPU] = resource.MustParse("5")
		},
		"memory 11% above the entry": func(capacity, allocatable corev1.ResourceList) {
			setMemory(capacity, 4124032)
			setMemory(allocatable, 4021632)
		},
		"memory 11% below the entry": func(capacity, allocatable corev1.ResourceList) {
			setMemory(capacity, 3306656)
			setMemory(allocatable, 3204256)
		},
		"allocatable memory 11% below the entry's": func(_, allocatable corev1.ResourceList) {
			setMemory(allocatable, 3215520)
		},
		"allocatable memory above capacity": func(_, allocatable corev1.ResourceList) {
			setMemory(allocatable, 3715345)
		},
		"pods inflated": func(capacity, allocatable corev1.ResourceList) {
			capacity[corev1.ResourcePods] = resource.MustParse("1000")
			allocatable[corev1.ResourcePods] = resource.MustParse("1000")
		},
		"pods deflated": func(capacity, allocatable corev1.ResourceList) {
			capacity[corev1.ResourcePods] = resource.MustParse("1")
			allocatable[corev1.ResourcePods] = resource.MustParse("1")
		},
		"ephemeral storage inflated": func(capacity, allocatable corev1.ResourceList) {
			capacity[corev1.ResourceEphemeralStorage] = resource.MustParse("400Gi")
			allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse("360Gi")
		},
		"resource missing from allocatable": func(_, allocatable corev1.ResourceList) {
			delete(allocatable, corev1.ResourcePods)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := instancetype.NewProvider("par", nil, nil)
			capacity, allocatable := nodeReport(4, 3715344, 3612944)
			mutate(capacity, allocatable)

			_, err := p.RecordObservedCapacity("2XS", capacity, allocatable)
			if !errors.Is(err, instancetype.ErrImplausibleCapacity) {
				t.Fatalf("expected ErrImplausibleCapacity, got %v", err)
			}
			assertServed(t, p, "2XS", 4, 3715344, 3612944)
			it, _ := p.Get("2XS")
			if pods := it.Capacity[corev1.ResourcePods]; pods.Value() != 110 {
				t.Errorf("served pods %s, want the entry's 110", pods.String())
			}
			if storage, want := it.Capacity[corev1.ResourceEphemeralStorage], resource.MustParse("40971488Ki"); storage.Cmp(want) != 0 {
				t.Errorf("served ephemeral storage %s, want the entry's %s", storage.String(), want.String())
			}
		})
	}
}

func TestRecordObservedCapacityToleratesImageDrift(t *testing.T) {
	// Nodes of one flavor legitimately differ by node image: the previous
	// image exposed 5.3% more memory than the current seed. Within 10% the
	// report is accepted and its deviation returned so the caller can flag
	// a stale entry.
	for _, tc := range []struct {
		name              string
		memoryKi, allocKi int64
	}{
		{"previous node image", 3911884, 3809484},
		{"9% below the entry", 3380963, 3278563},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := instancetype.NewProvider("par", nil, nil)
			capacity, allocatable := nodeReport(4, tc.memoryKi, tc.allocKi)
			deviation, err := p.RecordObservedCapacity("2XS", capacity, allocatable)
			if err != nil {
				t.Fatalf("report within tolerance refused: %v", err)
			}
			if want := float64(tc.memoryKi)/3715344 - 1; math.Abs(deviation-want) > 1e-9 {
				t.Errorf("deviation = %v, want %v", deviation, want)
			}
			assertServed(t, p, "2XS", 4, tc.memoryKi, tc.allocKi)
		})
	}
}

func TestRecordObservedCapacityKeepsTheSmallestNode(t *testing.T) {
	// Last-writer-wins made a mixed-image fleet flip the catalogue with
	// every node update, and let any node overwrite what the others
	// reported. The minimum is order-independent and never promises more
	// than the smallest node delivered — per resource.
	small, smallAlloc := nodeReport(12, 22000000, 21897600)
	big, bigAlloc := nodeReport(12, 23500000, 23397600)
	big[corev1.ResourcePods] = resource.MustParse("105")
	bigAlloc[corev1.ResourcePods] = resource.MustParse("105")

	for _, order := range [][2]string{{"small", "big"}, {"big", "small"}} {
		t.Run(order[0]+" then "+order[1], func(t *testing.T) {
			p := instancetype.NewProvider("par", nil, nil)
			reports := map[string][2]corev1.ResourceList{"small": {small, smallAlloc}, "big": {big, bigAlloc}}
			for _, name := range order {
				record(t, p, "L", reports[name][0], reports[name][1])
			}
			assertServed(t, p, "L", 12, 22000000, 21897600)
			it, _ := p.Get("L")
			if pods := it.Capacity[corev1.ResourcePods]; pods.Value() != 105 {
				t.Errorf("served pods capacity %s, want the smaller report's 105", pods.String())
			}
			if pods := it.Allocatable()[corev1.ResourcePods]; pods.Value() != 105 {
				t.Errorf("served pods allocatable %s, want the smaller report's 105", pods.String())
			}
		})
	}
}

func TestRecordObservedCapacityBoundsFollowTheServedEntry(t *testing.T) {
	// The reference is what the catalogue serves, so an operator's
	// settings.flavors pin moves the bounds with it: nodes consistent with
	// the pin correct it, nodes that contradict it are refused.
	p := instancetype.NewProvider("par", nil, []instancetype.FlavorOverride{{Name: "M", MemoryKi: ptr(int64(20000000))}})

	seedCapacity, seedAllocatable := nodeReport(10, 15229256, 15126856)
	if _, err := p.RecordObservedCapacity("M", seedCapacity, seedAllocatable); !errors.Is(err, instancetype.ErrImplausibleCapacity) {
		t.Fatalf("a report 24%% below the pinned entry must be refused, got %v", err)
	}
	assertServed(t, p, "M", 10, 20000000, 19897600)

	pinnedCapacity, pinnedAllocatable := nodeReport(10, 20500000, 20397600)
	record(t, p, "M", pinnedCapacity, pinnedAllocatable)
	assertServed(t, p, "M", 10, 20500000, 20397600)

	cpuPinned := instancetype.NewProvider("par", nil, []instancetype.FlavorOverride{{Name: "M", CPU: ptr(int64(12))}})
	if _, err := cpuPinned.RecordObservedCapacity("M", seedCapacity, seedAllocatable); !errors.Is(err, instancetype.ErrImplausibleCapacity) {
		t.Fatalf("a report whose cpu differs from the pinned entry must be refused, got %v", err)
	}
}

func TestRecordObservedCapacityBoundsAnUnservedSeedFlavorBySeed(t *testing.T) {
	// A known flavor the catalogue does not serve is described by
	// Synthesize with its seed sizing; the seed bounds its reports too.
	p := instancetype.NewProvider("par", []instancetype.Flavor{{Name: "M", CPU: 10, MemoryKi: 15229256}}, nil)
	forged, forgedAllocatable := nodeReport(64, 268435456, 268333056)
	if _, err := p.RecordObservedCapacity("2XS", forged, forgedAllocatable); !errors.Is(err, instancetype.ErrImplausibleCapacity) {
		t.Fatalf("expected ErrImplausibleCapacity, got %v", err)
	}
	if cpu := p.Synthesize("2XS").Capacity[corev1.ResourceCPU]; cpu.Value() != 4 {
		t.Errorf("synthesized 2XS cpu = %v, want the seed's 4", cpu.Value())
	}
}

func TestRecordObservedCapacityWithoutReferenceChecksConsistency(t *testing.T) {
	// An override-only flavor whose override was removed has nothing to be
	// bounded against. Its report only enriches Synthesize, but it must
	// still describe a node: an allocatable above capacity would serve a
	// negative overhead.
	p := instancetype.NewProvider("par", nil, nil)
	capacity := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("8Gi")}
	inflated := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("9Gi")}
	if _, err := p.RecordObservedCapacity("CUSTOM", capacity, inflated); !errors.Is(err, instancetype.ErrImplausibleCapacity) {
		t.Fatalf("expected ErrImplausibleCapacity, got %v", err)
	}
	if cpu := p.Synthesize("CUSTOM").Capacity[corev1.ResourceCPU]; !cpu.IsZero() {
		t.Errorf("refused report enriched the synthesis: cpu = %v", cpu.Value())
	}

	allocatable := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("7Gi")}
	deviation, err := p.RecordObservedCapacity("CUSTOM", capacity, allocatable)
	if err != nil {
		t.Fatalf("consistent report refused: %v", err)
	}
	if deviation != 0 {
		t.Errorf("deviation = %v without a catalogue entry, want 0", deviation)
	}
	if cpu := p.Synthesize("CUSTOM").Capacity[corev1.ResourceCPU]; cpu.Value() != 6 {
		t.Errorf("synthesized cpu = %v, want the observed 6", cpu.Value())
	}
}
