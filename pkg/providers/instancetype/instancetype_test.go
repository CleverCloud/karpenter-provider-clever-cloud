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
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

func ptr[T any](v T) *T { return &v }

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
	p := instancetype.NewProvider("par", nil, nil)

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
	p := instancetype.NewProvider("par", nil, nil)

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
	p := instancetype.NewProvider("par", nil, nil)

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
	p := instancetype.NewProvider("par", nil, nil)
	lookupsBefore := metricstest.Value(t, "karpenter_clevercloud_instancetype_unknown_flavor_lookups_total")

	if _, err := p.Get("3XL"); err == nil {
		t.Fatal("expected error for unknown flavor")
	}
	if delta := metricstest.Value(t, "karpenter_clevercloud_instancetype_unknown_flavor_lookups_total") - lookupsBefore; delta != 1 {
		t.Errorf("unknown_flavor_lookups_total delta = %v, want 1", delta)
	}
}

func TestRecordObservedCapacityOverridesEstimate(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
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
	p := instancetype.NewProvider("par", nil, nil)

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

func TestNewProviderNilFlavorsUsesDefault(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
	if got := len(p.List()); got != len(instancetype.DefaultFlavors) {
		t.Fatalf("expected %d flavors from default catalog, got %d", len(instancetype.DefaultFlavors), got)
	}
}

func TestNewProviderCustomBaseReplacesCatalog(t *testing.T) {
	custom := []instancetype.Flavor{
		{Name: "M", CPU: 10, MemoryKi: 15988992, PriceHourly: 0.1167},
		{Name: "CUSTOM", CPU: 2, MemoryKi: 2097152, PriceHourly: 0.01},
	}
	p := instancetype.NewProvider("par", custom, nil)

	its := p.List()
	if len(its) != len(custom) {
		t.Fatalf("expected %d flavors, got %d", len(custom), len(its))
	}
	// With no overrides, the base entirely defines the catalog; a default-only
	// flavor is gone.
	if _, err := p.Get("2XS"); err == nil {
		t.Error("expected 2XS to be absent from a custom base catalog")
	}
	it := findInstanceType(t, its, "CUSTOM")
	if got := it.Capacity.Cpu().Value(); got != 2 {
		t.Errorf("expected custom flavor 2 vCPU, got %d", got)
	}
	if got := it.Offerings[0].Price; got != 0.01 {
		t.Errorf("expected custom flavor price 0.01, got %v", got)
	}
}

func TestParseFlavorOverrides(t *testing.T) {
	t.Run("valid full", func(t *testing.T) {
		data := []byte(`
- name: M
  cpu: 10
  memoryKi: 15988992
  priceHourly: 0.1167
- name: XS
  cpu: 6
  memoryKi: 7937580
  priceHourly: 0.0611
`)
		overrides, err := instancetype.ParseFlavorOverrides(data)
		if err != nil {
			t.Fatalf("ParseFlavorOverrides: %v", err)
		}
		if len(overrides) != 2 {
			t.Fatalf("expected 2 overrides, got %d", len(overrides))
		}
		o := overrides[0]
		if o.Name != "M" || o.CPU == nil || *o.CPU != 10 || o.MemoryKi == nil || *o.MemoryKi != 15988992 || o.PriceHourly == nil || *o.PriceHourly != 0.1167 {
			t.Errorf("unexpected first override: %+v", o)
		}
	})

	t.Run("valid partial price only", func(t *testing.T) {
		overrides, err := instancetype.ParseFlavorOverrides([]byte("- name: M\n  priceHourly: 0.1\n"))
		if err != nil {
			t.Fatalf("ParseFlavorOverrides: %v", err)
		}
		o := overrides[0]
		if o.CPU != nil || o.MemoryKi != nil {
			t.Errorf("expected cpu/memoryKi unset, got %+v", o)
		}
		if o.PriceHourly == nil || *o.PriceHourly != 0.1 {
			t.Errorf("expected priceHourly 0.1, got %+v", o.PriceHourly)
		}
	})

	cases := map[string]string{
		"empty":          `[]`,
		"empty name":     "- name: \"\"\n  cpu: 4\n",
		"zero cpu":       "- name: M\n  cpu: 0\n",
		"zero memory":    "- name: M\n  memoryKi: 0\n",
		"negative price": "- name: M\n  priceHourly: -1\n",
		"duplicate name": "- name: M\n  cpu: 4\n- name: M\n  cpu: 8\n",
		"malformed":      "not: a list",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := instancetype.ParseFlavorOverrides([]byte(data)); err == nil {
				t.Errorf("expected error for %s, got nil", name)
			}
		})
	}
}

func TestComputePriceReproducesDefaultRounding(t *testing.T) {
	want := map[string]float64{"2XS": 0.0333, "XS": 0.0611, "S": 0.0889, "M": 0.1167, "L": 0.1667, "XL": 0.2222}
	for name, s := range instancetype.SizingByName {
		got := instancetype.ComputePrice(s.CPU, s.NominalGB, instancetype.DefaultVCPURate, instancetype.DefaultRAMRate)
		if got != want[name] {
			t.Errorf("flavor %s: ComputePrice = %v, want %v", name, got, want[name])
		}
	}
}

func TestDefaultFlavorsMatchSeed(t *testing.T) {
	if len(instancetype.DefaultFlavors) != len(instancetype.FlavorSizing) {
		t.Fatalf("DefaultFlavors (%d) and FlavorSizing (%d) length mismatch", len(instancetype.DefaultFlavors), len(instancetype.FlavorSizing))
	}
	for _, f := range instancetype.DefaultFlavors {
		s, ok := instancetype.SizingByName[f.Name]
		if !ok {
			t.Errorf("flavor %s has no sizing seed", f.Name)
			continue
		}
		if f.CPU != s.CPU || f.MemoryKi != s.MemoryKi {
			t.Errorf("flavor %s: cpu/memory diverge from seed (%d/%d vs %d/%d)", f.Name, f.CPU, f.MemoryKi, s.CPU, s.MemoryKi)
		}
		want := instancetype.ComputePrice(s.CPU, s.NominalGB, instancetype.DefaultVCPURate, instancetype.DefaultRAMRate)
		if f.PriceHourly != want {
			t.Errorf("flavor %s: hardcoded price %v diverges from seed-derived %v", f.Name, f.PriceHourly, want)
		}
	}
}

func TestApplyOverrides(t *testing.T) {
	base := instancetype.DefaultFlavors

	t.Run("price only", func(t *testing.T) {
		got, skipped := instancetype.ApplyOverrides(base, []instancetype.FlavorOverride{{Name: "M", PriceHourly: ptr(0.10)}})
		if len(skipped) != 0 {
			t.Fatalf("unexpected skipped: %v", skipped)
		}
		if len(got) != len(base) {
			t.Fatalf("expected %d flavors, got %d", len(base), len(got))
		}
		m := findFlavor(t, got, "M")
		if m.PriceHourly != 0.10 || m.CPU != 10 || m.MemoryKi != 15988992 {
			t.Errorf("expected only price overridden, got %+v", m)
		}
	})

	t.Run("dimension only", func(t *testing.T) {
		got, _ := instancetype.ApplyOverrides(base, []instancetype.FlavorOverride{{Name: "M", CPU: ptr(int64(99))}})
		m := findFlavor(t, got, "M")
		if m.CPU != 99 || m.PriceHourly != 0.1167 {
			t.Errorf("expected only cpu overridden, got %+v", m)
		}
	})

	t.Run("seed-only flavor added from override with no fields", func(t *testing.T) {
		// Base lacks L; an override naming L (no fields) seeds it from FlavorSizing.
		smallBase := []instancetype.Flavor{{Name: "M", CPU: 10, MemoryKi: 15988992, PriceHourly: 0.1167}}
		got, skipped := instancetype.ApplyOverrides(smallBase, []instancetype.FlavorOverride{{Name: "L"}})
		if len(skipped) != 0 {
			t.Fatalf("unexpected skipped: %v", skipped)
		}
		l := findFlavor(t, got, "L")
		if l.CPU != 12 || l.MemoryKi != 23983488 || l.PriceHourly != 0.1667 {
			t.Errorf("expected L seeded from sizing table, got %+v", l)
		}
	})

	t.Run("brand-new flavor fully specified", func(t *testing.T) {
		got, skipped := instancetype.ApplyOverrides(base, []instancetype.FlavorOverride{
			{Name: "CUSTOM", CPU: ptr(int64(2)), MemoryKi: ptr(int64(2097152)), PriceHourly: ptr(0.01)},
		})
		if len(skipped) != 0 {
			t.Fatalf("unexpected skipped: %v", skipped)
		}
		c := findFlavor(t, got, "CUSTOM")
		if c.CPU != 2 || c.MemoryKi != 2097152 || c.PriceHourly != 0.01 {
			t.Errorf("unexpected custom flavor: %+v", c)
		}
	})

	t.Run("non-constructible override skipped", func(t *testing.T) {
		got, skipped := instancetype.ApplyOverrides(base, []instancetype.FlavorOverride{{Name: "GHOST", PriceHourly: ptr(0.05)}})
		if len(skipped) != 1 || skipped[0] != "GHOST" {
			t.Fatalf("expected GHOST skipped, got %v", skipped)
		}
		for _, f := range got {
			if f.Name == "GHOST" {
				t.Errorf("GHOST must not appear in the result")
			}
		}
	})
}

func findFlavor(t *testing.T, flavors []instancetype.Flavor, name string) instancetype.Flavor {
	t.Helper()
	for _, f := range flavors {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("flavor %q not found", name)
	return instancetype.Flavor{}
}

func TestSetBaseFlavorsReappliesOverrides(t *testing.T) {
	overrides := []instancetype.FlavorOverride{{Name: "M", PriceHourly: ptr(0.10)}}
	p := instancetype.NewProvider("par", nil, overrides)

	it, err := p.Get("M")
	if err != nil {
		t.Fatalf("Get(M): %v", err)
	}
	if it.Offerings[0].Price != 0.10 {
		t.Errorf("expected pinned price 0.10 at startup, got %v", it.Offerings[0].Price)
	}

	// A dynamic refresh updates the base; the override must still win for M.
	p.SetBaseFlavors([]instancetype.Flavor{
		{Name: "M", CPU: 10, MemoryKi: 15988992, PriceHourly: 0.20},
		{Name: "XS", CPU: 6, MemoryKi: 7937580, PriceHourly: 0.07},
	})

	m, _ := p.Get("M")
	if m.Offerings[0].Price != 0.10 {
		t.Errorf("expected override to survive refresh (0.10), got %v", m.Offerings[0].Price)
	}
	xs, err := p.Get("XS")
	if err != nil {
		t.Fatalf("Get(XS): %v", err)
	}
	if xs.Offerings[0].Price != 0.07 {
		t.Errorf("expected refreshed XS price 0.07, got %v", xs.Offerings[0].Price)
	}
	// The refresh narrowed the base to M+XS; the old default-only flavors are gone.
	if _, err := p.Get("2XS"); err == nil {
		t.Error("expected 2XS gone after a base refresh that omitted it")
	}
}

func TestSetBaseFlavorsIgnoresEmpty(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
	before := len(p.List())

	p.SetBaseFlavors(nil)
	p.SetBaseFlavors([]instancetype.Flavor{})

	if got := len(p.List()); got != before {
		t.Errorf("empty SetBaseFlavors must keep the catalog: before %d, after %d", before, got)
	}
}

func TestProviderConcurrentAccess(t *testing.T) {
	p := instancetype.NewProvider("par", nil, []instancetype.FlavorOverride{{Name: "M", PriceHourly: ptr(0.10)}})
	capacity, allocatable := observedL(t)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if got := p.List(); len(got) == 0 {
					t.Errorf("List returned empty catalog under concurrency")
					return
				}
				_, _ = p.Get("M")
				p.RecordObservedCapacity("L", capacity, allocatable)
				p.SetBaseFlavors([]instancetype.Flavor{
					{Name: "M", CPU: 10, MemoryKi: 15988992, PriceHourly: 0.20},
					{Name: "L", CPU: 12, MemoryKi: 23983488, PriceHourly: 0.1667},
				})
			}
		}()
	}
	wg.Wait()
}

func TestRecordObservedCapacityDeepCopies(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
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
