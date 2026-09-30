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
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"

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

// observedL returns a plausible capacity/allocatable pair for an L node that
// differs from the catalogue entry (5% more memory, within the tolerance).
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

// record feeds an observation the test expects to be accepted.
func record(t *testing.T, p *instancetype.Provider, flavor string, capacity, allocatable corev1.ResourceList) {
	t.Helper()
	if _, err := p.RecordObservedCapacity(flavor, capacity, allocatable); err != nil {
		t.Fatalf("RecordObservedCapacity(%s): %v", flavor, err)
	}
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
		// Region and zone both carry the configured region, in the
		// requirements (the labels buildNodeClaim puts on the claim) and in
		// the offering (what core's scheduler and drift read).
		if len(it.Offerings) != 1 {
			t.Fatalf("flavor %s: expected one offering, got %d", it.Name, len(it.Offerings))
		}
		for _, key := range []string{corev1.LabelTopologyRegion, corev1.LabelTopologyZone} {
			for where, reqs := range map[string]scheduling.Requirements{"requirements": it.Requirements, "offering": it.Offerings[0].Requirements} {
				if !reqs.Has(key) || reqs.Get(key).Len() != 1 || !reqs.Get(key).Has("par") {
					t.Errorf("flavor %s: expected %s %s In [par], got %s", it.Name, where, key, reqs.Get(key))
				}
			}
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
	wantMemory := resource.MustParse("15229256Ki")
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

	record(t, p, "L", capacity, allocatable)

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
	record(t, p, "L", zeroCPU, zeroCPU)
	record(t, p, "L", zeroMemory, zeroMemory)

	it, err := p.Get("L")
	if err != nil {
		t.Fatalf("Get(L): %v", err)
	}
	if got := it.Capacity.Cpu().Value(); got != 12 {
		t.Errorf("expected the catalogue entry's cpu 12, got %d", got)
	}
	wantMemory := resource.MustParse("22896304Ki")
	if it.Capacity.Memory().Cmp(wantMemory) != 0 {
		t.Errorf("expected the catalogue entry's memory %s, got %s", wantMemory.String(), it.Capacity.Memory())
	}
	wantReserved := resource.MustParse("100Mi")
	gotReserved := it.Overhead.KubeReserved[corev1.ResourceMemory]
	if gotReserved.Cmp(wantReserved) != 0 {
		t.Errorf("expected static KubeReserved %s, got %s", wantReserved.String(), gotReserved.String())
	}
}

func TestRecordObservedCapacityDropsExtendedResources(t *testing.T) {
	// Extended resources come from per-node software (device plugins,
	// hugepages), not from the flavor's hardware: one plugin-equipped node
	// reporting them must not make the whole flavor advertise them, or
	// karpenter provisions fresh plugin-less nodes for pods that request
	// them — the pod stays Pending and the node sits empty and billed.
	p := instancetype.NewProvider("par", nil, nil)
	capacity, allocatable := observedL(t)
	gpu := corev1.ResourceName("nvidia.com/gpu")
	hugepages := corev1.ResourceName("hugepages-2Mi")
	capacity[gpu] = resource.MustParse("1")
	capacity[hugepages] = resource.MustParse("512Mi")
	allocatable[gpu] = resource.MustParse("1")
	allocatable[hugepages] = resource.MustParse("512Mi")

	record(t, p, "L", capacity, allocatable)

	it, err := p.Get("L")
	if err != nil {
		t.Fatalf("Get(L): %v", err)
	}
	for _, name := range []corev1.ResourceName{gpu, hugepages} {
		if _, ok := it.Capacity[name]; ok {
			t.Errorf("observed per-node resource %s leaked into the flavor's capacity", name)
		}
		if q, ok := it.Allocatable()[name]; ok && !q.IsZero() {
			t.Errorf("observed per-node resource %s leaked into the flavor's allocatable: %s", name, q.String())
		}
	}
	// The cpu/memory correction must still apply alongside the filtering.
	if it.Capacity.Memory().Cmp(*capacity.Memory()) != 0 {
		t.Errorf("expected observed memory capacity %s, got %s", capacity.Memory(), it.Capacity.Memory())
	}
	if it.Capacity.Cpu().Cmp(*capacity.Cpu()) != 0 {
		t.Errorf("expected observed cpu capacity %s, got %s", capacity.Cpu(), it.Capacity.Cpu())
	}
	for _, listed := range p.List() {
		if listed.Name != "L" {
			continue
		}
		if _, ok := listed.Capacity[gpu]; ok {
			t.Errorf("observed per-node resource %s leaked into List() output", gpu)
		}
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
		{Name: "M", CPU: 10, MemoryKi: 15988992},
		// A price carried by the base is never served: it is derived from
		// cpu and memoryKi like every other.
		{Name: "CUSTOM", CPU: 2, MemoryKi: 2097152, Price: 0.01},
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
	if got, want := it.Offerings[0].Price, instancetype.RelativePrice(2, 2097152); got != want {
		t.Errorf("custom flavor price = %v, want %v derived from its cpu and memoryKi", got, want)
	}
}

func TestParseFlavorOverrides(t *testing.T) {
	t.Run("valid full", func(t *testing.T) {
		data := []byte(`
- name: M
  cpu: 10
  memoryKi: 15988992
- name: XS
  cpu: 6
  memoryKi: 7937580
`)
		overrides, err := instancetype.ParseFlavorOverrides(data)
		if err != nil {
			t.Fatalf("ParseFlavorOverrides: %v", err)
		}
		if len(overrides) != 2 {
			t.Fatalf("expected 2 overrides, got %d", len(overrides))
		}
		o := overrides[0]
		if o.Name != "M" || o.CPU == nil || *o.CPU != 10 || o.MemoryKi == nil || *o.MemoryKi != 15988992 {
			t.Errorf("unexpected first override: %+v", o)
		}
	})

	t.Run("valid partial memory only", func(t *testing.T) {
		overrides, err := instancetype.ParseFlavorOverrides([]byte("- name: M\n  memoryKi: 15229256\n"))
		if err != nil {
			t.Fatalf("ParseFlavorOverrides: %v", err)
		}
		o := overrides[0]
		if o.CPU != nil {
			t.Errorf("expected cpu unset, got %+v", o)
		}
		if o.MemoryKi == nil || *o.MemoryKi != 15229256 {
			t.Errorf("expected memoryKi 15229256, got %+v", o.MemoryKi)
		}
	})

	t.Run("mistyped field key", func(t *testing.T) {
		// Non-strict unmarshaling used to drop the unknown key silently:
		// every field is optional, so "memory" instead of "memoryKi"
		// decoded into an all-nil override that passed every validation —
		// the operator's pin never applied and nothing surfaced it.
		_, err := instancetype.ParseFlavorOverrides([]byte("- name: M\n  memory: 15229256\n"))
		if err == nil {
			t.Fatal("expected an error for a mistyped field key")
		}
		if !strings.Contains(err.Error(), "memory") {
			t.Errorf("error must name the unknown field so the operator can fix it: %v", err)
		}
	})

	t.Run("legacy priceHourly key", func(t *testing.T) {
		// Prices are derived from cpu and memoryKi now. A file written for
		// an earlier release must be refused as a whole, and name the key,
		// rather than load with the operator's price silently dropped.
		_, err := instancetype.ParseFlavorOverrides([]byte("- name: M\n  cpu: 10\n  priceHourly: 0.1167\n"))
		if err == nil {
			t.Fatal("expected an error for the removed priceHourly key")
		}
		if !strings.Contains(err.Error(), "priceHourly") {
			t.Errorf("error must name the removed field so the operator can fix it: %v", err)
		}
	})

	cases := map[string]string{
		"empty":          `[]`,
		"empty name":     "- name: \"\"\n  cpu: 4\n",
		"zero cpu":       "- name: M\n  cpu: 0\n",
		"zero memory":    "- name: M\n  memoryKi: 0\n",
		"negative cpu":   "- name: M\n  cpu: -4\n",
		"duplicate name": "- name: M\n  cpu: 4\n- name: M\n  cpu: 8\n",
		"duplicate key":  "- name: M\n  memoryKi: 15229256\n  memoryKi: 15988992\n",
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

func TestRelativePrice(t *testing.T) {
	ref := instancetype.FlavorSizing[0].Sizing
	for _, tc := range []struct {
		name          string
		cpu, memoryKi int64
		want          float64
	}{
		{"the reference flavor costs 1", ref.CPU, ref.MemoryKi, 1},
		{"twice the cpu adds a third", 2 * ref.CPU, ref.MemoryKi, 1.3333},
		{"twice the memory adds two thirds", ref.CPU, 2 * ref.MemoryKi, 1.6667},
		{"twice both doubles the price", 2 * ref.CPU, 2 * ref.MemoryKi, 2},
		{"a name-only floor is free", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := instancetype.RelativePrice(tc.cpu, tc.memoryKi); got != tc.want {
				t.Errorf("RelativePrice(%d, %d) = %v, want %v", tc.cpu, tc.memoryKi, got, tc.want)
			}
		})
	}
}

// TestDefaultFlavorsPriceTable pins the prices the built-in catalogue serves.
// A change to FlavorSizing or to RelativePrice moves them: update the table
// on purpose, after TestRelativePriceKeepsConsolidationOrder has confirmed
// that the new prices still order every consolidation the right way.
func TestDefaultFlavorsPriceTable(t *testing.T) {
	want := []struct {
		name  string
		price float64
	}{
		{"2XS", 1},
		{"XS", 1.8554},
		{"S", 2.7097},
		{"M", 3.566},
		{"L", 5.1084},
		{"XL", 6.8212},
	}
	if len(instancetype.DefaultFlavors) != len(want) {
		t.Fatalf("DefaultFlavors has %d flavors, the pinned table %d", len(instancetype.DefaultFlavors), len(want))
	}
	for i, f := range instancetype.DefaultFlavors {
		if f.Name != want[i].name || f.Price != want[i].price {
			t.Errorf("DefaultFlavors[%d] = %s at %v, want %s at %v", i, f.Name, f.Price, want[i].name, want[i].price)
		}
		if i > 0 && f.Price <= instancetype.DefaultFlavors[i-1].Price {
			t.Errorf("%s (%v) must cost more than the smaller %s (%v)", f.Name, f.Price, instancetype.DefaultFlavors[i-1].Name, instancetype.DefaultFlavors[i-1].Price)
		}
	}
}

// nominalGB is the memory CKE advertises for each built-in flavor. Its public
// worker price is proportional to cpu + 2*nominalGB (a nominal GB costs as
// much as two vCPUs): that is the order the relative prices must reproduce.
var nominalGB = map[string]int64{"2XS": 4, "XS": 8, "S": 12, "M": 16, "L": 24, "XL": 32}

// flavorMultisets returns every multiset of 1 to maxSize indexes into a
// catalogue of n flavors, each as a non-decreasing index slice.
func flavorMultisets(n, maxSize int) [][]int {
	var out [][]int
	var grow func(prefix []int, from int)
	grow = func(prefix []int, from int) {
		if len(prefix) > 0 {
			out = append(out, append([]int(nil), prefix...))
		}
		if len(prefix) == maxSize {
			return
		}
		for i := from; i < n; i++ {
			grow(append(prefix, i), i)
		}
	}
	grow(nil, 0)
	return out
}

// orderMismatches counts the pairs of multisets of up to 4 of flavors that the
// public price structure ranks strictly while price (in ten-thousandths, so
// sums are exact) ranks them the other way or ties them.
func orderMismatches(t *testing.T, flavors []instancetype.Flavor, price func(instancetype.Flavor) int64) int {
	t.Helper()
	sets := flavorMultisets(len(flavors), 4)
	public := make([]int64, len(sets))
	relative := make([]int64, len(sets))
	for i, set := range sets {
		for _, idx := range set {
			gb, ok := nominalGB[flavors[idx].Name]
			if !ok {
				t.Fatalf("flavor %s has no nominal memory in this test: add it to nominalGB", flavors[idx].Name)
			}
			public[i] += flavors[idx].CPU + 2*gb
			relative[i] += price(flavors[idx])
		}
	}
	mismatches := 0
	for a := range sets {
		for b := range sets {
			if public[a] < public[b] && relative[a] >= relative[b] {
				mismatches++
			}
		}
	}
	return mismatches
}

// TestRelativePriceKeepsConsolidationOrder is the property the pricing model
// exists for. karpenter-core sums the prices of the nodes it could remove and
// compares the total with the price of their replacement: if the relative
// prices ranked two sets of nodes differently from the prices CKE charges,
// consolidation would replace nodes with capacity that really costs more, or
// never make a saving that is really there. Every multiset of up to four
// built-in flavors is compared with every other; public ties are left out,
// since either choice costs the same.
func TestRelativePriceKeepsConsolidationOrder(t *testing.T) {
	served := func(f instancetype.Flavor) int64 { return int64(math.Round(f.Price * 1e4)) }
	if n := orderMismatches(t, instancetype.DefaultFlavors, served); n != 0 {
		t.Errorf("the relative prices order %d pairs of flavor sets differently from the public prices", n)
	}

	// The check is not vacuous: weighting cpu and memory equally, the
	// obvious alternative, inverts some of those comparisons.
	ref := instancetype.FlavorSizing[0].Sizing
	equalWeights := func(f instancetype.Flavor) int64 {
		return int64(math.Round((float64(f.CPU)/float64(ref.CPU)/2 + float64(f.MemoryKi)/float64(ref.MemoryKi)/2) * 1e4))
	}
	if n := orderMismatches(t, instancetype.DefaultFlavors, equalWeights); n == 0 {
		t.Error("equal cpu and memory weights must fail the ordering check, or the check proves nothing")
	}

	// Overrides reprice their flavor against the unchanged 2XS reference.
	// When the node image moves, the README tells operators to pin every
	// flavor to the memory its nodes then report. Pinning every flavor to
	// what the previous image reported (the seed before the 2026-09-30
	// measurements, 4.4-5.0% above today's) keeps the property; the same
	// pin on XL alone, or 5% below its built-in memory, does not, which is
	// why the docs say to pin every flavor the same way, or none.
	previousImageMemoryKi := map[string]int64{
		"2XS": 3911884, "XS": 7937580, "S": 11957148, "M": 15988992, "L": 23983488, "XL": 31977984,
	}
	var allPinned []instancetype.FlavorOverride
	for _, s := range instancetype.FlavorSizing {
		allPinned = append(allPinned, instancetype.FlavorOverride{Name: s.Name, MemoryKi: ptr(previousImageMemoryKi[s.Name])})
	}
	pinned, skipped := instancetype.ApplyOverrides(instancetype.DefaultFlavors, allPinned)
	if len(skipped) != 0 {
		t.Fatalf("unexpected skipped overrides: %v", skipped)
	}
	if n := orderMismatches(t, pinned, served); n != 0 {
		t.Errorf("pinning every flavor to the previous image's memory orders %d pairs of flavor sets differently from the public prices", n)
	}
	xl := instancetype.SizingByName["XL"].MemoryKi
	for name, memoryKi := range map[string]int64{"the previous image's": previousImageMemoryKi["XL"], "5% less": xl * 95 / 100} {
		xlOnly, _ := instancetype.ApplyOverrides(instancetype.DefaultFlavors, []instancetype.FlavorOverride{{Name: "XL", MemoryKi: ptr(memoryKi)}})
		if n := orderMismatches(t, xlOnly, served); n == 0 {
			t.Errorf("pinning XL alone to %s memory was expected to reorder some flavor sets; if it no longer does, revisit the README's pin-every-flavor advice", name)
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
		if want := instancetype.RelativePrice(s.CPU, s.MemoryKi); f.Price != want {
			t.Errorf("flavor %s: price %v diverges from the seed-derived %v", f.Name, f.Price, want)
		}
	}
}

func TestApplyOverrides(t *testing.T) {
	base := instancetype.DefaultFlavors

	t.Run("memory only reprices the flavor", func(t *testing.T) {
		got, skipped := instancetype.ApplyOverrides(base, []instancetype.FlavorOverride{{Name: "M", MemoryKi: ptr(int64(15988992))}})
		if len(skipped) != 0 {
			t.Fatalf("unexpected skipped: %v", skipped)
		}
		if len(got) != len(base) {
			t.Fatalf("expected %d flavors, got %d", len(base), len(got))
		}
		m := findFlavor(t, got, "M")
		if m.CPU != 10 || m.MemoryKi != 15988992 {
			t.Errorf("expected only memoryKi overridden, got %+v", m)
		}
		// The price follows the pinned memory, not the seed's.
		if want := instancetype.RelativePrice(10, 15988992); m.Price != want {
			t.Errorf("M price = %v, want %v derived from the pinned memoryKi", m.Price, want)
		}
		if seed := findFlavor(t, base, "M"); m.Price <= seed.Price {
			t.Errorf("more memory must cost more: pinned M at %v, seed M at %v", m.Price, seed.Price)
		}
	})

	t.Run("cpu only reprices the flavor", func(t *testing.T) {
		got, _ := instancetype.ApplyOverrides(base, []instancetype.FlavorOverride{{Name: "M", CPU: ptr(int64(99))}})
		m := findFlavor(t, got, "M")
		if m.CPU != 99 || m.MemoryKi != 15229256 {
			t.Errorf("expected only cpu overridden, got %+v", m)
		}
		if want := instancetype.RelativePrice(99, 15229256); m.Price != want {
			t.Errorf("M price = %v, want %v derived from the pinned cpu", m.Price, want)
		}
	})

	t.Run("seed-only flavor added from override with no fields", func(t *testing.T) {
		// Base lacks L; an override naming L (no fields) seeds it from FlavorSizing.
		smallBase := []instancetype.Flavor{{Name: "M", CPU: 10, MemoryKi: 15988992}}
		got, skipped := instancetype.ApplyOverrides(smallBase, []instancetype.FlavorOverride{{Name: "L"}})
		if len(skipped) != 0 {
			t.Fatalf("unexpected skipped: %v", skipped)
		}
		l := findFlavor(t, got, "L")
		if l.CPU != 12 || l.MemoryKi != 22896304 || l.Price != findFlavor(t, base, "L").Price {
			t.Errorf("expected L seeded from sizing table and priced like the built-in L, got %+v", l)
		}
	})

	t.Run("brand-new flavor priced from its sizing", func(t *testing.T) {
		got, skipped := instancetype.ApplyOverrides(base, []instancetype.FlavorOverride{
			{Name: "CUSTOM", CPU: ptr(int64(2)), MemoryKi: ptr(int64(2097152))},
		})
		if len(skipped) != 0 {
			t.Fatalf("unexpected skipped: %v", skipped)
		}
		c := findFlavor(t, got, "CUSTOM")
		if c.CPU != 2 || c.MemoryKi != 2097152 || c.Price != instancetype.RelativePrice(2, 2097152) {
			t.Errorf("unexpected custom flavor: %+v", c)
		}
	})

	t.Run("non-constructible override skipped", func(t *testing.T) {
		got, skipped := instancetype.ApplyOverrides(base, []instancetype.FlavorOverride{{Name: "GHOST", CPU: ptr(int64(2))}})
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

func TestNewProviderOverlaysOverridesOnTheSeed(t *testing.T) {
	p := instancetype.NewProvider("par", nil, []instancetype.FlavorOverride{{Name: "M", MemoryKi: ptr(int64(15988992))}})

	if got := len(p.List()); got != len(instancetype.DefaultFlavors) {
		t.Fatalf("a memory-only override must not change the catalogue size: got %d flavors, want %d", got, len(instancetype.DefaultFlavors))
	}
	m, err := p.Get("M")
	if err != nil {
		t.Fatalf("Get(M): %v", err)
	}
	if want := resource.MustParse("15988992Ki"); m.Capacity.Memory().Cmp(want) != 0 {
		t.Errorf("expected the pinned M memory %s, got %s", want.String(), m.Capacity.Memory())
	}
	if want := instancetype.RelativePrice(10, 15988992); m.Offerings[0].Price != want {
		t.Errorf("the offering must carry the price of the pinned sizing %v, got %v", want, m.Offerings[0].Price)
	}
	if got := m.Capacity.Cpu().Value(); got != 10 {
		t.Errorf("unset override fields must fall through to the seed: M cpu = %d, want 10", got)
	}
	xs, err := p.Get("XS")
	if err != nil {
		t.Fatalf("Get(XS): %v", err)
	}
	if want := findFlavor(t, instancetype.DefaultFlavors, "XS").Price; xs.Offerings[0].Price != want {
		t.Errorf("a flavor without an override must keep its seed price %v, got %v", want, xs.Offerings[0].Price)
	}
}

func TestProviderConcurrentAccess(t *testing.T) {
	p := instancetype.NewProvider("par", nil, []instancetype.FlavorOverride{{Name: "M", MemoryKi: ptr(int64(15229256))}})
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
				if _, err := p.RecordObservedCapacity("L", capacity, allocatable); err != nil {
					t.Errorf("RecordObservedCapacity: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestRecordObservedCapacityDeepCopies(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)
	capacity, allocatable := observedL(t)

	record(t, p, "L", capacity, allocatable)

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

func TestParseFlavorOverridesRequiresCompleteNewFlavor(t *testing.T) {
	// A name outside the seed introduces a new flavor: cpu and memoryKi are
	// all it can be sized and priced from.
	for name, data := range map[string]string{
		"without memoryKi": "- name: CUSTOM\n  cpu: 2\n",
		"without cpu":      "- name: CUSTOM\n  memoryKi: 2097152\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := instancetype.ParseFlavorOverrides([]byte(data)); err == nil {
				t.Fatalf("expected an error for a new flavor %s", name)
			}
		})
	}
	if _, err := instancetype.ParseFlavorOverrides([]byte("- name: CUSTOM\n  cpu: 2\n  memoryKi: 2097152\n")); err != nil {
		t.Fatalf("complete new flavor must parse: %v", err)
	}
}

func TestApplyOverridesSkipsUnsizedNewFlavor(t *testing.T) {
	// Defense in depth below the parse-time gate: a from-scratch flavor
	// missing a dimension has nothing to be sized or priced from, and would
	// enter the catalogue free of charge.
	base := []instancetype.Flavor{{Name: "M", CPU: 10, MemoryKi: 15988992}}
	got, skipped := instancetype.ApplyOverrides(base, []instancetype.FlavorOverride{
		{Name: "CUSTOM", CPU: ptr(int64(2))},
		{Name: "OTHER", MemoryKi: ptr(int64(2097152))},
	})
	if len(skipped) != 2 || skipped[0] != "CUSTOM" || skipped[1] != "OTHER" {
		t.Fatalf("expected CUSTOM and OTHER skipped, got %v", skipped)
	}
	for _, f := range got {
		if f.Name == "CUSTOM" || f.Name == "OTHER" {
			t.Errorf("unsized new flavor %s must not appear in the result", f.Name)
		}
	}
}

func TestSynthesizeServesDegradedTypesWithoutTouchingTheCatalog(t *testing.T) {
	p := instancetype.NewProvider("par", nil, nil)

	t.Run("seed-known flavor keeps seed sizing", func(t *testing.T) {
		it := p.Synthesize("2XS")
		if it.Name != "2XS" {
			t.Fatalf("name = %q", it.Name)
		}
		if cpu := it.Capacity[corev1.ResourceCPU]; cpu.Value() != 4 {
			t.Errorf("cpu = %v, want the 2XS seed value 4", cpu.Value())
		}
		if got, want := it.Offerings[0].Price, findFlavor(t, instancetype.DefaultFlavors, "2XS").Price; got != want {
			t.Errorf("price = %v, want the built-in 2XS price %v", got, want)
		}
	})

	t.Run("unknown flavor gets a name-only floor", func(t *testing.T) {
		it := p.Synthesize("CUSTOM")
		if it.Name != "CUSTOM" {
			t.Fatalf("name = %q", it.Name)
		}
		if cpu := it.Capacity[corev1.ResourceCPU]; !cpu.IsZero() {
			t.Errorf("expected zero cpu, got %v", cpu.Value())
		}
		// The degraded node still describes where it runs.
		if got := it.Requirements.Get(corev1.LabelTopologyRegion); !got.Has("par") {
			t.Errorf("region requirement = %s, want par", got)
		}
	})

	t.Run("observed capacity enriches the synthesis", func(t *testing.T) {
		record(t, p, "CUSTOM",
			corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("8Gi")},
			corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("7Gi")},
		)
		it := p.Synthesize("CUSTOM")
		if cpu := it.Capacity[corev1.ResourceCPU]; cpu.Value() != 6 {
			t.Errorf("cpu = %v, want observed 6", cpu.Value())
		}
	})

	t.Run("the served catalogue is untouched", func(t *testing.T) {
		if _, err := p.Get("CUSTOM"); err == nil {
			t.Error("Get must stay strict for unknown flavors")
		}
		for _, it := range p.List() {
			if it.Name == "CUSTOM" {
				t.Error("Synthesize must not add the flavor to List()")
			}
		}
	})
}

func TestLoadFlavorsOrDegradeNeverFails(t *testing.T) {
	t.Run("empty path is a no-op", func(t *testing.T) {
		if got := instancetype.LoadFlavorsOrDegrade(""); got != nil {
			t.Errorf("expected nil overrides, got %v", got)
		}
		if v := metricstest.Value(t, "karpenter_clevercloud_instancetype_flavors_config_invalid"); v != 0 {
			t.Errorf("flavors_config_invalid = %v, want 0", v)
		}
	})

	t.Run("invalid file degrades and raises the gauge", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "flavors.yaml")
		if err := os.WriteFile(path, []byte("not: a list"), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := instancetype.LoadFlavorsOrDegrade(path); got != nil {
			t.Errorf("expected nil overrides on invalid file, got %v", got)
		}
		if v := metricstest.Value(t, "karpenter_clevercloud_instancetype_flavors_config_invalid"); v != 1 {
			t.Errorf("flavors_config_invalid = %v, want 1", v)
		}
	})

	// An unknown key degrades the whole file: the controller serves the
	// untouched base catalogue, never a half-applied one.
	for name, data := range map[string]string{
		// "memory" instead of "memoryKi" used to parse into an all-nil
		// override: the file loaded "successfully", the gauge stayed 0 and
		// the operator's pin never applied. Strict parsing turns it into
		// the same loud degradation as any other invalid file.
		"mistyped field key degrades instead of silently no-oping": "- name: M\n  memory: 15988992\n",
		// A file written for a release that still took priceHourly: the
		// whole file is refused, its memoryKi pin included, and the gauge
		// tells the operator to remove the key.
		"legacy priceHourly key degrades": "- name: M\n  memoryKi: 15988992\n  priceHourly: 0.1167\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "flavors.yaml")
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			got := instancetype.LoadFlavorsOrDegrade(path)
			if got != nil {
				t.Errorf("expected nil overrides, got %v", got)
			}
			if v := metricstest.Value(t, "karpenter_clevercloud_instancetype_flavors_config_invalid"); v != 1 {
				t.Errorf("flavors_config_invalid = %v, want 1", v)
			}
			p := instancetype.NewProvider("par", nil, got)
			m, err := p.Get("M")
			if err != nil {
				t.Fatalf("Get(M): %v", err)
			}
			if want := resource.MustParse("15229256Ki"); m.Capacity.Memory().Cmp(want) != 0 {
				t.Errorf("expected the base M memory %s, got %s", want.String(), m.Capacity.Memory())
			}
			if want := findFlavor(t, instancetype.DefaultFlavors, "M").Price; m.Offerings[0].Price != want {
				t.Errorf("expected the base price %v, got %v", want, m.Offerings[0].Price)
			}
		})
	}

	t.Run("valid file loads and clears the gauge", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "flavors.yaml")
		if err := os.WriteFile(path, []byte("- name: M\n  memoryKi: 15229256\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		got := instancetype.LoadFlavorsOrDegrade(path)
		if len(got) != 1 || got[0].Name != "M" {
			t.Errorf("unexpected overrides: %v", got)
		}
		if v := metricstest.Value(t, "karpenter_clevercloud_instancetype_flavors_config_invalid"); v != 0 {
			t.Errorf("flavors_config_invalid = %v, want 0", v)
		}
	})
}
