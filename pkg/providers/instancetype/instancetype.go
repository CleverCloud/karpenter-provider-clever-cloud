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

// Package instancetype exposes the Clever Kubernetes Engine node flavors as
// Karpenter instance types.
//
// Capacity figures for every flavor were measured on live CKE nodes
// (status.capacity on 2026-09-30: Kubernetes 1.37.0, kernel 7.2.8). The
// kernel-visible memory moves with the platform's node image, so nodes of this
// provider's NodeGroups keep correcting it at runtime, within bounds
// (RecordObservedCapacity). Offering prices are not a currency: they are a
// unitless cost relative to the smallest flavor, derived from each flavor's cpu
// and memory (RelativePrice). The served catalogue is this static seed with the
// operator's settings.flavors overrides on top: nothing is fetched from a
// Clever Cloud endpoint at runtime.
package instancetype

import (
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
	"sigs.k8s.io/yaml"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics"
)

const (
	// cpuPriceWeight and memoryPriceWeight are the shares of cpu and memory in
	// RelativePrice. They sum to 1, so the reference flavor costs exactly 1.
	cpuPriceWeight    = 1.0 / 3
	memoryPriceWeight = 2.0 / 3

	// observedTolerance is how far a node's reported memory, ephemeral
	// storage and pod capacity, and every allocatable figure, may stray from
	// what its flavor's catalogue entry advertises before
	// RecordObservedCapacity refuses the report (cpu capacity must be equal). The VMs of a flavor are
	// identical, and the two node images measured so far differ by at most
	// 5.3%; a report beyond 10% does not describe a VM of that flavor.
	observedTolerance = 0.10
)

// Flavor describes one Clever Cloud node flavor.
type Flavor struct {
	// Name as accepted by the NodeGroup API (uppercase).
	Name string `json:"name"`
	// CPU is the number of vCPUs.
	CPU int64 `json:"cpu"`
	// MemoryKi is the kernel-visible memory capacity in KiB.
	MemoryKi int64 `json:"memoryKi"`
	// Price is the unitless relative cost RelativePrice(CPU, MemoryKi).
	// ApplyOverrides recomputes it from the final CPU and MemoryKi, so it
	// always matches the sizing it is served with.
	Price float64 `json:"price"`
}

// Sizing is the static per-flavor seed: vCPU count and kernel-visible memory
// (KiB). Both price the flavor through RelativePrice. MemoryKi additionally
// self-corrects at runtime, within bounds, via RecordObservedCapacity (CPU
// must match it exactly); the price does not follow, so it stays stable for
// the controller's lifetime.
type Sizing struct {
	CPU      int64
	MemoryKi int64
}

// AtLeast reports whether s is at least as large as o in both cpu and memory:
// whatever capacity o does not fit in, s does not fit in either.
func (s Sizing) AtLeast(o Sizing) bool {
	return s.CPU >= o.CPU && s.MemoryKi >= o.MemoryKi
}

var (
	// FlavorSizing is the canonical sizing table (ordered smallest-to-largest).
	// DefaultFlavors, ApplyOverrides and Synthesize all derive from it; it is
	// the single source of truth for per-flavor sizing.
	FlavorSizing = []struct {
		Name string
		Sizing
	}{
		// MemoryKi is status.capacity.memory measured on one live node of each
		// flavor on 2026-09-30 (Kubernetes 1.37.0, kernel 7.2.8). The previous
		// values came from an older node image — L and XL extrapolated from M —
		// and nodes of the current one expose 4.4-5.0% less than they said.
		// Until a node of a flavor reports, karpenter packs pods against this
		// table: an overstated entry launches nodes the pods do not fit on.
		{"2XS", Sizing{CPU: 4, MemoryKi: 3715344}},
		{"XS", Sizing{CPU: 6, MemoryKi: 7553664}},
		{"S", Sizing{CPU: 8, MemoryKi: 11385832}},
		{"M", Sizing{CPU: 10, MemoryKi: 15229256}},
		{"L", Sizing{CPU: 12, MemoryKi: 22896304}},
		{"XL", Sizing{CPU: 16, MemoryKi: 30584176}},
	}

	// priceReference is the flavor every price is relative to: the smallest
	// built-in one (FlavorSizing is ordered smallest-to-largest), which costs
	// exactly 1.
	priceReference = FlavorSizing[0].Sizing

	// SizingByName indexes FlavorSizing for O(1) lookup by flavor name.
	SizingByName = func() map[string]Sizing {
		m := make(map[string]Sizing, len(FlavorSizing))
		for _, s := range FlavorSizing {
			m[s.Name] = s.Sizing
		}
		return m
	}()

	// DefaultFlavors is the built-in CKE flavor catalog, the base the
	// settings.flavors overrides are overlaid on. It is derived from
	// FlavorSizing, prices included, so the two cannot drift;
	// TestDefaultFlavorsPriceTable pins the prices that result.
	DefaultFlavors = func() []Flavor {
		flavors := make([]Flavor, 0, len(FlavorSizing))
		for _, s := range FlavorSizing {
			flavors = append(flavors, Flavor{Name: s.Name, CPU: s.CPU, MemoryKi: s.MemoryKi, Price: RelativePrice(s.CPU, s.MemoryKi)})
		}
		return flavors
	}()

	// All flavors share the same disk and pod capacity (measured).
	ephemeralStorage = resource.MustParse("40971488Ki")
	maxPods          = resource.MustParse("110")

	// Measured overhead: exactly 100MiB of memory reserved for the kubelet,
	// and a 10% hard eviction threshold on ephemeral storage.
	kubeReservedMemory         = resource.MustParse("100Mi")
	evictionEphemeralThreshold = resource.MustParse("4097149Ki")
)

// RelativePrice is the price of every offering: the unitless cost of a flavor
// with cpu vCPUs and memoryKi KiB of kernel-visible memory, relative to the
// smallest built-in flavor (priceReference, which costs exactly 1):
//
//	price = 1/3 * cpu/cpuRef + 2/3 * memoryKi/memoryKiRef
//
// rounded to 4 decimals. karpenter-core only compares and sums offering
// prices (cheapest-first launches, consolidation savings, and balanced
// scoring, which divides by the NodePool total), so their unit does not
// matter; their ordering does. In CKE's public worker price structure a
// nominal GB of memory costs as much as two vCPUs, so memory makes up 2/3 of
// the reference flavor's price and cpu 1/3. With those weights, whenever the
// public prices rank one combination of up to four built-in flavors strictly
// below another, so does the relative price
// (TestRelativePriceKeepsConsolidationOrder). Equal weights would invert 12 of
// those comparisons and consolidate several small nodes into an L or XL that
// really costs more. The reference is fixed, so an override reprices its
// flavor alone: a catalogue keeps this property only while its cpu and
// memoryKi pins stay proportionate across flavors.
func RelativePrice(cpu, memoryKi int64) float64 {
	price := cpuPriceWeight*float64(cpu)/float64(priceReference.CPU) +
		memoryPriceWeight*float64(memoryKi)/float64(priceReference.MemoryKi)
	return math.Round(price*1e4) / 1e4
}

// FlavorNamePattern is the shape of a Clever Cloud flavor name, which every
// settings.flavors override name must have (ParseFlavorOverrides; the chart's
// values.schema.json carries the same pattern). Clever Cloud names its flavors
// as T-shirt sizes: S, M and L, with S and L extended by an X prefix that a
// digit multiplies. The NodeGroup API's spec.flavor is an uppercase-only enum
// of 2XS, XS, S, M, L and XL, and Clever Cloud's application instances carry
// the same scheme on to 2XL and 3XL. Nothing of another shape can ever be
// created: a case typo (2xs), a made-up name (CUSTOM) or a spelling the
// platform does not use (XXL for 2XL) would enter the catalogue, could win
// cheapest-first, and fail every launch at admission. A new name of that
// shape stays possible, so a flavor Clever Cloud adds (2XL) can be declared
// before a release of this provider carries it; until the NodeGroup API
// accepts it, nodegroup.Provider.Create turns the admission refusal into a
// held-out flavor.
const FlavorNamePattern = `^(M|([2-9]?X)?[SL])$`

var flavorNameRegexp = regexp.MustCompile(FlavorNamePattern)

// flavorNameError explains why name cannot be a Clever Cloud flavor, naming
// the uppercase spelling when that one could be.
func flavorNameError(name string) error {
	if upper := strings.ToUpper(name); flavorNameRegexp.MatchString(upper) {
		return fmt.Errorf("flavor %q: Clever Cloud flavor names are uppercase, did you mean %q?", name, upper)
	}
	builtin := make([]string, 0, len(FlavorSizing))
	for _, s := range FlavorSizing {
		builtin = append(builtin, s.Name)
	}
	return fmt.Errorf("flavor %q cannot be a Clever Cloud flavor: the NodeGroup API accepts %s, "+
		"and a new flavor must follow the same naming (M, or S or L optionally prefixed by X or 2X to 9X, such as 2XL)",
		name, strings.Join(builtin, ", "))
}

// FlavorOverride is a partial, per-flavor override loaded from settings.flavors
// (FLAVORS_CONFIG_PATH). Only Name is required; CPU and MemoryKi are optional
// and, when set, replace the corresponding value from the base catalog (the
// built-in seed). Unset fields fall through. There is no price field: the
// price is always derived from the resulting cpu and memoryKi.
type FlavorOverride struct {
	Name     string `json:"name"`
	CPU      *int64 `json:"cpu,omitempty"`
	MemoryKi *int64 `json:"memoryKi,omitempty"`
}

// observedCapacity is a capacity/allocatable pair for a flavor: what nodes of
// it reported (in Provider.observed), or what its catalogue entry advertises
// (reference).
type observedCapacity struct {
	capacity    corev1.ResourceList
	allocatable corev1.ResourceList
}

// Provider builds Karpenter instance types from a flavor catalog computed by
// overlaying per-flavor overrides on top of the built-in seed.
type Provider struct {
	// region is advertised as both topology.kubernetes.io/region and
	// topology.kubernetes.io/zone: CKE is single-zone, so the region is its
	// only topology domain.
	region string
	// flavors is computed once by NewProvider and never written afterwards,
	// so List and Get read it without locking.
	flavors []Flavor

	// mu guards observed, the only state that changes at runtime: per
	// flavor, the smallest figures its nodes reported (RecordObservedCapacity).
	mu       sync.RWMutex
	observed map[string]observedCapacity
}

// NewProvider builds a Provider for the given region: the overrides are
// overlaid on base (the built-in DefaultFlavors when empty, which is what the
// controller passes) and the resulting catalog is fixed for the Provider's
// lifetime. An override that cannot be constructed is logged and skipped.
func NewProvider(region string, base []Flavor, overrides []FlavorOverride) *Provider {
	if len(base) == 0 {
		base = DefaultFlavors
	}
	flavors, skipped := ApplyOverrides(base, overrides)
	for _, name := range skipped {
		log.Log.WithName("instancetype").Info(
			"skipping flavor override: not in base/seed and missing cpu or memoryKi",
			"flavor", name)
	}
	return &Provider{
		region:   region,
		flavors:  flavors,
		observed: map[string]observedCapacity{},
	}
}

// LoadFlavorsFromFile reads and validates per-flavor overrides from a YAML file
// (typically a mounted ConfigMap). See ParseFlavorOverrides for the rules.
func LoadFlavorsFromFile(path string) ([]FlavorOverride, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading flavors file %q: %w", path, err)
	}
	overrides, err := ParseFlavorOverrides(data)
	if err != nil {
		return nil, fmt.Errorf("parsing flavors file %q: %w", path, err)
	}
	return overrides, nil
}

// LoadFlavorsOrDegrade loads the overrides file but never fails the caller:
// the chart rolls the pod on every settings.flavors change, so an invalid
// file used to convert a typo straight into a CrashLoopBackOff of the
// platform's autoscaler. Serving the base catalogue without the overrides is
// strictly better — the degradation is surfaced through the
// flavors_config_invalid gauge and an error log. An empty path is a no-op.
func LoadFlavorsOrDegrade(path string) []FlavorOverride {
	metrics.FlavorsConfigInvalid.Set(0, nil)
	if path == "" {
		return nil
	}
	overrides, err := LoadFlavorsFromFile(path)
	if err != nil {
		metrics.FlavorsConfigInvalid.Set(1, nil)
		log.Log.WithName("instancetype").Error(err,
			"invalid flavor overrides file; running WITHOUT the configured overrides (base catalogue only) — fix settings.flavors")
		return nil
	}
	return overrides
}

// ParseFlavorOverrides unmarshals a YAML list of partial flavor overrides and
// validates it. Unknown or duplicate keys fail parsing (strict mode): every
// field here is optional, so a mistyped key ("memory" for "memoryKi") would
// otherwise decode into an all-nil override that passes every bound below —
// a silent no-op instead of the operator's intended pin. The same rule
// refuses the priceHourly key earlier releases accepted: prices are derived
// now, and a file that still sets one is invalid as a whole rather than
// applied without it. Each entry must have a unique name of a Clever Cloud
// flavor's shape (FlavorNamePattern), and any field that is set must be > 0.
// A name outside the static sizing seed introduces a new flavor and must set
// both cpu and memoryKi, which are all it is sized and priced from. The list
// must not be empty.
func ParseFlavorOverrides(data []byte) ([]FlavorOverride, error) {
	var overrides []FlavorOverride
	if err := yaml.UnmarshalStrict(data, &overrides); err != nil {
		return nil, fmt.Errorf("unmarshaling flavor overrides: %w", err)
	}
	if len(overrides) == 0 {
		return nil, fmt.Errorf("flavor override list is empty")
	}
	seen := make(map[string]struct{}, len(overrides))
	for i, o := range overrides {
		if o.Name == "" {
			return nil, fmt.Errorf("flavor[%d]: name must not be empty", i)
		}
		if !flavorNameRegexp.MatchString(o.Name) {
			return nil, flavorNameError(o.Name)
		}
		if _, dup := seen[o.Name]; dup {
			return nil, fmt.Errorf("flavor %q: duplicate name", o.Name)
		}
		seen[o.Name] = struct{}{}
		if o.CPU != nil && *o.CPU <= 0 {
			return nil, fmt.Errorf("flavor %q: cpu must be > 0", o.Name)
		}
		if o.MemoryKi != nil && *o.MemoryKi <= 0 {
			return nil, fmt.Errorf("flavor %q: memoryKi must be > 0", o.Name)
		}
		if _, seeded := SizingByName[o.Name]; !seeded {
			if o.CPU == nil || o.MemoryKi == nil {
				return nil, fmt.Errorf("flavor %q is not in the built-in catalogue: a new flavor must set cpu and memoryKi", o.Name)
			}
		}
	}
	return overrides, nil
}

// ApplyOverrides overlays per-flavor overrides on top of a base catalog and
// returns the merged catalog plus the names of overrides that had to be skipped
// (a brand-new flavor absent from both the base and the static sizing seed must
// supply cpu and memoryKi; otherwise it cannot be constructed).
//
// Field resolution per flavor (highest precedence first): the override field,
// then the base value, then the static sizing seed. Every flavor's price is
// then recomputed from its final cpu and memoryKi, so pinning a flavor's
// memory reprices it, and a Price carried by the base is never served. The
// base catalog's order is preserved; override-only flavors are appended in
// override order.
func ApplyOverrides(base []Flavor, overrides []FlavorOverride) ([]Flavor, []string) {
	byName := make(map[string]Flavor, len(base))
	order := make([]string, 0, len(base)+len(overrides))
	for _, f := range base {
		if _, ok := byName[f.Name]; !ok {
			order = append(order, f.Name)
		}
		byName[f.Name] = f
	}

	overrideByName := make(map[string]FlavorOverride, len(overrides))
	for _, o := range overrides {
		overrideByName[o.Name] = o
		if _, ok := byName[o.Name]; !ok {
			order = append(order, o.Name)
		}
	}

	var skipped []string
	result := make([]Flavor, 0, len(order))
	for _, name := range order {
		f, ok := byName[name]
		if !ok {
			// No base entry: seed from the static sizing table if known,
			// otherwise rely entirely on the override fields below.
			f = Flavor{Name: name}
			if s, seeded := SizingByName[name]; seeded {
				f.CPU, f.MemoryKi = s.CPU, s.MemoryKi
			}
		}
		if o, hasOverride := overrideByName[name]; hasOverride {
			if o.CPU != nil {
				f.CPU = *o.CPU
			}
			if o.MemoryKi != nil {
				f.MemoryKi = *o.MemoryKi
			}
		}
		// A from-scratch flavor missing cpu or memoryKi stops here: it has
		// nothing to be sized or priced from.
		if f.CPU <= 0 || f.MemoryKi <= 0 {
			skipped = append(skipped, name)
			continue
		}
		f.Price = RelativePrice(f.CPU, f.MemoryKi)
		result = append(result, f)
	}
	return result, skipped
}

// generalizableResources are the only observed keys that hold across every VM
// of a flavor: cpu, memory, ephemeral-storage and pods are per-flavor
// hardware/platform figures. Everything else a node reports (device-plugin
// extended resources like nvidia.com/gpu, hugepages) is per-node software:
// letting one plugin-equipped node advertise it for the whole flavor would
// make the scheduler provision fresh nodes of that flavor for pods the new
// node can never run.
var generalizableResources = []corev1.ResourceName{
	corev1.ResourceCPU,
	corev1.ResourceMemory,
	corev1.ResourceEphemeralStorage,
	corev1.ResourcePods,
}

// filterGeneralizable deep-copies the generalizable keys of a node-reported
// resource list, dropping everything per-node.
func filterGeneralizable(list corev1.ResourceList) corev1.ResourceList {
	out := make(corev1.ResourceList, len(generalizableResources))
	for _, name := range generalizableResources {
		if q, ok := list[name]; ok {
			out[name] = q.DeepCopy()
		}
	}
	return out
}

// ErrImplausibleCapacity is returned by RecordObservedCapacity for a report
// that cannot come from a VM of the flavor it is recorded for. The catalogue
// keeps what it served before.
var ErrImplausibleCapacity = errors.New("implausible observed capacity")

// RecordObservedCapacity feeds back what a running node of flavor reports, so
// the catalogue follows the capacity the platform really delivers: the
// kernel-visible memory moves with the node image, and a seed measured on
// another image makes karpenter pack pods no real node can hold. Only the
// resources that generalize across a flavor's homogeneous VMs are kept;
// per-node extended resources never enter the catalogue. A report without
// cpu or memory (a node that has not posted its status yet) is ignored.
//
// The caller vouches for the flavor — the node must belong to a NodeGroup
// this provider created with it — but not for the figures: a kubelet can
// rewrite its own Node's status, and whatever is recorded here becomes the
// flavor's capacity for the whole cluster. So a report is refused (an error
// wrapping ErrImplausibleCapacity) unless it could describe a VM of the
// flavor's catalogue entry (checkObservation), and accepted reports are
// aggregated per resource by MINIMUM: the catalogue never promises more than
// the smallest node of the flavor delivered, whatever order nodes report in,
// and no node can raise a figure another one lowered. The minimum lasts for
// the process lifetime — a node's departure does not raise it back, a
// restart re-learns it from the nodes then running.
//
// A flavor with no catalogue entry and no sizing seed (an override-only
// flavor whose override was removed while its nodes still run) has nothing to
// bound a report against: it is recorded after the consistency checks alone,
// and only ever enriches Synthesize, which never provisions or prices.
//
// memoryDeviation is the report's memory capacity relative to the catalogue
// entry (observed/catalogue - 1, 0 without an entry), so the caller can
// surface a catalogue that no longer matches the platform.
func (p *Provider) RecordObservedCapacity(flavor string, capacity, allocatable corev1.ResourceList) (memoryDeviation float64, err error) {
	if capacity.Cpu().IsZero() || capacity.Memory().IsZero() {
		return 0, nil
	}
	observed := observedCapacity{capacity: filterGeneralizable(capacity), allocatable: filterGeneralizable(allocatable)}
	if ref, ok := p.reference(flavor); ok {
		if err := checkObservation(ref, observed); err != nil {
			return 0, fmt.Errorf("%w for flavor %q: %v", ErrImplausibleCapacity, flavor, err)
		}
		memoryDeviation = deviation(observed.capacity[corev1.ResourceMemory], ref.capacity[corev1.ResourceMemory])
	} else if err := checkConsistent(observed); err != nil {
		return 0, fmt.Errorf("%w for flavor %q: %v", ErrImplausibleCapacity, flavor, err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	prev := p.observed[flavor]
	p.observed[flavor] = observedCapacity{
		capacity:    minMerge(prev.capacity, observed.capacity),
		allocatable: minMerge(prev.allocatable, observed.allocatable),
	}
	return memoryDeviation, nil
}

// reference is what the catalogue itself advertises for flavor before any
// node of it has reported: the served entry (built-in seed plus overrides —
// an operator's pin moves the bounds with it), else the sizing seed of a known
// flavor the catalogue does not serve (what Synthesize describes it with).
func (p *Provider) reference(flavor string) (observedCapacity, bool) {
	s, ok := p.Sizing(flavor)
	if !ok {
		return observedCapacity{}, false
	}
	capacity, overhead := staticCapacity(Flavor{Name: flavor, CPU: s.CPU, MemoryKi: s.MemoryKi})
	return observedCapacity{capacity: capacity, allocatable: resources.Subtract(capacity, overhead.Total())}, true
}

// Sizing returns the cpu and memoryKi the catalogue sizes flavor with: its
// served entry (built-in seed plus overrides), else the sizing seed of a known
// flavor the catalogue does not serve. ok is false for a flavor with neither.
// Never observed capacity: the result is fixed for the Provider's lifetime.
func (p *Provider) Sizing(flavor string) (Sizing, bool) {
	if f, ok := p.served(flavor); ok {
		return Sizing{CPU: f.CPU, MemoryKi: f.MemoryKi}, true
	}
	s, seeded := SizingByName[flavor]
	return s, seeded
}

// checkObservation accepts a report only when it could describe a VM of the
// catalogue entry ref: the same cpu capacity (the vCPU count is the flavor's
// identity, a node image does not change it), every other figure — capacity
// and allocatable alike — within observedTolerance of the entry's, and the
// consistency checks. Resources are checked in a fixed order so the error
// names the same one for the same report.
func checkObservation(ref, obs observedCapacity) error {
	if err := checkConsistent(obs); err != nil {
		return err
	}
	for _, name := range generalizableResources {
		got, ok := obs.capacity[name]
		if !ok {
			continue // not reported: the entry's own figure stays in force
		}
		want := ref.capacity[name]
		if name == corev1.ResourceCPU {
			if got.Cmp(want) != 0 {
				return fmt.Errorf("cpu capacity %s, the catalogue entry has %s", got.String(), want.String())
			}
		} else if math.Abs(deviation(got, want)) > observedTolerance {
			return fmt.Errorf("%s capacity %s is more than %.0f%% off the catalogue entry's %s", name, got.String(), observedTolerance*100, want.String())
		}
		got, want = obs.allocatable[name], ref.allocatable[name]
		if math.Abs(deviation(got, want)) > observedTolerance {
			return fmt.Errorf("%s allocatable %s is more than %.0f%% off the catalogue entry's %s", name, got.String(), observedTolerance*100, want.String())
		}
	}
	return nil
}

// checkConsistent rejects a report whose capacity and allocatable do not
// describe the same resources, or whose allocatable exceeds its capacity. The
// served overhead is capacity minus allocatable: either shape would turn it
// negative and advertise more than the node has.
func checkConsistent(obs observedCapacity) error {
	for _, name := range generalizableResources {
		c, hasCapacity := obs.capacity[name]
		a, hasAllocatable := obs.allocatable[name]
		if hasCapacity != hasAllocatable {
			return fmt.Errorf("%s is reported in only one of capacity and allocatable", name)
		}
		if hasCapacity && a.Cmp(c) > 0 {
			return fmt.Errorf("%s allocatable %s exceeds its capacity %s", name, a.String(), c.String())
		}
	}
	return nil
}

// deviation is got relative to want (got/want - 1), +Inf for a zero want so a
// figure the entry does not have is never within tolerance.
func deviation(got, want resource.Quantity) float64 {
	w := want.AsApproximateFloat64()
	if w == 0 {
		return math.Inf(1)
	}
	return got.AsApproximateFloat64()/w - 1
}

// minMerge returns, per resource, the smaller of the two lists' figures; a
// resource only one list has is taken from it. Neither input is modified.
func minMerge(prev, next corev1.ResourceList) corev1.ResourceList {
	out := make(corev1.ResourceList, len(generalizableResources))
	for name, q := range prev {
		out[name] = q.DeepCopy()
	}
	for name, q := range next {
		if cur, ok := out[name]; !ok || q.Cmp(cur) < 0 {
			out[name] = q.DeepCopy()
		}
	}
	return out
}

// List returns the full instance type catalog. Karpenter mutates the returned
// objects (lazy allocatable computation), so fresh objects are built on every
// call.
func (p *Provider) List() []*cloudprovider.InstanceType {
	its := make([]*cloudprovider.InstanceType, 0, len(p.flavors))
	for _, f := range p.flavors {
		its = append(its, p.newInstanceType(f))
	}
	return its
}

// served returns the catalogue entry for a flavor name.
func (p *Provider) served(flavor string) (Flavor, bool) {
	for _, f := range p.flavors {
		if f.Name == flavor {
			return f, true
		}
	}
	return Flavor{}, false
}

// ErrUnknownFlavor is returned by Get for a flavor absent from the served
// catalogue. Callers that degrade on it (CloudProvider.Get/List) must match
// with errors.Is so a future second failure class cannot be silently
// swallowed by the degradation path.
var ErrUnknownFlavor = errors.New("unknown flavor")

// Get returns the instance type for a flavor name, or an error if unknown.
func (p *Provider) Get(flavor string) (*cloudprovider.InstanceType, error) {
	if f, ok := p.served(flavor); ok {
		return p.newInstanceType(f), nil
	}
	// A running NodeGroup referencing a flavor the catalogue no longer
	// carries is served a synthesized type and rolled by drift — count the
	// lookups so the condition is visible without diffing logs.
	metrics.UnknownFlavorLookups.Inc(nil)
	return nil, fmt.Errorf("%w %q", ErrUnknownFlavor, flavor)
}

// Synthesize builds an instance type for a flavor absent from the served
// catalogue, so CloudProvider.Get/List keep describing a running NodeGroup
// whose flavor left it (a removed or invalid settings.flavors override, a
// release whose built-in seed no longer carries it). Sizing comes from the
// static seed when the name is known, enriched with observed capacity when a
// live node reported it; a fully unknown name yields a name-only instance
// type — sufficient for the consumers of degraded claims (karpenter-core's
// garbage collection and node termination read only the provider ID and
// existence). The result is NOT added to the catalogue: List() never returns
// it, so nothing new is ever provisioned or priced with it.
func (p *Provider) Synthesize(flavor string) *cloudprovider.InstanceType {
	f := Flavor{Name: flavor}
	if s, seeded := SizingByName[flavor]; seeded {
		f.CPU = s.CPU
		f.MemoryKi = s.MemoryKi
		f.Price = RelativePrice(s.CPU, s.MemoryKi)
	}
	it := p.newInstanceType(f)
	if it.Capacity.Memory().IsZero() {
		// Name-only floor: subtracting the standard 100Mi overhead from zero
		// capacity would advertise a negative allocatable.
		it.Overhead = &cloudprovider.InstanceTypeOverhead{}
	}
	return it
}

// staticCapacity is what a catalogue entry advertises before any node of its
// flavor has reported: its own cpu and memory, the disk and pod capacity every
// flavor shares, and the measured overhead.
func staticCapacity(f Flavor) (corev1.ResourceList, *cloudprovider.InstanceTypeOverhead) {
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:              *resource.NewQuantity(f.CPU, resource.DecimalSI),
		corev1.ResourceMemory:           *resource.NewQuantity(f.MemoryKi*1024, resource.BinarySI),
		corev1.ResourceEphemeralStorage: ephemeralStorage,
		corev1.ResourcePods:             maxPods,
	}
	overhead := &cloudprovider.InstanceTypeOverhead{
		KubeReserved: corev1.ResourceList{
			corev1.ResourceMemory: kubeReservedMemory,
		},
		EvictionThreshold: corev1.ResourceList{
			corev1.ResourceEphemeralStorage: evictionEphemeralThreshold,
		},
	}
	return capacity, overhead
}

func (p *Provider) newInstanceType(f Flavor) *cloudprovider.InstanceType {
	// Both topology labels are declared here, in the requirements AND in the
	// offering. The platform puts neither on its nodes and karpenter-core
	// never turns a well-known requirement into a label itself, so the
	// single-valued requirements copied onto the NodeClaim (buildNodeClaim)
	// are their only path to the node. An undeclared well-known label is
	// worse than unsupported: core admits a pod or a volume that requires it
	// onto a NEW claim (undefined well-known labels are allowed there) but
	// never onto that claim once launched (in-flight and existing nodes are
	// checked strictly), so every provisioning pass launches another node for
	// the same pod; and a NodePool that requires it drifts every node it
	// launches.
	requirements := scheduling.NewRequirements(
		scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, f.Name),
		scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, v1.ArchitectureAmd64),
		scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, string(corev1.Linux)),
		scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, p.region),
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, p.region),
		scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
		scheduling.NewRequirement(v1alpha1.FlavorLabelKey, corev1.NodeSelectorOpIn, f.Name),
		// Every node Karpenter provisions on Clever Cloud is a worker; expose
		// the role label so workloads can target workers with a nodeSelector.
		scheduling.NewRequirement(v1alpha1.NodeRoleLabelKey, corev1.NodeSelectorOpIn, v1alpha1.NodeRoleWorker),
		scheduling.NewRequirement(v1alpha1.InstanceCPULabelKey, corev1.NodeSelectorOpIn, fmt.Sprint(f.CPU)),
		scheduling.NewRequirement(v1alpha1.InstanceMemoryLabelKey, corev1.NodeSelectorOpIn, fmt.Sprint(f.MemoryKi)),
	)
	capacity, overhead := staticCapacity(f)
	p.mu.RLock()
	if obs, ok := p.observed[f.Name]; ok {
		capacity = lo.Assign(capacity, obs.capacity)
		overhead = &cloudprovider.InstanceTypeOverhead{
			KubeReserved: resources.Subtract(obs.capacity, obs.allocatable),
		}
	}
	p.mu.RUnlock()
	return &cloudprovider.InstanceType{
		Name:         f.Name,
		Requirements: requirements,
		Offerings: cloudprovider.Offerings{
			{
				Requirements: scheduling.NewRequirements(
					scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
					scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, p.region),
					scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, p.region),
				),
				Price:     f.Price,
				Available: true,
			},
		},
		Capacity: capacity,
		Overhead: overhead,
	}
}
