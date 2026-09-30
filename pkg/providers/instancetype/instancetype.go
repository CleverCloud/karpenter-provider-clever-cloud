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
// Capacity figures for 2XS/XS/S/M were measured on live CKE nodes
// (status.capacity); L and XL are derived from the documented specs
// (https://www.clever.cloud/developers/doc/kubernetes/) using the same
// kernel-visible-memory ratio as the measured flavors. Prices are the
// documented public-beta worker prices (EUR/hour, June 2026). The served
// catalogue is this static seed with the operator's settings.flavors overrides
// on top: nothing is fetched from a Clever Cloud endpoint at runtime.
package instancetype

import (
	"errors"
	"fmt"
	"math"
	"os"
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
	// DefaultVCPURate is the documented public CKE worker price per vCPU per
	// hour (EUR). With DefaultRAMRate it prices the seed flavors through
	// ComputePrice.
	DefaultVCPURate = 0.00277777777
	// DefaultRAMRate is the documented public CKE worker price per nominal GB
	// of RAM per hour (EUR).
	DefaultRAMRate = 0.00555555555
)

// Flavor describes one Clever Cloud node flavor.
type Flavor struct {
	// Name as accepted by the NodeGroup API (uppercase).
	Name string `json:"name"`
	// CPU is the number of vCPUs.
	CPU int64 `json:"cpu"`
	// MemoryKi is the kernel-visible memory capacity in KiB.
	MemoryKi int64 `json:"memoryKi"`
	// PriceHourly is the worker price in EUR/hour.
	PriceHourly float64 `json:"priceHourly"`
}

// Sizing is the static per-flavor seed: vCPU count, kernel-visible memory
// (KiB) and the NOMINAL advertised memory (GB) used only by the price
// formula. CPU and MemoryKi additionally self-correct at runtime via
// RecordObservedCapacity; NominalGB has no runtime source.
type Sizing struct {
	CPU       int64
	MemoryKi  int64
	NominalGB float64
}

var (
	// FlavorSizing is the canonical sizing table (ordered smallest-to-largest).
	// DefaultFlavors, ApplyOverrides and Synthesize all derive from it; it is
	// the single source of truth for per-flavor sizing.
	FlavorSizing = []struct {
		Name string
		Sizing
	}{
		{"2XS", Sizing{CPU: 4, MemoryKi: 3911884, NominalGB: 4}},
		{"XS", Sizing{CPU: 6, MemoryKi: 7937580, NominalGB: 8}},
		{"S", Sizing{CPU: 8, MemoryKi: 11957148, NominalGB: 12}},
		{"M", Sizing{CPU: 10, MemoryKi: 15988992, NominalGB: 16}},
		// L and XL capacities are estimated from documented specs (24/32 GB)
		// applying the measured kernel-visible ratio of the M flavor.
		{"L", Sizing{CPU: 12, MemoryKi: 23983488, NominalGB: 24}},
		{"XL", Sizing{CPU: 16, MemoryKi: 31977984, NominalGB: 32}},
	}

	// SizingByName indexes FlavorSizing for O(1) lookup by flavor name.
	SizingByName = func() map[string]Sizing {
		m := make(map[string]Sizing, len(FlavorSizing))
		for _, s := range FlavorSizing {
			m[s.Name] = s.Sizing
		}
		return m
	}()

	// DefaultFlavors is the built-in CKE flavor catalog, the base the
	// settings.flavors overrides are overlaid on. Prices are kept as literals
	// (audited public beta values); TestDefaultFlavorsMatchSeed pins them
	// against FlavorSizing and the default rates so the two never drift.
	DefaultFlavors = []Flavor{
		{Name: "2XS", CPU: 4, MemoryKi: 3911884, PriceHourly: 0.0333},
		{Name: "XS", CPU: 6, MemoryKi: 7937580, PriceHourly: 0.0611},
		{Name: "S", CPU: 8, MemoryKi: 11957148, PriceHourly: 0.0889},
		{Name: "M", CPU: 10, MemoryKi: 15988992, PriceHourly: 0.1167},
		{Name: "L", CPU: 12, MemoryKi: 23983488, PriceHourly: 0.1667},
		{Name: "XL", CPU: 16, MemoryKi: 31977984, PriceHourly: 0.2222},
	}

	// All flavors share the same disk and pod capacity (measured).
	ephemeralStorage = resource.MustParse("40971488Ki")
	maxPods          = resource.MustParse("110")

	// Measured overhead: exactly 100MiB of memory reserved for the kubelet,
	// and a 10% hard eviction threshold on ephemeral storage.
	kubeReservedMemory         = resource.MustParse("100Mi")
	evictionEphemeralThreshold = resource.MustParse("4097149Ki")
)

// ComputePrice returns the EUR/hour worker price for a flavor:
//
//	price = cpu*vcpuRate + nominalGB*ramRate
//
// rounded to 4 decimals (the precision of the documented catalog).
func ComputePrice(cpu int64, nominalGB, vcpuRate, ramRate float64) float64 {
	return math.Round((float64(cpu)*vcpuRate+nominalGB*ramRate)*1e4) / 1e4
}

// FlavorOverride is a partial, per-flavor override loaded from settings.flavors
// (FLAVORS_CONFIG_PATH). Only Name is required; every other field is optional
// and, when set, replaces the corresponding value from the base catalog (the
// built-in seed). Unset fields fall through.
type FlavorOverride struct {
	Name        string   `json:"name"`
	CPU         *int64   `json:"cpu,omitempty"`
	MemoryKi    *int64   `json:"memoryKi,omitempty"`
	PriceHourly *float64 `json:"priceHourly,omitempty"`
}

// observedCapacity is the live capacity reported by a node of a flavor.
type observedCapacity struct {
	capacity    corev1.ResourceList
	allocatable corev1.ResourceList
}

// Provider builds Karpenter instance types from a flavor catalog computed by
// overlaying per-flavor overrides on top of the built-in seed.
type Provider struct {
	region string
	// flavors is computed once by NewProvider and never written afterwards,
	// so List and Get read it without locking.
	flavors []Flavor

	// mu guards observed, the only state that changes at runtime.
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
			"skipping flavor override: not in base/seed and missing cpu, memoryKi or priceHourly",
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
// field here is optional, so a mistyped key ("price" for "priceHourly") would
// otherwise decode into an all-nil override that passes every bound below —
// a silent no-op instead of the operator's intended pin. Each entry must have
// a non-empty, unique name; any field that is set must satisfy its bound
// (cpu > 0, memoryKi > 0, priceHourly >= 0). A name outside the static sizing
// seed introduces a new flavor and must set all three fields — without the
// price bound an unpriced new flavor would enter the catalogue at 0 EUR/h and
// win every cheapest-first decision. The list must not be empty.
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
		if o.PriceHourly != nil && *o.PriceHourly < 0 {
			return nil, fmt.Errorf("flavor %q: priceHourly must be >= 0", o.Name)
		}
		if _, seeded := SizingByName[o.Name]; !seeded {
			if o.CPU == nil || o.MemoryKi == nil || o.PriceHourly == nil {
				return nil, fmt.Errorf("flavor %q is not in the built-in catalogue: a new flavor must set cpu, memoryKi and priceHourly", o.Name)
			}
		}
	}
	return overrides, nil
}

// ApplyOverrides overlays per-flavor overrides on top of a base catalog and
// returns the merged catalog plus the names of overrides that had to be skipped
// (a brand-new flavor absent from both the base and the static sizing seed must
// supply cpu, memoryKi and priceHourly; otherwise it cannot be constructed).
//
// Field resolution per flavor (highest precedence first): the override field,
// then the base value, then the static sizing seed. The base catalog's order is
// preserved; override-only flavors are appended in override order.
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
		fromScratch := false
		if !ok {
			// No base entry: seed from the static sizing table if known,
			// otherwise rely entirely on the override fields below.
			if s, seeded := SizingByName[name]; seeded {
				f = Flavor{
					Name:        name,
					CPU:         s.CPU,
					MemoryKi:    s.MemoryKi,
					PriceHourly: ComputePrice(s.CPU, s.NominalGB, DefaultVCPURate, DefaultRAMRate),
				}
			} else {
				f = Flavor{Name: name}
				fromScratch = true
			}
		}
		o, hasOverride := overrideByName[name]
		if hasOverride {
			if o.CPU != nil {
				f.CPU = *o.CPU
			}
			if o.MemoryKi != nil {
				f.MemoryKi = *o.MemoryKi
			}
			if o.PriceHourly != nil {
				f.PriceHourly = *o.PriceHourly
			}
		}
		// A from-scratch flavor must get its price from the override: the
		// zero value would otherwise pass the < 0 gate and enter the
		// catalogue free of charge, capturing every cheapest-first decision.
		if fromScratch && (!hasOverride || o.PriceHourly == nil) {
			skipped = append(skipped, name)
			continue
		}
		if f.CPU <= 0 || f.MemoryKi <= 0 || f.PriceHourly < 0 {
			skipped = append(skipped, name)
			continue
		}
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

// RecordObservedCapacity feeds back the real capacity of a running node so
// the catalog self-corrects (the static table entries for flavors never seen
// yet — L and XL in particular — are derived estimates). Only the resources
// that generalize across a flavor's homogeneous VMs are kept; per-node
// extended resources must never enter the flavor catalog.
func (p *Provider) RecordObservedCapacity(flavor string, capacity, allocatable corev1.ResourceList) {
	if capacity.Cpu().IsZero() || capacity.Memory().IsZero() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observed[flavor] = observedCapacity{capacity: filterGeneralizable(capacity), allocatable: filterGeneralizable(allocatable)}
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

// ErrUnknownFlavor is returned by Get for a flavor absent from the served
// catalogue. Callers that degrade on it (CloudProvider.Get/List) must match
// with errors.Is so a future second failure class cannot be silently
// swallowed by the degradation path.
var ErrUnknownFlavor = errors.New("unknown flavor")

// Get returns the instance type for a flavor name, or an error if unknown.
func (p *Provider) Get(flavor string) (*cloudprovider.InstanceType, error) {
	for _, f := range p.flavors {
		if f.Name == flavor {
			return p.newInstanceType(f), nil
		}
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
		f.PriceHourly = ComputePrice(s.CPU, s.NominalGB, DefaultVCPURate, DefaultRAMRate)
	}
	it := p.newInstanceType(f)
	if it.Capacity.Memory().IsZero() {
		// Name-only floor: subtracting the standard 100Mi overhead from zero
		// capacity would advertise a negative allocatable.
		it.Overhead = &cloudprovider.InstanceTypeOverhead{}
	}
	return it
}

func (p *Provider) newInstanceType(f Flavor) *cloudprovider.InstanceType {
	memory := resource.NewQuantity(f.MemoryKi*1024, resource.BinarySI)
	requirements := scheduling.NewRequirements(
		scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, f.Name),
		scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, v1.ArchitectureAmd64),
		scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, string(corev1.Linux)),
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, p.region),
		scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
		scheduling.NewRequirement(v1alpha1.FlavorLabelKey, corev1.NodeSelectorOpIn, f.Name),
		// Every node Karpenter provisions on Clever Cloud is a worker; expose
		// the role label so workloads can target workers with a nodeSelector.
		scheduling.NewRequirement(v1alpha1.NodeRoleLabelKey, corev1.NodeSelectorOpIn, v1alpha1.NodeRoleWorker),
		scheduling.NewRequirement(v1alpha1.InstanceCPULabelKey, corev1.NodeSelectorOpIn, fmt.Sprint(f.CPU)),
		scheduling.NewRequirement(v1alpha1.InstanceMemoryLabelKey, corev1.NodeSelectorOpIn, fmt.Sprint(f.MemoryKi)),
	)
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:              *resource.NewQuantity(f.CPU, resource.DecimalSI),
		corev1.ResourceMemory:           *memory,
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
					scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, p.region),
				),
				Price:     f.PriceHourly,
				Available: true,
			},
		},
		Capacity: capacity,
		Overhead: overhead,
	}
}
