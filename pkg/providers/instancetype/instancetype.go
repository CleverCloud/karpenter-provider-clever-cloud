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
// documented public-beta worker prices (EUR/hour, June 2026).
package instancetype

import (
	"fmt"
	"sync"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
)

// Flavor describes one Clever Cloud node flavor.
type Flavor struct {
	// Name as accepted by the NodeGroup API (uppercase).
	Name string
	// CPU is the number of vCPUs.
	CPU int64
	// MemoryKi is the kernel-visible memory capacity in KiB.
	MemoryKi int64
	// PriceHourly is the worker price in EUR/hour.
	PriceHourly float64
}

var (
	// Flavors is the static CKE flavor catalog.
	Flavors = []Flavor{
		{Name: "2XS", CPU: 4, MemoryKi: 3911884, PriceHourly: 0.0333},
		{Name: "XS", CPU: 6, MemoryKi: 7937580, PriceHourly: 0.0611},
		{Name: "S", CPU: 8, MemoryKi: 11957148, PriceHourly: 0.0889},
		{Name: "M", CPU: 10, MemoryKi: 15988992, PriceHourly: 0.1167},
		// L and XL capacities are estimated from documented specs (24/32 GB)
		// applying the measured kernel-visible ratio of the M flavor.
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

// observedCapacity is the live capacity reported by a node of a flavor.
type observedCapacity struct {
	capacity    corev1.ResourceList
	allocatable corev1.ResourceList
}

// Provider builds Karpenter instance types from the flavor catalog.
type Provider struct {
	region string

	mu       sync.RWMutex
	observed map[string]observedCapacity
}

func NewProvider(region string) *Provider {
	return &Provider{region: region, observed: map[string]observedCapacity{}}
}

// RecordObservedCapacity feeds back the real capacity of a running node so
// the catalog self-corrects (the static table entries for flavors never seen
// yet — L and XL in particular — are derived estimates).
func (p *Provider) RecordObservedCapacity(flavor string, capacity, allocatable corev1.ResourceList) {
	if capacity.Cpu().IsZero() || capacity.Memory().IsZero() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observed[flavor] = observedCapacity{capacity: capacity.DeepCopy(), allocatable: allocatable.DeepCopy()}
}

// List returns the full instance type catalog. Karpenter mutates the returned
// objects (lazy allocatable computation), so fresh objects are built on every
// call.
func (p *Provider) List() []*cloudprovider.InstanceType {
	its := make([]*cloudprovider.InstanceType, 0, len(Flavors))
	for _, f := range Flavors {
		its = append(its, p.newInstanceType(f))
	}
	return its
}

// Get returns the instance type for a flavor name, or an error if unknown.
func (p *Provider) Get(flavor string) (*cloudprovider.InstanceType, error) {
	for _, f := range Flavors {
		if f.Name == flavor {
			return p.newInstanceType(f), nil
		}
	}
	return nil, fmt.Errorf("unknown flavor %q", flavor)
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
