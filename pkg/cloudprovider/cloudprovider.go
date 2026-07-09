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

// Package cloudprovider implements the Karpenter CloudProvider interface for
// Clever Kubernetes Engine. Each NodeClaim is backed by a dedicated Clever
// Cloud NodeGroup with nodeCount=1; the Clever Cloud control plane provisions
// the VM, and the provider's auxiliary controllers stamp the node with a
// provider ID so Karpenter can match it to its NodeClaim.
package cloudprovider

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

const (
	// NodeClassLabelsDrifted is reported when the labels carried by the
	// NodeGroup no longer match the current CleverNodeClass spec.
	NodeClassDrifted cloudprovider.DriftReason = "NodeClassDrifted"
)

type CloudProvider struct {
	kubeClient           client.Client
	instanceTypeProvider *instancetype.Provider
	nodeGroupProvider    *nodegroup.Provider
	// warnedFlavors dedups the degradation log per flavor for this process
	// lifetime; the condition persists across the GC's 2-minute List sweeps.
	warnedFlavors sync.Map
}

func New(kubeClient client.Client, instanceTypeProvider *instancetype.Provider, nodeGroupProvider *nodegroup.Provider) *CloudProvider {
	return &CloudProvider{
		kubeClient:           kubeClient,
		instanceTypeProvider: instanceTypeProvider,
		nodeGroupProvider:    nodeGroupProvider,
	}
}

func (c *CloudProvider) Create(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*karpv1.NodeClaim, error) {
	nodeClass, err := c.resolveNodeClassFromNodeClaim(ctx, nodeClaim)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("resolving node class from nodeclaim, %w", err))
		}
		return nil, fmt.Errorf("resolving node class from nodeclaim, %w", err)
	}
	if readiness := nodeClass.StatusConditions().Get(status.ConditionReady); readiness.IsFalse() {
		return nil, cloudprovider.NewNodeClassNotReadyError(errors.New(readiness.Message))
	}
	instanceType, err := c.resolveInstanceType(nodeClaim)
	if err != nil {
		return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("resolving instance type, %w", err))
	}
	ng, err := c.nodeGroupProvider.Create(ctx, nodeClaim, nodeClass, instanceType.Name)
	if err != nil {
		quotaErr := &nodegroup.ErrQuotaExceeded{}
		if errors.As(err, &quotaErr) {
			// Surfacing an InsufficientCapacityError lets the scheduler mark
			// the offering unavailable and relax to other options instead of
			// waiting out the 15min registration TTL.
			return nil, cloudprovider.NewInsufficientCapacityError(err)
		}
		return nil, err
	}
	log.FromContext(ctx).WithValues("NodeGroup", ng.Name, "flavor", instanceType.Name).Info("created nodegroup")
	return c.buildNodeClaim(ng, instanceType), nil
}

func (c *CloudProvider) Delete(ctx context.Context, nodeClaim *karpv1.NodeClaim) error {
	name, err := nodegroup.ParseProviderID(nodeClaim.Status.ProviderID)
	if err != nil {
		// No provider ID means the NodeGroup carries the NodeClaim's name.
		name = nodeClaim.Name
	}
	ng, err := c.nodeGroupProvider.Get(ctx, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return cloudprovider.NewNodeClaimNotFoundError(err)
		}
		return fmt.Errorf("getting nodegroup, %w", err)
	}
	if !nodegroup.IsManaged(ng) {
		return fmt.Errorf("refusing to delete nodegroup %q: not managed by karpenter", name)
	}
	if !ng.DeletionTimestamp.IsZero() {
		// Deletion already in flight; Karpenter polls until NotFound.
		return nil
	}
	if err := c.nodeGroupProvider.Delete(ctx, name); err != nil {
		if apierrors.IsNotFound(err) {
			return cloudprovider.NewNodeClaimNotFoundError(err)
		}
		return fmt.Errorf("deleting nodegroup, %w", err)
	}
	return nil
}

func (c *CloudProvider) Get(ctx context.Context, providerID string) (*karpv1.NodeClaim, error) {
	name, err := nodegroup.ParseProviderID(providerID)
	if err != nil {
		return nil, fmt.Errorf("parsing provider id, %w", err)
	}
	ng, err := c.nodeGroupProvider.Get(ctx, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, cloudprovider.NewNodeClaimNotFoundError(err)
		}
		return nil, fmt.Errorf("getting nodegroup, %w", err)
	}
	if !nodegroup.IsManaged(ng) {
		return nil, cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("nodegroup %q is not managed by karpenter", name))
	}
	if !ng.DeletionTimestamp.IsZero() {
		// Treat a terminating NodeGroup as already gone: the Clever Cloud
		// finalizer needs the Node object to be deletable, which requires
		// Karpenter to release its node finalizer first.
		return nil, cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("nodegroup %q is terminating", name))
	}
	instanceType, err := c.resolveNodeGroupInstanceType(ctx, ng)
	if err != nil {
		return nil, err
	}
	return c.buildNodeClaim(ng, instanceType), nil
}

func (c *CloudProvider) List(ctx context.Context) ([]*karpv1.NodeClaim, error) {
	nodeGroups, err := c.nodeGroupProvider.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing nodegroups, %w", err)
	}
	nodeClaims := make([]*karpv1.NodeClaim, 0, len(nodeGroups))
	for i := range nodeGroups {
		instanceType, err := c.resolveNodeGroupInstanceType(ctx, &nodeGroups[i])
		if err != nil {
			return nil, err
		}
		nodeClaims = append(nodeClaims, c.buildNodeClaim(&nodeGroups[i], instanceType))
	}
	return nodeClaims, nil
}

// resolveNodeGroupInstanceType describes a running NodeGroup, degrading to a
// synthesized instance type when its flavor left the catalogue (upstream
// removal, topology change, removed override). Failing on that condition is
// never an option: a List error stalls karpenter-core's nodeclaim garbage
// collection cluster-wide and a Get error wedges node termination before
// drain — while SKIPPING the entry instead would make core GC read the
// missing provider ID as an orphaned claim and delete the node as soon as it
// is NotReady (a kubelet restart suffices). The synthesized type never
// enters GetInstanceTypes, so nothing new is provisioned or priced with it;
// the running node itself is replaced by karpenter-core's
// InstanceTypeNotFound drift path, paced by the NodePool's disruption
// budgets. Only the unknown-flavor error degrades — anything else surfaces.
func (c *CloudProvider) resolveNodeGroupInstanceType(ctx context.Context, ng *ngv1.NodeGroup) (*cloudprovider.InstanceType, error) {
	instanceType, err := c.instanceTypeProvider.Get(ng.Spec.Flavor)
	if err == nil {
		// The flavor is (back) in the catalogue: rearm the degradation log so
		// a later removal of the same flavor is named again.
		c.warnedFlavors.Delete(ng.Spec.Flavor)
		return instanceType, nil
	}
	if !errors.Is(err, instancetype.ErrUnknownFlavor) {
		return nil, fmt.Errorf("resolving instance type for nodegroup %q, %w", ng.Name, err)
	}
	if _, warned := c.warnedFlavors.LoadOrStore(ng.Spec.Flavor, struct{}{}); !warned {
		log.FromContext(ctx).WithValues("flavor", ng.Spec.Flavor, "NodeGroup", ng.Name).Info(
			"flavor is missing from the served catalogue; serving a synthesized instance type " +
				"(existing nodes are replaced through drift under disruption budgets, new nodes never use it — " +
				"restore the flavor via settings.flavors or check CLEVER_CLOUD_TOPOLOGY)")
	}
	return c.instanceTypeProvider.Synthesize(ng.Spec.Flavor), nil
}

func (c *CloudProvider) GetInstanceTypes(ctx context.Context, nodePool *karpv1.NodePool) ([]*cloudprovider.InstanceType, error) {
	return c.instanceTypeProvider.List(), nil
}

func (c *CloudProvider) IsDrifted(ctx context.Context, nodeClaim *karpv1.NodeClaim) (cloudprovider.DriftReason, error) {
	name, err := nodegroup.ParseProviderID(nodeClaim.Status.ProviderID)
	if err != nil {
		return "", nil
	}
	ng, err := c.nodeGroupProvider.Get(ctx, name)
	if err != nil {
		// A missing NodeGroup is handled by garbage collection, not drift.
		return "", client.IgnoreNotFound(err)
	}
	nodeClass := &v1alpha1.CleverNodeClass{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodeClaim.Spec.NodeClassRef.Name}, nodeClass); err != nil {
		return "", client.IgnoreNotFound(err)
	}
	if hash, ok := ng.Annotations[v1alpha1.NodeClassHashLabelKey]; ok && hash != nodeClass.Hash() {
		return NodeClassDrifted, nil
	}
	return "", nil
}

func (c *CloudProvider) RepairPolicies() []cloudprovider.RepairPolicy {
	return []cloudprovider.RepairPolicy{
		{
			ConditionType:      corev1.NodeReady,
			ConditionStatus:    corev1.ConditionFalse,
			TolerationDuration: 10 * time.Minute,
		},
		{
			ConditionType:      corev1.NodeReady,
			ConditionStatus:    corev1.ConditionUnknown,
			TolerationDuration: 10 * time.Minute,
		},
	}
}

func (c *CloudProvider) Name() string {
	return "clevercloud"
}

func (c *CloudProvider) GetSupportedNodeClasses() []status.Object {
	return []status.Object{&v1alpha1.CleverNodeClass{}}
}

func (c *CloudProvider) resolveNodeClassFromNodeClaim(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*v1alpha1.CleverNodeClass, error) {
	nodeClass := &v1alpha1.CleverNodeClass{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodeClaim.Spec.NodeClassRef.Name}, nodeClass); err != nil {
		return nil, err
	}
	return nodeClass, nil
}

// resolveInstanceType picks the cheapest catalog flavor that satisfies the
// NodeClaim's scheduling requirements and resource requests.
func (c *CloudProvider) resolveInstanceType(nodeClaim *karpv1.NodeClaim) (*cloudprovider.InstanceType, error) {
	requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)
	var best *cloudprovider.InstanceType
	bestPrice := 0.0
	for _, it := range c.instanceTypeProvider.List() {
		if it.Requirements.Intersects(requirements) != nil {
			continue
		}
		if !resources.Fits(nodeClaim.Spec.Resources.Requests, it.Allocatable()) {
			continue
		}
		offerings := it.Offerings.Available().Compatible(requirements)
		if len(offerings) == 0 {
			continue
		}
		price := offerings.Cheapest().Price
		if best == nil || price < bestPrice {
			best = it
			bestPrice = price
		}
	}
	if best == nil {
		return nil, fmt.Errorf("no clever cloud flavor satisfies the nodeclaim requirements and resource requests")
	}
	return best, nil
}

// buildNodeClaim converts a NodeGroup into the NodeClaim shape Karpenter
// expects from the cloud provider (labels resolved, provider ID, capacity).
func (c *CloudProvider) buildNodeClaim(ng *ngv1.NodeGroup, instanceType *cloudprovider.InstanceType) *karpv1.NodeClaim {
	labels := map[string]string{}
	for key, req := range instanceType.Requirements {
		if req.Len() == 1 {
			labels[key] = req.Values()[0]
		}
	}
	for _, o := range instanceType.Offerings {
		if o.Available {
			for key, req := range o.Requirements {
				if req.Len() == 1 {
					labels[key] = req.Values()[0]
				}
			}
			break
		}
	}
	if nodePool, ok := ng.Labels[v1alpha1.NodePoolLabelKey]; ok {
		labels[karpv1.NodePoolLabelKey] = nodePool
	}
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              ng.Name,
			Labels:            labels,
			CreationTimestamp: ng.CreationTimestamp,
		},
		Status: karpv1.NodeClaimStatus{
			ProviderID:  nodegroup.ProviderID(ng.Name),
			Capacity:    instanceType.Capacity,
			Allocatable: instanceType.Allocatable(),
		},
	}
}
