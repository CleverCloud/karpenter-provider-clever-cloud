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
	kubeClient client.Client
	// apiReader reads from the API server, bypassing the informer cache:
	// Create reads the NodeClaim and the NodeClass it launches from with it
	// (see resolveLaunch).
	apiReader            client.Reader
	instanceTypeProvider *instancetype.Provider
	nodeGroupProvider    *nodegroup.Provider
	// warnedFlavors dedups the degradation log per flavor for this process
	// lifetime; the condition persists across the GC's 2-minute List sweeps.
	warnedFlavors sync.Map
}

func New(kubeClient client.Client, apiReader client.Reader, instanceTypeProvider *instancetype.Provider, nodeGroupProvider *nodegroup.Provider) *CloudProvider {
	return &CloudProvider{
		kubeClient:           kubeClient,
		apiReader:            apiReader,
		instanceTypeProvider: instanceTypeProvider,
		nodeGroupProvider:    nodeGroupProvider,
	}
}

func (c *CloudProvider) Create(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*karpv1.NodeClaim, error) {
	// What to launch is decided under the nodegroup provider's creation lock,
	// right before the NodeGroup is created, never before the wait for it
	// (see resolveLaunch): instanceType is the flavor resolved there, the one
	// the NodeGroup is created with.
	var instanceType *cloudprovider.InstanceType
	ng, err := c.nodeGroupProvider.ResolveAndCreate(ctx, nodeClaim, func(ctx context.Context) (*v1alpha1.CleverNodeClass, string, error) {
		nodeClass, resolved, err := c.resolveLaunch(ctx, nodeClaim)
		if err != nil {
			return nil, "", err
		}
		instanceType = resolved
		return nodeClass, resolved.Name, nil
	})
	if err != nil {
		quotaErr := &nodegroup.ErrQuotaExceeded{}
		rejectedErr := &nodegroup.ErrFlavorRejected{}
		if errors.As(err, &quotaErr) || errors.As(err, &rejectedErr) || errors.Is(err, nodegroup.ErrNodeGroupVanished) {
			// An InsufficientCapacityError makes karpenter-core delete the
			// claim and re-plan now instead of waiting out the 15min
			// registration TTL; a plain error would retry the SAME claim with
			// the same flavor (for a vanish, a create→vanish loop holding the
			// creation mutex). Core remembers nothing of the error itself: it
			// re-plans over the same offerings. What makes the re-plan land
			// elsewhere is the nodegroup provider having recorded the refusal,
			// which GetInstanceTypes reports as unavailable offerings (the
			// quota-rejected flavor and every larger one, or the flavor refused
			// by the upstream operator or at admission, a flavor outside the
			// NodeGroup CRD's enum) — the only channel back to core's scheduler.
			return nil, cloudprovider.NewInsufficientCapacityError(err)
		}
		return nil, err
	}
	// The returned claim must describe the machine that exists, not the one
	// requested: on the AlreadyExists adoption path the group's immutable
	// flavor can differ from the one resolved by THIS attempt (a retry after a
	// transient error, with the original flavor since refused or re-priced),
	// and a claim built from the requested flavor would carry a capacity and
	// price no running node has — nothing downstream ever corrects it.
	if ng.Spec.Flavor != instanceType.Name {
		log.FromContext(ctx).WithValues("NodeGroup", ng.Name, "flavor", ng.Spec.Flavor, "requestedFlavor", instanceType.Name).Info(
			"adopted an existing nodegroup whose flavor differs from the one resolved for this attempt; describing the existing flavor")
		if instanceType, err = c.resolveNodeGroupInstanceType(ctx, ng); err != nil {
			return nil, err
		}
	}
	log.FromContext(ctx).WithValues("NodeGroup", ng.Name, "flavor", ng.Spec.Flavor).Info("created nodegroup")
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
	if err := c.nodeGroupProvider.Delete(ctx, ng); err != nil {
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
// synthesized instance type when its flavor left the catalogue (a removed or
// invalid settings.flavors override, a release whose built-in seed no longer
// carries it). Failing on that condition is never an option: a List error
// stalls karpenter-core's nodeclaim garbage collection cluster-wide and a Get
// error wedges node termination before drain — while SKIPPING the entry
// instead would make core GC read the missing provider ID as an orphaned claim
// and delete the node as soon as it is NotReady (a kubelet restart suffices).
// The synthesized type never enters GetInstanceTypes, so nothing new is
// provisioned or priced with it; the running node itself is replaced by
// karpenter-core's InstanceTypeNotFound drift path, paced by the NodePool's
// disruption budgets. Only the unknown-flavor error degrades — anything else
// surfaces.
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
				"restore the flavor via settings.flavors)")
	}
	return c.instanceTypeProvider.Synthesize(ng.Spec.Flavor), nil
}

func (c *CloudProvider) GetInstanceTypes(ctx context.Context, nodePool *karpv1.NodePool) ([]*cloudprovider.InstanceType, error) {
	return c.instanceTypes(), nil
}

// instanceTypes is the catalogue as karpenter-core's scheduler must see it:
// every flavor, with Available=false on the offerings of the flavors a launch
// is known to fail for right now (nodegroup.Provider.Unavailable): a flavor
// the organisation quota rejected for the quota backoff, and every flavor at
// least as large for twice as long; a flavor the operator refused, or the API
// server's admission refused (a settings.flavors name the NodeGroup CRD's enum
// does not carry), for its hold-out. Core keeps no memory of an InsufficientCapacityError, so without
// this its scheduler rebuilds the claim that was just rejected on every pass,
// and a weighted NodePool never falls back to the next one. No flavor is ever
// left out: core's InstanceTypeNotFound drift ignores availability but not
// presence, so dropping one would replace every node running it.
//
// Get and List describe running NodeGroups from the plain catalogue instead:
// buildNodeClaim takes the claim's zone, region and capacity-type labels from
// the first available offering.
func (c *CloudProvider) instanceTypes() []*cloudprovider.InstanceType {
	instanceTypes := c.instanceTypeProvider.List()
	for _, it := range instanceTypes {
		if !c.nodeGroupProvider.Unavailable(it.Name) {
			continue
		}
		for _, o := range it.Offerings {
			o.Available = false
		}
	}
	return instanceTypes
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
	// Compare like with like: a NodeGroup stamped by an older generation of
	// Hash() is checked against what THAT generation computes for the current
	// spec. Comparing it with today's Hash() would read as drift on every
	// NodeGroup at once after a controller upgrade; skipping it would hide a
	// NodeClass edit made before the nodeclass controller re-stamped the group
	// (it only re-stamps stamps that still match). A stamp from a generation
	// this controller cannot compute (a newer controller's, after a rollback),
	// or no stamp at all, proves nothing either way: no drift.
	hash, hasHash := ng.Annotations[v1alpha1.NodeClassHashLabelKey]
	if !hasHash {
		return "", nil
	}
	if match, known := nodeClass.HashMatches(ng.Annotations[v1alpha1.NodeClassHashVersionAnnotationKey], hash); known && !match {
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

// resolveLaunch decides what Create launches for nodeClaim: the NodeClass the
// NodeGroup is built from and the cheapest available flavor. It runs under the
// nodegroup provider's creation lock, immediately before the NodeGroup is
// created. Launches queue on that lock, each holding it until the operator's
// decision on its group (15 s when the operator does not answer), and
// anything checked before the wait can be stale once the lock is acquired.
// Checked before it, a launch queued behind a refusal POSTed the flavor that
// had just been refused (one more create, refuse and delete upstream); a
// NodeClass that started terminating meanwhile still launched, and its
// finalizer then waited on a machine created after its deletion; and a
// NodeClaim deleted meanwhile still got its billed machine, since
// karpenter-core only branches to finalize at the start of a reconcile, not
// in the launch it is already running.
//
// The NodeClaim and the NodeClass are read from the API server, not the
// informer cache: they are the last word before a billed machine and an
// upstream quota reservation, and one GET each is nothing next to a launch.
func (c *CloudProvider) resolveLaunch(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*v1alpha1.CleverNodeClass, *cloudprovider.InstanceType, error) {
	if err := c.checkNodeClaimWanted(ctx, nodeClaim); err != nil {
		return nil, nil, err
	}
	nodeClass, err := c.resolveNodeClassFromNodeClaim(ctx, nodeClaim)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("resolving node class from nodeclaim, %w", err))
		}
		return nil, nil, fmt.Errorf("resolving node class from nodeclaim, %w", err)
	}
	// Ready must be affirmatively True: an Unknown readiness (controller not
	// yet reconciled, stale status) must not launch machines either.
	if readiness := nodeClass.StatusConditions().Get(status.ConditionReady); !readiness.IsTrue() {
		return nil, nil, cloudprovider.NewNodeClassNotReadyError(errors.New(readiness.Message))
	}
	// Readiness conditions survive deletion untouched (the nodeclass finalize
	// path only lists NodeClaims), so DeletionTimestamp is the only terminating
	// signal here. Refusing lets karpenter-core delete the claim instead of
	// launching a VM the nodeclass finalizer would then wait on indefinitely.
	if !nodeClass.DeletionTimestamp.IsZero() {
		return nil, nil, cloudprovider.NewNodeClassNotReadyError(fmt.Errorf("nodeclass %s is terminating", nodeClass.Name))
	}
	instanceType, err := c.resolveInstanceType(nodeClaim)
	if err != nil {
		return nil, nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("resolving instance type, %w", err))
	}
	return nodeClass, instanceType, nil
}

// checkNodeClaimWanted fails the launch of a NodeClaim that is no longer
// wanted: gone, replaced by another NodeClaim of the same name (whose UID the
// NodeGroup's owner reference would not carry, so garbage collection would
// reap the new group at once), or being deleted. The error is a plain one, on
// purpose: an InsufficientCapacityError or a NodeClassNotReadyError would make
// karpenter-core count a capacity or nodeclass disruption for a claim that is
// going away anyway, while a plain error requeues the claim, whose next
// reconcile finalizes it. Without a provider ID core deletes no NodeGroup:
// this attempt created none, and a group an earlier attempt created carries
// the claim's owner reference, which hands it to garbage collection.
func (c *CloudProvider) checkNodeClaimWanted(ctx context.Context, nodeClaim *karpv1.NodeClaim) error {
	live := &karpv1.NodeClaim{}
	if err := c.apiReader.Get(ctx, types.NamespacedName{Name: nodeClaim.Name}, live); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("nodeclaim %s no longer exists; not launching it", nodeClaim.Name)
		}
		return fmt.Errorf("getting nodeclaim, %w", err)
	}
	if live.UID != nodeClaim.UID {
		return fmt.Errorf("nodeclaim %s was replaced by another nodeclaim of the same name (uid %s, launching for uid %s); not launching it",
			nodeClaim.Name, live.UID, nodeClaim.UID)
	}
	if !live.DeletionTimestamp.IsZero() {
		return fmt.Errorf("nodeclaim %s is being deleted; not launching it", nodeClaim.Name)
	}
	return nil
}

func (c *CloudProvider) resolveNodeClassFromNodeClaim(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*v1alpha1.CleverNodeClass, error) {
	nodeClass := &v1alpha1.CleverNodeClass{}
	if err := c.apiReader.Get(ctx, types.NamespacedName{Name: nodeClaim.Spec.NodeClassRef.Name}, nodeClass); err != nil {
		return nil, err
	}
	return nodeClass, nil
}

// resolveInstanceType picks the cheapest available catalog flavor that
// satisfies the NodeClaim's scheduling requirements and resource requests. It
// reads the catalogue GetInstanceTypes serves, so a flavor the scheduler sees
// unavailable is never launched: a claim planned before a rejection covered
// its cheapest option — including a rejection recorded while its launch
// waited for the creation lock (see resolveLaunch) — falls to the next one, or
// fails fast with none.
func (c *CloudProvider) resolveInstanceType(nodeClaim *karpv1.NodeClaim) (*cloudprovider.InstanceType, error) {
	requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)
	var best *cloudprovider.InstanceType
	bestPrice := 0.0
	// Flavors that would serve the claim but are unavailable right now, so
	// that a failure names that cause only when it is the cause.
	unavailable := 0
	for _, it := range c.instanceTypes() {
		if it.Requirements.Intersects(requirements) != nil {
			continue
		}
		if !resources.Fits(nodeClaim.Spec.Resources.Requests, it.Allocatable()) {
			continue
		}
		compatible := it.Offerings.Compatible(requirements)
		offerings := compatible.Available()
		if len(offerings) == 0 {
			if len(compatible) > 0 {
				unavailable++
			}
			continue
		}
		price := offerings.Cheapest().Price
		if best == nil || price < bestPrice {
			best = it
			bestPrice = price
		}
	}
	if best == nil {
		if unavailable > 0 {
			return nil, fmt.Errorf("no available clever cloud flavor satisfies the nodeclaim requirements and resource requests "+
				"(%d flavor(s) that would satisfy them are currently unavailable after a quota rejection or an upstream refusal)", unavailable)
		}
		return nil, fmt.Errorf("no clever cloud flavor satisfies the nodeclaim requirements and resource requests")
	}
	return best, nil
}

// buildNodeClaim converts a NodeGroup into the NodeClaim shape Karpenter
// expects from the cloud provider (labels resolved, provider ID, capacity).
// Every single-valued requirement of the instance type and of its offering
// becomes a label. karpenter-core merges them into the NodeClaim at launch and
// copies them onto the node at registration, which is the only way the
// well-known ones (topology.kubernetes.io/region and /zone above all, which
// the platform never sets) reach a Clever Cloud node.
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
