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

// Package nodereplacement keeps a NodeClaim, and the NodeGroup behind it,
// across the replacement of its node by the platform.
//
// Clever Cloud replaces the node of a NodeGroup in place: it adds a node to
// the group under the lowest free index ("<nodegroup>-node1" next to
// "-node0", "-node0" again the next time; spec.nodeCount stays 1), cordons
// the old node and labels it clever-cloud.com/draining=true, evicts its pods
// through the Eviction API, then deletes the old Node object and its VM. A
// Kubernetes upgrade of the cluster does that to every node of every group,
// and the platform's repair of a NotReady worker does it to one.
//
// Left to karpenter-core, that destroys the group instead of renewing it.
// The old Node carries core's termination finalizer, so its deletion makes
// core delete the NodeClaim, then the whole NodeGroup, replacement node
// included. The NodeClaim core creates for the evicted pods cannot start
// either while the cluster is upgrading: the platform refuses NodeGroup
// creations until the upgrade ends. Measured on an upgrade from 1.35 to 1.36
// (docs/E2E-RESULTS.md): the evicted pod stayed Pending, a PodDisruptionBudget
// then blocked the drain of the next node, and the platform failed the
// upgrade after 30 minutes.
//
// This controller makes the replacement node the NodeClaim's node, in two
// steps:
//
//   - release: once the platform takes the registered node out of a managed
//     group whose nodeCount is 1 — a replacement node has joined, and the
//     registered node is cordoned or not Ready — core's termination finalizer
//     is removed from it, so the platform's deletion removes the Node object
//     without reaching the NodeClaim. The node is annotated
//     karpenter.sh/do-not-disrupt for as long, which keeps core's own
//     disruption (an emptied node is a consolidation candidate) from deleting
//     the NodeClaim under the platform's drain;
//   - adoption: once the node carrying the NodeClaim's provider ID is not the
//     node core registered (another name, or still carrying the unregistered
//     taint), or no node carries it, the NodeClaim's Registered and
//     Initialized conditions are set back to Unknown. Core's own registration
//     then runs again, on the node the providerid controller stamped in the
//     meantime: labels, taints, finalizer and owner reference are synced, the
//     unregistered taint is lifted, and the NodeClaim points at the new node.
//
// A released node is a node core no longer drains: deleting it, or its
// NodeClaim, removes it at once. The release is therefore bound to the
// platform acting on that node, and to releaseWindow: a registered node gets
// its finalizer back as soon as it is neither cordoned nor NotReady, when its
// replacement goes away, when the group is resized, and in any case once the
// replacement is older than releaseWindow (a drain the platform gave up, a
// cluster left FAILED by its upgrade).
//
// Nothing else is taken over from core. A NodeClaim whose node is gone with
// no replacement is back to an unregistered launch, which core's registration
// timeout deletes after 15 minutes.
//
// Only NodeClaims of this provider are followed, and only through the
// NodeGroup named after them, managed and carrying their owner reference
// (name and UID). Every write is decided on uncached reads.
package nodereplacement

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis"
	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// ReasonNodeReplaced is the reason this controller writes on the Registered
// and Initialized conditions it sets back to Unknown. Core overwrites the one
// on Registered on its next registration pass, and leaves the one on
// Initialized until the NodeClaim is Registered again: that is how the
// nodegroupstatus controller tells a reopened registration from a launch.
const ReasonNodeReplaced = "NodeReplaced"

// releaseWindow is how long after a replacement node joined its group the
// registered node may stay released. The platform gives a drain 30 minutes
// before it fails the upgrade, and leaves both nodes in the group when it
// does: past that, nobody is draining the node any more, and it must be
// core's to drain again. Counted from the replacement's creation, which
// survives a restart of the controller and makes a node that lingers in a
// group (a replacement that never took over, an orphaned Node object) unable
// to release anything once it is that old.
const releaseWindow = 35 * time.Minute

// conflictRetry is how soon a write refused on a stale resourceVersion is
// attempted again. The watch re-triggers the reconcile on the change that
// caused the conflict anyway; this is a backstop.
const conflictRetry = time.Second

type Controller struct {
	kubeClient client.Client
	// uncached reads straight from the API server. Removing a finalizer and
	// reopening a registration both rest on which nodes of a group exist and
	// what they carry, which is exactly what a lagging cache gets wrong while
	// the platform adds one node and deletes another.
	uncached client.Reader
	recorder events.Recorder
	clock    clock.Clock
}

func NewController(kubeClient client.Client, uncached client.Reader, recorder events.Recorder, clk clock.Clock) *Controller {
	return &Controller{kubeClient: kubeClient, uncached: uncached, recorder: recorder, clock: clk}
}

func (c *Controller) Name() string {
	return "node.replacement"
}

// Reconcile is keyed by NodeGroup name, which is also the NodeClaim's.
func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	// The cache decides whether there is anything to do; observe then reads
	// everything again uncached before a write.
	seen, err := observe(ctx, c.kubeClient, req.Name)
	if err != nil || seen == nil {
		return reconcile.Result{}, err
	}
	if next, wait := seen.step(c.clock.Now()); next == stepNone {
		return reconcile.Result{RequeueAfter: wait}, nil
	}
	confirmed, err := observe(ctx, c.uncached, req.Name)
	if err != nil || confirmed == nil {
		return reconcile.Result{}, err
	}
	next, wait := confirmed.step(c.clock.Now())
	switch next {
	case stepAdopt:
		return c.adopt(ctx, confirmed)
	case stepRelease:
		return c.release(ctx, confirmed)
	case stepRestore:
		return c.restore(ctx, confirmed)
	}
	return reconcile.Result{RequeueAfter: wait}, nil
}

type step int

const (
	stepNone step = iota
	// stepRelease removes core's termination finalizer from the registered
	// node of a group the platform is replacing the node of.
	stepRelease
	// stepRestore puts that finalizer back once the platform is no longer
	// replacing the node.
	stepRestore
	// stepAdopt reopens the NodeClaim's registration for a node core has not
	// registered.
	stepAdopt
)

// observation is one consistent read of a registered NodeClaim, its NodeGroup
// and the nodes of that group.
type observation struct {
	nodeClaim *karpv1.NodeClaim
	nodeGroup *ngv1.NodeGroup
	// holder is the node carrying the NodeClaim's provider ID, nil when none
	// does. The providerid controller stamps at most one node per group.
	holder *corev1.Node
	// others are the group's nodes without a provider ID: the replacement the
	// platform added, until the old node goes and it is stamped.
	others []*corev1.Node
}

// observe reads a NodeClaim this controller follows and what its NodeGroup
// runs, through reader. It returns nil for anything else: a NodeClaim of
// another provider, terminating, not registered, or whose NodeGroup is not
// the managed group created for it.
func observe(ctx context.Context, reader client.Reader, name string) (*observation, error) {
	nodeClaim := &karpv1.NodeClaim{}
	if err := reader.Get(ctx, types.NamespacedName{Name: name}, nodeClaim); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	// A terminating NodeClaim is core's: its finalizer deletes the nodes and
	// the group. A NodeClaim that is not Registered is a launch, which core's
	// registration and liveness already own.
	if !isCleverNodeClaim(nodeClaim) || !nodeClaim.DeletionTimestamp.IsZero() ||
		nodeClaim.Status.ProviderID != nodegroup.ProviderID(name) ||
		!nodeClaim.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue() {
		return nil, nil
	}
	ng := &ngv1.NodeGroup{}
	if err := reader.Get(ctx, types.NamespacedName{Name: name}, ng); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if !ng.DeletionTimestamp.IsZero() || !nodegroup.OwnedByNodeClaim(ng, nodeClaim) {
		return nil, nil
	}
	nodes := &corev1.NodeList{}
	if err := reader.List(ctx, nodes, client.MatchingLabels{v1alpha1.NodeGroupNodeLabelKey: name}); err != nil {
		return nil, err
	}
	seen := &observation{nodeClaim: nodeClaim, nodeGroup: ng}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		// A kubelet can rewrite its own Node's clever-cloud.com labels; its
		// name is bound to its credential and is what places it in a group.
		if !isNodeOf(node.Name, name) {
			continue
		}
		switch node.Spec.ProviderID {
		case nodeClaim.Status.ProviderID:
			if seen.holder != nil {
				// Two nodes on one provider ID is the state the providerid
				// controller exists to prevent, and one karpenter-core cannot
				// register out of: nothing here would make it better.
				return nil, nil
			}
			seen.holder = node
		case "":
			if node.DeletionTimestamp.IsZero() {
				seen.others = append(seen.others, node)
			}
		}
	}
	// The node core registered is looked up by name too, unless the list
	// already returned it: should its kubelet drop or rewrite the nodegroup
	// label, the list misses it, and a registered node must never read as
	// replaced — nor as the only holder next to another stamped node.
	registered := nodeClaim.Status.NodeName
	if registered == "" || (seen.holder != nil && seen.holder.Name == registered) {
		return seen, nil
	}
	node := &corev1.Node{}
	if err := reader.Get(ctx, types.NamespacedName{Name: registered}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return seen, nil
		}
		return nil, err
	}
	if node.Spec.ProviderID != nodeClaim.Status.ProviderID {
		return seen, nil
	}
	// It still carries the provider ID: two holders if the list found
	// another, and nothing to follow if its name places it in no node of
	// this group (a provider ID this controller's rules never gave it).
	if seen.holder != nil || !isNodeOf(node.Name, name) {
		return nil, nil
	}
	seen.holder = node
	return seen, nil
}

// step returns what the observation calls for now, and, when that is
// nothing, how long until that changes without any watch event: the moment a
// released node's replacement leaves releaseWindow.
func (o *observation) step(now time.Time) (step, time.Duration) {
	if o.replaced() {
		// Core's repair already decided to replace this NodeClaim and deletes
		// it once its own replacement is up, without looking at the node
		// again: adopting the platform's replacement would hand that healthy
		// node, and the pods it took back, to a forced drain. The condition is
		// cleared if core gives the repair up, and the registration reopens.
		if isBeingRepaired(o.nodeClaim) {
			return stepNone, 0
		}
		return stepAdopt, 0
	}
	if !o.holder.DeletionTimestamp.IsZero() {
		// A registered node being deleted with its finalizer is core's
		// termination at work; without it, the object is about to disappear.
		return stepNone, 0
	}
	held := controllerutil.ContainsFinalizer(o.holder, karpv1.TerminationFinalizer)
	_, marked := o.holder.Annotations[v1alpha1.NodeReplacementAnnotationKey]
	replacement, remaining := o.replacement(now)
	switch {
	case replacement != nil && held:
		return stepRelease, 0
	case replacement != nil:
		return stepNone, remaining
	case !held || marked:
		return stepRestore, 0
	}
	return stepNone, 0
}

// replaced reports whether the node core registered for the NodeClaim is no
// longer the node carrying its provider ID: another node holds it (the
// replacement, stamped once the old node went), a new Node object does under
// the same name (it joined with the group's unregistered taint, which core
// lifts in the patch that registers a node), or no node does.
//
// The node's own karpenter.sh/registered label is deliberately not what
// decides: a NodeGroup created by v0.12.0 can carry that key in its immutable
// spec.labels, which the platform applies to every node of the group, and a
// kubelet can remove it from its own Node, while neither can name a node nor,
// under NodeRestriction, change its taints.
func (o *observation) replaced() bool {
	if o.holder == nil || o.holder.Name != o.nodeClaim.Status.NodeName {
		return true
	}
	for i := range o.holder.Spec.Taints {
		if o.holder.Spec.Taints[i].MatchTaint(&karpv1.UnregisteredNoExecuteTaint) {
			return true
		}
	}
	return false
}

// replacement returns the node the platform is replacing the registered node
// with, and how long the registered node may still stay released for it; nil
// and zero when the platform is not replacing it, or no longer.
//
// It takes all of: a group of one (a group resized from outside karpenter,
// nodeCount above 1, keeps both nodes), a registered node the platform is
// taking out — cordoned, which is how its drain starts, or not Ready, which
// is what it repairs and deletes within two seconds of the cordon — and a
// node without a provider ID that joined the group less than releaseWindow
// ago. A second node alone is not enough: until the platform cordons the
// registered node nobody is draining it, and a Node object can linger in a
// group for other reasons.
func (o *observation) replacement(now time.Time) (*corev1.Node, time.Duration) {
	if o.nodeGroup.Spec.NodeCount != 1 || !isLeaving(o.holder) {
		return nil, 0
	}
	newest := o.newestOther()
	if newest == nil {
		return nil, 0
	}
	remaining := releaseWindow - now.Sub(newest.CreationTimestamp.Time)
	if remaining <= 0 {
		return nil, 0
	}
	return newest, remaining
}

// newestOther returns the node that joined the group last among those without
// a provider ID, nil when there is none.
func (o *observation) newestOther() *corev1.Node {
	var newest *corev1.Node
	for _, node := range o.others {
		if newest == nil || node.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = node
		}
	}
	return newest
}

// stalled returns the replacement node a released node outlived: the platform
// started replacing the registered node, and releaseWindow later both nodes
// are still in the group.
func (o *observation) stalled() *corev1.Node {
	if o.nodeGroup.Spec.NodeCount != 1 || !isLeaving(o.holder) {
		return nil
	}
	return o.newestOther()
}

// release removes core's termination finalizer from the node the platform is
// replacing. With the finalizer, the platform's deletion of that Node makes
// core delete the NodeClaim and the NodeGroup, replacement node included.
// Without it the Node object simply goes, and adopt hands the NodeClaim to
// the replacement. The platform drains the node itself before deleting it.
//
// The same patch annotates the node karpenter.sh/do-not-disrupt. The pods the
// platform evicts are terminating, which core does not count: the node reads
// as empty while they stop, and with consolidateAfter 0s core's emptiness
// deletes the NodeClaim 15 seconds later — the NodeGroup and the replacement
// with it, in the middle of the upgrade.
func (c *Controller) release(ctx context.Context, seen *observation) (reconcile.Result, error) {
	node := seen.holder
	replacement, remaining := seen.replacement(c.clock.Now())
	if replacement == nil {
		// The window closed between the decision and here: the next pass
		// decides again.
		return reconcile.Result{RequeueAfter: conflictRetry}, nil
	}
	stored := node.DeepCopy()
	controllerutil.RemoveFinalizer(node, karpv1.TerminationFinalizer)
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	// The marker records whether the do-not-disrupt annotation is this
	// controller's to remove: one the user set stays when the node is restored.
	node.Annotations[v1alpha1.NodeReplacementAnnotationKey] = replacementKeepsDoNotDisrupt
	if node.Annotations[karpv1.DoNotDisruptAnnotationKey] != "true" {
		node.Annotations[karpv1.DoNotDisruptAnnotationKey] = "true"
		node.Annotations[v1alpha1.NodeReplacementAnnotationKey] = replacementOwnsDoNotDisrupt
	}
	// The finalizers are a list, which a merge patch replaces whole: the
	// optimistic lock keeps a concurrent change of it from being undone.
	if err := c.kubeClient.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		return retryOnConflict(err)
	}
	log.FromContext(ctx).WithValues("NodeClaim", seen.nodeClaim.Name, "NodeGroup", seen.nodeGroup.Name, "Node", node.Name, "replacement", replacement.Name).Info(
		"the platform is replacing the node of this nodegroup: released the registered node from karpenter's termination finalizer, so its deletion by the platform does not delete the nodeclaim and the nodegroup")
	c.recorder.Publish(events.Event{
		InvolvedObject: seen.nodeClaim,
		Type:           corev1.EventTypeNormal,
		Reason:         "NodeReplacementStarted",
		Message: fmt.Sprintf("The platform is replacing %s with %s in NodeGroup %s (cluster upgrade or node repair). Released %s from karpenter's termination finalizer so that its deletion by the platform does not delete this NodeClaim; %s is adopted once %s is gone. Until then karpenter does not drain %s: drain it by hand before deleting it or this NodeClaim",
			node.Name, replacement.Name, seen.nodeGroup.Name, node.Name, replacement.Name, node.Name, node.Name),
		DedupeValues: []string{seen.nodeClaim.Name, node.Name, replacement.Name},
	})
	return reconcile.Result{RequeueAfter: remaining}, nil
}

// restore puts core's termination finalizer back on a registered node that
// lost it while the platform is not, or no longer, replacing it: the node was
// uncordoned or is Ready again, the replacement went away, the group was
// resized, releaseWindow ran out, or something else removed the finalizer. A
// node of karpenter must not stay without it, or deleting the Node would no
// longer drain it nor delete its NodeClaim.
func (c *Controller) restore(ctx context.Context, seen *observation) (reconcile.Result, error) {
	node := seen.holder
	stored := node.DeepCopy()
	controllerutil.AddFinalizer(node, karpv1.TerminationFinalizer)
	if node.Annotations[v1alpha1.NodeReplacementAnnotationKey] == replacementOwnsDoNotDisrupt {
		delete(node.Annotations, karpv1.DoNotDisruptAnnotationKey)
	}
	delete(node.Annotations, v1alpha1.NodeReplacementAnnotationKey)
	if err := c.kubeClient.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		return retryOnConflict(err)
	}
	logger := log.FromContext(ctx).WithValues("NodeClaim", seen.nodeClaim.Name, "NodeGroup", seen.nodeGroup.Name, "Node", node.Name)
	replacement := seen.stalled()
	if replacement == nil {
		logger.Info("restored karpenter's termination finalizer on a registered node: the platform is not replacing it")
		return reconcile.Result{}, nil
	}
	logger.WithValues("replacement", replacement.Name, "after", releaseWindow.String()).Info(
		"restored karpenter's termination finalizer on a registered node: the platform did not finish replacing it")
	c.recorder.Publish(events.Event{
		InvolvedObject: seen.nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         "NodeReplacementStalled",
		Message: fmt.Sprintf("The platform started replacing %s with %s in NodeGroup %s more than %s ago and both nodes are still there (a drain it gave up, a failed cluster upgrade). Put karpenter's termination finalizer back on %s: karpenter drains it again when it or this NodeClaim is deleted. If the platform deletes %s after all, this NodeClaim and its NodeGroup are deleted with it and karpenter provisions again",
			node.Name, replacement.Name, seen.nodeGroup.Name, releaseWindow, node.Name, node.Name),
		DedupeValues: []string{seen.nodeClaim.Name, node.Name, replacement.Name},
	})
	return reconcile.Result{}, nil
}

// adopt sets the NodeClaim's Registered and Initialized conditions back to
// Unknown, which is the state core's registration acts on: it finds the node
// carrying the provider ID, syncs it as it does at a launch and records its
// name on the NodeClaim. Core returns early on any other Registered status,
// so without this the replacement node would keep the unregistered taint and
// never get the NodePool's labels.
//
// Reopening the registration also restarts core's registration timeout: a
// NodeClaim no node registers for within 15 minutes is deleted, as at launch.
func (c *Controller) adopt(ctx context.Context, seen *observation) (reconcile.Result, error) {
	nodeClaim := seen.nodeClaim
	previous := nodeClaim.Status.NodeName
	message := fmt.Sprintf("node %s is no longer the registered node of nodegroup %s", previous, seen.nodeGroup.Name)
	stored := nodeClaim.DeepCopy()
	// Dated on the clock core reads them with: its registration timeout
	// counts from the transition of Registered written here.
	conditions := nodeClaim.StatusConditions(status.WithClock(c.clock))
	conditions.SetUnknownWithReason(karpv1.ConditionTypeRegistered, ReasonNodeReplaced, message)
	conditions.SetUnknownWithReason(karpv1.ConditionTypeInitialized, ReasonNodeReplaced, message)
	nodeClaim.Status.NodeName = ""
	// The conditions are a list, which a merge patch replaces whole: the
	// optimistic lock keeps a condition core wrote since the read.
	if err := c.kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		return retryOnConflict(err)
	}
	metrics.NodeReplacements.Inc(nil)
	logger := log.FromContext(ctx).WithValues("NodeClaim", nodeClaim.Name, "NodeGroup", seen.nodeGroup.Name, "previous", previous)
	switch {
	case seen.holder != nil:
		logger.WithValues("Node", seen.holder.Name).Info("the node of this nodeclaim was replaced: reopened its registration so karpenter adopts the node now carrying its provider id")
		c.publishAdoption(nodeClaim, corev1.EventTypeNormal, fmt.Sprintf(
			"Node %s of NodeGroup %s was replaced by %s. Reopened the registration of this NodeClaim so that karpenter adopts %s instead of deleting the NodeGroup",
			previous, seen.nodeGroup.Name, seen.holder.Name, seen.holder.Name), seen.holder.Name)
		return reconcile.Result{}, nil
	case len(seen.others) > 0:
		logger.WithValues("Node", seen.others[0].Name).Info("the node of this nodeclaim was replaced: reopened its registration, the replacement node is waiting for its provider id")
		c.publishAdoption(nodeClaim, corev1.EventTypeNormal, fmt.Sprintf(
			"Node %s of NodeGroup %s was replaced by %s. Reopened the registration of this NodeClaim so that karpenter adopts %s instead of deleting the NodeGroup",
			previous, seen.nodeGroup.Name, seen.others[0].Name, seen.others[0].Name), seen.others[0].Name)
		return reconcile.Result{}, nil
	}
	logger.Info("the node of this nodeclaim is gone and its nodegroup has no other node: reopened its registration, karpenter's registration timeout deletes the nodeclaim if no node joins")
	c.publishAdoption(nodeClaim, corev1.EventTypeWarning, fmt.Sprintf(
		"Node %s of NodeGroup %s is gone and the group has no other node. Reopened the registration of this NodeClaim: karpenter adopts a node the platform adds to the group, and its registration timeout deletes the NodeClaim after 15 minutes otherwise",
		previous, seen.nodeGroup.Name), "")
	return reconcile.Result{}, nil
}

func (c *Controller) publishAdoption(nodeClaim *karpv1.NodeClaim, eventType, message, replacement string) {
	c.recorder.Publish(events.Event{
		InvolvedObject: nodeClaim,
		Type:           eventType,
		Reason:         "NodeReplaced",
		Message:        message,
		DedupeValues:   []string{nodeClaim.Name, replacement},
	})
}

// retryOnConflict turns a write refused on a stale resourceVersion, or on an
// object that went away, into a quiet retry: the next reconcile reads again.
func retryOnConflict(err error) (reconcile.Result, error) {
	if apierrors.IsNotFound(err) {
		return reconcile.Result{}, nil
	}
	if apierrors.IsConflict(err) {
		return reconcile.Result{RequeueAfter: conflictRetry}, nil
	}
	return reconcile.Result{}, err
}

// The values of v1alpha1.NodeReplacementAnnotationKey on a released node.
const (
	// replacementOwnsDoNotDisrupt: release added karpenter.sh/do-not-disrupt,
	// and restore removes it.
	replacementOwnsDoNotDisrupt = "do-not-disrupt"
	// replacementKeepsDoNotDisrupt: the node already carried it.
	replacementKeepsDoNotDisrupt = "released"
)

// isLeaving reports whether the platform is taking a node out of its group:
// it cordons the node it drains (and labels it clever-cloud.com/draining in
// the same write, which is not required here: the cordon is the part of a
// drain that cannot change), and the node it repairs is not Ready.
func isLeaving(node *corev1.Node) bool {
	return node.Spec.Unschedulable || nodeutils.GetCondition(node, corev1.NodeReady).Status != corev1.ConditionTrue
}

// isBeingRepaired reports whether karpenter-core's repair has a command in
// flight for this NodeClaim.
func isBeingRepaired(nodeClaim *karpv1.NodeClaim) bool {
	cond := nodeClaim.StatusConditions().Get(karpv1.ConditionTypeDisruptionReason)
	return cond.IsTrue() && cond.Reason == string(karpv1.DisruptionReasonUnhealthy)
}

// isNodeOf reports whether a node's name places it in a NodeGroup.
func isNodeOf(nodeName, nodeGroupName string) bool {
	group, ok := nodegroup.NodeGroupOfNode(nodeName)
	return ok && group == nodeGroupName
}

// isCleverNodeClaim reports whether a NodeClaim belongs to this provider. Any
// other provider's claims are never ours to touch.
func isCleverNodeClaim(nodeClaim *karpv1.NodeClaim) bool {
	ref := nodeClaim.Spec.NodeClassRef
	return ref != nil && ref.Group == apis.Group && ref.Kind == "CleverNodeClass"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&karpv1.NodeClaim{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			nodeClaim, ok := obj.(*karpv1.NodeClaim)
			return ok && isCleverNodeClaim(nodeClaim)
		}))).
		// A node joining, changing or leaving a group is what moves a
		// replacement forward, deletions included.
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(nodeGroupOfNode)).
		// A group resized from outside karpenter stops, or starts again, being
		// a group whose second node is a replacement.
		Watches(&ngv1.NodeGroup{}, &handler.EnqueueRequestForObject{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			ng, ok := obj.(*ngv1.NodeGroup)
			return ok && nodegroup.IsManaged(ng)
		}))).
		Complete(c)
}

// nodeGroupOfNode maps a node to the NodeGroup its name places it in, when its
// clever-cloud.com/nodegroup label agrees. Nodes of unmanaged groups map to a
// name no NodeClaim carries, which the reconcile drops on its first read.
func nodeGroupOfNode(_ context.Context, obj client.Object) []reconcile.Request {
	group, ok := nodegroup.NodeGroupOfNode(obj.GetName())
	if !ok || obj.GetLabels()[v1alpha1.NodeGroupNodeLabelKey] != group {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: group}}}
}
