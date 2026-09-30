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

// Package nodegroupstatus follows the NodeGroups this provider launched until
// the Clever Cloud node-group operator has synced them. Create reads the
// operator's verdict only inside its 15-second acceptance poll, while the VM is
// built well after that window: live, the group reached Synced 38-58 s after
// its creation, and one stayed Creating for 6 min 30 s. Before this
// controller, a refusal published after the poll, a group stuck in its first
// reconcile and an operator that never answers produced no provider signal at
// all. The first signal was karpenter-core's registration timeout, 15 minutes
// after the NodeClaim was created.
//
// It follows a group only while the NodeClaim it was launched for is Launched
// and not Registered. In that window core has handed the launch over and
// nothing else reads the group's status. What it does depends on what the
// operator reports:
//
//   - a terminal refusal (QuotaExceeded, or any ReconcileFailed reason that is
//     not transient) fails the launch. The NodeClaim is deleted so core
//     re-plans now, and core's termination deletes the group. The refusal is
//     recorded the way the acceptance poll records one: quota backoff or
//     flavor hold-out, counter, event;
//   - a transient failure (UpstreamError), which the operator retries on its
//     own, is only surfaced;
//   - a group still not synced syncOverdueAfter after its creation is counted
//     by the nodegroup_sync_overdue gauge and surfaced on the NodeClaim.
//
// Ready wins over every other condition, as in the acceptance poll: a Ready
// group is a booted VM whatever else its status carries.
//
// The destructive branch follows the garbage collector's rules. Only managed
// NodeGroups are followed, and only through the NodeClaim owner reference
// Create stamps, since the managed label alone is forgeable. Claims of other
// providers are never touched. The decision is confirmed with uncached reads,
// and the delete carries the confirming read's resourceVersion as a
// precondition, so a claim that registered in between is left alone: a
// Registered claim is never deleted.
package nodegroupstatus

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis"
	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// syncOverdueAfter is how long after its creation a launched NodeGroup may
// stay unsynced before it is reported. Healthy launches sync well within it:
// live, the node registered about 28 s after the group was created and the
// group reached Synced at 38-58 s (docs/E2E-RESULTS.md: node Ready at
// 26-60 s). Five minutes is several times the slowest healthy launch, so a
// healthy launch never trips it. It still leaves most of karpenter-core's
// 15-minute registration TTL, which counts from the NodeClaim's creation, for
// someone to look at the group before core writes the launch off.
const syncOverdueAfter = 5 * time.Minute

// conflictRetry is how soon a launch whose NodeClaim changed between the
// confirming read and the delete is re-examined. The NodeClaim watch
// re-triggers the reconcile on that change anyway; this is a backstop.
const conflictRetry = time.Second

type Controller struct {
	kubeClient client.Client
	// uncached reads straight from the API server. Failing a launch deletes a
	// NodeClaim, and the refusal and the unregistered claim that decision
	// rests on are exactly what a lagging cache can still show after the
	// operator or core moved on, so both are confirmed there first.
	uncached          client.Reader
	nodeGroupProvider *nodegroup.Provider
	recorder          events.Recorder

	mu sync.Mutex
	// overdue holds the NodeGroups the nodegroup_sync_overdue gauge counts. A
	// group leaves it as soon as a reconcile finds it synced, refused, gone or
	// terminating, or its NodeClaim registered, gone or terminating.
	overdue map[string]struct{}
	// logged dedups the log lines per NodeGroup and kind: this reconcile runs
	// on every status write of the group and every update of its NodeClaim.
	// Entries leave with the group, like overdue ones.
	logged map[string]struct{}
}

func NewController(kubeClient client.Client, uncached client.Reader, nodeGroupProvider *nodegroup.Provider, recorder events.Recorder) *Controller {
	return &Controller{
		kubeClient:        kubeClient,
		uncached:          uncached,
		nodeGroupProvider: nodeGroupProvider,
		recorder:          recorder,
		overdue:           map[string]struct{}{},
		logged:            map[string]struct{}{},
	}
}

func (c *Controller) Name() string {
	return "nodegroup.status"
}

func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ng := &ngv1.NodeGroup{}
	if err := c.kubeClient.Get(ctx, req.NamespacedName, ng); err != nil {
		if apierrors.IsNotFound(err) {
			c.forget(req.Name)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	nodeClaim, err := launchedNodeClaim(ctx, c.kubeClient, ng)
	if err != nil {
		return reconcile.Result{}, err
	}
	if nodeClaim == nil || ng.IsSynced() {
		c.forget(ng.Name)
		return reconcile.Result{}, nil
	}
	if ng.IsRefused() {
		c.forget(ng.Name)
		return c.failLaunch(ctx, ng.Name)
	}
	if reason, message, transient := ng.TransientFailure(); transient {
		c.surfaceTransientFailure(ctx, ng, nodeClaim, reason, message)
	}
	if remaining := syncOverdueAfter - time.Since(ng.CreationTimestamp.Time); remaining > 0 {
		return reconcile.Result{RequeueAfter: remaining}, nil
	}
	c.reportOverdue(ctx, ng, nodeClaim)
	return reconcile.Result{}, nil
}

// failLaunch deletes the NodeClaim of a NodeGroup the operator refused after
// the launch, so karpenter-core re-plans now instead of at its registration
// timeout, then records the refusal through the nodegroup provider so the
// re-plan lands elsewhere. Core's termination deletes the group, which frees
// its reservation; that deletion leaves an armed quota backoff in place, since
// a group refused before it synced held no capacity (nodegroup.Provider.Delete).
//
// Everything the decision rests on is read again uncached first. The refusal
// is recorded only once the claim is deleted: each count is a launch this
// controller failed, and a retry after a failed delete cannot count it twice.
func (c *Controller) failLaunch(ctx context.Context, name string) (reconcile.Result, error) {
	ng := &ngv1.NodeGroup{}
	if err := c.uncached.Get(ctx, types.NamespacedName{Name: name}, ng); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	nodeClaim, err := launchedNodeClaim(ctx, c.uncached, ng)
	if err != nil {
		return reconcile.Result{}, err
	}
	if nodeClaim == nil || ng.IsSynced() || !ng.IsRefused() {
		// The cache was behind: the claim registered or went away, or the
		// group recovered. The watch event carrying that state re-triggers
		// this reconcile once the cache has it.
		return reconcile.Result{}, nil
	}
	// The preconditions pin the delete to the claim just confirmed: a claim
	// that registered, or changed in any other way, since that read is left
	// alone and examined again.
	if err := c.kubeClient.Delete(ctx, nodeClaim, client.Preconditions{UID: &nodeClaim.UID, ResourceVersion: &nodeClaim.ResourceVersion}); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		if apierrors.IsConflict(err) {
			return reconcile.Result{RequeueAfter: conflictRetry}, nil
		}
		return reconcile.Result{}, fmt.Errorf("deleting nodeclaim %q, %w", nodeClaim.Name, err)
	}
	c.nodeGroupProvider.RecordLateRefusal(nodeClaim, ng)
	log.FromContext(ctx).WithValues("NodeClaim", nodeClaim.Name, "NodeGroup", ng.Name, "flavor", ng.Spec.Flavor, "status", operatorState(ng)).Info(
		"deleted the nodeclaim of a nodegroup the node-group operator refused after its launch")
	return reconcile.Result{}, nil
}

// surfaceTransientFailure reports a failure the operator retries on its own,
// seen on a launched group. As in the acceptance poll, nothing is deleted or
// held out for it. The event shares the poll's reason and dedupe key, so it
// does not repeat what the poll has just published for the same claim.
func (c *Controller) surfaceTransientFailure(ctx context.Context, ng *ngv1.NodeGroup, nodeClaim *karpv1.NodeClaim, reason, message string) {
	if c.firstLog("transient/" + ng.Name) {
		log.FromContext(ctx).WithValues("NodeClaim", nodeClaim.Name, "NodeGroup", ng.Name, "reason", reason, "message", message).Info(
			"node-group operator reports a transient failure on a launched nodegroup; not treated as a refusal")
	}
	c.recorder.Publish(events.Event{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         "NodeGroupTransientFailure",
		Message: fmt.Sprintf("The node-group operator reports a transient failure on NodeGroup %s after its launch (%s); it retries such failures on its own, so nothing was deleted or held out for it and the launch keeps waiting (the registration TTL is the backstop)",
			ng.Name, nodegroup.DescribeFailure(reason, message)),
		DedupeValues: []string{nodeClaim.Name},
	})
}

// reportOverdue counts a launched group still unsynced syncOverdueAfter after
// its creation, and says so on its NodeClaim. Nothing is deleted: the
// operator may still bring the group up, and karpenter-core's registration
// timeout already writes the launch off at 15 minutes.
func (c *Controller) reportOverdue(ctx context.Context, ng *ngv1.NodeGroup, nodeClaim *karpv1.NodeClaim) {
	c.setOverdue(ng.Name)
	state := operatorState(ng)
	if c.firstLog("overdue/" + ng.Name) {
		log.FromContext(ctx).WithValues("NodeClaim", nodeClaim.Name, "NodeGroup", ng.Name, "status", state).Info(
			fmt.Sprintf("launched nodegroup still not synced by the node-group operator %s after its creation", syncOverdueAfter))
	}
	c.recorder.Publish(events.Event{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         "NodeGroupSyncOverdue",
		Message: fmt.Sprintf("NodeGroup %s is still not synced by the node-group operator more than %s after its creation (%s) and this NodeClaim has not registered; karpenter's registration TTL deletes the NodeClaim 15 minutes after its creation. Inspect the group with kubectl get nodegroup %s -o yaml",
			ng.Name, syncOverdueAfter, state, ng.Name),
		DedupeValues: []string{nodeClaim.Name},
	})
}

func (c *Controller) setOverdue(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.overdue[name] = struct{}{}
	metrics.NodeGroupSyncOverdue.Set(float64(len(c.overdue)), nil)
}

// forget drops what is kept about a group this controller no longer follows.
func (c *Controller) forget(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.overdue, name)
	delete(c.logged, "transient/"+name)
	delete(c.logged, "overdue/"+name)
	metrics.NodeGroupSyncOverdue.Set(float64(len(c.overdue)), nil)
}

// firstLog reports whether key has not been logged yet, and marks it logged.
func (c *Controller) firstLog(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.logged[key]; ok {
		return false
	}
	c.logged[key] = struct{}{}
	return true
}

// launchedNodeClaim returns the NodeClaim a NodeGroup was launched for when
// this controller follows that launch, and nil otherwise. The group must be
// managed, not terminating, and carry the owner reference Create stamps for
// the claim it is named after. The claim must be this provider's, not
// terminating, Launched and not Registered, and its provider ID must name the
// group.
func launchedNodeClaim(ctx context.Context, reader client.Reader, ng *ngv1.NodeGroup) (*karpv1.NodeClaim, error) {
	if !nodegroup.IsManaged(ng) || !ng.DeletionTimestamp.IsZero() || !slices.Contains(nodegroup.NodeClaimOwners(ng), ng.Name) {
		return nil, nil
	}
	nodeClaim := &karpv1.NodeClaim{}
	if err := reader.Get(ctx, types.NamespacedName{Name: ng.Name}, nodeClaim); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if !isCleverNodeClaim(nodeClaim) || !nodeClaim.DeletionTimestamp.IsZero() ||
		!nodeClaim.StatusConditions().Get(karpv1.ConditionTypeLaunched).IsTrue() ||
		nodeClaim.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue() ||
		nodeClaim.Status.ProviderID != nodegroup.ProviderID(ng.Name) {
		return nil, nil
	}
	return nodeClaim, nil
}

// isCleverNodeClaim reports whether a NodeClaim belongs to this provider. Any
// other provider's claims are never ours to touch.
func isCleverNodeClaim(nodeClaim *karpv1.NodeClaim) bool {
	ref := nodeClaim.Spec.NodeClassRef
	return ref != nil && ref.Group == apis.Group && ref.Kind == "CleverNodeClass"
}

// operatorState summarises what the operator last wrote on a group, for the
// logs and the overdue event. An empty status means the operator never
// picked the group up.
func operatorState(ng *ngv1.NodeGroup) string {
	if ng.Status.Phase == "" && len(ng.Status.Conditions) == 0 {
		return "no status: the node-group operator has not picked it up"
	}
	parts := []string{fmt.Sprintf("phase %q", ng.Status.Phase)}
	for _, cond := range ng.Status.Conditions {
		if cond.Status == corev1.ConditionTrue {
			parts = append(parts, fmt.Sprintf("%s (%s)", cond.Type, nodegroup.DescribeFailure(cond.Reason, cond.Message)))
		}
	}
	return strings.Join(parts, ", ")
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&ngv1.NodeGroup{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool { return isManaged(e.Object) },
			// Old or new: a group that loses the managed label is reconciled
			// once more, to leave the overdue gauge.
			UpdateFunc:  func(e event.UpdateEvent) bool { return isManaged(e.ObjectOld) || isManaged(e.ObjectNew) },
			DeleteFunc:  func(e event.DeleteEvent) bool { return isManaged(e.Object) },
			GenericFunc: func(e event.GenericEvent) bool { return isManaged(e.Object) },
		})).
		// Core writes Launched on the NodeClaim after Create has returned, and
		// Registered later still. A refusal the group already carries when the
		// claim turns Launched, or the registration that ends the follow-up,
		// arrives through the claim, not through the group.
		Watches(&karpv1.NodeClaim{}, handler.EnqueueRequestsFromMapFunc(nodeGroupOfNodeClaim)).
		Complete(c)
}

func isManaged(obj client.Object) bool {
	ng, ok := obj.(*ngv1.NodeGroup)
	return ok && nodegroup.IsManaged(ng)
}

// nodeGroupOfNodeClaim maps one of this provider's NodeClaims to the NodeGroup
// named after it.
func nodeGroupOfNodeClaim(_ context.Context, obj client.Object) []reconcile.Request {
	nodeClaim, ok := obj.(*karpv1.NodeClaim)
	if !ok || !isCleverNodeClaim(nodeClaim) {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: nodeClaim.Name}}}
}
