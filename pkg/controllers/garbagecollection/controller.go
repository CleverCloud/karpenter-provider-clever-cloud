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

// Package garbagecollection deletes Karpenter-managed NodeGroups whose
// NodeClaim no longer exists. Owner references already cover most paths;
// this controller is the safety net for NodeClaims force-deleted with their
// finalizer stripped while the kubernetes garbage collector missed the
// dependent. Reaping requires the NodeClaim owner reference stamped at
// creation, not just the managed label: the label survives copying a
// karpenter-created manifest by hand, and deleting on the label alone would
// destroy nodes this provider never created. The reference is also the
// identity — a group is never reaped while any NodeClaim it references is
// alive, whatever its labels claim. Deliberate trade-off: a group whose
// owner reference was stripped (kubectl delete --cascade=orphan, restore
// tooling) is never reaped and must be deleted manually — leaking a VM beats
// destroying one that was never ours.
package garbagecollection

import (
	"context"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis"
	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

const (
	pollPeriod = 2 * time.Minute
	// minAge protects freshly created NodeGroups from racing with NodeClaim
	// informer lag.
	minAge = 2 * time.Minute
)

type Controller struct {
	kubeClient        client.Client
	nodeGroupProvider *nodegroup.Provider
	recorder          events.Recorder
	// warned dedups the refusal log/event per NodeGroup for this process
	// lifetime; Reconcile is a singleton, no locking needed.
	warned map[string]struct{}
}

func NewController(kubeClient client.Client, nodeGroupProvider *nodegroup.Provider, recorder events.Recorder) *Controller {
	return &Controller{kubeClient: kubeClient, nodeGroupProvider: nodeGroupProvider, recorder: recorder, warned: map[string]struct{}{}}
}

func (c *Controller) Name() string {
	return "nodegroup.garbagecollection"
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	nodeGroups, err := c.nodeGroupProvider.List(ctx)
	if err != nil {
		return reconciler.Result{}, err
	}
	nodeClaims := &karpv1.NodeClaimList{}
	if err := c.kubeClient.List(ctx, nodeClaims); err != nil {
		return reconciler.Result{}, err
	}
	claimNames := map[string]struct{}{}
	for i := range nodeClaims.Items {
		claimNames[nodeClaims.Items[i].Name] = struct{}{}
	}
	if err := c.reapVanishedNodeClaims(ctx, nodeClaims); err != nil {
		return reconciler.Result{}, err
	}
	// Set through a defer so the gauge reflects this sweep's (possibly
	// partial) count even when a Delete below errors out mid-loop; a stale
	// value from a previous sweep must not keep an alert firing or silent.
	refused := 0
	defer func() { metrics.GCRefusedNodeGroups.Set(float64(refused), nil) }()
	for i := range nodeGroups {
		ng := &nodeGroups[i]
		if !ng.DeletionTimestamp.IsZero() || time.Since(ng.CreationTimestamp.Time) < minAge {
			continue
		}
		claimName := ng.Labels[v1alpha1.NodeClaimLabelKey]
		if claimName == "" {
			claimName = ng.Name
		}
		if _, ok := claimNames[claimName]; ok {
			continue
		}
		owners := nodegroup.NodeClaimOwners(ng)
		if len(owners) == 0 {
			refused++
			c.refuse(ctx, ng, "refusing to garbage collect nodegroup carrying the managed label without a NodeClaim owner reference; "+
				"remove the label if it was copied from a karpenter-created manifest, or delete the nodegroup manually if its owner was deliberately orphaned")
			continue
		}
		if ownerAlive(owners, claimNames) {
			// The mutable nodeclaim label disagrees with a living owner —
			// never reap a group whose recorded owner still exists.
			refused++
			c.refuse(ctx, ng, "refusing to garbage collect nodegroup whose NodeClaim owner is alive but whose nodeclaim label matches nothing; fix the label")
			continue
		}
		if err := c.nodeGroupProvider.Delete(ctx, ng.Name); err != nil {
			if apierrors.IsNotFound(err) {
				// Someone else (owner-reference cascade, operator) deleted it
				// between List and Delete — not this safety net's work.
				continue
			}
			return reconciler.Result{}, err
		}
		metrics.GCReapedNodeGroups.Inc(nil)
		c.recorder.Publish(events.Event{
			InvolvedObject: ng,
			Type:           corev1.EventTypeNormal,
			Reason:         "GarbageCollected",
			Message:        "deleted orphaned nodegroup: no NodeClaim references it",
			DedupeValues:   []string{ng.Name},
		})
		log.FromContext(ctx).WithValues("NodeGroup", ng.Name).Info("garbage collected orphaned nodegroup")
	}
	return reconciler.Result{RequeueAfter: pollPeriod}, nil
}

// reapVanishedNodeClaims fast-fails NodeClaims whose NodeGroup disappeared
// after launch while the claim never registered — the quota engine reclaiming
// an accepted group is the documented cause (docs/E2E-RESULTS.md). Without
// this, each such claim burns karpenter's 15-minute registration TTL on a
// group that no longer exists: karpenter-core's own GC deliberately skips
// unregistered claims, so this window is the provider's to cover. Claims
// younger than minAge are skipped (informer lag), and existence is checked
// against ALL NodeGroups — a label-stripped group must never read as
// vanished. Deletion routes through karpenter's termination flow, which
// resolves to NodeClaimNotFound once it sees the group is gone.
func (c *Controller) reapVanishedNodeClaims(ctx context.Context, nodeClaims *karpv1.NodeClaimList) error {
	allGroups := &ngv1.NodeGroupList{}
	if err := c.kubeClient.List(ctx, allGroups); err != nil {
		return err
	}
	groupNames := map[string]struct{}{}
	for i := range allGroups.Items {
		groupNames[allGroups.Items[i].Name] = struct{}{}
	}
	for i := range nodeClaims.Items {
		claim := &nodeClaims.Items[i]
		// Never touch claims that are not this provider's: a second karpenter
		// provider's launched-but-unregistered claims would otherwise read as
		// vanished (their provider IDs do not parse, their groups do not
		// exist here) and get their machines destroyed mid-bootstrap.
		if ref := claim.Spec.NodeClassRef; ref == nil || ref.Group != apis.Group || ref.Kind != "CleverNodeClass" {
			continue
		}
		if !claim.DeletionTimestamp.IsZero() || time.Since(claim.CreationTimestamp.Time) < minAge {
			continue
		}
		if !claim.StatusConditions().Get(karpv1.ConditionTypeLaunched).IsTrue() ||
			claim.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue() {
			continue
		}
		groupName, err := nodegroup.ParseProviderID(claim.Status.ProviderID)
		if err != nil {
			// No provider ID yet: the NodeGroup carries the NodeClaim's name.
			groupName = claim.Name
		}
		if _, ok := groupNames[groupName]; ok {
			continue
		}
		if err := c.kubeClient.Delete(ctx, claim); client.IgnoreNotFound(err) != nil {
			return err
		}
		metrics.NodeGroupVanished.Inc(nil)
		c.recorder.Publish(events.Event{
			InvolvedObject: claim,
			Type:           corev1.EventTypeWarning,
			Reason:         "NodeGroupVanished",
			Message:        "NodeGroup disappeared after launch before the node registered (usually the quota engine reclaiming an accepted group); deleting the nodeclaim instead of waiting out the registration TTL",
			DedupeValues:   []string{claim.Name},
		})
		log.FromContext(ctx).WithValues("NodeClaim", claim.Name, "NodeGroup", groupName).Info(
			"deleted nodeclaim whose nodegroup vanished before registration")
	}
	return nil
}

// refuse surfaces a reap refusal: a gauge tick per sweep (done by the
// caller), a log line once per NodeGroup per process lifetime (the condition
// persists, one line per 2-minute sweep would drown the signal), and a
// Kubernetes Event republished hourly — events age out of etcd after ~1h,
// and an operator investigating a days-old refusal must still find it on
// kubectl describe.
func (c *Controller) refuse(ctx context.Context, ng *ngv1.NodeGroup, msg string) {
	if _, ok := c.warned[ng.Name]; !ok {
		c.warned[ng.Name] = struct{}{}
		log.FromContext(ctx).WithValues("NodeGroup", ng.Name).Info(msg)
	}
	c.recorder.Publish(events.Event{
		InvolvedObject: ng,
		Type:           corev1.EventTypeWarning,
		Reason:         "GarbageCollectionRefused",
		Message:        msg,
		DedupeValues:   []string{ng.Name},
		DedupeTimeout:  time.Hour,
	})
}

func ownerAlive(owners []string, claimNames map[string]struct{}) bool {
	for _, owner := range owners {
		if _, ok := claimNames[owner]; ok {
			return true
		}
	}
	return false
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}
