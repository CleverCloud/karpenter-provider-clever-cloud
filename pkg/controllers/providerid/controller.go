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

// Package providerid stamps Karpenter-managed worker nodes with a provider
// ID. Clever Cloud does not set node.spec.providerID, but Karpenter can only
// match a Node to its NodeClaim through it. The provider ID is derived from
// the node's clever-cloud.com/nodegroup label, which Clever Cloud sets on
// every worker node it provisions.
package providerid

import (
	"context"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

type Controller struct {
	kubeClient client.Client
	// uncached reads straight from the API server. The uniqueness check below
	// must not run off the informer cache: reconciles are serialised, so the
	// previous node's patch is already committed upstream when the next one
	// starts, but it may not have reached the cache yet — and two unstamped
	// nodes read through that lag would both be told the ID is free.
	uncached client.Reader
	// warnedResized dedups the refusal log per node: the reconcile fires on
	// every update of a node that still has no provider ID. That is the
	// replacement node of every platform replacement, until the node it
	// replaces is gone (about 30 s), and the extra node of a resized
	// NodeGroup for its whole life. Entries are dropped as soon as a node is
	// stamped; what remains is bounded by the number of external resizes,
	// which are anomalies, not routine.
	warnedResized sync.Map
	// warnedStripped dedups the log of stripNodePoolLabel the same way, in
	// case the platform re-applies the group's labels to the node.
	warnedStripped sync.Map
}

func NewController(kubeClient client.Client, uncached client.Reader) *Controller {
	return &Controller{kubeClient: kubeClient, uncached: uncached}
}

func (c *Controller) Name() string {
	return "node.providerid"
}

func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	node := &corev1.Node{}
	if err := c.kubeClient.Get(ctx, req.NamespacedName, node); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if node.Spec.ProviderID != "" {
		return reconcile.Result{}, nil
	}
	nodeGroupName, ok := node.Labels[v1alpha1.NodeGroupNodeLabelKey]
	if !ok {
		return reconcile.Result{}, nil
	}
	// Only stamp nodes whose NodeGroup is managed by Karpenter: other
	// NodeGroups belong to the user.
	ng := &ngv1.NodeGroup{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodeGroupName}, ng); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !nodegroup.IsManaged(ng) {
		return reconcile.Result{}, nil
	}
	// The provider ID names the NodeGroup, so it identifies a single node only
	// while the 1 NodeClaim = 1 NodeGroup invariant holds. Something outside
	// karpenter can break it — the platform's alert-driven scaler through an
	// inherited autoscalingEnabled, a human, anything with nodegroups/scale
	// RBAC — and the garbage collector deliberately only surfaces that, never
	// fights it. Two Nodes on one provider ID would turn that surfaced anomaly
	// into destruction: karpenter-core's NodeForNodeClaim matches both, sets
	// Registered=False/MultipleNodesFound (terminal — registration returns
	// early on any non-Unknown Registered), and liveness deletes the NodeClaim
	// 15 minutes later, taking the NodeGroup and every VM in it.
	//
	// The check is on the Nodes that EXIST, not on spec.nodeCount. nodeCount is
	// desired state: on the recovery path (revert the resize back to 1) it
	// flips immediately while the extra Node object survives ~40s as its VM is
	// torn down, and any event on it in that window — cordon, drain, kubelet
	// heartbeat — would pass a nodeCount check and re-create the collision.
	//
	// Refusing every node of a resized group is not an option either: the
	// NodeClaim would never register, and liveness would delete it — the very
	// outcome above. So exactly one node keeps the ID and the extras are left
	// inert, for the GC's NodeGroupExternallyResized signal to carry — inert
	// includes not carrying karpenter.sh/nodepool (see stripNodePoolLabel).
	providerID := nodegroup.ProviderID(nodeGroupName)
	// Confirmed uncached: see the field comment on Controller.uncached.
	siblings := &corev1.NodeList{}
	if err := c.uncached.List(ctx, siblings, client.MatchingLabels{v1alpha1.NodeGroupNodeLabelKey: nodeGroupName}); err != nil {
		return reconcile.Result{}, err
	}
	for i := range siblings.Items {
		if siblings.Items[i].Name == node.Name || siblings.Items[i].Spec.ProviderID != providerID {
			continue
		}
		if _, warned := c.warnedResized.LoadOrStore(node.Name, struct{}{}); !warned {
			// A second node in a group still sized for one is the platform
			// replacing the first (cluster upgrade, node repair): this node
			// takes the ID over when the other is gone, see the deletion watch
			// in Register and the nodereplacement controller.
			cause := "something outside karpenter resized the group; this node stays unregistered and is reported by the garbage collector"
			if ng.Spec.NodeCount == 1 {
				cause = "the platform is replacing the node of this group; this node takes the id over once the other node is gone"
			}
			log.FromContext(ctx).WithValues("Node", node.Name, "NodeGroup", nodeGroupName, "owner", siblings.Items[i].Name).Info(
				"refusing to stamp a provider id: another node of this nodegroup already carries it, so the id would not be unique (" + cause + ")")
		}
		return reconcile.Result{}, c.stripNodePoolLabel(ctx, node, nodeGroupName)
	}
	c.warnedResized.Delete(node.Name)
	c.warnedStripped.Delete(node.Name)
	stored := node.DeepCopy()
	node.Spec.ProviderID = nodegroup.ProviderID(nodeGroupName)
	if err := c.kubeClient.Patch(ctx, node, client.MergeFrom(stored)); err != nil {
		return reconcile.Result{}, err
	}
	log.FromContext(ctx).WithValues("Node", node.Name, "provider-id", node.Spec.ProviderID).Info("stamped provider id on node")
	return reconcile.Result{}, nil
}

// stripNodePoolLabel removes karpenter.sh/nodepool from a node this controller
// refuses to stamp. NodeGroups created before that key was filtered out of the
// payload still carry it in their immutable spec.labels, and the platform
// applies those to every node of the group — so the extra node of a resized
// group joins with it and, being refused, never gets a provider ID.
// karpenter-core's cluster state ignores a node carrying karpenter.sh/nodepool
// without a provider ID, and its first sync after a restart waits for every
// node: one such node stops provisioning and disruption cluster-wide at the
// next controller restart. Without the label, core tracks the node as the
// unmanaged, tainted node it effectively is.
//
// Only ever called for the refused node of a managed group: the node holding
// the provider ID got the label from core's registration sync and keeps it,
// and nodes of unmanaged groups are not ours to touch. If the refused node
// later takes the ID over (its sibling gone), the registration sync puts the
// label back: at once while the NodeClaim is still unregistered, and after the
// nodereplacement controller reopened the registration when the sibling was
// the NodeClaim's registered node, replaced by the platform.
func (c *Controller) stripNodePoolLabel(ctx context.Context, node *corev1.Node, nodeGroupName string) error {
	if _, ok := node.Labels[karpv1.NodePoolLabelKey]; !ok {
		return nil
	}
	stored := node.DeepCopy()
	delete(node.Labels, karpv1.NodePoolLabelKey)
	// A merge patch removes only that key, leaving concurrent label writes
	// from the kubelet or the platform untouched.
	if err := c.kubeClient.Patch(ctx, node, client.MergeFrom(stored)); err != nil {
		return client.IgnoreNotFound(err)
	}
	if _, warned := c.warnedStripped.LoadOrStore(node.Name, struct{}{}); !warned {
		log.FromContext(ctx).WithValues("Node", node.Name, "NodeGroup", nodeGroupName).Info(
			"removed " + karpv1.NodePoolLabelKey + " from a node refused a provider id: an earlier version put it in the nodegroup's " +
				"immutable spec.labels, and on a node without a provider id it keeps karpenter-core's cluster state from syncing after a restart")
	}
	return nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&corev1.Node{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc:  func(e event.CreateEvent) bool { return needsProviderID(e.Object) },
			UpdateFunc:  func(e event.UpdateEvent) bool { return needsProviderID(e.ObjectNew) },
			DeleteFunc:  func(event.DeleteEvent) bool { return false },
			GenericFunc: func(e event.GenericEvent) bool { return needsProviderID(e.Object) },
		})).
		// The deletion of the node carrying a group's provider ID frees that
		// ID for the node this controller refused: the replacement the
		// platform added next to it (cluster upgrade, node repair). Nothing
		// else wakes that node up, and waiting for its next update left it
		// unstamped for 3 min 35 s on a live upgrade, with the unregistered
		// taint keeping every evicted pod off it.
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(c.unstampedSiblings), builder.WithPredicates(predicate.Funcs{
			CreateFunc:  func(event.CreateEvent) bool { return false },
			UpdateFunc:  func(event.UpdateEvent) bool { return false },
			DeleteFunc:  func(e event.DeleteEvent) bool { return freesProviderID(e.Object) },
			GenericFunc: func(event.GenericEvent) bool { return false },
		})).
		// Serialised on purpose. The uniqueness check reads the API server and
		// then patches; concurrent reconciles could interleave those two steps
		// and stamp the same provider id on two nodes. One at a time makes the
		// read-then-write safe, and this controller only ever fires on nodes
		// that still lack a provider id, so throughput is not a concern.
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(c)
}

// unstampedSiblings maps a deleted node to the nodes that still lack a
// provider ID in the NodeGroup its provider ID named. Read from the cache:
// the nodes it names are only reconciled, and Reconcile confirms everything
// uncached.
func (c *Controller) unstampedSiblings(ctx context.Context, obj client.Object) []reconcile.Request {
	nodeGroupName, ok := freedNodeGroup(obj)
	if !ok {
		return nil
	}
	siblings := &corev1.NodeList{}
	if err := c.kubeClient.List(ctx, siblings, client.MatchingLabels{v1alpha1.NodeGroupNodeLabelKey: nodeGroupName}); err != nil {
		log.FromContext(ctx).WithValues("NodeGroup", nodeGroupName).Error(err, "listing the nodes left in the nodegroup of a deleted node")
		return nil
	}
	var requests []reconcile.Request
	for i := range siblings.Items {
		if siblings.Items[i].Name != obj.GetName() && siblings.Items[i].Spec.ProviderID == "" {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: siblings.Items[i].Name}})
		}
	}
	return requests
}

// freesProviderID reports whether a deleted node carried the provider ID of a
// NodeGroup, which another node of that group can now take.
func freesProviderID(obj client.Object) bool {
	_, ok := freedNodeGroup(obj)
	return ok
}

// freedNodeGroup returns the NodeGroup whose provider ID a deleted node
// carried. Read from the provider ID, which is immutable once set, not from
// the clever-cloud.com/nodegroup label, which the node's kubelet can rewrite.
func freedNodeGroup(obj client.Object) (string, bool) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return "", false
	}
	name, err := nodegroup.ParseProviderID(node.Spec.ProviderID)
	return name, err == nil
}

func needsProviderID(obj client.Object) bool {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return false
	}
	_, hasLabel := node.Labels[v1alpha1.NodeGroupNodeLabelKey]
	return hasLabel && node.Spec.ProviderID == ""
}
