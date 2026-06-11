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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

type Controller struct {
	kubeClient client.Client
}

func NewController(kubeClient client.Client) *Controller {
	return &Controller{kubeClient: kubeClient}
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
	stored := node.DeepCopy()
	node.Spec.ProviderID = nodegroup.ProviderID(nodeGroupName)
	if err := c.kubeClient.Patch(ctx, node, client.MergeFrom(stored)); err != nil {
		return reconcile.Result{}, err
	}
	log.FromContext(ctx).WithValues("Node", node.Name, "provider-id", node.Spec.ProviderID).Info("stamped provider id on node")
	return reconcile.Result{}, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&corev1.Node{}).
		WithEventFilter(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool { return needsProviderID(e.Object) },
			UpdateFunc: func(e event.UpdateEvent) bool { return needsProviderID(e.ObjectNew) },
			DeleteFunc: func(event.DeleteEvent) bool { return false },
		}).
		Complete(c)
}

func needsProviderID(obj client.Object) bool {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return false
	}
	_, hasLabel := node.Labels[v1alpha1.NodeGroupNodeLabelKey]
	return hasLabel && node.Spec.ProviderID == ""
}
