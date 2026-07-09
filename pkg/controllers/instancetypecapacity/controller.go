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

// Package instancetypecapacity feeds the real capacity of running Clever
// Cloud nodes back into the instance type catalog, so estimated entries
// (flavors not yet observed) self-correct as soon as a node of that flavor
// joins the cluster.
package instancetypecapacity

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

type Controller struct {
	kubeClient           client.Client
	instanceTypeProvider *instancetype.Provider
}

func NewController(kubeClient client.Client, instanceTypeProvider *instancetype.Provider) *Controller {
	return &Controller{kubeClient: kubeClient, instanceTypeProvider: instanceTypeProvider}
}

func (c *Controller) Name() string {
	return "instancetype.capacity"
}

func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	node := &corev1.Node{}
	if err := c.kubeClient.Get(ctx, req.NamespacedName, node); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	flavor, ok := node.Labels[v1alpha1.FlavorLabelKey]
	if !ok || node.Labels[v1alpha1.NodeRoleLabelKey] != v1alpha1.NodeRoleWorker {
		return reconcile.Result{}, nil
	}
	// Deliberately trusts any worker node, karpenter-managed or not: the
	// flavor/role labels are stamped by the platform (reserved prefix, not
	// settable through NodeGroup spec.labels), and a fixed nodegroup's S node
	// reports the same kernel-visible capacity as a karpenter-created one.
	// Anyone with node-update RBAC is trusted too — an admin mislabeling a
	// node skews that flavor's estimate until a real node overwrites it.
	c.instanceTypeProvider.RecordObservedCapacity(flavor, node.Status.Capacity, node.Status.Allocatable)
	return reconcile.Result{}, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&corev1.Node{}).
		WithEventFilter(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool { return isWorkerWithCapacity(e.Object) },
			UpdateFunc: func(e event.UpdateEvent) bool { return isWorkerWithCapacity(e.ObjectNew) },
			DeleteFunc: func(event.DeleteEvent) bool { return false },
		}).
		Complete(c)
}

func isWorkerWithCapacity(obj client.Object) bool {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return false
	}
	_, hasFlavor := node.Labels[v1alpha1.FlavorLabelKey]
	return hasFlavor && node.Labels[v1alpha1.NodeRoleLabelKey] == v1alpha1.NodeRoleWorker && !node.Status.Capacity.Cpu().IsZero()
}
