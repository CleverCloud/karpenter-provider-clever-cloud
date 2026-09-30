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

// Package instancetypecapacity feeds the capacity reported by the nodes of
// this provider's NodeGroups back into the instance type catalog, so it
// follows what the platform's current node image really exposes instead of
// a seed measured on an earlier one.
package instancetypecapacity

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

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
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

const (
	// rejectionLogInterval bounds the refusal log to one line per node per
	// interval: a refused node is reconciled again on every update of its
	// status, and the counter, not the log volume, is the persistent signal.
	rejectionLogInterval = time.Hour
	// driftLogThreshold is the gap between a node's memory and its flavor's
	// catalogue entry beyond which the entry is reported as stale (once per
	// flavor): the VMs of a flavor are identical, so a gap that large means
	// the node image moved since the entry was measured.
	driftLogThreshold = 0.01
)

type Controller struct {
	kubeClient           client.Client
	instanceTypeProvider *instancetype.Provider
	// rejectionLogged holds, per node name, when a refused report of that
	// node was last logged. An entry is dropped as soon as the node's report
	// is accepted; what remains is bounded by the nodes that misreport,
	// which are anomalies, not routine.
	rejectionLogged sync.Map
	// driftLogged dedups the stale-catalogue log per flavor for the process
	// lifetime: it describes the node image, not any one node.
	driftLogged sync.Map
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
	if !isWorkerWithCapacity(node) {
		return reconcile.Result{}, nil
	}
	// What a report becomes is the flavor's capacity for the whole cluster,
	// and nothing a kubelet can write on its Node is proof of anything: it
	// can rewrite the clever-cloud.com labels (NodeRestriction does not cover
	// that domain) as well as status.capacity. What it cannot change is the
	// Node's NAME: the node authorizer binds its credential to the one Node
	// named after it, and the platform names a group's nodes
	// "<nodegroup>-node<N>". So the nodegroup label only locates the group
	// and must agree with the name, and the flavor is vouched for by that
	// group — spec.flavor is immutable and, on a managed group, written by
	// this provider. Only this provider's groups count: nodes of a fixed
	// nodegroup, the control plane or anything else never enter the
	// catalogue, whatever their labels claim. The figures themselves are
	// bounded by RecordObservedCapacity, since the kubelet of a managed node
	// can still lie about them.
	flavor := node.Labels[v1alpha1.FlavorLabelKey]
	ng := &ngv1.NodeGroup{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: node.Labels[v1alpha1.NodeGroupNodeLabelKey]}, ng); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !nodegroup.IsManaged(ng) {
		return reconcile.Result{}, nil
	}
	if owner, ok := nodegroup.NodeGroupOfNode(node.Name); !ok || owner != ng.Name {
		c.reject(ctx, node, fmt.Errorf("the node's %s label names the managed nodegroup %s, but the node is not one of its nodes (named %s-node<N>)",
			v1alpha1.NodeGroupNodeLabelKey, ng.Name, ng.Name))
		return reconcile.Result{}, nil
	}
	if ng.Spec.Flavor != flavor {
		c.reject(ctx, node, fmt.Errorf("the node's %s label says %q but its nodegroup %s has spec.flavor %q",
			v1alpha1.FlavorLabelKey, flavor, ng.Name, ng.Spec.Flavor))
		return reconcile.Result{}, nil
	}
	deviation, err := c.instanceTypeProvider.RecordObservedCapacity(ng.Spec.Flavor, node.Status.Capacity, node.Status.Allocatable)
	if err != nil {
		c.reject(ctx, node, err)
		return reconcile.Result{}, nil
	}
	c.rejectionLogged.Delete(node.Name)
	if math.Abs(deviation) > driftLogThreshold {
		if _, logged := c.driftLogged.LoadOrStore(ng.Spec.Flavor, struct{}{}); !logged {
			log.FromContext(ctx).WithValues(
				"Node", node.Name, "flavor", ng.Spec.Flavor,
				"memory", node.Status.Capacity.Memory().String(), "deviation", fmt.Sprintf("%+.1f%%", deviation*100),
			).Info("node memory differs from the flavor's catalogue entry by more than 1%: the node image moved since the entry was measured. " +
				"The catalogue now follows the smallest node of this flavor, but a controller that has not seen one yet packs pods " +
				"against the entry — if the entry is above the nodes, pin memoryKi through settings.flavors until a release re-measures it")
		}
	}
	return reconcile.Result{}, nil
}

// reject counts a refused report and logs it, at most once per node per
// rejectionLogInterval. The catalogue keeps what it served before.
func (c *Controller) reject(ctx context.Context, node *corev1.Node, reason error) {
	metrics.ObservedCapacityRejections.Inc(nil)
	now := time.Now()
	if last, ok := c.rejectionLogged.Load(node.Name); ok && now.Sub(last.(time.Time)) < rejectionLogInterval {
		return
	}
	c.rejectionLogged.Store(node.Name, now)
	log.FromContext(ctx).WithValues("Node", node.Name, "reason", reason.Error()).Info(
		"refusing the node's capacity report for the instance-type catalogue: a kubelet can rewrite its own node's labels " +
			"and status, so this may be a compromised or misconfigured node — or a node image whose capacity moved beyond " +
			"the catalogue entry's bounds (then correct the entry through settings.flavors)")
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

// isWorkerWithCapacity selects the nodes that can carry a report: a worker
// of a NodeGroup (the one that vouches for its flavor) that has posted its
// capacity.
func isWorkerWithCapacity(obj client.Object) bool {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return false
	}
	_, hasFlavor := node.Labels[v1alpha1.FlavorLabelKey]
	return hasFlavor && node.Labels[v1alpha1.NodeGroupNodeLabelKey] != "" &&
		node.Labels[v1alpha1.NodeRoleLabelKey] == v1alpha1.NodeRoleWorker && !node.Status.Capacity.Cpu().IsZero()
}
