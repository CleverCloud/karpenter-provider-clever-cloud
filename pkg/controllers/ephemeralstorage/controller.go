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

// Package ephemeralstorage uses the controller's own node as the explicitly
// trusted disk reference for future workers. It assumes that node's disk and
// reservations represent new workers; it cannot discover a platform change
// that has not reached the reference node.
package ephemeralstorage

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/singleton"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

type Controller struct {
	reader   client.Reader
	provider *instancetype.Provider
	nodeName string
}

// NewController is only wired when NODE_NAME is set. The chart supplies it
// from the pod's spec.nodeName, not a label a different kubelet can forge.
// This node may be a fixed worker or control plane, so the normal managed
// NodeGroup observation rules deliberately do not apply to this one source.
func NewController(reader client.Reader, provider *instancetype.Provider, nodeName string) *Controller {
	provider.RequireEphemeralStorageReference()
	metrics.HostEphemeralStorageReady.Set(0, nil)
	metrics.HostEphemeralStorageCapacity.Set(0, nil)
	metrics.HostEphemeralStorageAllocatable.Set(0, nil)
	return &Controller{reader: reader, provider: provider, nodeName: nodeName}
}

func (c *Controller) Name() string { return "instancetype.ephemeralstorage" }

func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	// The empty request is the initial/periodic singleton reconciliation.
	if req.Name != "" && req.Name != c.nodeName {
		return reconcile.Result{}, nil
	}
	node := &corev1.Node{}
	if err := c.reader.Get(ctx, types.NamespacedName{Name: c.nodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			c.invalidate()
		}
		// A transient API failure preserves the last successful observation.
		return reconcile.Result{}, fmt.Errorf("reading ephemeral storage reference node %q: %w", c.nodeName, err)
	}
	if !node.DeletionTimestamp.IsZero() {
		c.invalidate()
		return reconcile.Result{}, fmt.Errorf("ephemeral storage reference node %q is terminating", c.nodeName)
	}
	capacity := node.Status.Capacity[corev1.ResourceEphemeralStorage]
	allocatable := node.Status.Allocatable[corev1.ResourceEphemeralStorage]
	changed, err := c.provider.SetEphemeralStorageReference(capacity, allocatable)
	if err != nil {
		c.invalidate()
		return reconcile.Result{}, fmt.Errorf("invalid ephemeral storage reference node %q: %w", c.nodeName, err)
	}
	metrics.HostEphemeralStorageCapacity.Set(capacity.AsApproximateFloat64(), nil)
	metrics.HostEphemeralStorageAllocatable.Set(allocatable.AsApproximateFloat64(), nil)
	metrics.HostEphemeralStorageReady.Set(1, nil)
	if changed {
		log.FromContext(ctx).WithValues("Node", node.Name, "uid", node.UID,
			"capacity", capacity.String(), "allocatable", allocatable.String()).
			Info("using controller node ephemeral storage for new workers")
	}
	return reconcile.Result{RequeueAfter: time.Minute}, nil
}

func (c *Controller) invalidate() {
	c.provider.InvalidateEphemeralStorageReference()
	metrics.HostEphemeralStorageReady.Set(0, nil)
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&corev1.Node{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(obj client.Object) bool { return obj.GetName() == c.nodeName })).
		// Also reconcile when the node is missing at startup: no Node event
		// exists in that case, but provisioning must wait and explain why.
		WatchesRawSource(singleton.Source()).
		Complete(c)
}
