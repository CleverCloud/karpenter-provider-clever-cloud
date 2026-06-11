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
// this controller is the safety net for NodeGroups that lost their owner
// (e.g. a NodeClaim force-deleted with its finalizer stripped while the
// kubernetes garbage collector missed the dependent).
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

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
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
}

func NewController(kubeClient client.Client, nodeGroupProvider *nodegroup.Provider) *Controller {
	return &Controller{kubeClient: kubeClient, nodeGroupProvider: nodeGroupProvider}
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
		if err := c.nodeGroupProvider.Delete(ctx, ng.Name); client.IgnoreNotFound(err) != nil {
			return reconciler.Result{}, err
		}
		log.FromContext(ctx).WithValues("NodeGroup", ng.Name).Info("garbage collected orphaned nodegroup")
	}
	return reconciler.Result{RequeueAfter: pollPeriod}, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}
