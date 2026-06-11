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

// Package nodeclass reconciles CleverNodeClass readiness and guards deletion:
// a NodeClass cannot disappear while NodeClaims still reference it.
package nodeclass

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
)

type Controller struct {
	kubeClient client.Client
}

func NewController(kubeClient client.Client) *Controller {
	return &Controller{kubeClient: kubeClient}
}

func (c *Controller) Name() string {
	return "nodeclass"
}

func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	nodeClass := &v1alpha1.CleverNodeClass{}
	if err := c.kubeClient.Get(ctx, req.NamespacedName, nodeClass); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !nodeClass.DeletionTimestamp.IsZero() {
		return c.finalize(ctx, nodeClass)
	}
	stored := nodeClass.DeepCopy()
	if controllerutil.AddFinalizer(nodeClass, v1alpha1.TerminationFinalizer) {
		if err := c.kubeClient.Patch(ctx, nodeClass, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}

	stored = nodeClass.DeepCopy()
	if err := validate(nodeClass); err != nil {
		nodeClass.StatusConditions().SetFalse(v1alpha1.ConditionTypeValidationSucceeded, "ValidationFailed", err.Error())
	} else {
		nodeClass.StatusConditions().SetTrue(v1alpha1.ConditionTypeValidationSucceeded)
	}
	if !equality.Semantic.DeepEqual(stored, nodeClass) {
		if err := c.kubeClient.Status().Patch(ctx, nodeClass, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}
	return reconcile.Result{}, nil
}

// finalize blocks NodeClass deletion while NodeClaims still reference it.
func (c *Controller) finalize(ctx context.Context, nodeClass *v1alpha1.CleverNodeClass) (reconcile.Result, error) {
	if !controllerutil.ContainsFinalizer(nodeClass, v1alpha1.TerminationFinalizer) {
		return reconcile.Result{}, nil
	}
	nodeClaims := &karpv1.NodeClaimList{}
	if err := c.kubeClient.List(ctx, nodeClaims); err != nil {
		return reconcile.Result{}, err
	}
	for i := range nodeClaims.Items {
		ref := nodeClaims.Items[i].Spec.NodeClassRef
		if ref != nil && ref.Name == nodeClass.Name {
			log.FromContext(ctx).WithValues("NodeClaim", nodeClaims.Items[i].Name).Info("waiting on nodeclaim before removing nodeclass finalizer")
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}
	stored := nodeClass.DeepCopy()
	controllerutil.RemoveFinalizer(nodeClass, v1alpha1.TerminationFinalizer)
	if err := c.kubeClient.Patch(ctx, nodeClass, client.MergeFrom(stored)); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	return reconcile.Result{}, nil
}

// validate rejects NodeClass labels that the Clever Cloud NodeGroup API would
// refuse, so misconfiguration surfaces on the NodeClass instead of at node
// launch.
func validate(nodeClass *v1alpha1.CleverNodeClass) error {
	for k, v := range nodeClass.Spec.Labels {
		for _, prefix := range []string{"kubernetes.io/", "node.kubernetes.io/", "clever-cloud.com/"} {
			if strings.HasPrefix(k, prefix) {
				return fmt.Errorf("label key %q uses reserved prefix %q", k, prefix)
			}
		}
		if len(v) > 63 {
			return fmt.Errorf("label %q value exceeds 63 characters", k)
		}
	}
	return nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&v1alpha1.CleverNodeClass{}).
		Complete(c)
}
