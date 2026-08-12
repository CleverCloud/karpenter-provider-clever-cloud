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
	goerrors "errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
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
	// Probe that the cluster actually serves the NodeGroup API: on a non-CKE
	// cluster the NodeClass must not go Ready, so provisioning fails here with
	// a readable condition instead of at the first Create.
	apiServed := true
	if err := c.kubeClient.List(ctx, &ngv1.NodeGroupList{}, client.Limit(1)); err != nil {
		apiServed = false
		nodeClass.StatusConditions().SetFalse(v1alpha1.ConditionTypeNodeGroupAPIServed, "NodeGroupAPIUnavailable",
			fmt.Sprintf("listing nodegroups.api.clever-cloud.com: %s (is this a Clever Kubernetes Engine cluster?)", err))
	} else {
		nodeClass.StatusConditions().SetTrue(v1alpha1.ConditionTypeNodeGroupAPIServed)
	}
	if !equality.Semantic.DeepEqual(stored, nodeClass) {
		if err := c.kubeClient.Status().Patch(ctx, nodeClass, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}
	if !apiServed {
		// Re-probe on a short cadence: without it, a transient discovery
		// failure (apiserver blip at startup, CRD applied moments later)
		// would park the NodeClass NotReady — and provisioning with it —
		// until the next informer resync, which defaults to 10 hours.
		return reconcile.Result{RequeueAfter: time.Minute}, nil
	}
	if err := c.migrateHashVersion(ctx, nodeClass); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// migrateHashVersion brings NodeGroups stamped by an older generation of
// Hash() up to the current one. Without it, upgrading the controller after any
// change to CleverNodeClassSpec or to the hashing would make every existing
// NodeGroup read as drifted and replace the whole fleet — real, hourly-billed
// VMs, for no configuration change at all.
//
// A NodeGroup whose NodeClaim is ALREADY Drifted keeps its stored hash: the old
// and new hashes are not comparable, so re-stamping would silently cancel a
// replacement the user actually asked for. It still gets the new version, so
// the next genuine spec change is evaluated normally.
func (c *Controller) migrateHashVersion(ctx context.Context, nodeClass *v1alpha1.CleverNodeClass) error {
	nodeGroups := &ngv1.NodeGroupList{}
	if err := c.kubeClient.List(ctx, nodeGroups, client.MatchingLabels{
		v1alpha1.ManagedLabelKey:   "true",
		v1alpha1.NodeClassLabelKey: nodeClass.Name,
	}); err != nil {
		return err
	}
	hash := nodeClass.Hash()
	var errs []error
	for i := range nodeGroups.Items {
		ng := &nodeGroups.Items[i]
		if ng.Annotations[v1alpha1.NodeClassHashVersionAnnotationKey] == v1alpha1.NodeClassHashVersion {
			continue
		}
		stored := ng.DeepCopy()
		if ng.Annotations == nil {
			ng.Annotations = map[string]string{}
		}
		ng.Annotations[v1alpha1.NodeClassHashVersionAnnotationKey] = v1alpha1.NodeClassHashVersion
		drifted, err := c.nodeClaimDrifted(ctx, ng)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !drifted {
			ng.Annotations[v1alpha1.NodeClassHashLabelKey] = hash
		}
		if equality.Semantic.DeepEqual(stored, ng) {
			continue
		}
		if err := c.kubeClient.Patch(ctx, ng, client.MergeFrom(stored)); err != nil {
			errs = append(errs, client.IgnoreNotFound(err))
			continue
		}
		log.FromContext(ctx).WithValues("NodeGroup", ng.Name, "hash-version", v1alpha1.NodeClassHashVersion, "drifted", drifted).
			Info("migrated nodegroup to the current clevernodeclass hash version")
	}
	return goerrors.Join(errs...)
}

// nodeClaimDrifted reports whether the NodeClaim backing this NodeGroup already
// carries the Drifted condition.
func (c *Controller) nodeClaimDrifted(ctx context.Context, ng *ngv1.NodeGroup) (bool, error) {
	name := ng.Labels[v1alpha1.NodeClaimLabelKey]
	if name == "" {
		name = ng.Name
	}
	nodeClaim := &karpv1.NodeClaim{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: name}, nodeClaim); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return nodeClaim.StatusConditions().Get(karpv1.ConditionTypeDrifted) != nil, nil
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

// validate rejects NodeClass labels the NodeGroup payload cannot deliver —
// keys its filter drops, or syntax the apiserver would refuse on the Node — so
// misconfiguration surfaces on the NodeClass instead of a label silently never
// reaching any node. The rule itself lives in v1alpha1.ValidateNodeClassLabel,
// shared with the NodeGroup label filter: anything that filter would drop must
// be rejected here, because NodeClass labels have no registration-sync
// fallback.
func validate(nodeClass *v1alpha1.CleverNodeClass) error {
	for k, v := range nodeClass.Spec.Labels {
		if err := v1alpha1.ValidateNodeClassLabel(k, v); err != nil {
			return err
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
