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
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
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
	"sigs.k8s.io/karpenter/pkg/events"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
)

type Controller struct {
	kubeClient client.Client
	recorder   events.Recorder
}

func NewController(kubeClient client.Client, recorder events.Recorder) *Controller {
	return &Controller{kubeClient: kubeClient, recorder: recorder}
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
	ignored, err := validate(nodeClass)
	if err != nil {
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
	// The migration runs before the ignored keys are reported: whether
	// removing them drifts nodes depends on what it leaves behind.
	pending, migrationErr := pendingUnknown, error(nil)
	if apiServed {
		pending, migrationErr = c.migrateHashVersion(ctx, nodeClass)
	}
	if err := c.reportIgnoredLabels(ctx, nodeClass, ignored, pending); err != nil {
		return reconcile.Result{}, err
	}
	if !equality.Semantic.DeepEqual(stored, nodeClass) {
		if err := c.kubeClient.Status().Patch(ctx, nodeClass, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			if errors.IsConflict(err) {
				// The NodeClass moved on since the informer handed it over;
				// the newer version's watch event re-triggers this reconcile
				// anyway, so this is only a backstop. A fixed second rather
				// than the rate limiter's 5ms first step, which would mostly
				// re-read the same stale cache and conflict again.
				return reconcile.Result{RequeueAfter: time.Second}, nil
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
	if migrationErr != nil {
		return reconcile.Result{}, migrationErr
	}
	if len(ignored) > 0 && pending > 0 {
		// What unblocks the migration (a drifted node replaced, its NodeClaim
		// condition cleared) is no event on the NodeClass: without a recheck,
		// LabelsIgnored would keep advising against removing the keys until
		// the next informer resync.
		return reconcile.Result{RequeueAfter: hashMigrationRecheck}, nil
	}
	return reconcile.Result{}, nil
}

// pendingUnknown is migrateHashVersion's count of NodeGroups still on an older
// hash generation when it could not list them: how many there are is unknown.
const pendingUnknown = -1

// hashMigrationRecheck is how often a NodeClass carrying ignored keys is
// reconciled while some of its NodeGroups still carry an older hash
// generation.
const hashMigrationRecheck = time.Minute

// migrateHashVersion re-stamps NodeGroups carrying an older generation of
// Hash() with the current one, so that nodes built under an older generation
// stop depending on its frozen implementation as soon as that is provably
// safe. Drift itself does not wait for it: IsDrifted evaluates every
// generation it knows like with like.
//
// A stamp is re-stamped only when it still describes the CURRENT spec — its
// own generation's hash of the current spec matches it. Otherwise the
// NodeClass changed since the NodeGroup was created, possibly while no leader
// evaluated drift (lease handover, controller down during the upgrade, or
// core's drift reconciler clearing the old leader's Drifted condition first).
// Stamping today's hash over it would record the node as built from a
// configuration it never received: no drift, ever, and a fleet that silently
// diverges from its NodeClass. Such a NodeGroup keeps its stamp, and IsDrifted
// reports it drifted.
//
// A stamp from a generation this controller cannot compute (a newer
// controller's, after a rollback), or a NodeGroup without a stamp, is left
// alone: nothing proves what its node was built from. And a NodeGroup whose
// NodeClaim is ALREADY Drifted keeps its stamp whatever the hashes say, so the
// migration can never cancel a replacement in flight; a later pass migrates it
// if the condition clears.
//
// It returns how many NodeGroups of the NodeClass still carry a generation this
// controller knows and were not re-stamped: the NodeClass changed since they
// were built, their NodeClaim is already Drifted, or a read or the write
// failed. Older generations hashed labels v3 leaves out (v0.12.0 hashed every
// label), so removing an ignored key drifts exactly those NodeGroups. The count
// is pendingUnknown when the NodeGroups could not be listed.
func (c *Controller) migrateHashVersion(ctx context.Context, nodeClass *v1alpha1.CleverNodeClass) (int, error) {
	nodeGroups := &ngv1.NodeGroupList{}
	if err := c.kubeClient.List(ctx, nodeGroups, client.MatchingLabels{
		v1alpha1.ManagedLabelKey:   "true",
		v1alpha1.NodeClassLabelKey: nodeClass.Name,
	}); err != nil {
		return pendingUnknown, err
	}
	pending := 0
	var errs []error
	for i := range nodeGroups.Items {
		ng := &nodeGroups.Items[i]
		version := ng.Annotations[v1alpha1.NodeClassHashVersionAnnotationKey]
		if version == v1alpha1.NodeClassHashVersion {
			continue
		}
		logger := log.FromContext(ctx).WithValues("NodeGroup", ng.Name, "hash-version", version)
		hash, hasHash := ng.Annotations[v1alpha1.NodeClassHashLabelKey]
		if !hasHash {
			continue
		}
		match, known := nodeClass.HashMatches(version, hash)
		if !known {
			logger.Info("nodegroup was stamped by a clevernodeclass hash version this controller does not know; " +
				"leaving it untouched, it is not evaluated for drift until a controller that knows it runs")
			continue
		}
		if !match {
			pending++
			logger.Info("clevernodeclass changed since this nodegroup was created; keeping its hash so the node drifts")
			continue
		}
		drifted, err := c.nodeClaimDrifted(ctx, ng)
		if err != nil {
			pending++
			errs = append(errs, err)
			continue
		}
		if drifted {
			pending++
			logger.Info("nodeclaim is already drifted; keeping the nodegroup's hash")
			continue
		}
		stored := ng.DeepCopy()
		ng.Annotations[v1alpha1.NodeClassHashLabelKey] = nodeClass.Hash()
		ng.Annotations[v1alpha1.NodeClassHashVersionAnnotationKey] = v1alpha1.NodeClassHashVersion
		if err := c.kubeClient.Patch(ctx, ng, client.MergeFrom(stored)); err != nil {
			if !errors.IsNotFound(err) {
				pending++
				errs = append(errs, err)
			}
			continue
		}
		logger.Info("migrated nodegroup to the current clevernodeclass hash version", "to", v1alpha1.NodeClassHashVersion)
	}
	return pending, goerrors.Join(errs...)
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
//
// The one exception is a key v0.12.0 accepted (v1alpha1.IsLegacyNodeClassLabel):
// rejecting it would stop provisioning, on upgrade, from a NodeClass that has
// provisioned for months. It is returned in ignored, sorted, instead of failing
// validation. err is the first other rejection, in key order so that the
// condition message does not flap between reconciles.
func validate(nodeClass *v1alpha1.CleverNodeClass) (ignored []string, err error) {
	for _, k := range slices.Sorted(maps.Keys(nodeClass.Spec.Labels)) {
		v := nodeClass.Spec.Labels[k]
		rejection := v1alpha1.ValidateNodeClassLabel(k, v)
		switch {
		case rejection == nil:
		case v1alpha1.IsLegacyNodeClassLabel(k, v):
			ignored = append(ignored, k)
		case err == nil:
			err = rejection
		}
	}
	return ignored, err
}

// reportIgnoredLabels surfaces the legacy keys validate tolerated, without
// touching readiness: the LabelsIgnored condition is the persistent signal
// (set while they remain, removed with them), and a Warning event on the
// NodeClass makes them visible in kubectl get events after the upgrade. The
// event is published on every reconcile that finds them — at controller start
// and on every change — deduplicated per reason and key set for an hour, so
// the status write this reconcile triggers does not repeat it.
//
// The advice depends on the hash migration (pending, from migrateHashVersion):
// a NodeGroup still stamped by v0.12.0 or earlier carries a hash of these keys,
// so removing them drifts it. Only when none is left does the condition say
// that removing them drifts no node (reason LegacyLabelKeys); until then it
// says how many would (reason HashMigrationPending).
func (c *Controller) reportIgnoredLabels(ctx context.Context, nodeClass *v1alpha1.CleverNodeClass, ignored []string, pending int) error {
	if len(ignored) == 0 {
		return nodeClass.StatusConditions().Clear(v1alpha1.ConditionTypeLabelsIgnored)
	}
	reason, advice := "LegacyLabelKeys", "Remove them: no node drifts for it"
	switch {
	case pending == pendingUnknown:
		reason, advice = "HashMigrationPending", fmt.Sprintf("Do not remove them yet: the NodeGroups of the NodeClass could not be "+
			"listed, so whether some are still stamped with a hash version older than %s (annotation %s), which hashed them, is unknown",
			v1alpha1.NodeClassHashVersion, v1alpha1.NodeClassHashVersionAnnotationKey)
	case pending > 0:
		reason, advice = "HashMigrationPending", fmt.Sprintf("Do not remove them yet: removing them now drifts the %d NodeGroup(s) "+
			"of the NodeClass still stamped with a hash version older than %s (annotation %s), which hashed them; the reason of "+
			"this condition turns LegacyLabelKeys once none is left",
			pending, v1alpha1.NodeClassHashVersion, v1alpha1.NodeClassHashVersionAnnotationKey)
	}
	msg := fmt.Sprintf("ignored spec.labels keys: %s. No node launched now gets them (the NodeGroup payload carries no key "+
		"in the kubernetes.io/ or karpenter.sh domains); v0.12.0 accepted them, so the NodeClass stays Ready, but the CRD "+
		"refuses them on new NodeClasses and on any edit of spec.labels that keeps them. %s",
		strings.Join(ignored, ", "), advice)
	if nodeClass.StatusConditions().SetTrueWithReason(v1alpha1.ConditionTypeLabelsIgnored, reason, msg) {
		log.FromContext(ctx).WithValues("keys", ignored, "reason", reason).Info("ignoring clevernodeclass label keys accepted up to v0.12.0 that are not delivered to nodes")
	}
	c.recorder.Publish(events.Event{
		InvolvedObject: nodeClass,
		Type:           corev1.EventTypeWarning,
		Reason:         "LabelsIgnored",
		Message:        msg,
		DedupeValues:   append([]string{nodeClass.Name, reason}, ignored...),
		DedupeTimeout:  time.Hour,
	})
	return nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&v1alpha1.CleverNodeClass{}).
		Complete(c)
}
