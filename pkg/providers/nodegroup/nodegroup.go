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

// Package nodegroup manages the Clever Cloud NodeGroups backing Karpenter
// NodeClaims. The mapping is strictly one NodeClaim to one NodeGroup with
// nodeCount=1: the NodeGroup carries the NodeClaim's name, and the single
// node it produces is named "<nodegroup>-node0" by Clever Cloud.
package nodegroup

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
)

const (
	// ProviderIDPrefix prefixes every provider ID handed to Karpenter.
	// The full form is "clevercloud://<nodegroup-name>".
	ProviderIDPrefix = "clevercloud://"

	// quotaCheckTimeout bounds how long Create waits for the Clever Cloud
	// operator to accept or reject (quota) a new NodeGroup. Rejections are
	// observed within 1-3s; on timeout we optimistically assume the group
	// will converge and let Karpenter's registration TTL be the backstop.
	quotaCheckTimeout = 15 * time.Second
	// quotaCheckInterval is the poll period during the quota check.
	quotaCheckInterval = time.Second

	// quotaBackoff is how long Create fails fast after a quota rejection
	// instead of churning create/delete cycles against the Clever Cloud API.
	// Any NodeGroup deletion clears the backoff since it frees capacity.
	quotaBackoff = time.Minute
)

// ErrQuotaExceeded is returned by Create when the organisation quota rejects
// the requested capacity.
type ErrQuotaExceeded struct {
	Message string
}

func (e *ErrQuotaExceeded) Error() string {
	return fmt.Sprintf("clever cloud quota exceeded: %s", e.Message)
}

// Provider performs CRUD operations on Clever Cloud NodeGroups.
type Provider struct {
	kubeClient client.Client

	// createMu serializes NodeGroup creations. Concurrent creations make the
	// upstream quota engine evaluate all in-flight groups together, rejecting
	// several at once; deleting groups while their first upstream reconcile
	// is still running has been observed to leak upstream reservations.
	createMu sync.Mutex

	mu              sync.Mutex
	quotaRejectedAt time.Time
	quotaMessage    string
}

func NewProvider(kubeClient client.Client) *Provider {
	return &Provider{kubeClient: kubeClient}
}

// quotaBackoffActive reports whether a recent quota rejection should fail
// creates fast, and the message of that rejection.
func (p *Provider) quotaBackoffActive() (bool, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return time.Since(p.quotaRejectedAt) < quotaBackoff, p.quotaMessage
}

func (p *Provider) recordQuotaRejection(message string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.quotaRejectedAt = time.Now()
	p.quotaMessage = message
}

func (p *Provider) clearQuotaRejection() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.quotaRejectedAt = time.Time{}
}

// ProviderID returns the provider ID for a NodeGroup name.
func ProviderID(nodeGroupName string) string {
	return ProviderIDPrefix + nodeGroupName
}

// ParseProviderID extracts the NodeGroup name from a provider ID.
func ParseProviderID(providerID string) (string, error) {
	name := strings.TrimPrefix(providerID, ProviderIDPrefix)
	if name == providerID || name == "" {
		return "", fmt.Errorf("provider id %q is not a clever cloud provider id", providerID)
	}
	return name, nil
}

// IsManaged reports whether the NodeGroup was created by this provider.
func IsManaged(ng *ngv1.NodeGroup) bool {
	return ng.Labels[v1alpha1.ManagedLabelKey] == "true"
}

// NodeClaimOwners returns the names of the NodeClaim owner references stamped
// by Create. The managed label alone is forgeable — copying a karpenter-created
// manifest keeps it — so destructive paths that act on label-matched NodeGroups
// must require this ownership proof, and must key their decision on the
// referenced claim itself, never on the mutable labels.
func NodeClaimOwners(ng *ngv1.NodeGroup) []string {
	var names []string
	for _, ref := range ng.OwnerReferences {
		if ref.Kind == "NodeClaim" && strings.HasPrefix(ref.APIVersion, "karpenter.sh/") {
			names = append(names, ref.Name)
		}
	}
	return names
}

// Create creates the NodeGroup backing a NodeClaim and waits briefly for the
// Clever Cloud operator to accept it. It returns ErrQuotaExceeded (after
// cleaning up the NodeGroup) when the organisation quota rejects it.
// It is idempotent: an already-existing NodeGroup owned by the same NodeClaim
// is reused.
func (p *Provider) Create(ctx context.Context, nodeClaim *karpv1.NodeClaim, nodeClass *v1alpha1.CleverNodeClass, flavor string) (*ngv1.NodeGroup, error) {
	p.createMu.Lock()
	defer p.createMu.Unlock()
	if active, message := p.quotaBackoffActive(); active {
		return nil, &ErrQuotaExceeded{Message: fmt.Sprintf("%s (cached for up to %s; freed capacity clears it immediately)", message, quotaBackoff)}
	}
	ng := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeClaim.Name,
			Labels: map[string]string{
				v1alpha1.ManagedLabelKey:   "true",
				v1alpha1.NodeClaimLabelKey: nodeClaim.Name,
				v1alpha1.NodePoolLabelKey:  nodeClaim.Labels[karpv1.NodePoolLabelKey],
				v1alpha1.NodeClassLabelKey: nodeClass.Name,
			},
			Annotations: map[string]string{
				v1alpha1.NodeClassHashLabelKey: nodeClass.Hash(),
			},
			// The NodeClaim owns the NodeGroup: if the NodeClaim disappears
			// without going through the termination flow, Kubernetes garbage
			// collection removes the NodeGroup (and Clever Cloud the VM).
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "karpenter.sh/v1",
					Kind:       "NodeClaim",
					Name:       nodeClaim.Name,
					UID:        nodeClaim.UID,
				},
			},
		},
		Spec: ngv1.NodeGroupSpec{
			Flavor:    flavor,
			NodeCount: 1,
			Labels:    nodeGroupLabels(nodeClaim, nodeClass),
			// The unregistered taint closes the race between node readiness
			// and Karpenter's label/taint sync; Karpenter removes it once
			// registration completes.
			Taints: []ngv1.NodeGroupTaint{
				{Key: karpv1.UnregisteredTaintKey, Effect: corev1.TaintEffectNoExecute},
			},
		},
	}
	if err := p.kubeClient.Create(ctx, ng); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("creating nodegroup, %w", err)
		}
		existing := &ngv1.NodeGroup{}
		if getErr := p.kubeClient.Get(ctx, types.NamespacedName{Name: ng.Name}, existing); getErr != nil {
			return nil, fmt.Errorf("getting existing nodegroup, %w", getErr)
		}
		if !IsManaged(existing) || existing.Labels[v1alpha1.NodeClaimLabelKey] != nodeClaim.Name ||
			!slices.Contains(NodeClaimOwners(existing), nodeClaim.Name) {
			return nil, fmt.Errorf("nodegroup %q already exists and is not managed by this nodeclaim", ng.Name)
		}
		ng = existing
	}
	if err := p.waitForAcceptance(ctx, ng.Name); err != nil {
		return nil, err
	}
	return ng, nil
}

// waitForAcceptance polls the NodeGroup until the Clever Cloud operator
// reports it Synced, rejects it on quota, or the timeout elapses (treated as
// optimistic success).
func (p *Provider) waitForAcceptance(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, quotaCheckTimeout)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, quotaCheckInterval, true, func(ctx context.Context) (bool, error) {
		ng := &ngv1.NodeGroup{}
		if err := p.kubeClient.Get(ctx, types.NamespacedName{Name: name}, ng); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if ng.IsQuotaExceeded() {
			msg := ""
			if cond := ng.GetCondition(ngv1.ConditionTypeReconcileFailed); cond != nil {
				msg = cond.Message
			}
			// Free the rejected reservation immediately so it does not
			// starve other NodeGroups in the org. Deleted directly (not via
			// p.Delete) because removing a rejected group frees no real
			// capacity and must not clear the quota backoff.
			if err := p.kubeClient.Delete(ctx, &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil && !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("cleaning up quota-rejected nodegroup, %w", err)
			}
			p.recordQuotaRejection(msg)
			return false, &ErrQuotaExceeded{Message: msg}
		}
		return ng.IsSynced(), nil
	})
	if err == nil || wait.Interrupted(err) {
		return nil
	}
	return err
}

// Get fetches a NodeGroup by name.
func (p *Provider) Get(ctx context.Context, name string) (*ngv1.NodeGroup, error) {
	ng := &ngv1.NodeGroup{}
	if err := p.kubeClient.Get(ctx, types.NamespacedName{Name: name}, ng); err != nil {
		return nil, err
	}
	return ng, nil
}

// List returns all NodeGroups managed by this provider.
func (p *Provider) List(ctx context.Context) ([]ngv1.NodeGroup, error) {
	list := &ngv1.NodeGroupList{}
	if err := p.kubeClient.List(ctx, list, client.MatchingLabels{v1alpha1.ManagedLabelKey: "true"}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// Delete removes a NodeGroup; the Clever Cloud operator finalizer tears down
// the VM and the Node object (~40s observed). Deletions free quota, so the
// quota backoff is reset.
func (p *Provider) Delete(ctx context.Context, name string) error {
	p.clearQuotaRejection()
	return p.kubeClient.Delete(ctx, &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: name}})
}

// nodeGroupLabels computes the node labels carried by the NodeGroup so that
// they are present on the node as soon as it joins, before Karpenter's
// registration sync. Keys rejected by the Clever Cloud API (reserved
// prefixes) are filtered out: Karpenter applies those at registration anyway.
func nodeGroupLabels(nodeClaim *karpv1.NodeClaim, nodeClass *v1alpha1.CleverNodeClass) map[string]string {
	labels := map[string]string{}
	for k, v := range nodeClass.Spec.Labels {
		if isNodeGroupLabelAllowed(k, v) {
			labels[k] = v
		}
	}
	for k, v := range nodeClaim.Labels {
		if isNodeGroupLabelAllowed(k, v) {
			labels[k] = v
		}
	}
	return labels
}

// isNodeGroupLabelAllowed mirrors the CEL validation of the NodeGroup CRD:
// reserved prefixes are rejected, and values must match the (restrictive)
// upstream pattern. Anything filtered here still reaches the node through
// Karpenter's registration sync.
func isNodeGroupLabelAllowed(key, value string) bool {
	for _, prefix := range []string{"kubernetes.io/", "node.kubernetes.io/", "clever-cloud.com/"} {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}
	// Domain-prefixed variants of reserved kubernetes.io labels (e.g.
	// topology.kubernetes.io/zone) pass upstream validation but are owned by
	// Karpenter's sync; keep the NodeGroup payload minimal and predictable.
	if strings.Contains(key, "kubernetes.io/") {
		return false
	}
	if len(value) > 63 {
		return false
	}
	return true
}
