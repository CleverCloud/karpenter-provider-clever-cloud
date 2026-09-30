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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// Condition types reported by the Clever Cloud node-group-operator.
	ConditionTypeReady               = "Ready"
	ConditionTypeReconcileInProgress = "ReconcileInProgress"
	ConditionTypeReconcileFailed     = "ReconcileFailed"
	ConditionTypeTerminating         = "Terminating"

	// Phases observed in status.phase, a summary the operator computes from
	// its conditions.
	PhaseSynced        = "Synced"
	PhaseQuotaExceeded = "QuotaExceeded"
	PhaseUpstreamError = "UpstreamError"

	// ReasonQuotaExceeded is set on the ReconcileFailed condition when the
	// organisation vCPU/RAM quota blocks the requested capacity.
	ReasonQuotaExceeded = "QuotaExceeded"

	// ReasonUpstreamError is set on the ReconcileFailed condition when a call
	// the operator makes to the Clever Cloud API fails. It is transient: see
	// transientFailureReasons.
	ReasonUpstreamError = "UpstreamError"

	// MaxNodeCount is the maximum spec.nodeCount accepted by the API.
	MaxNodeCount = 16
)

// transientFailureReasons are the ReconcileFailed reasons the operator uses
// for a failure it keeps retrying on its own: they report trouble on the way,
// not a decision about the NodeGroup. Measured live on CKE (2026-09-30): a
// group carried Ready=True(Synced) + ReconcileInProgress=True(Scaling) +
// ReconcileFailed=True(UpstreamError, "API error: RequestDidntReturnSuccess"),
// phase=UpstreamError, and the operator retried until it succeeded about an
// hour later.
//
// This is an allowlist on purpose: every reason NOT listed here stays a
// refusal. Unknown reasons were refusals before transient ones were told
// apart, and reading a real refusal as transient would burn karpenter's
// 15-minute registration TTL on a group that is never coming up. Add a reason
// only once the operator has been seen retrying it to success.
var transientFailureReasons = map[string]bool{
	ReasonUpstreamError: true,
}

// NodeGroupSpec is the NodeGroup specification. A single NodeGroup represents
// a set of nodes of the same flavor. flavor, labels and taints are immutable
// after creation; only nodeCount may change.
type NodeGroupSpec struct {
	// Flavor of the nodes in this NodeGroup (2XS, XS, S, M, L, XL).
	// +optional
	Flavor string `json:"flavor,omitempty"`
	// NodeCount is the number of nodes expected in the NodeGroup (0-16).
	// +optional
	NodeCount int32 `json:"nodeCount"`
	// Labels applied to all nodes in this NodeGroup. Keys must not use the
	// kubernetes.io/, node.kubernetes.io/ or clever-cloud.com/ prefixes.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Taints applied to all nodes in this NodeGroup.
	// +optional
	Taints []NodeGroupTaint `json:"taints,omitempty"`
}

// NodeGroupTaint is a taint applied to all nodes of a NodeGroup.
type NodeGroupTaint struct {
	Key string `json:"key"`
	// +optional
	Value  string             `json:"value,omitempty"`
	Effect corev1.TaintEffect `json:"effect"`
}

// NodeGroupCondition mirrors the condition schema used by the Clever Cloud
// operator (close to metav1.Condition, but kept separate to avoid validation
// surprises on fields the upstream operator owns).
type NodeGroupCondition struct {
	Type   string                 `json:"type"`
	Status corev1.ConditionStatus `json:"status"`
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// NodeGroupStatus defines the observed state of a NodeGroup.
type NodeGroupStatus struct {
	// +optional
	Conditions []NodeGroupCondition `json:"conditions,omitempty"`
	// +optional
	NodeCount int32 `json:"nodeCount,omitempty"`
	// +optional
	TargetNodeCount int32 `json:"targetNodeCount,omitempty"`
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	UpstreamID string `json:"upstreamId,omitempty"`
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Taints []NodeGroupTaint `json:"taints,omitempty"`
}

// NodeGroup is the schema for the Clever Cloud NodeGroups API.
// +kubebuilder:object:root=true
type NodeGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec NodeGroupSpec `json:"spec"`
	// +optional
	Status NodeGroupStatus `json:"status,omitempty"`
}

// GetCondition returns the condition of the given type, or nil.
func (in *NodeGroup) GetCondition(conditionType string) *NodeGroupCondition {
	for i := range in.Status.Conditions {
		if in.Status.Conditions[i].Type == conditionType {
			return &in.Status.Conditions[i]
		}
	}
	return nil
}

// IsQuotaExceeded reports whether the upstream operator rejected the desired
// capacity because of the organisation quota. Like Refusal, it does not
// outrank IsSynced: a Ready group's machines are up whatever else its status
// reports.
func (in *NodeGroup) IsQuotaExceeded() bool {
	if in.Status.Phase == PhaseQuotaExceeded {
		return true
	}
	if cond := in.GetCondition(ConditionTypeReconcileFailed); cond != nil {
		return cond.Status == corev1.ConditionTrue && cond.Reason == ReasonQuotaExceeded
	}
	return false
}

// Refusal returns the reason and message of a ReconcileFailed=True condition
// whose reason is not transient (transientFailureReasons), and whether one is
// present. Quota rejections are one reason among several (IsQuotaExceeded is
// that subset): a flavor the cluster cannot provision, a spec the operator
// refuses, an upstream image failure, and any reason this provider has never
// seen all land here too. Waiting for a group the operator has already refused
// only burns karpenter's registration TTL, so callers treat these as terminal
// — unless the group is Ready (IsSynced): the operator reports several
// conditions at once, and Ready means the machine is up whatever else the
// status carries.
func (in *NodeGroup) Refusal() (reason, message string, refused bool) {
	cond := in.reconcileFailed()
	if cond == nil || transientFailureReasons[cond.Reason] {
		return "", "", false
	}
	return cond.Reason, cond.Message, true
}

// TransientFailure returns the reason and message of a ReconcileFailed=True
// condition whose reason the operator retries on its own
// (transientFailureReasons), and whether one is present. It is not a refusal:
// the NodeGroup is still in progress, exactly as if the condition were absent.
func (in *NodeGroup) TransientFailure() (reason, message string, transient bool) {
	cond := in.reconcileFailed()
	if cond == nil || !transientFailureReasons[cond.Reason] {
		return "", "", false
	}
	return cond.Reason, cond.Message, true
}

// reconcileFailed returns the ReconcileFailed condition when it is True.
func (in *NodeGroup) reconcileFailed() *NodeGroupCondition {
	cond := in.GetCondition(ConditionTypeReconcileFailed)
	if cond == nil || cond.Status != corev1.ConditionTrue {
		return nil
	}
	return cond
}

// IsSynced reports whether the upstream operator reconciled the NodeGroup to
// its desired state. Ready=True is not exclusive: it stays set while a later
// reconcile is in progress or failing (see transientFailureReasons for the
// live shape), and it still means the group's machines are up.
func (in *NodeGroup) IsSynced() bool {
	cond := in.GetCondition(ConditionTypeReady)
	return cond != nil && cond.Status == corev1.ConditionTrue
}

// NodeGroupList contains a list of NodeGroup
// +kubebuilder:object:root=true
type NodeGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeGroup `json:"items"`
}
