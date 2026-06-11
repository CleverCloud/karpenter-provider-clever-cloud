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

	// Phases observed in status.phase.
	PhaseSynced        = "Synced"
	PhaseQuotaExceeded = "QuotaExceeded"

	// ReasonQuotaExceeded is set on the ReconcileFailed condition when the
	// organisation vCPU/RAM quota blocks the requested capacity.
	ReasonQuotaExceeded = "QuotaExceeded"

	// MaxNodeCount is the maximum spec.nodeCount accepted by the API.
	MaxNodeCount = 16
)

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
// capacity because of the organisation quota.
func (in *NodeGroup) IsQuotaExceeded() bool {
	if in.Status.Phase == PhaseQuotaExceeded {
		return true
	}
	if cond := in.GetCondition(ConditionTypeReconcileFailed); cond != nil {
		return cond.Status == corev1.ConditionTrue && cond.Reason == ReasonQuotaExceeded
	}
	return false
}

// IsSynced reports whether the upstream operator reconciled the NodeGroup to
// its desired state.
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
