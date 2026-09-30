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

package v1_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
)

func nodeGroupWithConditions(conditions ...ngv1.NodeGroupCondition) *ngv1.NodeGroup {
	return &ngv1.NodeGroup{Status: ngv1.NodeGroupStatus{Conditions: conditions}}
}

func TestGetCondition(t *testing.T) {
	ng := nodeGroupWithConditions(
		ngv1.NodeGroupCondition{Type: ngv1.ConditionTypeReconcileInProgress, Status: corev1.ConditionFalse},
		ngv1.NodeGroupCondition{Type: ngv1.ConditionTypeReady, Status: corev1.ConditionTrue, Reason: "Synced"},
		ngv1.NodeGroupCondition{Type: ngv1.ConditionTypeReconcileFailed, Status: corev1.ConditionFalse},
	)

	cond := ng.GetCondition(ngv1.ConditionTypeReady)
	if cond == nil {
		t.Fatal("expected Ready condition, got nil")
	}
	// The returned pointer must reference the stored condition, not a copy.
	if cond != &ng.Status.Conditions[1] {
		t.Errorf("expected pointer into Status.Conditions, got %p", cond)
	}
	if cond.Status != corev1.ConditionTrue || cond.Reason != "Synced" {
		t.Errorf("unexpected condition fields: %+v", cond)
	}

	if got := ng.GetCondition(ngv1.ConditionTypeTerminating); got != nil {
		t.Errorf("expected nil for absent condition type, got %+v", got)
	}

	empty := nodeGroupWithConditions()
	if got := empty.GetCondition(ngv1.ConditionTypeReady); got != nil {
		t.Errorf("expected nil when there are no conditions, got %+v", got)
	}
}

func TestIsQuotaExceededByPhase(t *testing.T) {
	ng := &ngv1.NodeGroup{Status: ngv1.NodeGroupStatus{Phase: ngv1.PhaseQuotaExceeded}}
	if !ng.IsQuotaExceeded() {
		t.Error("expected IsQuotaExceeded to be true on QuotaExceeded phase alone")
	}
}

func TestIsQuotaExceededByCondition(t *testing.T) {
	tests := []struct {
		name       string
		conditions []ngv1.NodeGroupCondition
		want       bool
	}{
		{
			name: "reconcile failed true with quota reason",
			conditions: []ngv1.NodeGroupCondition{
				{Type: ngv1.ConditionTypeReconcileFailed, Status: corev1.ConditionTrue, Reason: ngv1.ReasonQuotaExceeded},
			},
			want: true,
		},
		{
			name: "reconcile failed true with other reason",
			conditions: []ngv1.NodeGroupCondition{
				{Type: ngv1.ConditionTypeReconcileFailed, Status: corev1.ConditionTrue, Reason: "UpstreamError"},
			},
			want: false,
		},
		{
			name: "reconcile failed false with quota reason",
			conditions: []ngv1.NodeGroupCondition{
				{Type: ngv1.ConditionTypeReconcileFailed, Status: corev1.ConditionFalse, Reason: ngv1.ReasonQuotaExceeded},
			},
			want: false,
		},
		{
			name: "no conditions",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ng := nodeGroupWithConditions(tt.conditions...)
			if got := ng.IsQuotaExceeded(); got != tt.want {
				t.Errorf("IsQuotaExceeded() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsSynced(t *testing.T) {
	tests := []struct {
		name       string
		conditions []ngv1.NodeGroupCondition
		want       bool
	}{
		{
			name: "ready true",
			conditions: []ngv1.NodeGroupCondition{
				{Type: ngv1.ConditionTypeReady, Status: corev1.ConditionTrue},
			},
			want: true,
		},
		{
			name: "ready false",
			conditions: []ngv1.NodeGroupCondition{
				{Type: ngv1.ConditionTypeReady, Status: corev1.ConditionFalse},
			},
			want: false,
		},
		{
			name: "only other conditions",
			conditions: []ngv1.NodeGroupCondition{
				{Type: ngv1.ConditionTypeReconcileInProgress, Status: corev1.ConditionTrue},
				{Type: ngv1.ConditionTypeTerminating, Status: corev1.ConditionTrue},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ng := nodeGroupWithConditions(tt.conditions...)
			if got := ng.IsSynced(); got != tt.want {
				t.Errorf("IsSynced() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestReconcileFailureClassification pins which ReconcileFailed reasons are
// refusals and which the operator retries on its own. The split is an
// allowlist of transient reasons: anything else, including a reason never
// seen before, must stay a refusal — a real refusal read as transient would
// burn karpenter's 15-minute registration TTL.
func TestReconcileFailureClassification(t *testing.T) {
	failed := func(reason string) ngv1.NodeGroupCondition {
		return ngv1.NodeGroupCondition{
			Type: ngv1.ConditionTypeReconcileFailed, Status: corev1.ConditionTrue,
			Reason: reason, Message: "msg-" + reason,
		}
	}
	tests := []struct {
		name          string
		conditions    []ngv1.NodeGroupCondition
		wantRefused   bool
		wantTransient bool
		wantReason    string
	}{
		{
			name:          "upstream error is transient",
			conditions:    []ngv1.NodeGroupCondition{failed(ngv1.ReasonUpstreamError)},
			wantTransient: true,
			wantReason:    ngv1.ReasonUpstreamError,
		},
		{
			// Verbatim live shape: Ready + a reconcile in progress + a failing
			// upstream call, which the operator retried to success.
			name: "live multi-condition upstream error is transient",
			conditions: []ngv1.NodeGroupCondition{
				{Type: ngv1.ConditionTypeReady, Status: corev1.ConditionTrue, Reason: "Synced"},
				{Type: ngv1.ConditionTypeReconcileInProgress, Status: corev1.ConditionTrue, Reason: "Scaling"},
				failed(ngv1.ReasonUpstreamError),
			},
			wantTransient: true,
			wantReason:    ngv1.ReasonUpstreamError,
		},
		{
			name:        "quota is a refusal",
			conditions:  []ngv1.NodeGroupCondition{failed(ngv1.ReasonQuotaExceeded)},
			wantRefused: true,
			wantReason:  ngv1.ReasonQuotaExceeded,
		},
		{
			name:        "unknown reason is a refusal",
			conditions:  []ngv1.NodeGroupCondition{failed("FlavorNotAvailable")},
			wantRefused: true,
			wantReason:  "FlavorNotAvailable",
		},
		{
			name:        "empty reason is a refusal",
			conditions:  []ngv1.NodeGroupCondition{failed("")},
			wantRefused: true,
		},
		{
			name: "reconcile failed false is neither",
			conditions: []ngv1.NodeGroupCondition{
				{Type: ngv1.ConditionTypeReconcileFailed, Status: corev1.ConditionFalse, Reason: "FlavorNotAvailable"},
			},
		},
		{
			name: "in progress alone is neither",
			conditions: []ngv1.NodeGroupCondition{
				{Type: ngv1.ConditionTypeReconcileInProgress, Status: corev1.ConditionTrue, Reason: "Creating"},
			},
		},
		{
			name: "no conditions is neither",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ng := nodeGroupWithConditions(tt.conditions...)
			reason, message, refused := ng.Refusal()
			if refused != tt.wantRefused {
				t.Errorf("Refusal() refused = %v, want %v", refused, tt.wantRefused)
			}
			if refused && (reason != tt.wantReason || message != "msg-"+tt.wantReason) {
				t.Errorf("Refusal() = (%q, %q), want (%q, %q)", reason, message, tt.wantReason, "msg-"+tt.wantReason)
			}
			reason, message, transient := ng.TransientFailure()
			if transient != tt.wantTransient {
				t.Errorf("TransientFailure() transient = %v, want %v", transient, tt.wantTransient)
			}
			if transient && (reason != tt.wantReason || message != "msg-"+tt.wantReason) {
				t.Errorf("TransientFailure() = (%q, %q), want (%q, %q)", reason, message, tt.wantReason, "msg-"+tt.wantReason)
			}
			if refused && transient {
				t.Error("a failure cannot be both a refusal and transient")
			}
		})
	}
}
