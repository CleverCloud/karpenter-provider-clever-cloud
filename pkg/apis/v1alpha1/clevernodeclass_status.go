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

package v1alpha1

import (
	"github.com/awslabs/operatorpkg/status"
)

const (
	// ConditionTypeValidationSucceeded is true when the NodeClass spec passed
	// provider-side validation.
	ConditionTypeValidationSucceeded = "ValidationSucceeded"
	// ConditionTypeNodeGroupAPIServed is true when the cluster actually
	// serves the nodegroups.api.clever-cloud.com API this provider drives —
	// on anything but a CKE cluster the NodeClass must not go Ready, so the
	// failure surfaces here instead of at the first Create.
	ConditionTypeNodeGroupAPIServed = "NodeGroupAPIServed"
)

// CleverNodeClassStatus contains the resolved state of the CleverNodeClass
type CleverNodeClassStatus struct {
	// Conditions contains signals for health and readiness
	// +optional
	Conditions []status.Condition `json:"conditions,omitempty"`
}

func (in *CleverNodeClass) StatusConditions(opts ...status.ForOption) status.ConditionSet {
	return status.NewReadyConditions(
		ConditionTypeValidationSucceeded,
		ConditionTypeNodeGroupAPIServed,
	).For(in, opts...)
}

func (in *CleverNodeClass) GetConditions() []status.Condition {
	return in.Status.Conditions
}

func (in *CleverNodeClass) SetConditions(conditions []status.Condition) {
	in.Status.Conditions = conditions
}
