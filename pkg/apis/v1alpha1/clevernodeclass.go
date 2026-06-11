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
	"fmt"

	"github.com/mitchellh/hashstructure/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CleverNodeClassSpec defines Clever Cloud specific configuration applied to
// the NodeGroups (and therefore the nodes) Karpenter provisions.
type CleverNodeClassSpec struct {
	// Labels are additional node labels applied through the Clever Cloud
	// NodeGroup, making them visible on nodes as soon as they join the
	// cluster (before Karpenter registration completes).
	// Keys must not use the kubernetes.io/, node.kubernetes.io/ or
	// clever-cloud.com/ prefixes (rejected by the Clever Cloud API).
	// +kubebuilder:validation:XValidation:message="label keys with reserved prefixes (kubernetes.io/, node.kubernetes.io/, clever-cloud.com/) are not allowed",rule="self.all(k, !k.startsWith('kubernetes.io/') && !k.startsWith('node.kubernetes.io/') && !k.startsWith('clever-cloud.com/'))"
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

// CleverNodeClass is the Schema for the CleverNodeClass API.
// +kubebuilder:object:root=true
// +kubebuilder:resource:path=clevernodeclasses,scope=Cluster,categories=karpenter,shortName={cnc,cncs}
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status",description=""
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description=""
type CleverNodeClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CleverNodeClassSpec   `json:"spec,omitempty"`
	Status CleverNodeClassStatus `json:"status,omitempty"`
}

// Hash returns a stable hash of the fields that, when changed, must trigger
// drift on NodeClaims provisioned from this NodeClass.
func (in *CleverNodeClass) Hash() string {
	hash, _ := hashstructure.Hash(in.Spec, hashstructure.FormatV2, &hashstructure.HashOptions{
		SlicesAsSets:    true,
		IgnoreZeroValue: true,
		ZeroNil:         true,
	})
	return fmt.Sprint(hash)
}

// CleverNodeClassList contains a list of CleverNodeClass
// +kubebuilder:object:root=true
type CleverNodeClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CleverNodeClass `json:"items"`
}
