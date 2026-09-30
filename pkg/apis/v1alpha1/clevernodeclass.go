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
	"reflect"

	"github.com/mitchellh/hashstructure/v2"
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CleverNodeClassSpec defines Clever Cloud specific configuration applied to
// the NodeGroups (and therefore the nodes) Karpenter provisions.
type CleverNodeClassSpec struct {
	// Labels are additional node labels applied through the Clever Cloud
	// NodeGroup, making them visible on nodes as soon as they join the
	// cluster (before Karpenter registration completes). The NodeGroup is the
	// ONLY path these labels take to the node, so keys must not use the
	// clever-cloud.com/ prefix (rejected by the Clever Cloud API), any
	// kubernetes.io/ domain (filtered from the NodeGroup payload — including
	// subdomains like app.kubernetes.io/ or topology.kubernetes.io/) or the
	// karpenter.sh domain or its subdomains (owned by karpenter-core, which
	// applies its own keys at registration), and ValidateNodeClassLabel is the
	// single owner of the full rule. The CEL rule below covers what CEL can
	// express (contains subsumes both kubernetes.io/ prefixes of the older
	// rule); label syntax degrades to ValidationSucceeded=False on the
	// nodeclass controller.
	// +kubebuilder:validation:XValidation:message="label keys in the kubernetes.io/ or karpenter.sh domains or with the reserved prefix clever-cloud.com/ are not allowed",rule="self.all(k, !k.contains('kubernetes.io/') && !k.startsWith('clever-cloud.com/') && !k.startsWith('karpenter.sh/') && !k.contains('.karpenter.sh/'))"
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

// NodeClassHashVersion is the generation of the Hash() function below. Every
// NodeGroup records the generation that stamped it
// (NodeClassHashVersionAnnotationKey), and a stamp is only ever compared with
// what its OWN generation computes for the current spec (HashMatches). Bump it
// in the SAME commit as any change to CleverNodeClassSpec or to the hashing
// itself: a new generation then neither reads as drift on every existing
// NodeGroup (replacing the whole fleet for no configuration change) nor hides
// a NodeClass edit that lands while the upgrade rolls out.
//
// A bump is required for ANY spec change, not only for those that move
// existing hashes. Adding a field is one that does not (`IgnoreZeroValue`
// skips a field left unset, so every existing NodeClass keeps its hash), yet
// it still needs a generation: after a rollback, an older controller decodes
// the spec without that field and would read every NodeGroup whose NodeClass
// sets it as drifted, whereas it leaves alone a stamp from a generation it
// cannot compute. TestHashVersionPinnedToSpec pins the spec's fields as well
// as the hash of a fixed spec, and fails if either changes without a bump.
//
// Adding a generation:
//  1. Freeze the outgoing one in previousNodeClassHashes; the v1 entry is the
//     model. Declare the spec fields it had: HashMatches refuses a spec that
//     sets any other field, since every node the generation stamped was built
//     before that field existed, so a frozen generation stays correct however
//     the spec grows (TestFrozenGenerationsRefuseFieldsTheyDidNotHave). Then
//     write a function that reproduces, byte for byte and from the NEW spec,
//     what the outgoing generation stamped: project the new spec onto a
//     function-local copy of the outgoing spec type (hashstructure mixes the
//     struct's type name into the hash, so the copy keeps the name
//     CleverNodeClassSpec), and return every hash the outgoing generation gave
//     to a configuration the new one treats as identical. A new field whose
//     value reproduces what nodes got before it existed (a CRD default) must
//     be normalised to its zero value in normalised(), or every
//     older-generation NodeGroup drifts at upgrade. A renamed or removed field
//     is renamed or removed in every declaration too
//     (TestFrozenGenerationsDeclareLiveFields).
//  2. Bump NodeClassHashVersion and update TestHashVersionPinnedToSpec.
//  3. Keep the outgoing generation's rows in TestHashGenerationsMatchTheirStamps
//     (they now prove the frozen copy) and add rows for the new one.
//  4. Never delete an entry. A NodeGroup keeps its generation for as long as
//     its node lives (do-not-disrupt pods, zero disruption budgets), and a
//     stamp from a generation this controller cannot compute is never
//     evaluated for drift. TestEveryHashGenerationIsSupported enforces it.
//
// v1 (implicit, annotation absent — NodeGroups created up to v0.11.x): the
// original unversioned hash, which also distinguished `labels: {}` from an
// absent `labels`. v2: v1 over the normalised spec.
const NodeClassHashVersion = "v2"

// nodeClassHashVersionV1 is the generation of NodeGroups stamped before the
// version annotation existed: an absent (or empty) annotation means v1.
const nodeClassHashVersionV1 = "v1"

// frozenHashGeneration is a generation of Hash() older than
// NodeClassHashVersion, frozen.
type frozenHashGeneration struct {
	// fields lists, by Go field name, the CleverNodeClassSpec fields the
	// generation's spec had. A spec that sets any other field cannot describe
	// a node this generation stamped, since that node was built before the
	// field existed: HashMatches reports a mismatch without consulting hashes.
	fields []string
	// hashes returns every hash the generation may have stamped on a NodeGroup
	// built from a configuration the current generation considers identical to
	// spec. It only reads the declared fields.
	hashes func(spec CleverNodeClassSpec) []string
}

// previousNodeClassHashes reproduces every generation of Hash() older than
// NodeClassHashVersion. Append-only — see NodeClassHashVersion.
var previousNodeClassHashes = map[string]frozenHashGeneration{
	// The v1 spec had nothing but labels.
	nodeClassHashVersionV1: {fields: []string{"Labels"}, hashes: hashV1},
}

// hashV1 is generation v1 frozen: hashstructure over the RAW spec, exactly as
// shipped up to v0.11.x (git show 42740db^:pkg/apis/v1alpha1/clevernodeclass.go).
// v1 hashed `labels: {}` and an absent `labels` differently while v2 treats
// them as the same configuration, so an empty spec matches both: dropping a
// no-op `labels: {}` line while the upgrade rolls out must not read as drift.
func hashV1(spec CleverNodeClassSpec) []string {
	// The v1 spec type. hashstructure mixes the struct's type name into the
	// hash, so this copy must keep the original name; it shadows the live type
	// in this function only, and never changes when the live type does.
	type CleverNodeClassSpec struct {
		Labels map[string]string
	}
	labels := spec.Labels
	if len(labels) == 0 {
		return []string{
			hashOf(CleverNodeClassSpec{}),
			hashOf(CleverNodeClassSpec{Labels: map[string]string{}}),
		}
	}
	return []string{hashOf(CleverNodeClassSpec{Labels: labels})}
}

// Hash returns a stable hash of the fields that, when changed, must trigger
// drift on NodeClaims provisioned from this NodeClass — generation
// NodeClassHashVersion.
//
// The spec is normalised first: `labels: {}` and an absent `labels` are the
// same configuration, but hashstructure skips a nil map while hashing an empty
// one, so without this they produce different hashes — and deleting a no-op
// `labels: {}` line (which 9 of the 10 shipped examples used to carry) would
// replace every node backed by the NodeClass.
func (in *CleverNodeClass) Hash() string {
	return hashOf(in.Spec.normalised())
}

// HashMatches reports whether hash, stamped on a NodeGroup by the given
// generation of Hash() (the NodeGroup's NodeClassHashVersionAnnotationKey;
// empty means v1), still describes this NodeClass's CURRENT spec. It compares
// like with like: an older generation's stamp is checked against what THAT
// generation computes for the current spec, never against today's Hash(),
// which is not comparable to it. So a mismatch means the NodeClass changed
// since the NodeGroup was created, whatever generation stamped it. A spec that
// sets a field the stamping generation did not have never matches: the node
// was built before that field existed.
//
// known is false for a generation this controller cannot compute (a newer
// controller's, seen after a rollback). Such a stamp neither proves nor
// disproves drift: callers must not report drift from it, and must not
// overwrite it, so that a controller knowing its generation can still
// evaluate it after a re-upgrade.
func (in *CleverNodeClass) HashMatches(version, hash string) (match, known bool) {
	if version == NodeClassHashVersion {
		return hash == in.Hash(), true
	}
	if version == "" {
		version = nodeClassHashVersionV1
	}
	previous, ok := previousNodeClassHashes[version]
	if !ok {
		return false, false
	}
	if !setsOnly(in.Spec.normalised(), previous.fields) {
		return false, true
	}
	return lo.Contains(previous.hashes(in.Spec), hash), true
}

// setsOnly reports whether spec, a struct, leaves every field not named in
// fields at its zero value. It reads the live type by reflection, so a field
// added to CleverNodeClassSpec is outside every frozen generation's fields
// without anyone having to remember it.
func setsOnly(spec any, fields []string) bool {
	v := reflect.ValueOf(spec)
	for i := range v.NumField() {
		if !v.Field(i).IsZero() && !lo.Contains(fields, v.Type().Field(i).Name) {
			return false
		}
	}
	return true
}

// hashOf is the hashing every generation so far has used. hashstructure can
// only fail on types the spec does not contain (channels, funcs, bad hash
// tags); lo.Must turns a future spec change introducing one into a loud panic
// instead of silently hashing every NodeClass to the same value and mass-
// drifting or never-drifting the fleet. The frozen generations depend on
// these exact options: a generation needing others gets its own helper.
func hashOf(spec any) string {
	return fmt.Sprint(lo.Must(hashstructure.Hash(spec, hashstructure.FormatV2, &hashstructure.HashOptions{
		SlicesAsSets:    true,
		IgnoreZeroValue: true,
		ZeroNil:         true,
	})))
}

// normalised returns a copy of the spec in which empty-but-non-nil collections
// are nil, so that two wire representations of the same configuration hash
// identically.
func (in CleverNodeClassSpec) normalised() CleverNodeClassSpec {
	if len(in.Labels) == 0 {
		in.Labels = nil
	}
	return in
}

// CleverNodeClassList contains a list of CleverNodeClass
// +kubebuilder:object:root=true
type CleverNodeClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CleverNodeClass `json:"items"`
}
