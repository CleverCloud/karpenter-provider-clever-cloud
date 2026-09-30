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

package v1alpha1_test

import (
	"strings"
	"testing"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
)

// TestValidateNodeClassLabel pins the single shared label rule. Before it
// existed the CEL rule, the controller's validate() and the NodeGroup label
// filter each implemented their own variant: a key like app.kubernetes.io/name
// validated cleanly, the NodeClass went Ready, and the label silently never
// reached any node (the NodeGroup payload — which filters it — is a NodeClass
// label's only delivery path); a value with a space went Ready too and then
// failed every provisioning downstream.
func TestValidateNodeClassLabel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		key     string
		value   string
		wantErr string // substring of the error; empty means the label is valid
	}{
		{name: "plain label", key: "team", value: "data"},
		{name: "domain-prefixed label", key: "example.com/team", value: "data"},
		// Empty is a valid label value everywhere (apiserver included); the
		// rule must not invent a stricter contract than the Node accepts.
		{name: "empty value", key: "team", value: ""},
		// The provider's own domain is not karpenter-core's: it must not be
		// caught by the karpenter.sh rule below.
		{name: "provider domain label", key: "karpenter.clever-cloud.com/team", value: "data"},
		// Only karpenter.sh itself and its subdomains are core's; a domain
		// that merely ends in the same letters is someone else's.
		{name: "look-alike domain", key: "notkarpenter.sh/team", value: "data"},
		// karpenter.sh/nodepool is stamped on every NodeClaim, and the
		// NodeGroup filter shares this rule: letting it through put it in the
		// group's immutable spec.labels, which the platform applies to every
		// node of the group — including the unstamped extra node of a resized
		// group, which then kept karpenter-core's cluster state unsynced
		// after every restart. The node gets it from core's registration sync.
		{name: "karpenter nodepool label", key: karpv1.NodePoolLabelKey, value: "default", wantErr: "karpenter.sh domain"},
		{name: "karpenter capacity-type label", key: karpv1.CapacityTypeLabelKey, value: karpv1.CapacityTypeOnDemand, wantErr: "karpenter.sh domain"},
		// From a NodeClass, a lifecycle key would reach the node at join and
		// tell core it is registered or initialized before it is.
		{name: "karpenter lifecycle label", key: karpv1.NodeInitializedLabelKey, value: "true", wantErr: "karpenter.sh domain"},
		{name: "karpenter.sh subdomain", key: "compatibility.karpenter.sh/x", value: "1", wantErr: "karpenter.sh domain"},
		{name: "bare kubernetes.io prefix", key: "kubernetes.io/role", value: "worker", wantErr: "kubernetes.io/ domain"},
		{name: "node.kubernetes.io prefix", key: "node.kubernetes.io/instance-type", value: "XS", wantErr: "kubernetes.io/ domain"},
		// Subdomained kubernetes.io keys are dropped by the NodeGroup filter
		// but passed the old prefix-only validation — the silent-loss bug.
		{name: "app.kubernetes.io subdomain", key: "app.kubernetes.io/name", value: "web", wantErr: "kubernetes.io/ domain"},
		{name: "topology.kubernetes.io subdomain", key: "topology.kubernetes.io/region", value: "par", wantErr: "kubernetes.io/ domain"},
		{name: "clever-cloud.com prefix", key: "clever-cloud.com/flavor", value: "XS", wantErr: "reserved prefix"},
		{name: "key with a space", key: "bad key", value: "x", wantErr: "not a valid label key"},
		{name: "key over 63 characters", key: strings.Repeat("k", 64), value: "x", wantErr: "not a valid label key"},
		{name: "value with a space", key: "team", value: "not valid", wantErr: "not a valid label value"},
		{name: "value over 63 characters", key: "team", value: strings.Repeat("v", 64), wantErr: "not a valid label value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := v1alpha1.ValidateNodeClassLabel(tc.key, tc.value)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateNodeClassLabel(%q, %q) = %v, want nil", tc.key, tc.value, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateNodeClassLabel(%q, %q) = nil, want an error containing %q", tc.key, tc.value, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// acceptedByV012 is v0.12.0's NodeClass label rule, verbatim: its CRD CEL rule
// and its nodeclass controller's validate() refused the same three prefixes,
// and the controller also refused values over 63 characters (git show
// v0.12.0:pkg/controllers/nodeclass/controller.go). Nothing else was checked.
func acceptedByV012(key, value string) bool {
	for _, prefix := range []string{"kubernetes.io/", "node.kubernetes.io/", "clever-cloud.com/"} {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}
	return len(value) <= 63
}

// TestIsLegacyNodeClassLabel pins which rejected labels an upgrade tolerates.
// v0.12.0 accepted subdomained kubernetes.io/ keys and the karpenter.sh domain,
// so a NodeClass that has provisioned for months can carry one: failing its
// validation after the upgrade stopped provisioning for every NodePool using
// it. In those two domains, which the NodeGroup payload no longer carries at
// all, exactly what v0.12.0 accepted is tolerated (ignored, reported) —
// whatever its label syntax, which v0.12.0 never checked: a subdomained
// kubernetes.io/ key with a value such as "My Platform" left a v0.12.0
// NodeClass Ready and provisioning, since the key was dropped before its value
// reached the NodeGroup. Nothing else is: a label v0.12.0 refused itself, or
// invalid syntax on a key the payload carries, stays fatal.
func TestIsLegacyNodeClassLabel(t *testing.T) {
	for _, tc := range []struct {
		name  string
		key   string
		value string
		want  bool
	}{
		{name: "app.kubernetes.io subdomain", key: "app.kubernetes.io/part-of", value: "shop", want: true},
		{name: "topology.kubernetes.io subdomain", key: "topology.kubernetes.io/zone", value: "par", want: true},
		{name: "look-alike kubernetes.io domain", key: "examplekubernetes.io/x", value: "1", want: true},
		{name: "karpenter.sh key", key: karpv1.CapacityTypeLabelKey, value: karpv1.CapacityTypeOnDemand, want: true},
		{name: "karpenter.sh lifecycle key", key: karpv1.NodeInitializedLabelKey, value: "true", want: true},
		{name: "karpenter.sh subdomain", key: "compatibility.karpenter.sh/x", value: "1", want: true},
		{name: "empty value", key: "app.kubernetes.io/part-of", value: "", want: true},
		{name: "value of 63 characters", key: "app.kubernetes.io/part-of", value: strings.Repeat("v", 63), want: true},
		// Not label syntax, but accepted by v0.12.0 and never delivered by
		// it (kubernetes.io) or now (karpenter.sh): syntax is irrelevant.
		{name: "legacy kubernetes.io key, value with a space", key: "app.kubernetes.io/part-of", value: "My Platform", want: true},
		{name: "legacy kubernetes.io key, value with a slash", key: "app.kubernetes.io/name", value: "a/b", want: true},
		{name: "legacy kubernetes.io key with two slashes", key: "example.com/app.kubernetes.io/x", value: "1", want: true},
		{name: "legacy kubernetes.io key with a space", key: "app.kubernetes.io/bad key", value: "1", want: true},
		{name: "karpenter.sh key with a space", key: "karpenter.sh/bad key", value: "1", want: true},
		{name: "karpenter.sh key, value with a space", key: karpv1.CapacityTypeLabelKey, value: "not valid", want: true},
		// Delivered: nothing to tolerate.
		{name: "plain label", key: "team", value: "data"},
		{name: "provider domain label", key: "karpenter.clever-cloud.com/team", value: "data"},
		// Refused by v0.12.0 already.
		{name: "bare kubernetes.io prefix", key: "kubernetes.io/role", value: "worker"},
		{name: "node.kubernetes.io prefix", key: "node.kubernetes.io/instance-type", value: "XS"},
		{name: "clever-cloud.com prefix", key: "clever-cloud.com/flavor", value: "XS"},
		{name: "legacy kubernetes.io key, value over 63 characters", key: "app.kubernetes.io/part-of", value: strings.Repeat("v", 64)},
		{name: "karpenter.sh key, value over 63 characters", key: karpv1.CapacityTypeLabelKey, value: strings.Repeat("v", 64)},
		// Invalid syntax on a key the payload carries: dropping the label
		// would silently lose it.
		{name: "key with a space", key: "bad key", value: "x"},
		{name: "value with a space", key: "team", value: "not valid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := v1alpha1.IsLegacyNodeClassLabel(tc.key, tc.value)
			if got != tc.want {
				t.Fatalf("IsLegacyNodeClassLabel(%q, %q) = %v, want %v", tc.key, tc.value, got, tc.want)
			}
			if got && !acceptedByV012(tc.key, tc.value) {
				t.Errorf("%s=%s is tolerated, but v0.12.0 refused it: it cannot sit on an existing NodeClass", tc.key, tc.value)
			}
			if got && v1alpha1.ValidateNodeClassLabel(tc.key, tc.value) == nil {
				t.Errorf("%s=%s is tolerated although the shared rule delivers it", tc.key, tc.value)
			}
		})
	}
}
