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
		// karpenter.sh/nodepool is stamped on every NodeClaim and must keep
		// flowing through the NodeGroup filter, which shares this rule.
		{name: "karpenter nodepool label", key: karpv1.NodePoolLabelKey, value: "default"},
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
