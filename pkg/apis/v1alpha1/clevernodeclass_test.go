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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
)

func testNodeClass(t *testing.T, labels map[string]string) *v1alpha1.CleverNodeClass {
	t.Helper()
	return &v1alpha1.CleverNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec:       v1alpha1.CleverNodeClassSpec{Labels: labels},
	}
}

func TestHashIsDeterministic(t *testing.T) {
	nc := testNodeClass(t, map[string]string{"team": "data", "env": "prod"})

	first := nc.Hash()
	second := nc.Hash()
	if first == "" {
		t.Fatal("expected non-empty hash")
	}
	if first != second {
		t.Errorf("hashing the same object twice diverged: %q vs %q", first, second)
	}

	// A separately-built but identical object must hash the same.
	other := testNodeClass(t, map[string]string{"env": "prod", "team": "data"})
	if got := other.Hash(); got != first {
		t.Errorf("identical specs produced different hashes: %q vs %q", got, first)
	}
}

func TestHashChangesWhenLabelsChange(t *testing.T) {
	base := testNodeClass(t, map[string]string{"team": "data"})
	baseHash := base.Hash()

	// Changing a label value is the drift trigger: the hash stamped on the
	// NodeGroup at creation must no longer match.
	changed := testNodeClass(t, map[string]string{"team": "platform"})
	if got := changed.Hash(); got == baseHash {
		t.Errorf("expected hash to change when a label value changes, got %q twice", got)
	}

	added := testNodeClass(t, map[string]string{"team": "data", "env": "prod"})
	if got := added.Hash(); got == baseHash {
		t.Errorf("expected hash to change when a label is added, got %q twice", got)
	}
}

func TestHashIgnoresObjectMetadata(t *testing.T) {
	a := testNodeClass(t, map[string]string{"team": "data"})
	b := testNodeClass(t, map[string]string{"team": "data"})
	b.Name = "other"
	b.Annotations = map[string]string{"example.com/note": "ignored"}

	if a.Hash() != b.Hash() {
		t.Errorf("expected metadata to be ignored: %q vs %q", a.Hash(), b.Hash())
	}
}
