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

// TestHashTreatsEmptyAndAbsentLabelsAlike pins the normalisation. hashstructure
// skips a nil map but hashes an empty one, so without it `labels: {}` and an
// absent `labels` — the same configuration — produced different hashes, and
// deleting that no-op line replaced every node backed by the NodeClass.
func TestHashTreatsEmptyAndAbsentLabelsAlike(t *testing.T) {
	absent := testNodeClass(t, nil)
	empty := testNodeClass(t, map[string]string{})

	if absent.Hash() != empty.Hash() {
		t.Errorf("`labels: {}` and an absent `labels` must hash alike, got %q vs %q",
			empty.Hash(), absent.Hash())
	}
}

// TestHashVersionPinnedToSpec is a tripwire, not a behavioural test: it fails
// whenever the hash of a fixed spec changes. Any change to
// CleverNodeClassSpec, or to the hashing options, moves it — including adding
// a field and leaving it unset, which `IgnoreZeroValue` does NOT absorb.
//
// When it fails: bump NodeClassHashVersion and update the constant below IN
// THE SAME COMMIT. The nodeclass controller then re-stamps existing NodeGroups
// instead of letting the new hash read as drift and replace the whole fleet.
func TestHashVersionPinnedToSpec(t *testing.T) {
	const (
		wantVersion = "v2"
		// Hash of CleverNodeClassSpec{Labels: {"team": "data"}} under v2.
		wantHash = "3789529822245891689"
	)
	if v1alpha1.NodeClassHashVersion != wantVersion {
		t.Fatalf("NodeClassHashVersion moved to %q: update wantVersion and wantHash together",
			v1alpha1.NodeClassHashVersion)
	}
	if got := testNodeClass(t, map[string]string{"team": "data"}).Hash(); got != wantHash {
		t.Errorf("the hash of a fixed spec changed (%q, want %q).\n"+
			"CleverNodeClassSpec or the hashing changed, so every existing NodeGroup now carries a "+
			"stale, incomparable hash. Bump NodeClassHashVersion (currently %q) and update wantHash "+
			"in this test, in the same commit — otherwise upgrading the controller replaces every "+
			"node in every fleet.", got, wantHash, v1alpha1.NodeClassHashVersion)
	}
}
