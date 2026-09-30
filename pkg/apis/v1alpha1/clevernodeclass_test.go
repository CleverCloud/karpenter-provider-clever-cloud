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
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
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

// TestHashCoversOnlyDeliveredLabels pins what the hash describes: the labels
// the NodeGroup payload carries, since those are all a NodeClass contributes
// to a node. A label the shared rule rejects is not delivered, so adding or
// removing one must not drift any node: when the hash covered the raw labels,
// removing a subdomained kubernetes.io/ key that v0.12.0 accepted — the only
// way to clear the warning it now raises — replaced every node of the
// NodeClass for a NodeGroup spec identical to theirs.
func TestHashCoversOnlyDeliveredLabels(t *testing.T) {
	delivered := map[string]string{"team": "data"}
	want := testNodeClass(t, delivered).Hash()
	for _, tc := range []struct {
		name  string
		key   string
		value string
	}{
		{name: "legacy kubernetes.io subdomain key", key: "app.kubernetes.io/part-of", value: "shop"},
		{name: "legacy topology.kubernetes.io key", key: "topology.kubernetes.io/zone", value: "par"},
		{name: "legacy karpenter.sh key", key: "karpenter.sh/capacity-type", value: "on-demand"},
		{name: "legacy karpenter.sh subdomain key", key: "compatibility.karpenter.sh/x", value: "1"},
		{name: "reserved prefix", key: "clever-cloud.com/flavor", value: "XS"},
		{name: "bare kubernetes.io prefix", key: "kubernetes.io/role", value: "worker"},
		{name: "invalid key", key: "bad key", value: "x"},
		{name: "invalid value", key: "env", value: "not valid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if v1alpha1.ValidateNodeClassLabel(tc.key, tc.value) == nil {
				t.Fatalf("fixture %s=%s is delivered: pick a label the shared rule rejects", tc.key, tc.value)
			}
			labels := map[string]string{tc.key: tc.value}
			for k, v := range delivered {
				labels[k] = v
			}
			if got := testNodeClass(t, labels).Hash(); got != want {
				t.Errorf("adding undelivered %s=%s moved the hash (%q, want %q): removing it again would drift "+
					"every node of the NodeClass although no NodeGroup payload changes", tc.key, tc.value, got, want)
			}
		})
	}
	// Only undelivered labels: the same configuration as no labels at all.
	if got, want := testNodeClass(t, map[string]string{"app.kubernetes.io/part-of": "shop"}).Hash(), testNodeClass(t, nil).Hash(); got != want {
		t.Errorf("a spec whose only label is undelivered must hash like an empty spec, got %q, want %q", got, want)
	}
	// A delivered label still counts, in the provider's own domain too.
	for _, key := range []string{"env", "karpenter.clever-cloud.com/team"} {
		labels := map[string]string{"app.kubernetes.io/part-of": "shop", key: "x"}
		for k, v := range delivered {
			labels[k] = v
		}
		if got := testNodeClass(t, labels).Hash(); got == want {
			t.Errorf("adding delivered label %s must move the hash, got %q twice", key, got)
		}
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
// whenever CleverNodeClassSpec's fields or the hash of a fixed spec change.
// The hash catches changes to the hashing and to existing fields; the field
// list catches an added field, which `IgnoreZeroValue` keeps out of the hash
// of every spec that leaves it unset, so the hash alone would miss it.
//
// When it fails: add a generation IN THE SAME COMMIT, following the steps on
// NodeClassHashVersion — freeze the outgoing one in previousNodeClassHashes,
// bump NodeClassHashVersion and update the constants below. Existing NodeGroups
// are then compared with what their own generation computes, instead of the
// new hash reading as drift and replacing the whole fleet.
func TestHashVersionPinnedToSpec(t *testing.T) {
	const (
		wantVersion = "v3"
		// Hash of CleverNodeClassSpec{Labels: {"team": "data"}} under v3.
		wantHash = "3789529822245891689"
	)
	// CleverNodeClassSpec's fields under v3, with their types.
	wantFields := []string{"Labels map[string]string"}
	// Labels v3 leaves out of the hash (ValidateNodeClassLabel rejects them):
	// a spec adding them to the fixed one must keep wantHash. The rule is part
	// of the hashing, and a change to it for these keys moves this hash.
	undelivered := map[string]string{
		"team":                       "data",
		"app.kubernetes.io/part-of":  "shop",
		"karpenter.sh/capacity-type": "on-demand",
		"clever-cloud.com/flavor":    "XS",
		"env":                        "not valid",
	}

	if v1alpha1.NodeClassHashVersion != wantVersion {
		t.Fatalf("NodeClassHashVersion moved to %q: update wantVersion, wantHash and wantFields together",
			v1alpha1.NodeClassHashVersion)
	}
	typ := reflect.TypeFor[v1alpha1.CleverNodeClassSpec]()
	var fields []string
	for i := range typ.NumField() {
		fields = append(fields, fmt.Sprintf("%s %s", typ.Field(i).Name, typ.Field(i).Type))
	}
	if !slices.Equal(fields, wantFields) {
		t.Errorf("CleverNodeClassSpec's fields changed (%q, want %q).\n"+
			"Even a field that leaves existing hashes alone needs a new generation (see "+
			"NodeClassHashVersion). Freeze the outgoing generation in previousNodeClassHashes, "+
			"declaring the fields it had, bump NodeClassHashVersion (currently %q) and update "+
			"wantFields and wantHash in this test, in the same commit.",
			fields, wantFields, v1alpha1.NodeClassHashVersion)
	}
	for _, labels := range []map[string]string{{"team": "data"}, undelivered} {
		got := testNodeClass(t, labels).Hash()
		if got == wantHash {
			continue
		}
		t.Errorf("the hash of a fixed spec (labels %v) changed (%q, want %q).\n"+
			"CleverNodeClassSpec, the hashing or the label rule it applies (ValidateNodeClassLabel) changed, "+
			"so every existing NodeGroup now carries a stale, incomparable hash. Freeze the outgoing "+
			"generation in previousNodeClassHashes, bump NodeClassHashVersion (currently %q) and update "+
			"wantHash in this test, in the same commit — otherwise upgrading the controller replaces every "+
			"node in every fleet.",
			labels, got, wantHash, v1alpha1.NodeClassHashVersion)
	}
}

// TestHashGenerationsMatchTheirStamps pins, for every generation of Hash() a
// NodeGroup may carry, stamps that generation actually wrote. They are
// historical facts: when a generation is frozen into previousNodeClassHashes
// its rows stay, and then prove the frozen copy reproduces it byte for byte.
// Each stamp must match the spec it describes and never an edited one — that
// is what lets an upgrade tell "stamped by an older controller" apart from
// "built from an older NodeClass".
func TestHashGenerationsMatchTheirStamps(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version string
		labels  map[string]string
		stamp   string
	}{
		// v1: NodeGroups created up to v0.11.x, with no version annotation.
		{name: "v1", version: "", labels: map[string]string{"team": "data"}, stamp: "3789529822245891689"},
		{name: "v1 explicit version", version: "v1", labels: map[string]string{"team": "data"}, stamp: "3789529822245891689"},
		{name: "v1 other labels", version: "", labels: map[string]string{"team": "ml"}, stamp: "1760892634253892809"},
		{name: "v1 absent labels", version: "", labels: nil, stamp: "7038317156814165261"},
		{name: "v1 empty labels", version: "", labels: map[string]string{}, stamp: "14514438007709706818"},
		// v1 told `labels: {}` from an absent `labels`; v2 made them the same
		// configuration, so a v1 stamp of either form matches both.
		{name: "v1 stamp of empty labels, now absent", version: "", labels: nil, stamp: "14514438007709706818"},
		{name: "v1 stamp of absent labels, now empty", version: "", labels: map[string]string{}, stamp: "7038317156814165261"},
		// v2: NodeGroups created by v0.12.0.
		{name: "v2", version: "v2", labels: map[string]string{"team": "data"}, stamp: "3789529822245891689"},
		{name: "v2 absent labels", version: "v2", labels: nil, stamp: "7038317156814165261"},
		{name: "v2 empty labels", version: "v2", labels: map[string]string{}, stamp: "7038317156814165261"},
		// v2 hashed every label, delivered or not: its stamp of a spec
		// carrying one is its own, and must still match that spec unchanged,
		// or the upgrade that stops hashing them drifts the whole NodeClass.
		{name: "v2 with a legacy kubernetes.io subdomain key", version: "v2",
			labels: map[string]string{"team": "data", "app.kubernetes.io/part-of": "shop"}, stamp: "16335349422164111380"},
		{name: "v2 with a legacy karpenter.sh key", version: "v2",
			labels: map[string]string{"team": "data", "karpenter.sh/capacity-type": "on-demand"}, stamp: "14653552794626866276"},
		// v3: undelivered labels are not part of the configuration.
		{name: "v3", version: "v3", labels: map[string]string{"team": "data"}, stamp: "3789529822245891689"},
		{name: "v3 absent labels", version: "v3", labels: nil, stamp: "7038317156814165261"},
		{name: "v3 with a legacy key", version: "v3",
			labels: map[string]string{"team": "data", "app.kubernetes.io/part-of": "shop"}, stamp: "3789529822245891689"},
		{name: "v3 stamp of a legacy key, removed since", version: "v3",
			labels: map[string]string{"team": "data"}, stamp: "3789529822245891689"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			match, known := testNodeClass(t, tc.labels).HashMatches(tc.version, tc.stamp)
			if !known {
				t.Fatalf("hash version %q must be supported: NodeGroups carrying it would never drift", tc.version)
			}
			if !match {
				t.Errorf("the %q stamp %s must match labels %v: every NodeGroup it stamped would drift "+
					"on upgrade, for no configuration change", tc.version, tc.stamp, tc.labels)
			}
			edited := map[string]string{"edited": "true"}
			for k, v := range tc.labels {
				edited[k] = v
			}
			if match, _ := testNodeClass(t, edited).HashMatches(tc.version, tc.stamp); match {
				t.Errorf("the %q stamp %s matched a NodeClass edited since (labels %v): the edit would "+
					"never drift the node", tc.version, tc.stamp, edited)
			}
		})
	}
}

// TestHashMatchesRefusesUnknownGenerations pins the rollback behaviour: a stamp
// from a generation this controller cannot compute (a newer controller's) is
// neither a match — the migration would overwrite it with a hash of a spec the
// node may never have received — nor a mismatch, which would replace the whole
// fleet on rollback. It is reported unknown, and left for a controller that
// knows it.
func TestHashMatchesRefusesUnknownGenerations(t *testing.T) {
	nodeClass := testNodeClass(t, map[string]string{"team": "data"})
	for _, version := range []string{"v99", "v0", "V2", "2", "garbage"} {
		if match, known := nodeClass.HashMatches(version, nodeClass.Hash()); known || match {
			t.Errorf("HashMatches(%q) = (match %v, known %v), want (false, false)", version, match, known)
		}
	}
}

// TestEveryHashGenerationIsSupported is the append-only guard: every
// generation ever shipped, v1 up to NodeClassHashVersion, must stay
// computable. Deleting a frozen generation — or bumping the version without
// freezing the outgoing one — would silently stop evaluating drift on every
// NodeGroup it stamped.
func TestEveryHashGenerationIsSupported(t *testing.T) {
	current, err := strconv.Atoi(strings.TrimPrefix(v1alpha1.NodeClassHashVersion, "v"))
	if err != nil {
		t.Fatalf("NodeClassHashVersion %q is not of the form vN: %v", v1alpha1.NodeClassHashVersion, err)
	}
	nodeClass := testNodeClass(t, nil)
	versions := []string{""} // no annotation: v1
	for n := 1; n <= current; n++ {
		versions = append(versions, fmt.Sprintf("v%d", n))
	}
	for _, version := range versions {
		if _, known := nodeClass.HashMatches(version, ""); !known {
			t.Errorf("hash version %q is no longer supported: freeze it in previousNodeClassHashes "+
				"(see NodeClassHashVersion) — NodeGroups it stamped would never drift again", version)
		}
	}
}

// specFields returns the Go names of CleverNodeClassSpec's fields.
func specFields() []string {
	typ := reflect.TypeFor[v1alpha1.CleverNodeClassSpec]()
	names := make([]string, 0, typ.NumField())
	for i := range typ.NumField() {
		names = append(names, typ.Field(i).Name)
	}
	return names
}

// withNonZero returns a copy of spec in which the named field holds a non-zero
// value, whatever its type.
func withNonZero(t *testing.T, spec v1alpha1.CleverNodeClassSpec, field string) v1alpha1.CleverNodeClassSpec {
	t.Helper()
	v := reflect.ValueOf(&spec).Elem().FieldByName(field)
	if !v.IsValid() {
		t.Fatalf("CleverNodeClassSpec has no field %s", field)
	}
	v.Set(nonZero(t, v.Type()))
	return spec
}

// nonZero builds a non-zero value of typ. Collections get one non-zero element
// so that normalising empty collections away does not turn them back to zero.
func nonZero(t *testing.T, typ reflect.Type) reflect.Value {
	t.Helper()
	v := reflect.New(typ).Elem()
	switch typ.Kind() {
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1)
	case reflect.String:
		v.SetString("x")
	case reflect.Pointer:
		v.Set(reflect.New(typ.Elem()))
		v.Elem().Set(nonZero(t, typ.Elem()))
	case reflect.Slice:
		v.Set(reflect.Append(v, nonZero(t, typ.Elem())))
	case reflect.Map:
		v.Set(reflect.MakeMap(typ))
		v.SetMapIndex(nonZero(t, typ.Key()), nonZero(t, typ.Elem()))
	case reflect.Struct:
		for i := range typ.NumField() {
			if typ.Field(i).IsExported() {
				v.Field(i).Set(nonZero(t, typ.Field(i).Type))
				return v
			}
		}
		t.Fatalf("nonZero: %s has no exported field to set: extend nonZero for it", typ)
	default:
		t.Fatalf("nonZero: extend it for %s (kind %s)", typ, typ.Kind())
	}
	return v
}

// TestFrozenGenerationsRefuseFieldsTheyDidNotHave is the guard behind step 1
// of the note on NodeClassHashVersion, and it grows with the spec: for every
// frozen generation and every live CleverNodeClassSpec field it did not have,
// a spec that sets that field must never match a stamp the generation wrote.
// The node carrying the stamp was built before the field existed, so setting
// it is an edit: matching would hide it from IsDrifted, and the migration would
// record it as the configuration the node was built from.
func TestFrozenGenerationsRefuseFieldsTheyDidNotHave(t *testing.T) {
	for version, declared := range v1alpha1.FrozenHashGenerationFields() {
		// Built from an empty spec, and from one setting every field the
		// generation had.
		full := v1alpha1.CleverNodeClassSpec{}
		for _, field := range declared {
			full = withNonZero(t, full, field)
		}
		for _, built := range []v1alpha1.CleverNodeClassSpec{{}, full} {
			stamps := v1alpha1.FrozenHashes(version, built)
			if len(stamps) == 0 {
				t.Fatalf("generation %q computes no hash for a spec it had (%+v)", version, built)
			}
			for _, field := range specFields() {
				if slices.Contains(declared, field) {
					continue
				}
				edited := &v1alpha1.CleverNodeClass{Spec: withNonZero(t, built, field)}
				for _, stamp := range stamps {
					if match, known := edited.HashMatches(version, stamp); match || !known {
						t.Errorf("generation %q: a stamp of %+v gave (match %v, known %v) for a spec that "+
							"also sets %s, a field the generation did not have: want (false, true), or "+
							"setting %s never drifts the nodes it stamped", version, built, match, known,
							field, field)
					}
				}
			}
		}
	}
}

// TestFrozenGenerationsDeclareLiveFields keeps the declarations honest: a
// declared name that is no longer a CleverNodeClassSpec field (renamed without
// updating the declaration) would make that field read as one the generation
// never had, and drift every NodeGroup it stamped that sets it.
func TestFrozenGenerationsDeclareLiveFields(t *testing.T) {
	live := specFields()
	for version, declared := range v1alpha1.FrozenHashGenerationFields() {
		for _, field := range declared {
			if !slices.Contains(live, field) {
				t.Errorf("generation %q declares %s, which is not a CleverNodeClassSpec field (%v): "+
					"rename or remove it in the declaration", version, field, live)
			}
		}
	}
}

// TestHashMatchesRefusesUndeclaredFields proves the declared fields are
// enforced by HashMatches itself, not left to each frozen hash function: a
// generation that had no field, whose hashes ignore the spec entirely, still
// must not match a spec that sets labels.
func TestHashMatchesRefusesUndeclaredFields(t *testing.T) {
	const version = "v-no-fields"
	emptyStamp := testNodeClass(t, nil).Hash()
	t.Cleanup(v1alpha1.RegisterFrozenHashGeneration(version, nil,
		func(v1alpha1.CleverNodeClassSpec) []string { return []string{emptyStamp} }))

	if match, known := testNodeClass(t, nil).HashMatches(version, emptyStamp); !match || !known {
		t.Errorf("an empty spec must match the generation's stamp, got (match %v, known %v)", match, known)
	}
	// `labels: {}` sets nothing either.
	if match, known := testNodeClass(t, map[string]string{}).HashMatches(version, emptyStamp); !match || !known {
		t.Errorf("`labels: {}` must match the generation's stamp, got (match %v, known %v)", match, known)
	}
	if match, known := testNodeClass(t, map[string]string{"team": "data"}).HashMatches(version, emptyStamp); match || !known {
		t.Errorf("a spec setting a field the generation did not have gave (match %v, known %v), want (false, true)",
			match, known)
	}
}

func TestSetsOnly(t *testing.T) {
	type spec struct {
		Labels map[string]string
		Disk   int
		Image  *string
	}
	image := "x"
	for _, tc := range []struct {
		name   string
		spec   spec
		fields []string
		want   bool
	}{
		{name: "zero, nothing declared", spec: spec{}, want: true},
		{name: "declared field set", spec: spec{Labels: map[string]string{"a": "b"}}, fields: []string{"Labels"}, want: true},
		{name: "undeclared map set", spec: spec{Labels: map[string]string{"a": "b"}}, want: false},
		{name: "undeclared empty map is set", spec: spec{Labels: map[string]string{}}, want: false},
		{name: "undeclared int set", spec: spec{Disk: 200}, fields: []string{"Labels"}, want: false},
		{name: "undeclared pointer set", spec: spec{Image: &image}, fields: []string{"Labels", "Disk"}, want: false},
		{name: "every field declared", spec: spec{Labels: map[string]string{"a": "b"}, Disk: 200, Image: &image},
			fields: []string{"Labels", "Disk", "Image"}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := v1alpha1.SetsOnly(tc.spec, tc.fields); got != tc.want {
				t.Errorf("SetsOnly(%+v, %v) = %v, want %v", tc.spec, tc.fields, got, tc.want)
			}
		})
	}
}
