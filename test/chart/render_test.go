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

// Package chart locks the controller's default placement.
//
// The controller must never run on capacity Karpenter can deprovision, or it
// can delete the node it is running on. That rule used to be expressed as
// `nodeSelector: clever-cloud.com/cluster-node-role: control-plane`, which
// only holds on the ALL_IN_ONE CKE topology: on DEDICATED_COMPUTE and
// DISTRIBUTED the control plane runs outside the cluster and no node carries
// that label, so the pod stayed Pending forever while `helm install` reported
// success. These tests fail if that selector — or any other topology-specific
// node label — comes back as a chart default.
package chart

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	sigsyaml "sigs.k8s.io/yaml"
)

const (
	// topologyLabel is the CKE node-role label. It is legitimate in a user's
	// own values override; it must never be a chart default.
	topologyLabel = "clever-cloud.com/cluster-node-role"
	// nodePoolLabel is stamped by karpenter-core on every node it provisions.
	// Requiring its ABSENCE is the topology-independent way to say "not on
	// capacity this controller manages".
	nodePoolLabel = "karpenter.sh/nodepool"
	// requireHelmEnv makes a missing helm binary a failure instead of a skip.
	// The Makefile target sets it so the CI run cannot degrade to a green
	// no-op the way an unconditional skip silently would.
	requireHelmEnv = "CHART_TEST_REQUIRE_HELM"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// test/chart -> repo root
	return filepath.Dir(filepath.Dir(wd))
}

// helmTemplate renders charts/karpenter with the given --set overrides.
func helmTemplate(t *testing.T, sets ...string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		if os.Getenv(requireHelmEnv) != "" {
			t.Fatalf("helm is required (%s is set) but not on PATH: %v", requireHelmEnv, err)
		}
		t.Skipf("helm not on PATH; run 'make test-chart' to require it")
	}
	args := []string{"template", "karpenter", filepath.Join(repoRoot(t), "charts", "karpenter")}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	cmd := exec.Command("helm", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// controllerPodSpec extracts the controller Deployment's pod spec from a
// rendered multi-document manifest.
func controllerPodSpec(t *testing.T, manifest string) corev1.PodSpec {
	t.Helper()
	reader := yaml.NewYAMLReader(bufio.NewReader(bytes.NewReader([]byte(manifest))))
	for {
		doc, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("splitting rendered manifest: %v", err)
		}
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := sigsyaml.Unmarshal(doc, &meta); err != nil || meta.Kind != "Deployment" {
			continue
		}
		var deploy appsv1.Deployment
		if err := sigsyaml.Unmarshal(doc, &deploy); err != nil {
			t.Fatalf("decoding Deployment: %v", err)
		}
		return deploy.Spec.Template.Spec
	}
	t.Fatal("no Deployment found in the rendered chart")
	return corev1.PodSpec{}
}

// TestDefaultPlacementIsTopologyIndependent is the regression lock: the
// default placement must exclude Karpenter-managed nodes without naming a
// single CKE topology.
func TestDefaultPlacementIsTopologyIndependent(t *testing.T) {
	manifest := helmTemplate(t)

	if strings.Contains(manifest, topologyLabel) {
		t.Errorf("the rendered chart references %q with default values.\n"+
			"That label only exists on ALL_IN_ONE clusters: on DEDICATED_COMPUTE and DISTRIBUTED "+
			"the control plane is outside the cluster, so a default that depends on it strands the "+
			"controller in Pending while the install reports success. Express the rule as the absence "+
			"of %q instead.", topologyLabel, nodePoolLabel)
	}

	spec := controllerPodSpec(t, manifest)

	if len(spec.NodeSelector) != 0 {
		t.Errorf("default nodeSelector must be empty, got %v: a positive selector encodes the set of "+
			"nodes that exist on one topology; the guarantee belongs in affinity", spec.NodeSelector)
	}

	if spec.Affinity == nil || spec.Affinity.NodeAffinity == nil ||
		spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatal("default affinity must require node affinity excluding Karpenter-managed nodes; got none")
	}
	terms := spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		t.Fatal("required node affinity has no nodeSelectorTerms")
	}
	// Terms are ORed: EVERY term must exclude Karpenter's own nodes, or a pod
	// can be placed on capacity it may delete.
	for i, term := range terms {
		if !excludesManagedNodes(term) {
			t.Errorf("nodeSelectorTerms[%d] does not require %q to be absent; "+
				"terms are ORed, so every one of them must exclude Karpenter-managed nodes", i, nodePoolLabel)
		}
	}
}

func excludesManagedNodes(term corev1.NodeSelectorTerm) bool {
	for _, expr := range term.MatchExpressions {
		if expr.Key == nodePoolLabel && expr.Operator == corev1.NodeSelectorOpDoesNotExist {
			return true
		}
	}
	return false
}

// TestPlacementRemainsValuesDriven pins that every placement field is still
// fully overridable from values. Clever Cloud's control plane vendors these
// manifests and applies a render-time override; that override must keep
// working, and an operator must be able to narrow placement further.
func TestPlacementRemainsValuesDriven(t *testing.T) {
	t.Run("nodeSelector can be set", func(t *testing.T) {
		spec := controllerPodSpec(t, helmTemplate(t, `nodeSelector.kubernetes\.io/os=linux`))
		if spec.NodeSelector["kubernetes.io/os"] != "linux" {
			t.Errorf("nodeSelector override was not rendered, got %v", spec.NodeSelector)
		}
		// Narrowing must not drop the guarantee: both constraints apply.
		if spec.Affinity == nil || spec.Affinity.NodeAffinity == nil {
			t.Error("setting nodeSelector must not remove the default affinity")
		}
	})

	t.Run("affinity can be replaced", func(t *testing.T) {
		spec := controllerPodSpec(t, helmTemplate(t, "affinity=null"))
		if spec.Affinity != nil {
			t.Errorf("affinity=null must remove the block entirely, got %+v", spec.Affinity)
		}
	})

	t.Run("tolerations can be replaced", func(t *testing.T) {
		spec := controllerPodSpec(t, helmTemplate(t, "tolerations=null"))
		if len(spec.Tolerations) != 0 {
			t.Errorf("tolerations=null must remove the block entirely, got %+v", spec.Tolerations)
		}
	})
}

// TestDefaultTolerationsDoNotReachManagedNodes guards the one taint the
// provider relies on for correctness: NodeGroups are created with
// karpenter.sh/unregistered:NoExecute to close the race between node readiness
// and Karpenter's label sync. Tolerating it would let the controller land on a
// node that is still being adopted.
func TestDefaultTolerationsDoNotReachManagedNodes(t *testing.T) {
	spec := controllerPodSpec(t, helmTemplate(t))
	for _, tol := range spec.Tolerations {
		if strings.HasPrefix(tol.Key, "karpenter.sh/") {
			t.Errorf("default tolerations must not tolerate %q: Karpenter-managed nodes are excluded "+
				"by affinity, and tolerating its own taints reopens the adoption race", tol.Key)
		}
		if tol.Key == "" && tol.Operator == corev1.TolerationOpExists {
			t.Error("default tolerations must not tolerate everything (empty key with operator Exists)")
		}
	}
}

// TestRawManifestPlacement locks the generated raw manifest, which is what
// downstream consumers vendor and what `kubectl apply -f deploy/karpenter.yaml`
// installs. It needs no helm, so it also runs under a plain `go test ./...`.
func TestRawManifestPlacement(t *testing.T) {
	path := filepath.Join(repoRoot(t), "deploy", "karpenter.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	manifest := string(raw)

	if strings.Contains(manifest, topologyLabel) {
		t.Errorf("deploy/karpenter.yaml references %q; regenerate it with 'make raw-manifest' after "+
			"removing the topology-specific default from charts/karpenter/values.yaml", topologyLabel)
	}
	if !strings.Contains(manifest, nodePoolLabel) {
		t.Errorf("deploy/karpenter.yaml does not mention %q: the generated manifest lost the affinity "+
			"that keeps the controller off Karpenter-managed nodes — regenerate it with 'make raw-manifest'", nodePoolLabel)
	}

	spec := controllerPodSpec(t, manifest)
	if len(spec.NodeSelector) != 0 {
		t.Errorf("deploy/karpenter.yaml pins a nodeSelector by default: %v", spec.NodeSelector)
	}
	if spec.Affinity == nil || spec.Affinity.NodeAffinity == nil ||
		spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatal("deploy/karpenter.yaml has no required node affinity on the controller pod")
	}
	for i, term := range spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		if !excludesManagedNodes(term) {
			t.Errorf("deploy/karpenter.yaml nodeSelectorTerms[%d] does not exclude Karpenter-managed nodes", i)
		}
	}
}
