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

package chart

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

var diskReferenceModes = []string{"static", "host", "node"}

var diskReferenceUpgradeSources = []string{"legacy", "static", "host", "node"}

const explicitDiskReference = "fixed-workers-node0"

func diskReferenceDeployment(t *testing.T, mode string) *appsv1.Deployment {
	t.Helper()
	if mode == "legacy" {
		deployment := diskReferenceDeployment(t, "static")
		for i := range deployment.Spec.Template.Spec.Containers {
			container := &deployment.Spec.Template.Spec.Containers[i]
			for j, env := range container.Env {
				if env.Name == "NODE_NAME" {
					container.Env = append(container.Env[:j], container.Env[j+1:]...)
					break
				}
			}
		}
		return deployment
	}
	sets := []string{"settings.ephemeralStorageReference.mode=" + mode}
	if mode == "node" {
		sets = append(sets, "settings.ephemeralStorageReference.nodeName="+explicitDiskReference)
	}
	return controllerDeployment(t, helmTemplate(t, sets...))
}

func assertDiskReferenceEnv(t *testing.T, spec corev1.PodSpec, mode string) {
	t.Helper()
	want := corev1.EnvVar{Name: "NODE_NAME"}
	switch mode {
	case "host":
		want.ValueFrom = &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}
	case "node":
		want.Value = explicitDiskReference
	}
	count := 0
	for _, container := range spec.Containers {
		if container.Name != "controller" {
			continue
		}
		for _, env := range container.Env {
			if env.Name != "NODE_NAME" {
				continue
			}
			count++
			// The API defaults fieldRef.apiVersion; both pre-admission and
			// admitted manifests must otherwise have exactly the same source.
			if env.ValueFrom != nil && env.ValueFrom.FieldRef != nil {
				env.ValueFrom = env.ValueFrom.DeepCopy()
				env.ValueFrom.FieldRef.APIVersion = ""
			}
			if !reflect.DeepEqual(env, want) {
				t.Errorf("mode %s: NODE_NAME = %+v, want %+v", mode, env, want)
			}
		}
	}
	if count != 1 {
		t.Errorf("mode %s: got %d NODE_NAME entries, want exactly one", mode, count)
	}
}

func TestDiskReferenceModes(t *testing.T) {
	t.Run("default preserves static sizing", func(t *testing.T) {
		assertDiskReferenceEnv(t, controllerPodSpec(t, helmTemplate(t)), "static")
	})
	for _, mode := range diskReferenceModes {
		t.Run(mode, func(t *testing.T) {
			assertDiskReferenceEnv(t, diskReferenceDeployment(t, mode).Spec.Template.Spec, mode)
		})
	}
	t.Run("raw manifest defaults to static", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join(repoRoot(t), "deploy", "karpenter.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		assertDiskReferenceEnv(t, controllerPodSpec(t, string(raw)), "static")
	})
	t.Run("unrelated environment is preserved", func(t *testing.T) {
		manifest, err := helmTemplateArgs(t, "--set-json", `controller.env=[{"name":"EXTRA_SETTING","value":"kept"}]`)
		if err != nil {
			t.Fatalf("rendering extra env: %v\n%s", err, manifest)
		}
		spec := controllerPodSpec(t, manifest)
		assertDiskReferenceEnv(t, spec, "static")
		if got := controllerEnv(spec, "EXTRA_SETTING"); got == nil || got.Value != "kept" {
			t.Fatalf("extra setting lost: %+v", got)
		}
	})
}

func TestDiskReferenceInvalidSettings(t *testing.T) {
	cases := map[string][]string{
		"unknown mode":       {"--set", "settings.ephemeralStorageReference.mode=automatic"},
		"empty mode":         {"--set-string", "settings.ephemeralStorageReference.mode="},
		"boolean mode":       {"--set", "settings.ephemeralStorageReference.mode=true"},
		"node without name":  {"--set", "settings.ephemeralStorageReference.mode=node"},
		"static with name":   {"--set", "settings.ephemeralStorageReference.nodeName=" + explicitDiskReference},
		"host with name":     {"--set", "settings.ephemeralStorageReference.mode=host", "--set", "settings.ephemeralStorageReference.nodeName=" + explicitDiskReference},
		"invalid node name":  {"--set", "settings.ephemeralStorageReference.mode=node", "--set-string", "settings.ephemeralStorageReference.nodeName=Not_A_Node"},
		"unknown option":     {"--set", "settings.ephemeralStorageReference.nodename=" + explicitDiskReference},
		"extra env empty":    {"--set-json", `controller.env=[{"name":"NODE_NAME","value":""}]`},
		"extra env explicit": {"--set-json", `controller.env=[{"name":"NODE_NAME","value":"fixed-workers-node0"}]`},
		"extra env source":   {"--set-json", `controller.env=[{"name":"NODE_NAME","valueFrom":{"fieldRef":{"fieldPath":"spec.nodeName"}}}]`},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := helmTemplateArgs(t, args...)
			if err == nil || !strings.Contains(out, "schema") {
				t.Fatalf("expected schema rejection, got err=%v\n%s", err, out)
			}
		})
	}
}

// Templates also refuse the conflicting env var when a caller bypasses schema
// validation. Copying the chart without its schema works on Helm 3.14 as well
// as newer Helm versions, without depending on --skip-schema-validation.
func TestDiskReferenceTemplateGuard(t *testing.T) {
	chartPath := filepath.Join(t.TempDir(), "karpenter")
	if err := os.CopyFS(chartPath, os.DirFS(filepath.Join(repoRoot(t), "charts", "karpenter"))); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(chartPath, "values.schema.json")); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"duplicate env":     {"--set-json", `controller.env=[{"name":"NODE_NAME","value":""}]`},
		"unknown mode":      {"--set", "settings.ephemeralStorageReference.mode=automatic"},
		"node without name": {"--set", "settings.ephemeralStorageReference.mode=node"},
		"host with name":    {"--set", "settings.ephemeralStorageReference.mode=host", "--set", "settings.ephemeralStorageReference.nodeName=" + explicitDiskReference},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := helmTemplateIn(t, chartPath, args...)
			if err == nil || !strings.Contains(out, "settings.ephemeralStorageReference") {
				t.Fatalf("expected template guard, got err=%v\n%s", err, out)
			}
		})
	}
}

// Helm's client-side upgrade computes a three-way strategic merge patch. Test
// both directions: leaving host mode must remove fieldRef, and entering it must
// remove a nonempty literal value rather than submitting value + valueFrom.
func TestDiskReferenceUpgradePatch(t *testing.T) {
	for _, from := range diskReferenceUpgradeSources {
		old := diskReferenceDeployment(t, from)
		for _, to := range diskReferenceModes {
			t.Run(from+" to "+to, func(t *testing.T) {
				desired := diskReferenceDeployment(t, to)
				original := deploymentJSON(t, old)
				patch := diskReferenceUpgradePatch(t, original, deploymentJSON(t, desired), original)
				merged, err := strategicpatch.StrategicMergePatch(original, patch, appsv1.Deployment{})
				if err != nil {
					t.Fatal(err)
				}
				var result appsv1.Deployment
				if err := json.Unmarshal(merged, &result); err != nil {
					t.Fatal(err)
				}
				assertDiskReferenceEnv(t, result.Spec.Template.Spec, to)
			})
		}
	}
}

func deploymentJSON(t *testing.T, deployment *appsv1.Deployment) []byte {
	t.Helper()
	data, err := json.Marshal(deployment)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func diskReferenceUpgradePatch(t *testing.T, original, desired, current []byte) []byte {
	t.Helper()
	meta, err := strategicpatch.NewPatchMetaFromStruct(appsv1.Deployment{})
	if err != nil {
		t.Fatal(err)
	}
	patch, err := strategicpatch.CreateThreeWayMergePatch(original, desired, current, meta, true)
	if err != nil {
		t.Fatalf("computing upgrade patch: %v", err)
	}
	return patch
}
