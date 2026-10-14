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
	"context"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Besides checking the computed patch, have a real API server validate both
// Helm 3's strategic-merge upgrade and Helm 4's server-side apply. A repeated
// env name or a retained valueFrom can otherwise survive a render-only test.
func TestDiskReferenceAPITransitions(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run with make test-envtest")
	}
	environment := &envtest.Environment{}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kubeClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "disk-reference-chart"}}
	if err := kubeClient.Create(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	for _, strategy := range []string{"strategic", "ssa"} {
		for _, from := range diskReferenceUpgradeSources {
			for _, to := range diskReferenceModes {
				t.Run(strategy+"/"+from+" to "+to, func(t *testing.T) {
					original := diskReferenceDeployment(t, from)
					desired := diskReferenceDeployment(t, to)
					for _, deployment := range []*appsv1.Deployment{original, desired} {
						deployment.Namespace = namespace.Name
						deployment.Name = strategy + "-" + from + "-" + to
					}
					originalJSON := deploymentJSON(t, original)
					switch strategy {
					case "strategic":
						if err := kubeClient.Create(ctx, original); err != nil {
							t.Fatalf("creating original deployment: %v", err)
						}
						patch := diskReferenceUpgradePatch(t, originalJSON, deploymentJSON(t, desired), deploymentJSON(t, original))
						if err := kubeClient.Patch(ctx, original, client.RawPatch(types.StrategicMergePatchType, patch)); err != nil {
							t.Fatalf("upgrading deployment: %v", err)
						}
					case "ssa":
						for _, deployment := range []*appsv1.Deployment{original, desired} {
							if err := kubeClient.Patch(ctx, deployment, client.RawPatch(types.ApplyPatchType, deploymentJSON(t, deployment)), client.FieldOwner("chart-test"), client.ForceOwnership); err != nil {
								t.Fatalf("applying deployment: %v", err)
							}
						}
					}
					actual := &appsv1.Deployment{}
					if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(desired), actual); err != nil {
						t.Fatal(err)
					}
					assertDiskReferenceEnv(t, actual.Spec.Template.Spec, to)
				})
			}
		}
	}
}
