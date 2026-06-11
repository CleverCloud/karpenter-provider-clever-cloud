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

package providerid_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/providerid"
)

func node(name string, labels map[string]string, providerID string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec:       corev1.NodeSpec{ProviderID: providerID},
	}
}

func managedNodeGroup(name string) *ngv1.NodeGroup {
	return &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true"},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1},
	}
}

func unmanagedNodeGroup(name string) *ngv1.NodeGroup {
	return &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1},
	}
}

func reconcileNode(t *testing.T, kubeClient client.Client, nodeName string) error {
	t.Helper()
	c := providerid.NewController(kubeClient)
	_, err := c.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: nodeName}})
	return err
}

func getProviderID(t *testing.T, kubeClient client.Client, nodeName string) string {
	t.Helper()
	got := &corev1.Node{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeName}, got); err != nil {
		t.Fatalf("getting node: %v", err)
	}
	return got.Spec.ProviderID
}

func newClient(objs ...client.Object) client.Client {
	return fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).Build()
}

func TestReconcileStampsProviderID(t *testing.T) {
	kubeClient := newClient(
		node("worker-0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""),
		managedNodeGroup("ng1"),
	)
	if err := reconcileNode(t, kubeClient, "worker-0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "worker-0"); got != "clevercloud://ng1" {
		t.Errorf("expected provider id %q, got %q", "clevercloud://ng1", got)
	}
}

func TestReconcileSkipsNodeWithExistingProviderID(t *testing.T) {
	kubeClient := newClient(
		node("worker-0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, "other://x"),
		managedNodeGroup("ng1"),
	)
	if err := reconcileNode(t, kubeClient, "worker-0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "worker-0"); got != "other://x" {
		t.Errorf("expected provider id to stay %q, got %q", "other://x", got)
	}
}

func TestReconcileSkipsNodeWithoutNodeGroupLabel(t *testing.T) {
	kubeClient := newClient(node("worker-0", nil, ""))
	if err := reconcileNode(t, kubeClient, "worker-0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "worker-0"); got != "" {
		t.Errorf("expected provider id to stay empty, got %q", got)
	}
}

func TestReconcileSkipsUnmanagedNodeGroup(t *testing.T) {
	kubeClient := newClient(
		node("worker-0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "ng1"}, ""),
		unmanagedNodeGroup("ng1"),
	)
	if err := reconcileNode(t, kubeClient, "worker-0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "worker-0"); got != "" {
		t.Errorf("expected provider id to stay empty for unmanaged nodegroup, got %q", got)
	}
}

func TestReconcileSkipsMissingNodeGroup(t *testing.T) {
	kubeClient := newClient(node("worker-0", map[string]string{v1alpha1.NodeGroupNodeLabelKey: "absent"}, ""))
	if err := reconcileNode(t, kubeClient, "worker-0"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := getProviderID(t, kubeClient, "worker-0"); got != "" {
		t.Errorf("expected provider id to stay empty for missing nodegroup, got %q", got)
	}
}

func TestReconcileMissingNodeNoError(t *testing.T) {
	kubeClient := newClient()
	if err := reconcileNode(t, kubeClient, "ghost"); err != nil {
		t.Fatalf("expected nil error for missing node, got %v", err)
	}
}
