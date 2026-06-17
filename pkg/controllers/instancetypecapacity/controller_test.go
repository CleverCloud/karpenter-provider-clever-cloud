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

package instancetypecapacity_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/instancetypecapacity"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

func newTestController(t *testing.T, objs ...client.Object) (*instancetypecapacity.Controller, *instancetype.Provider) {
	t.Helper()
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		Build()
	itp := instancetype.NewProvider("par", nil, nil)
	return instancetypecapacity.NewController(kubeClient, itp), itp
}

func workerNode(name, flavor string, capacity, allocatable corev1.ResourceList) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				v1alpha1.FlavorLabelKey:   flavor,
				v1alpha1.NodeRoleLabelKey: v1alpha1.NodeRoleWorker,
			},
		},
		Status: corev1.NodeStatus{
			Capacity:    capacity,
			Allocatable: allocatable,
		},
	}
}

// observedL returns a measured capacity/allocatable pair for the L flavor
// that differs from the static estimate (memory 23983488Ki, 100Mi reserved).
func observedL(t *testing.T) (corev1.ResourceList, corev1.ResourceList) {
	t.Helper()
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("12"),
		corev1.ResourceMemory: resource.MustParse("25000000Ki"),
	}
	allocatable := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("12"),
		corev1.ResourceMemory: resource.MustParse("24897600Ki"),
	}
	return capacity, allocatable
}

func nodeRequest(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name}}
}

// assertStaticL verifies the L catalog entry still serves the static estimate.
func assertStaticL(t *testing.T, itp *instancetype.Provider) {
	t.Helper()
	it, err := itp.Get("L")
	if err != nil {
		t.Fatalf("Get(L): %v", err)
	}
	wantMemory := resource.MustParse("23983488Ki")
	if it.Capacity.Memory().Cmp(wantMemory) != 0 {
		t.Errorf("expected static memory estimate %s, got %s", wantMemory.String(), it.Capacity.Memory())
	}
	wantReserved := resource.MustParse("100Mi")
	gotReserved := it.Overhead.KubeReserved[corev1.ResourceMemory]
	if gotReserved.Cmp(wantReserved) != 0 {
		t.Errorf("expected static KubeReserved %s, got %s", wantReserved.String(), gotReserved.String())
	}
}

func TestReconcileFeedsObservedCapacityIntoCatalog(t *testing.T) {
	capacity, allocatable := observedL(t)
	node := workerNode("default-abc12-node0", "L", capacity, allocatable)
	ctrl, itp := newTestController(t, node)

	result, err := ctrl.Reconcile(context.Background(), nodeRequest(node.Name))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result != (reconcile.Result{}) {
		t.Errorf("expected no requeue, got %+v", result)
	}

	it, err := itp.Get("L")
	if err != nil {
		t.Fatalf("Get(L): %v", err)
	}
	if it.Capacity.Cpu().Cmp(*capacity.Cpu()) != 0 {
		t.Errorf("expected observed cpu capacity %s, got %s", capacity.Cpu(), it.Capacity.Cpu())
	}
	if it.Capacity.Memory().Cmp(*capacity.Memory()) != 0 {
		t.Errorf("expected observed memory capacity %s, got %s", capacity.Memory(), it.Capacity.Memory())
	}
	// The overhead is recomputed as capacity-allocatable, replacing the static
	// 100Mi estimate: 25000000Ki - 24897600Ki = 102400Ki.
	wantReserved := resource.MustParse("102400Ki")
	gotReserved := it.Overhead.KubeReserved[corev1.ResourceMemory]
	if gotReserved.Cmp(wantReserved) != 0 {
		t.Errorf("expected KubeReserved memory %s, got %s", wantReserved.String(), gotReserved.String())
	}
	alloc := it.Allocatable()
	if alloc.Memory().Cmp(*allocatable.Memory()) != 0 {
		t.Errorf("expected allocatable memory %s, got %s", allocatable.Memory(), alloc.Memory())
	}
	if alloc.Cpu().Cmp(*allocatable.Cpu()) != 0 {
		t.Errorf("expected allocatable cpu %s, got %s", allocatable.Cpu(), alloc.Cpu())
	}

	// List() must serve the corrected entry too.
	found := false
	for _, li := range itp.List() {
		if li.Name != "L" {
			continue
		}
		found = true
		if li.Capacity.Memory().Cmp(*capacity.Memory()) != 0 {
			t.Errorf("List(): expected observed memory capacity %s, got %s", capacity.Memory(), li.Capacity.Memory())
		}
	}
	if !found {
		t.Fatal("flavor L not found in List()")
	}
}

func TestReconcileIgnoresNonWorkerNodes(t *testing.T) {
	capacity, allocatable := observedL(t)
	noRole := workerNode("no-role", "L", capacity, allocatable)
	delete(noRole.Labels, v1alpha1.NodeRoleLabelKey)
	controlPlane := workerNode("control-plane-0", "L", capacity, allocatable)
	controlPlane.Labels[v1alpha1.NodeRoleLabelKey] = "control-plane"
	ctrl, itp := newTestController(t, noRole, controlPlane)

	if _, err := ctrl.Reconcile(context.Background(), nodeRequest(noRole.Name)); err != nil {
		t.Fatalf("Reconcile(no role): %v", err)
	}
	if _, err := ctrl.Reconcile(context.Background(), nodeRequest(controlPlane.Name)); err != nil {
		t.Fatalf("Reconcile(control-plane): %v", err)
	}
	assertStaticL(t, itp)
}

func TestReconcileIgnoresNodesWithoutFlavor(t *testing.T) {
	capacity, allocatable := observedL(t)
	node := workerNode("flavorless", "L", capacity, allocatable)
	delete(node.Labels, v1alpha1.FlavorLabelKey)
	ctrl, itp := newTestController(t, node)

	if _, err := ctrl.Reconcile(context.Background(), nodeRequest(node.Name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertStaticL(t, itp)
}

func TestReconcileMissingNodeNoError(t *testing.T) {
	ctrl, itp := newTestController(t)

	result, err := ctrl.Reconcile(context.Background(), nodeRequest("does-not-exist"))
	if err != nil {
		t.Fatalf("expected nil error for missing node, got %v", err)
	}
	if result != (reconcile.Result{}) {
		t.Errorf("expected no requeue, got %+v", result)
	}
	assertStaticL(t, itp)
}
