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

package nodeclass_test

import (
	"context"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/nodeclass"
)

func newTestController(t *testing.T, objs ...client.Object) (*nodeclass.Controller, client.Client) {
	t.Helper()
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		Build()
	return nodeclass.NewController(kubeClient), kubeClient
}

func testNodeClass(name string, labels map[string]string) *v1alpha1.CleverNodeClass {
	return &v1alpha1.CleverNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.CleverNodeClassSpec{Labels: labels},
	}
}

func testNodeClaim(name, nodeClassName string) *karpv1.NodeClaim {
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{
				Group: "karpenter.clever-cloud.com",
				Kind:  "CleverNodeClass",
				Name:  nodeClassName,
			},
		},
	}
}

func reconcileNodeClass(t *testing.T, c *nodeclass.Controller, name string) reconcile.Result {
	t.Helper()
	result, err := c.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return result
}

func getNodeClass(t *testing.T, kubeClient client.Client, name string) *v1alpha1.CleverNodeClass {
	t.Helper()
	nodeClass := &v1alpha1.CleverNodeClass{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, nodeClass); err != nil {
		t.Fatalf("getting nodeclass: %v", err)
	}
	return nodeClass
}

func TestReconcileAddsFinalizerAndMarksReady(t *testing.T) {
	c, kubeClient := newTestController(t, testNodeClass("default", map[string]string{"team": "data"}))

	if result := reconcileNodeClass(t, c, "default"); result != (reconcile.Result{}) {
		t.Errorf("unexpected result %+v", result)
	}

	nodeClass := getNodeClass(t, kubeClient, "default")
	if !controllerutil.ContainsFinalizer(nodeClass, v1alpha1.TerminationFinalizer) {
		t.Errorf("expected termination finalizer, got %v", nodeClass.Finalizers)
	}
	if !nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded).IsTrue() {
		t.Errorf("expected ValidationSucceeded true, got %+v", nodeClass.Status.Conditions)
	}
	if !nodeClass.StatusConditions().Root().IsTrue() {
		t.Errorf("expected Ready true, got %+v", nodeClass.Status.Conditions)
	}
}

func TestReconcileRejectsReservedLabelPrefixes(t *testing.T) {
	for _, prefix := range []string{"kubernetes.io/", "node.kubernetes.io/", "clever-cloud.com/"} {
		t.Run(prefix, func(t *testing.T) {
			c, kubeClient := newTestController(t, testNodeClass("default", map[string]string{prefix + "role": "worker"}))

			reconcileNodeClass(t, c, "default")

			cond := getNodeClass(t, kubeClient, "default").StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded)
			if !cond.IsFalse() {
				t.Fatalf("expected ValidationSucceeded false, got %+v", cond)
			}
			if cond.Reason != "ValidationFailed" {
				t.Errorf("unexpected reason %q", cond.Reason)
			}
		})
	}
}

func TestReconcileRejectsLongLabelValues(t *testing.T) {
	c, kubeClient := newTestController(t, testNodeClass("default", map[string]string{"team": strings.Repeat("a", 64)}))

	reconcileNodeClass(t, c, "default")

	cond := getNodeClass(t, kubeClient, "default").StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded)
	if !cond.IsFalse() {
		t.Fatalf("expected ValidationSucceeded false, got %+v", cond)
	}
	if cond.Reason != "ValidationFailed" {
		t.Errorf("unexpected reason %q", cond.Reason)
	}
}

func TestReconcileRecoversAfterFix(t *testing.T) {
	c, kubeClient := newTestController(t, testNodeClass("default", map[string]string{"kubernetes.io/role": "worker"}))

	reconcileNodeClass(t, c, "default")
	nodeClass := getNodeClass(t, kubeClient, "default")
	if !nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded).IsFalse() {
		t.Fatalf("expected ValidationSucceeded false before fix, got %+v", nodeClass.Status.Conditions)
	}

	nodeClass.Spec.Labels = map[string]string{"team": "data"}
	if err := kubeClient.Update(context.Background(), nodeClass); err != nil {
		t.Fatalf("updating nodeclass: %v", err)
	}

	reconcileNodeClass(t, c, "default")
	nodeClass = getNodeClass(t, kubeClient, "default")
	if !nodeClass.StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded).IsTrue() {
		t.Errorf("expected ValidationSucceeded true after fix, got %+v", nodeClass.Status.Conditions)
	}
}

func TestFinalizeBlocksWhileNodeClaimReferences(t *testing.T) {
	nodeClass := testNodeClass("default", nil)
	nodeClass.Finalizers = []string{v1alpha1.TerminationFinalizer}
	c, kubeClient := newTestController(t, nodeClass, testNodeClaim("default-abc12", "default"))

	if err := kubeClient.Delete(context.Background(), nodeClass); err != nil {
		t.Fatalf("deleting nodeclass: %v", err)
	}

	result := reconcileNodeClass(t, c, "default")
	if result.RequeueAfter != 30*time.Second {
		t.Errorf("expected 30s requeue while nodeclaim references the nodeclass, got %+v", result)
	}

	// The finalizer must keep the terminating object around.
	nodeClass = getNodeClass(t, kubeClient, "default")
	if !controllerutil.ContainsFinalizer(nodeClass, v1alpha1.TerminationFinalizer) {
		t.Errorf("expected finalizer to remain, got %v", nodeClass.Finalizers)
	}
}

func TestFinalizeRemovesFinalizerWhenUnreferenced(t *testing.T) {
	nodeClass := testNodeClass("default", nil)
	nodeClass.Finalizers = []string{v1alpha1.TerminationFinalizer}
	c, kubeClient := newTestController(t, nodeClass, testNodeClaim("other-abc12", "other"))

	if err := kubeClient.Delete(context.Background(), nodeClass); err != nil {
		t.Fatalf("deleting nodeclass: %v", err)
	}

	if result := reconcileNodeClass(t, c, "default"); result != (reconcile.Result{}) {
		t.Errorf("unexpected result %+v", result)
	}

	// Removing the last finalizer lets the fake client drop the object.
	err := kubeClient.Get(context.Background(), types.NamespacedName{Name: "default"}, &v1alpha1.CleverNodeClass{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected nodeclass to be gone, got err=%v", err)
	}
}

func TestReconcileMissingNodeClassNoError(t *testing.T) {
	c, _ := newTestController(t)

	if result := reconcileNodeClass(t, c, "absent"); result != (reconcile.Result{}) {
		t.Errorf("unexpected result %+v", result)
	}
}
