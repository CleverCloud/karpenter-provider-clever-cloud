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

package envtest_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/nodereplacement"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// TestNodeReplacementFollowsThePlatform proves the watches a replacement rests
// on against a real apiserver: nothing here calls a Reconcile. The platform
// adds a node to a registered NodeClaim's group, cordons the old one and
// deletes it; each step must reach the nodereplacement controller through its
// Node watch (updates and deletions), and the deletion must reach the node the
// providerid controller refused, through its deletion watch. Karpenter-core
// does not run in this suite, so the reopened registration stays open.
func TestNodeReplacementFollowsThePlatform(t *testing.T) {
	ctx := context.Background()
	const name = "replace-one"
	node0, node1 := name+"-node0", name+"-node1"
	unregistered := corev1.Taint{Key: karpv1.UnregisteredTaintKey, Effect: corev1.TaintEffectNoExecute}

	nodeClaim := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{Group: "karpenter.clever-cloud.com", Kind: "CleverNodeClass", Name: "default"},
			Requirements: []karpv1.NodeSelectorRequirementWithMinValues{{
				Key:      corev1.LabelInstanceTypeStable,
				Operator: corev1.NodeSelectorOpExists,
			}},
		},
	}
	if err := kubeClient.Create(ctx, nodeClaim); err != nil {
		t.Fatalf("creating nodeclaim: %v", err)
	}
	t.Cleanup(func() { _ = kubeClient.Delete(ctx, nodeClaim) })
	group := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{v1alpha1.ManagedLabelKey: "true", v1alpha1.NodeClaimLabelKey: name},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: name, UID: nodeClaim.UID},
			},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "XS", NodeCount: 1, Taints: []ngv1.NodeGroupTaint{{Key: unregistered.Key, Effect: unregistered.Effect}}},
	}
	if err := kubeClient.Create(ctx, group); err != nil {
		t.Fatalf("creating nodegroup: %v", err)
	}
	t.Cleanup(func() { _ = kubeClient.Delete(ctx, group) })
	join := func(nodeName string) {
		t.Helper()
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: map[string]string{v1alpha1.NodeGroupNodeLabelKey: name}},
			Spec:       corev1.NodeSpec{Taints: []corev1.Taint{unregistered}},
		}
		if err := kubeClient.Create(ctx, node); err != nil {
			t.Fatalf("creating node %s: %v", nodeName, err)
		}
		t.Cleanup(func() {
			// A test that fails midway can leave core's finalizer on the node.
			if err := apiReader.Get(ctx, types.NamespacedName{Name: nodeName}, node); err == nil {
				node.Finalizers = nil
				_ = kubeClient.Update(ctx, node)
				_ = kubeClient.Delete(ctx, node)
			}
		})
	}
	getNode := func(ctx context.Context, nodeName string) (*corev1.Node, error) {
		node := &corev1.Node{}
		return node, apiReader.Get(ctx, types.NamespacedName{Name: nodeName}, node)
	}

	// node0 boots, is stamped, and karpenter-core registers it.
	join(node0)
	eventually(t, 10*time.Second, func(ctx context.Context) error {
		node, err := getNode(ctx, node0)
		if err != nil {
			return err
		}
		if node.Spec.ProviderID != nodegroup.ProviderID(name) {
			return fmt.Errorf("%s has provider id %q", node0, node.Spec.ProviderID)
		}
		node.Spec.Taints = nil
		node.Labels[karpv1.NodeRegisteredLabelKey] = "true"
		controllerutil.AddFinalizer(node, karpv1.TerminationFinalizer)
		if err := kubeClient.Update(ctx, node); err != nil {
			return err
		}
		// No kubelet runs here: report the node Ready as one does, or it reads
		// as the NotReady node the platform repairs.
		node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
		return kubeClient.Status().Update(ctx, node)
	})
	eventually(t, 10*time.Second, func(ctx context.Context) error {
		got := &karpv1.NodeClaim{}
		if err := apiReader.Get(ctx, types.NamespacedName{Name: name}, got); err != nil {
			return err
		}
		got.Status.ProviderID, got.Status.NodeName = nodegroup.ProviderID(name), node0
		for _, condition := range []string{karpv1.ConditionTypeLaunched, karpv1.ConditionTypeRegistered, karpv1.ConditionTypeInitialized} {
			got.StatusConditions().SetTrue(condition)
		}
		return kubeClient.Status().Update(ctx, got)
	})

	// The replacement joins. Until the platform cordons node0 nobody drains
	// it: it keeps core's finalizer, and node1 gets no provider ID.
	join(node1)
	time.Sleep(2 * time.Second)
	if node, err := getNode(ctx, node0); err != nil || !controllerutil.ContainsFinalizer(node, karpv1.TerminationFinalizer) {
		t.Fatalf("%s lost karpenter's termination finalizer before the platform cordoned it (err %v)", node0, err)
	}
	if node, err := getNode(ctx, node1); err != nil || node.Spec.ProviderID != "" {
		t.Fatalf("%s was stamped next to %s (err %v)", node1, node0, err)
	}

	// The platform cordons node0: the update must release it.
	eventually(t, 10*time.Second, func(ctx context.Context) error {
		node, err := getNode(ctx, node0)
		if err != nil {
			return err
		}
		node.Spec.Unschedulable = true
		node.Labels["clever-cloud.com/draining"] = "true"
		return kubeClient.Update(ctx, node)
	})
	eventually(t, 10*time.Second, func(ctx context.Context) error {
		node, err := getNode(ctx, node0)
		if err != nil {
			return err
		}
		if controllerutil.ContainsFinalizer(node, karpv1.TerminationFinalizer) || node.Annotations[karpv1.DoNotDisruptAnnotationKey] != "true" {
			return fmt.Errorf("%s is not released: finalizers %v, annotations %v", node0, node.Finalizers, node.Annotations)
		}
		return nil
	})

	// Something resizes the group: both nodes now stay, so node0 must be
	// core's to drain again, and released again once the group is back to one
	// node. No Node changes in either case: only the NodeGroup watch can tell.
	released := func(want bool) {
		t.Helper()
		eventually(t, 10*time.Second, func(ctx context.Context) error {
			node, err := getNode(ctx, node0)
			if err != nil {
				return err
			}
			if got := !controllerutil.ContainsFinalizer(node, karpv1.TerminationFinalizer); got != want {
				return fmt.Errorf("%s released = %v, want %v", node0, got, want)
			}
			return nil
		})
	}
	for _, nodeCount := range []int32{2, 1} {
		eventually(t, 10*time.Second, func(ctx context.Context) error {
			ng := &ngv1.NodeGroup{}
			if err := apiReader.Get(ctx, types.NamespacedName{Name: name}, ng); err != nil {
				return err
			}
			ng.Spec.NodeCount = nodeCount
			return kubeClient.Update(ctx, ng)
		})
		released(nodeCount == 1)
	}

	// The platform deletes node0: node1 must take the provider ID over, and
	// the NodeClaim's registration must reopen for it.
	node, err := getNode(ctx, node0)
	if err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Delete(ctx, node); err != nil {
		t.Fatalf("deleting %s: %v", node0, err)
	}
	eventually(t, 10*time.Second, func(ctx context.Context) error {
		if _, err := getNode(ctx, node0); !apierrors.IsNotFound(err) {
			return fmt.Errorf("%s is still there: %v", node0, err)
		}
		node, err := getNode(ctx, node1)
		if err != nil {
			return err
		}
		if node.Spec.ProviderID != nodegroup.ProviderID(name) {
			return fmt.Errorf("%s has provider id %q: the deletion of %s did not wake it", node1, node.Spec.ProviderID, node0)
		}
		got := &karpv1.NodeClaim{}
		if err := apiReader.Get(ctx, types.NamespacedName{Name: name}, got); err != nil {
			return err
		}
		registered := got.StatusConditions().Get(karpv1.ConditionTypeRegistered)
		if !registered.IsUnknown() || registered.Reason != nodereplacement.ReasonNodeReplaced || got.Status.NodeName != "" {
			return fmt.Errorf("the registration was not reopened: Registered=%s (%s), node %q", registered.Status, registered.Reason, got.Status.NodeName)
		}
		return nil
	})
}
