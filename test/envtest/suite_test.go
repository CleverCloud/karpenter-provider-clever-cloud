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

// Package envtest_test runs the provider controllers against a real
// kube-apiserver (controller-runtime envtest). The unit tests use the fake
// client, which does not exercise CRD schema validation, status subresource
// semantics, optimistic-lock patches or watch-driven reconciles — the bug
// classes this suite exists to catch. It also proves that every vendored CRD
// under deploy/crds actually installs on the apiserver version we target,
// which is the regression the automated karpenter.sh CRD sync could
// otherwise introduce silently.
//
// Run with `make test-envtest` (downloads the apiserver/etcd binaries via
// setup-envtest). Without KUBEBUILDER_ASSETS the suite skips itself so a
// plain `go test ./...` stays green.
package envtest_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/nodeclass"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/providerid"
)

var kubeClient client.Client

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		// The skip is a local-only convenience so plain `go test ./...`
		// stays green. In CI a missing asset path means setup-envtest broke:
		// skipping would turn the PR gate into a silent no-op.
		if os.Getenv("CI") != "" {
			fmt.Println("KUBEBUILDER_ASSETS is not set but CI is: refusing to silently skip the envtest suite")
			os.Exit(1)
		}
		fmt.Println("skipping envtest suite: KUBEBUILDER_ASSETS not set (run via 'make test-envtest')")
		os.Exit(0)
	}

	env := &envtest.Environment{
		// deploy/crds carries the generated CleverNodeClass CRD and the
		// vendored karpenter.sh CRDs; testdata adds a loose NodeGroup CRD
		// stand-in (the real one is owned by Clever Cloud, not this repo).
		CRDDirectoryPaths:     []string{"../../deploy/crds", "testdata"},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Printf("starting envtest: %v\n", err)
		os.Exit(1)
	}

	// scheme.Scheme already carries corev1, karpenter.sh and both provider
	// API groups (registered by package inits); apiextensions is only needed
	// here, to assert on the CRDs themselves.
	if err := apiextensionsv1.AddToScheme(scheme.Scheme); err != nil {
		fmt.Printf("registering apiextensions: %v\n", err)
		os.Exit(1)
	}

	mgr, err := controllerruntime.NewManager(cfg, controllerruntime.Options{
		Scheme:                 scheme.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		fmt.Printf("building manager: %v\n", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := nodeclass.NewController(mgr.GetClient()).Register(ctx, mgr); err != nil {
		fmt.Printf("registering nodeclass controller: %v\n", err)
		os.Exit(1)
	}
	if err := providerid.NewController(mgr.GetClient()).Register(ctx, mgr); err != nil {
		fmt.Printf("registering providerid controller: %v\n", err)
		os.Exit(1)
	}
	go func() {
		if err := mgr.Start(ctx); err != nil {
			fmt.Printf("manager exited: %v\n", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		fmt.Println("cache never synced")
		os.Exit(1)
	}
	kubeClient = mgr.GetClient()

	code := m.Run()
	cancel()
	_ = env.Stop()
	os.Exit(code)
}

// eventually polls cond every 100ms until it returns nil or the timeout
// elapses, then fails the test with the last error.
func eventually(t *testing.T, timeout time.Duration, cond func(ctx context.Context) error) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		last = cond(ctx)
		return last == nil, nil
	})
	if err != nil {
		t.Fatalf("condition not met within %s: %v", timeout, last)
	}
}

// TestVendoredCRDsEstablished proves every CRD this repo ships installs and
// serves on the targeted apiserver version — the failure mode a karpenter
// bump or controller-gen change would otherwise only reveal on a live
// cluster.
func TestVendoredCRDsEstablished(t *testing.T) {
	for _, name := range []string{
		"clevernodeclasses.karpenter.clever-cloud.com",
		"nodeclaims.karpenter.sh",
		"nodepools.karpenter.sh",
		"nodeoverlays.karpenter.sh",
	} {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, crd); err != nil {
			t.Fatalf("getting CRD %s: %v", name, err)
		}
		established := false
		for _, cond := range crd.Status.Conditions {
			if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
				established = true
			}
		}
		if !established {
			t.Errorf("CRD %s is not Established", name)
		}
	}
}

// TestNodeClassLifecycle walks a CleverNodeClass through the full loop
// against a real apiserver: readiness conditions (including the NodeGroup
// API probe hitting real discovery), the termination finalizer, and deletion
// blocked while a NodeClaim references the class.
func TestNodeClassLifecycle(t *testing.T) {
	ctx := context.Background()
	nodeClass := &v1alpha1.CleverNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "lifecycle"}}
	if err := kubeClient.Create(ctx, nodeClass); err != nil {
		t.Fatalf("creating nodeclass: %v", err)
	}

	eventually(t, 10*time.Second, func(ctx context.Context) error {
		nc := &v1alpha1.CleverNodeClass{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Name: "lifecycle"}, nc); err != nil {
			return err
		}
		if !nc.StatusConditions().IsTrue(v1alpha1.ConditionTypeValidationSucceeded, v1alpha1.ConditionTypeNodeGroupAPIServed) {
			return fmt.Errorf("conditions not true yet: %+v", nc.Status.Conditions)
		}
		if len(nc.Finalizers) == 0 {
			return fmt.Errorf("termination finalizer not stamped yet")
		}
		return nil
	})

	nodeClaim := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "lifecycle-claim"},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{
				Group: "karpenter.clever-cloud.com",
				Kind:  "CleverNodeClass",
				Name:  "lifecycle",
			},
			Requirements: []karpv1.NodeSelectorRequirementWithMinValues{{
				Key:      corev1.LabelInstanceTypeStable,
				Operator: corev1.NodeSelectorOpExists,
			}},
		},
	}
	if err := kubeClient.Create(ctx, nodeClaim); err != nil {
		t.Fatalf("creating nodeclaim (does the vendored CRD accept the Go types?): %v", err)
	}
	// The finalize path lists NodeClaims through the manager's cache: only
	// delete the NodeClass once the cache demonstrably contains the claim,
	// otherwise the blocking assertion races the informer.
	eventually(t, 10*time.Second, func(ctx context.Context) error {
		return kubeClient.Get(ctx, types.NamespacedName{Name: "lifecycle-claim"}, &karpv1.NodeClaim{})
	})

	if err := kubeClient.Delete(ctx, nodeClass); err != nil {
		t.Fatalf("deleting nodeclass: %v", err)
	}
	// The finalizer must hold the object while the claim references it.
	eventually(t, 10*time.Second, func(ctx context.Context) error {
		nc := &v1alpha1.CleverNodeClass{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Name: "lifecycle"}, nc); err != nil {
			return fmt.Errorf("nodeclass disappeared while a nodeclaim still references it: %w", err)
		}
		if nc.DeletionTimestamp.IsZero() {
			return fmt.Errorf("deletion timestamp not set yet")
		}
		return nil
	})

	if err := kubeClient.Delete(ctx, nodeClaim); err != nil {
		t.Fatalf("deleting nodeclaim: %v", err)
	}
	// The finalize path requeues on a 30s cadence; nudge a reconcile on
	// every poll iteration (a single nudge could itself race the informer
	// still holding the claim) so the 15s bound stays honest.
	eventually(t, 15*time.Second, func(ctx context.Context) error {
		nc := &v1alpha1.CleverNodeClass{}
		err := kubeClient.Get(ctx, types.NamespacedName{Name: "lifecycle"}, nc)
		if err != nil {
			return client.IgnoreNotFound(err)
		}
		stored := nc.DeepCopy()
		nc.Annotations = map[string]string{"envtest/nudge": time.Now().Format(time.RFC3339Nano)}
		if err := kubeClient.Patch(ctx, nc, client.MergeFrom(stored)); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return err
		}
		return fmt.Errorf("nodeclass still present after its last nodeclaim was deleted")
	})
}

// TestNodeClassValidation pins the two validation layers where a real
// apiserver splits them: reserved-prefix label keys are rejected at admission
// by the CRD's CEL rule (the fake client never sees this), while everything
// CEL does not cover — here an over-long label value — must land on the
// controller as ValidationSucceeded=False instead of failing at node launch.
func TestNodeClassValidation(t *testing.T) {
	ctx := context.Background()

	rejected := &v1alpha1.CleverNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "reserved-labels"},
		Spec:       v1alpha1.CleverNodeClassSpec{Labels: map[string]string{"kubernetes.io/role": "worker"}},
	}
	if err := kubeClient.Create(ctx, rejected); err == nil {
		_ = kubeClient.Delete(ctx, rejected)
		t.Fatalf("a reserved-prefix label must be rejected at admission by the CRD CEL rule")
	} else if !apierrors.IsInvalid(err) {
		t.Fatalf("expected an Invalid admission error, got: %v", err)
	}

	notReady := &v1alpha1.CleverNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "long-label-value"},
		Spec:       v1alpha1.CleverNodeClassSpec{Labels: map[string]string{"team": strings.Repeat("x", 64)}},
	}
	if err := kubeClient.Create(ctx, notReady); err != nil {
		t.Fatalf("creating nodeclass: %v", err)
	}
	t.Cleanup(func() { _ = kubeClient.Delete(ctx, notReady) })

	eventually(t, 10*time.Second, func(ctx context.Context) error {
		nc := &v1alpha1.CleverNodeClass{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Name: "long-label-value"}, nc); err != nil {
			return err
		}
		cond := nc.StatusConditions().Get(v1alpha1.ConditionTypeValidationSucceeded)
		if cond == nil || cond.Status != metav1.ConditionFalse {
			return fmt.Errorf("ValidationSucceeded is not False yet: %+v", nc.Status.Conditions)
		}
		if nc.StatusConditions().Root().Status == metav1.ConditionTrue {
			return fmt.Errorf("Ready must not be True with a failed validation")
		}
		return nil
	})
}

// TestProviderIDStamping proves the providerid controller stamps managed
// worker nodes through a real watch-driven reconcile, and leaves nodes of
// unmanaged NodeGroups alone.
func TestProviderIDStamping(t *testing.T) {
	ctx := context.Background()

	managed := &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{
		Name:   "env-managed",
		Labels: map[string]string{v1alpha1.ManagedLabelKey: "true"},
	}}
	unmanaged := &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: "env-unmanaged"}}
	for _, ng := range []*ngv1.NodeGroup{managed, unmanaged} {
		if err := kubeClient.Create(ctx, ng); err != nil {
			t.Fatalf("creating nodegroup %s: %v", ng.Name, err)
		}
		t.Cleanup(func() { _ = kubeClient.Delete(ctx, ng) })
	}

	managedNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "env-managed-node0",
		Labels: map[string]string{v1alpha1.NodeGroupNodeLabelKey: "env-managed"},
	}}
	unmanagedNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "env-unmanaged-node0",
		Labels: map[string]string{v1alpha1.NodeGroupNodeLabelKey: "env-unmanaged"},
	}}
	for _, n := range []*corev1.Node{managedNode, unmanagedNode} {
		if err := kubeClient.Create(ctx, n); err != nil {
			t.Fatalf("creating node %s: %v", n.Name, err)
		}
		t.Cleanup(func() { _ = kubeClient.Delete(ctx, n) })
	}

	eventually(t, 10*time.Second, func(ctx context.Context) error {
		node := &corev1.Node{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Name: "env-managed-node0"}, node); err != nil {
			return err
		}
		if node.Spec.ProviderID != "clevercloud://env-managed" {
			return fmt.Errorf("providerID = %q, want clevercloud://env-managed", node.Spec.ProviderID)
		}
		return nil
	})
	// By the time the managed node is stamped, the unmanaged one has been
	// through the same watch; it must have stayed untouched.
	node := &corev1.Node{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: "env-unmanaged-node0"}, node); err != nil {
		t.Fatalf("getting unmanaged node: %v", err)
	}
	if node.Spec.ProviderID != "" {
		t.Errorf("unmanaged node providerID = %q, want empty", node.Spec.ProviderID)
	}
}
