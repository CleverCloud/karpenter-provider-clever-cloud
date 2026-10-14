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

package ephemeralstorage_test

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/ephemeralstorage"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

const readyMetric = "karpenter_clevercloud_instancetype_host_ephemeral_storage_ready"

func host(capacity, allocatable string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "controller-host", UID: "first", Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}},
		Status: corev1.NodeStatus{
			Capacity:    corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse(capacity)},
			Allocatable: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse(allocatable)},
		},
	}
}

func newController(objects ...client.Object) (*ephemeralstorage.Controller, *instancetype.Provider, client.Client) {
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithStatusSubresource(&corev1.Node{}).WithObjects(objects...).Build()
	provider := instancetype.NewProvider("par", nil, nil)
	return ephemeralstorage.NewController(kubeClient, provider, "controller-host"), provider, kubeClient
}

func assertDisk(t *testing.T, provider *instancetype.Provider, capacity, allocatable string) {
	t.Helper()
	for _, it := range provider.List() {
		if got := it.Capacity[corev1.ResourceEphemeralStorage]; got.Cmp(resource.MustParse(capacity)) != 0 {
			t.Fatalf("%s capacity = %s, want %s", it.Name, got.String(), capacity)
		}
		if got := it.Allocatable()[corev1.ResourceEphemeralStorage]; got.Cmp(resource.MustParse(allocatable)) != 0 {
			t.Fatalf("%s allocatable = %s, want %s", it.Name, got.String(), allocatable)
		}
	}
}

func TestHostReferenceUsesOnlyNamedNodeAndTracksBothDirections(t *testing.T) {
	ctx := context.Background()
	node := host("40Gi", "36Gi")
	other := host("500Gi", "450Gi")
	other.Name = "unrelated"
	ctrl, provider, kubeClient := newController(node, other)
	if provider.EphemeralStorageReady() {
		t.Fatal("must await first observation")
	}
	if _, err := ctrl.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: other.Name}}); err != nil {
		t.Fatal(err)
	}
	if provider.EphemeralStorageReady() {
		t.Fatal("an unrelated node enabled provisioning")
	}
	// The singleton request also initializes a reference that is not a managed
	// worker and has no CPU/memory report: only its disk is copied.
	if _, err := ctrl.Reconcile(ctx, reconcile.Request{}); err != nil {
		t.Fatal(err)
	}
	assertDisk(t, provider, "40Gi", "36Gi")
	if !provider.EphemeralStorageReady() || metricstest.Value(t, readyMetric) != 1 {
		t.Fatal("valid reference did not enable provisioning")
	}
	for _, disk := range []struct{ capacity, allocatable string }{{"200Gi", "180Gi"}, {"20Gi", "17Gi"}} {
		node.Status = host(disk.capacity, disk.allocatable).Status
		if err := kubeClient.Status().Update(ctx, node); err != nil {
			t.Fatal(err)
		}
		if _, err := ctrl.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: node.Name}}); err != nil {
			t.Fatal(err)
		}
		assertDisk(t, provider, disk.capacity, disk.allocatable)
	}
}

func TestInvalidReferenceBlocksProvisioningUntilRecovery(t *testing.T) {
	for _, failure := range []string{"missing", "missing-capacity", "missing-allocatable", "allocatable-exceeds-capacity", "terminating"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			node := host("160Gi", "144Gi")
			ctrl, provider, kubeClient := newController(node)
			if _, err := ctrl.Reconcile(ctx, reconcile.Request{}); err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "missing":
				if err := kubeClient.Delete(ctx, node); err != nil {
					t.Fatal(err)
				}
			case "terminating":
				node.Finalizers = []string{"test/hold"}
				if err := kubeClient.Update(ctx, node); err != nil {
					t.Fatal(err)
				}
				if err := kubeClient.Delete(ctx, node); err != nil {
					t.Fatal(err)
				}
			default:
				switch failure {
				case "missing-capacity":
					delete(node.Status.Capacity, corev1.ResourceEphemeralStorage)
				case "missing-allocatable":
					delete(node.Status.Allocatable, corev1.ResourceEphemeralStorage)
				case "allocatable-exceeds-capacity":
					node.Status.Allocatable[corev1.ResourceEphemeralStorage] = resource.MustParse("200Gi")
				}
				if err := kubeClient.Status().Update(ctx, node); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := ctrl.Reconcile(ctx, reconcile.Request{}); err == nil {
				t.Fatal("invalid reference was accepted")
			}
			if provider.EphemeralStorageReady() || metricstest.Value(t, readyMetric) != 0 {
				t.Fatal("invalid reference still allows provisioning")
			}
			// Existing instances can still be described with the last snapshot.
			assertDisk(t, provider, "160Gi", "144Gi")
			if failure == "terminating" {
				return
			}
			if failure == "missing" {
				node = host("160Gi", "144Gi")
				node.UID = "replacement"
				if err := kubeClient.Create(ctx, node); err != nil {
					t.Fatal(err)
				}
			} else {
				node.Status = host("160Gi", "144Gi").Status
				if err := kubeClient.Status().Update(ctx, node); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := ctrl.Reconcile(ctx, reconcile.Request{}); err != nil {
				t.Fatal(err)
			}
			if !provider.EphemeralStorageReady() || metricstest.Value(t, readyMetric) != 1 {
				t.Fatal("valid unchanged quantities did not restore readiness")
			}
		})
	}
}

type failingReader struct {
	client.Reader
	err error
}

func (r *failingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if r.err != nil {
		return r.err
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestAPIFailurePreservesLastObservationButCannotInitialize(t *testing.T) {
	ctx := context.Background()
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(host("80Gi", "72Gi")).Build()
	reader := &failingReader{Reader: kubeClient, err: errors.New("API unavailable")}
	provider := instancetype.NewProvider("par", nil, nil)
	ctrl := ephemeralstorage.NewController(reader, provider, "controller-host")
	if _, err := ctrl.Reconcile(ctx, reconcile.Request{}); err == nil {
		t.Fatal("expected API error")
	}
	if provider.EphemeralStorageReady() {
		t.Fatal("API error cannot enable static fallback")
	}
	reader.err = nil
	if _, err := ctrl.Reconcile(ctx, reconcile.Request{}); err != nil {
		t.Fatal(err)
	}
	reader.err = errors.New("API unavailable")
	if _, err := ctrl.Reconcile(ctx, reconcile.Request{}); err == nil {
		t.Fatal("expected API error")
	}
	if !provider.EphemeralStorageReady() {
		t.Fatal("transient API error discarded the valid reference")
	}
	assertDisk(t, provider, "80Gi", "72Gi")
}
