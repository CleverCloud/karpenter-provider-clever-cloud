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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis"
	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// These tests send what nodegroup.Provider.Create builds to an apiserver
// serving Clever Cloud's own NodeGroup CRD (testdata/nodegroup-crd.yaml,
// captured from a CKE cluster). The unit tests' fake client validates nothing,
// so a payload the platform refuses — a flavor outside the enum, a label key
// with a reserved prefix, a value or taint its patterns reject — used to show
// up on the first live launch, and a field the schema does not declare would
// be pruned without any error at all.

// sentRecorder is a client that keeps a copy of every NodeGroup it creates as
// it was sent, before the apiserver's response overwrites the object, so that
// what the apiserver stored can be compared with what the provider built.
type sentRecorder struct {
	client.Client
	mu   sync.Mutex
	sent map[string]*ngv1.NodeGroup
}

func (c *sentRecorder) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if ng, ok := obj.(*ngv1.NodeGroup); ok {
		c.mu.Lock()
		c.sent[ng.Name] = ng.DeepCopy()
		c.mu.Unlock()
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *sentRecorder) sentNodeGroup(name string) *ngv1.NodeGroup {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sent[name]
}

// acknowledgeOnceCreated plays the Clever Cloud operator: once the NodeGroup
// exists on the apiserver, it writes the operator's first status on a group it
// accepted (phase Creating, ReconcileInProgress=True), through the status
// subresource and therefore the live status schema, which requires
// lastTransitionTime on every condition. Create's acceptance poll returns on
// it, reading through the manager's cache — which, right after the create,
// may not hold the group yet.
func acknowledgeOnceCreated(t *testing.T, name string) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			ng := &ngv1.NodeGroup{}
			if err := apiReader.Get(context.Background(), types.NamespacedName{Name: name}, ng); err != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			ng.Status = ngv1.NodeGroupStatus{
				Phase: "Creating",
				Conditions: []ngv1.NodeGroupCondition{{
					Type: ngv1.ConditionTypeReconcileInProgress, Status: corev1.ConditionTrue, Reason: "Creating",
					LastTransitionTime: metav1.Now(),
				}},
			}
			if err := kubeClient.Status().Update(context.Background(), ng); err != nil {
				t.Errorf("acknowledging nodegroup %s: %v", name, err)
			}
			return
		}
		t.Errorf("nodegroup %s was never created", name)
	}()
	return done
}

// TestNodeGroupPayloadIsAdmittedByTheLiveSchema creates, for every built-in
// flavor, the NodeGroup nodegroup.Provider.Create builds for a NodeClaim
// labelled the way karpenter-core labels one at creation: its NodePool and
// NodeClass, its NodePool template's labels — node.kubernetes.io/ is a domain
// karpenter admits there and the NodeGroup CRD refuses, so the provider's
// label filter must drop it — and the NodeClass's own labels. The apiserver
// must admit it and store it unchanged: an equal spec is what proves nothing
// was pruned or defaulted away.
func TestNodeGroupPayloadIsAdmittedByTheLiveSchema(t *testing.T) {
	ctx := context.Background()
	sent := &sentRecorder{Client: kubeClient, sent: map[string]*ngv1.NodeGroup{}}
	provider := nodegroup.NewProvider(sent, discardRecorder{}, instancetype.NewProvider("par", nil, nil), clock.RealClock{})
	nodeClass := &v1alpha1.CleverNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "live-schema"},
		Spec:       v1alpha1.CleverNodeClassSpec{Labels: map[string]string{"env": "prod", "example.com/cost-center": "4242"}},
	}
	nodeClassLabel := karpv1.NodeClassLabelKey(schema.GroupKind{Group: apis.Group, Kind: "CleverNodeClass"})

	for _, flavor := range instancetype.DefaultFlavors {
		name := "live-schema-" + strings.ToLower(flavor.Name)
		t.Run(flavor.Name, func(t *testing.T) {
			nodeClaim := &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
				Name: name,
				UID:  types.UID("uid-" + name),
				Labels: map[string]string{
					karpv1.NodePoolLabelKey:          "general-purpose",
					nodeClassLabel:                   nodeClass.Name,
					corev1.LabelNodeExcludeBalancers: "true",
					"team":                           "data",
				},
			}}
			t.Cleanup(func() {
				_ = kubeClient.Delete(ctx, &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: name}})
			})

			done := acknowledgeOnceCreated(t, name)
			_, err := provider.Create(ctx, nodeClaim, nodeClass, flavor.Name)
			<-done
			if err != nil {
				t.Fatalf("the live NodeGroup schema refused the payload for %s: %v", flavor.Name, err)
			}
			stored := &ngv1.NodeGroup{}
			if err := apiReader.Get(ctx, types.NamespacedName{Name: name}, stored); err != nil {
				t.Fatalf("getting the stored nodegroup: %v", err)
			}
			payload := sent.sentNodeGroup(name)
			if payload == nil {
				t.Fatal("the provider never sent the nodegroup")
			}
			if !equality.Semantic.DeepEqual(stored.Spec, payload.Spec) {
				t.Errorf("the apiserver stored spec %+v, the provider sent %+v: the schema pruned or defaulted part of the payload", stored.Spec, payload.Spec)
			}
			if !equality.Semantic.DeepEqual(stored.Labels, payload.Labels) ||
				!equality.Semantic.DeepEqual(stored.Annotations, payload.Annotations) ||
				!equality.Semantic.DeepEqual(stored.OwnerReferences, payload.OwnerReferences) {
				t.Errorf("the apiserver stored metadata labels %v, annotations %v, owners %+v; the provider sent %v, %v, %+v",
					stored.Labels, stored.Annotations, stored.OwnerReferences, payload.Labels, payload.Annotations, payload.OwnerReferences)
			}
			// The payload must carry what the schema checks, or the comparison
			// above proves nothing.
			if stored.Spec.Flavor != flavor.Name || stored.Spec.NodeCount != 1 {
				t.Errorf("stored flavor %q and nodeCount %d, want %q and 1", stored.Spec.Flavor, stored.Spec.NodeCount, flavor.Name)
			}
			if len(stored.Spec.Taints) != 1 || stored.Spec.Taints[0].Key != karpv1.UnregisteredTaintKey || stored.Spec.Taints[0].Effect != corev1.TaintEffectNoExecute {
				t.Errorf("stored taints %+v, want the %s:NoExecute taint", stored.Spec.Taints, karpv1.UnregisteredTaintKey)
			}
			for _, key := range []string{"env", "example.com/cost-center", "team", nodeClassLabel} {
				if _, ok := stored.Spec.Labels[key]; !ok {
					t.Errorf("stored spec.labels %v lack %s", stored.Spec.Labels, key)
				}
			}
		})
	}
}

// TestNodeGroupFlavorOutsideTheLiveEnumIsRefusedAtAdmission creates a
// NodeGroup whose flavor the live CRD's enum does not carry: 2XL, a name of
// the right shape that a settings.flavors override may declare before Clever
// Cloud's CRD carries it. nodegroup.Create must turn the apiserver's own
// Invalid error — the unit tests can only imitate its shape, including the
// field-less cause the apiserver adds when it skips the CEL rules — into the
// typed refusal the cloud provider maps to an InsufficientCapacityError, and
// hold that flavor out so karpenter re-plans onto another one. As a plain
// error, core retried the claim for 5 minutes, then re-planned onto the same
// flavor, forever.
func TestNodeGroupFlavorOutsideTheLiveEnumIsRefusedAtAdmission(t *testing.T) {
	ctx := context.Background()
	provider := nodegroup.NewProvider(kubeClient, discardRecorder{}, instancetype.NewProvider("par", nil, nil), clock.RealClock{})
	const name = "live-schema-2xl"
	nodeClaim := &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		UID:    types.UID("uid-" + name),
		Labels: map[string]string{karpv1.NodePoolLabelKey: "general-purpose", "team": "data"},
	}}
	nodeClass := &v1alpha1.CleverNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "live-schema"}}

	_, err := provider.Create(ctx, nodeClaim, nodeClass, "2XL")
	var rejected *nodegroup.ErrFlavorRejected
	if !errors.As(err, &rejected) {
		t.Fatalf("expected the live enum's refusal as *nodegroup.ErrFlavorRejected, got %T: %v", err, err)
	}
	if rejected.Flavor != "2XL" || rejected.Reason != string(metav1.StatusReasonInvalid) || !strings.Contains(rejected.Message, `"2XL"`) {
		t.Errorf("refusal %+v does not carry the flavor, reason Invalid and the apiserver's message", rejected)
	}
	if err := apiReader.Get(ctx, types.NamespacedName{Name: name}, &ngv1.NodeGroup{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected no nodegroup to exist after an admission refusal, got %v", err)
	}
	if !provider.Unavailable("2XL") {
		t.Error("expected 2XL to be held out after the admission refusal")
	}
	if provider.Unavailable("XL") {
		t.Error("an admission refusal of 2XL must hold out no other flavor")
	}
}
