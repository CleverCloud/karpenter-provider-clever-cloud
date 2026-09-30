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

package cloudprovider_test

// These tests pin that a launch decides what it creates under the nodegroup
// provider's creation lock, not before waiting for it. Launches queue on that
// lock, and a launch that checked its NodeClass, its NodeClaim and its flavor
// before the wait acted on what it saw then: it POSTed the flavor the launch
// ahead had just seen refused, launched from a NodeClass deleted meanwhile, and
// created a billed machine for a NodeClaim deleted meanwhile. What is decided
// there is read from the API server, not the informer cache, which could still
// miss a change made moments before the lock was acquired.

import (
	"context"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// createGate holds the first NodeGroup create inside the API call until
// release, its launch holding the creation lock the way a launch holds it
// while it waits for the operator's decision, so that the launches started
// meanwhile queue behind it.
type createGate struct {
	held, released        chan struct{}
	holdOnce, releaseOnce sync.Once
}

func (g *createGate) hold() {
	first := false
	g.holdOnce.Do(func() { first = true })
	if first {
		close(g.held)
		<-g.released
	}
}

func (g *createGate) release() {
	g.releaseOnce.Do(func() { close(g.released) })
}

// newGatedProvider builds the provider over a fake client whose NodeGroups the
// fake operator decides on at creation, the first of them held by the
// returned gate.
func newGatedProvider(t *testing.T, operator *fakeOperator, objs ...client.Object) (*cloudprovider.CloudProvider, client.WithWatch, *createGate) {
	t.Helper()
	gate := &createGate{held: make(chan struct{}), released: make(chan struct{})}
	// A failing test must not leave the held launch blocked.
	t.Cleanup(gate.release)
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				err := operator.create(ctx, c, obj, opts...)
				if _, ok := obj.(*ngv1.NodeGroup); ok && err == nil {
					gate.hold()
				}
				return err
			},
		}).
		Build()
	itp := instancetype.NewProvider("par", nil, nil)
	return cloudprovider.New(kubeClient, kubeClient, itp, nodegroup.NewProvider(kubeClient, noopRecorder{}, itp, clock.RealClock{})), kubeClient, gate
}

// launched is the outcome of a Create.
type launched struct {
	nodeClaim *karpv1.NodeClaim
	err       error
}

// queueBehind launches first and, once its NodeGroup create is held, launches
// each of queued and waits until every one of them waits for the creation
// lock. The returned function lets the held create through and returns the
// outcome of every launch, first's included, in order.
func queueBehind(t *testing.T, cp *cloudprovider.CloudProvider, gate *createGate, first *karpv1.NodeClaim, queued ...*karpv1.NodeClaim) func() []launched {
	t.Helper()
	claims := append([]*karpv1.NodeClaim{first}, queued...)
	results := make([]launched, len(claims))
	var wg sync.WaitGroup
	launch := func(i int) {
		wg.Go(func() {
			results[i].nodeClaim, results[i].err = cp.Create(context.Background(), claims[i])
		})
	}
	launch(0)
	select {
	case <-gate.held:
	case <-time.After(10 * time.Second):
		t.Fatalf("the launch of %s never created its NodeGroup", first.Name)
	}
	for i := range queued {
		launch(i + 1)
	}
	waitQueuedOnCreateLock(t, len(queued))
	return func() []launched {
		gate.release()
		wg.Wait()
		return results
	}
}

// waitQueuedOnCreateLock waits until n goroutines wait for the nodegroup
// provider's creation lock. Nothing a queued launch does is observable before
// it gets the lock — which is the point — so only a goroutine dump tells a
// queued launch from one that has not started yet; without it, these tests
// would also pass against launches that check before the wait.
func waitQueuedOnCreateLock(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		buf := make([]byte, 1<<22)
		buf = buf[:runtime.Stack(buf, true)]
		queued := 0
		for _, goroutine := range strings.Split(string(buf), "\n\n") {
			if waitsForCreateLock(goroutine) {
				queued++
			}
		}
		if queued == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d launch(es) wait for the creation lock, want %d", queued, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitsForCreateLock reports whether a goroutine of a dump is blocked locking
// a mutex in nodegroup.(*Provider).ResolveAndCreate itself: createMu, not the
// refusal state a resolver reads once it holds createMu.
func waitsForCreateLock(goroutine string) bool {
	if !strings.Contains(goroutine, "[sync.Mutex.Lock") {
		return false
	}
	// Function lines alternate with their file:line lines.
	lines := strings.Split(goroutine, "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "sync.(*Mutex).Lock(") && i+2 < len(lines) {
			return strings.Contains(lines[i+2], "/nodegroup.(*Provider).ResolveAndCreate(")
		}
	}
	return false
}

// requireNoNodeGroup fails the test if a NodeGroup was created for name.
func requireNoNodeGroup(t *testing.T, kubeClient client.Client, name string) {
	t.Helper()
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, &ngv1.NodeGroup{}); err == nil {
		t.Errorf("a NodeGroup was created for %s", name)
	}
}

// TestQueuedLaunchResolvesAfterTheRefusalAhead queues two launches behind one
// whose flavor, 2XS, the operator refuses. They had resolved 2XS, the cheapest
// flavor, before the wait, and POSTed it again once they got the lock: one
// more create, refuse and delete upstream each. Resolved under the lock, the
// one that allows XS launches an XS, described as one, and the one only 2XS
// serves fails fast without reaching the API.
func TestQueuedLaunchResolvesAfterTheRefusalAhead(t *testing.T) {
	operator := &fakeOperator{headroomGB: 100, refused: map[string]bool{"2XS": true}}
	cp, kubeClient, gate := newGatedProvider(t, operator, readyNodeClass("default"))
	ahead := stored(t, kubeClient, claimFor("default-ahead", "1Gi", "2XS", "XS"))
	relaxed := stored(t, kubeClient, claimFor("default-relax", "1Gi", "2XS", "XS"))
	only2XS := stored(t, kubeClient, claimFor("default-only1", "1Gi", "2XS"))

	results := queueBehind(t, cp, gate, ahead, relaxed, only2XS)()
	if !corecloudprovider.IsInsufficientCapacityError(results[0].err) {
		t.Fatalf("the launch ahead: want the refusal of 2XS as an InsufficientCapacityError, got %T: %v", results[0].err, results[0].err)
	}
	if got := operator.createdFlavors(); !slices.Equal(got, []string{"2XS", "XS"}) {
		t.Errorf("NodeGroups created upstream: %v, want [2XS XS]: the refused 2XS reached the API again", got)
	}

	if err := results[1].err; err != nil {
		t.Fatalf("the queued launch that allows XS: %v", err)
	}
	created := results[1].nodeClaim
	if got := created.Labels[corev1.LabelInstanceTypeStable]; got != "XS" {
		t.Errorf("the queued launch is described as %q, want the XS it created", got)
	}
	want, err := instancetype.NewProvider("par", nil, nil).Get("XS")
	if err != nil {
		t.Fatalf("Get(XS): %v", err)
	}
	for name, wantQty := range want.Capacity {
		if gotQty := created.Status.Capacity[name]; gotQty.Cmp(wantQty) != 0 {
			t.Errorf("capacity[%s] = %v, want XS's %v", name, gotQty.String(), wantQty.String())
		}
	}

	if err := results[2].err; !corecloudprovider.IsInsufficientCapacityError(err) || !strings.Contains(err.Error(), "currently unavailable") {
		t.Errorf("the queued launch only 2XS serves: want an InsufficientCapacityError naming the unavailable flavor, got %T: %v", err, err)
	}
	requireNoNodeGroup(t, kubeClient, only2XS.Name)
}

// TestQueuedLaunchFromANodeClassDeletedOrNotReadyMeanwhile deletes the
// NodeClass, or makes it not Ready, while a launch from it waits for the
// creation lock. Checked before the wait, the launch went ahead: from a
// deleted NodeClass, the nodeclass finalizer then waited on a machine created
// after the deletion.
func TestQueuedLaunchFromANodeClassDeletedOrNotReadyMeanwhile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(t *testing.T, kubeClient client.Client, nodeClass *v1alpha1.CleverNodeClass)
	}{
		{name: "deleted", change: func(t *testing.T, kubeClient client.Client, nodeClass *v1alpha1.CleverNodeClass) {
			if err := kubeClient.Delete(context.Background(), nodeClass); err != nil {
				t.Fatalf("deleting nodeclass: %v", err)
			}
		}},
		{name: "no longer ready", change: func(t *testing.T, kubeClient client.Client, nodeClass *v1alpha1.CleverNodeClass) {
			live := &v1alpha1.CleverNodeClass{}
			if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(nodeClass), live); err != nil {
				t.Fatal(err)
			}
			live.StatusConditions().SetFalse(v1alpha1.ConditionTypeValidationSucceeded, "ValidationFailed", "invalid spec")
			if err := kubeClient.Status().Update(context.Background(), live); err != nil {
				t.Fatalf("making nodeclass not ready: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeClass := readyNodeClass("default")
			// Kept, with a deletion timestamp, while NodeClaims still use it.
			nodeClass.Finalizers = []string{v1alpha1.TerminationFinalizer}
			operator := &fakeOperator{headroomGB: 100}
			cp, kubeClient, gate := newGatedProvider(t, operator, nodeClass)
			ahead := stored(t, kubeClient, testNodeClaim("default-ahead"))
			queued := stored(t, kubeClient, testNodeClaim("default-queue"))

			release := queueBehind(t, cp, gate, ahead, queued)
			tc.change(t, kubeClient, nodeClass)
			results := release()
			if err := results[0].err; err != nil {
				t.Fatalf("the launch ahead, which got the lock before the change: %v", err)
			}
			if err := results[1].err; !corecloudprovider.IsNodeClassNotReadyError(err) {
				t.Errorf("the queued launch: want a NodeClassNotReadyError, got %T: %v", err, err)
			}
			if got := operator.createCount(); got != 1 {
				t.Errorf("NodeGroups created upstream: %d, want only the launch ahead's", got)
			}
			requireNoNodeGroup(t, kubeClient, queued.Name)
		})
	}
}

// TestQueuedLaunchBuildsFromTheNodeClassAsItIs edits the NodeClass while a
// launch from it waits for the creation lock. The NodeGroup is built from the
// NodeClass as it is when the lock is acquired: built from the copy read
// before the wait, it carried the old labels and a hash stamp of the old spec,
// so the machine was reported drifted as soon as it launched.
func TestQueuedLaunchBuildsFromTheNodeClassAsItIs(t *testing.T) {
	nodeClass := readyNodeClass("default")
	nodeClass.Spec.Labels = map[string]string{"team": "data"}
	operator := &fakeOperator{headroomGB: 100}
	cp, kubeClient, gate := newGatedProvider(t, operator, nodeClass)
	ahead := stored(t, kubeClient, testNodeClaim("default-ahead"))
	queued := stored(t, kubeClient, testNodeClaim("default-queue"))

	release := queueBehind(t, cp, gate, ahead, queued)
	edited := &v1alpha1.CleverNodeClass{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: nodeClass.Name}, edited); err != nil {
		t.Fatal(err)
	}
	edited.Spec.Labels = map[string]string{"team": "ml"}
	if err := kubeClient.Update(context.Background(), edited); err != nil {
		t.Fatalf("editing nodeclass: %v", err)
	}
	for i, result := range release() {
		if result.err != nil {
			t.Fatalf("launch %d: %v", i, result.err)
		}
	}
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: queued.Name}, ng); err != nil {
		t.Fatalf("getting the queued launch's nodegroup: %v", err)
	}
	if got := ng.Spec.Labels["team"]; got != "ml" {
		t.Errorf("the queued launch's NodeGroup carries team=%q, want the edited ml", got)
	}
	if got := ng.Annotations[v1alpha1.NodeClassHashLabelKey]; got != edited.Hash() {
		t.Errorf("the queued launch's NodeGroup is stamped %q, want the edited spec's hash %q", got, edited.Hash())
	}
}

// TestQueuedLaunchOfANodeClaimDeletedMeanwhile deletes the NodeClaim while its
// launch waits for the creation lock. karpenter-core only branches to finalize
// at the start of a reconcile, so the launch it was already running created a
// billed machine for a claim on its way out. The launch must fail without
// reaching the API, and with a plain error: core requeues the claim into its
// finalize path, instead of counting a capacity or nodeclass disruption for a
// claim that is going away anyway.
func TestQueuedLaunchOfANodeClaimDeletedMeanwhile(t *testing.T) {
	for _, tc := range []struct {
		name string
		// finalizers of the queued claim: with one, its deletion leaves it
		// terminating; without, it is gone.
		finalizers []string
		// replaced creates another NodeClaim of the same name once it is gone.
		replaced bool
	}{
		{name: "terminating", finalizers: []string{karpv1.TerminationFinalizer}},
		{name: "gone"},
		{name: "replaced by a claim of the same name", replaced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operator := &fakeOperator{headroomGB: 100}
			cp, kubeClient, gate := newGatedProvider(t, operator, readyNodeClass("default"))
			ahead := stored(t, kubeClient, testNodeClaim("default-ahead"))
			queued := testNodeClaim("default-queue")
			queued.Finalizers = tc.finalizers
			stored(t, kubeClient, queued)

			release := queueBehind(t, cp, gate, ahead, queued)
			if err := kubeClient.Delete(context.Background(), queued.DeepCopy()); err != nil {
				t.Fatalf("deleting nodeclaim: %v", err)
			}
			if tc.replaced {
				replacement := testNodeClaim(queued.Name)
				replacement.UID = "uid-replacement"
				stored(t, kubeClient, replacement)
			}
			results := release()
			if err := results[0].err; err != nil {
				t.Fatalf("the launch ahead: %v", err)
			}
			err := results[1].err
			if err == nil {
				t.Fatal("the launch of the deleted NodeClaim succeeded")
			}
			if corecloudprovider.IsInsufficientCapacityError(err) || corecloudprovider.IsNodeClassNotReadyError(err) {
				t.Errorf("want a plain error, got %T: %v", err, err)
			}
			if got := operator.createCount(); got != 1 {
				t.Errorf("NodeGroups created upstream: %d, want only the launch ahead's", got)
			}
			requireNoNodeGroup(t, kubeClient, queued.Name)
		})
	}
}

// TestLaunchPreconditionsAreReadFromTheAPIServer gives the provider an API
// reader apart from its cached client, the informer cache lagging a change
// made moments before the launch got the creation lock. What the launch
// decides must follow the API server: read from the cache, it went ahead for
// a NodeClaim, or from a NodeClass, whose deletion the cache had not seen yet,
// which is the very window deciding under the lock closes.
func TestLaunchPreconditionsAreReadFromTheAPIServer(t *testing.T) {
	const claimName = "default-claim"
	claim := func() client.Object { return testNodeClaim(claimName) }
	terminating := func(obj client.Object, finalizer string) client.Object {
		now := metav1.Now()
		obj.SetDeletionTimestamp(&now)
		obj.SetFinalizers([]string{finalizer})
		return obj
	}
	withLabels := func(team string) *v1alpha1.CleverNodeClass {
		nodeClass := readyNodeClass("default")
		nodeClass.Spec.Labels = map[string]string{"team": team}
		return nodeClass
	}
	notReady := readyNodeClass("default")
	notReady.StatusConditions().SetFalse(v1alpha1.ConditionTypeValidationSucceeded, "ValidationFailed", "invalid spec")

	for _, tc := range []struct {
		name string
		// cached is what the informer cache holds, live what the API server
		// holds.
		cached, live []client.Object
		// want is the launch's outcome: "launched", or the kind of error.
		want string
	}{
		{
			name:   "nodeclaim deleted",
			cached: []client.Object{readyNodeClass("default"), claim()},
			live:   []client.Object{readyNodeClass("default")},
			want:   "plain",
		},
		{
			name:   "nodeclaim being deleted",
			cached: []client.Object{readyNodeClass("default"), claim()},
			live:   []client.Object{readyNodeClass("default"), terminating(claim(), karpv1.TerminationFinalizer)},
			want:   "plain",
		},
		{
			name:   "nodeclass deleted",
			cached: []client.Object{readyNodeClass("default"), claim()},
			live:   []client.Object{claim()},
			want:   "insufficient capacity",
		},
		{
			name:   "nodeclass being deleted",
			cached: []client.Object{readyNodeClass("default"), claim()},
			live:   []client.Object{terminating(readyNodeClass("default"), v1alpha1.TerminationFinalizer), claim()},
			want:   "nodeclass not ready",
		},
		{
			name:   "nodeclass no longer ready",
			cached: []client.Object{readyNodeClass("default"), claim()},
			live:   []client.Object{notReady.DeepCopy(), claim()},
			want:   "nodeclass not ready",
		},
		{
			name:   "nodeclass edited",
			cached: []client.Object{withLabels("data"), claim()},
			live:   []client.Object{withLabels("ml"), claim()},
			want:   "launched",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operator := &fakeOperator{headroomGB: 100}
			build := func(objs []client.Object, funcs interceptor.Funcs) client.WithWatch {
				return fake.NewClientBuilder().
					WithScheme(scheme.Scheme).
					WithObjects(objs...).
					WithStatusSubresource(&v1alpha1.CleverNodeClass{}).
					WithInterceptorFuncs(funcs).
					Build()
			}
			cache := build(tc.cached, interceptor.Funcs{Create: operator.create})
			apiServer := build(tc.live, interceptor.Funcs{})
			itp := instancetype.NewProvider("par", nil, nil)
			cp := cloudprovider.New(cache, apiServer, itp, nodegroup.NewProvider(cache, noopRecorder{}, itp, clock.RealClock{}))

			_, err := cp.Create(context.Background(), testNodeClaim(claimName))
			switch tc.want {
			case "launched":
				if err != nil {
					t.Fatalf("Create: %v", err)
				}
				ng := &ngv1.NodeGroup{}
				if err := cache.Get(context.Background(), types.NamespacedName{Name: claimName}, ng); err != nil {
					t.Fatalf("getting the nodegroup: %v", err)
				}
				if got := ng.Spec.Labels["team"]; got != "ml" {
					t.Errorf("the NodeGroup carries team=%q, want the API server's ml", got)
				}
				if got, want := ng.Annotations[v1alpha1.NodeClassHashLabelKey], withLabels("ml").Hash(); got != want {
					t.Errorf("the NodeGroup is stamped %q, want the API server's spec hash %q", got, want)
				}
				return
			case "plain":
				if err == nil || corecloudprovider.IsInsufficientCapacityError(err) || corecloudprovider.IsNodeClassNotReadyError(err) {
					t.Errorf("want a plain error, got %T: %v", err, err)
				}
			case "insufficient capacity":
				if !corecloudprovider.IsInsufficientCapacityError(err) {
					t.Errorf("want an InsufficientCapacityError, got %T: %v", err, err)
				}
			case "nodeclass not ready":
				if !corecloudprovider.IsNodeClassNotReadyError(err) {
					t.Errorf("want a NodeClassNotReadyError, got %T: %v", err, err)
				}
			}
			if got := operator.createCount(); got != 0 {
				t.Errorf("NodeGroups created upstream: %d, want none", got)
			}
			requireNoNodeGroup(t, cache, claimName)
		})
	}
}
