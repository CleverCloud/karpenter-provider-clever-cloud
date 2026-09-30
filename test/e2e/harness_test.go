//go:build e2e

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

package e2e

// Tests of the harness's own mechanics: port reservation, listener
// ownership, the controller exit watcher, the wait causes and how the
// cleanup treats the run's NodeGroups (over the controller-runtime fake
// client). They need no cluster and no E2E_CONTEXT:
//
//	go test -tags e2e -run '^TestHarness' ./test/e2e/
//
// make e2e runs them with the suite.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
)

func listen(t *testing.T, network, addr string) (net.Listener, int) {
	t.Helper()
	l, err := net.Listen(network, addr)
	if err != nil {
		t.Fatalf("listening on %s %s: %v", network, addr, err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, l.Addr().(*net.TCPAddr).Port
}

func assertListens(t *testing.T, pid, port int, want bool) {
	t.Helper()
	got, err := listensOn(pid, port)
	if err != nil {
		t.Fatalf("listensOn(pid %d, port %d): %v", pid, port, err)
	}
	if got != want {
		t.Errorf("listensOn(pid %d, port %d) = %v, want %v", pid, port, got, want)
	}
}

func TestHarnessListensOn(t *testing.T) {
	self := os.Getpid()

	t.Run("own IPv4 listener", func(t *testing.T) {
		_, port := listen(t, "tcp4", "127.0.0.1:0")
		assertListens(t, self, port, true)
	})

	t.Run("own wildcard listener, as the controller binds", func(t *testing.T) {
		// Dual-stack hosts list it in /proc/net/tcp6.
		_, port := listen(t, "tcp", ":0")
		assertListens(t, self, port, true)
	})

	t.Run("a connection on the port is not a listener", func(t *testing.T) {
		l, port := listen(t, "tcp4", "127.0.0.1:0")
		client, err := net.Dial("tcp4", l.Addr().String())
		if err != nil {
			t.Fatalf("dialing: %v", err)
		}
		defer client.Close()
		server, err := l.Accept()
		if err != nil {
			t.Fatalf("accepting: %v", err)
		}
		defer server.Close()
		// This process still holds a socket on port (the accepted one,
		// established), but no longer a listening one.
		_ = l.Close()
		assertListens(t, self, port, false)
	})

	t.Run("another process's listener", func(t *testing.T) {
		l, port := listen(t, "tcp", ":0")
		file, err := l.(*net.TCPListener).File()
		if err != nil {
			t.Fatalf("duplicating the listener: %v", err)
		}
		child := exec.Command("sleep", "60")
		child.ExtraFiles = []*os.File{file} // inherited as fd 3
		if err := child.Start(); err != nil {
			t.Fatalf("starting the child: %v", err)
		}
		t.Cleanup(func() {
			_ = child.Process.Kill()
			_ = child.Wait()
		})
		// From here on only the child holds the listening socket.
		_ = file.Close()
		_ = l.Close()
		assertListens(t, child.Process.Pid, port, true)
		assertListens(t, self, port, false)
	})

	t.Run("an exited process", func(t *testing.T) {
		_, port := listen(t, "tcp", ":0")
		gone := exec.Command("true")
		if err := gone.Run(); err != nil {
			t.Fatalf("running true: %v", err)
		}
		assertListens(t, gone.Process.Pid, port, false)
	})

	t.Run("nobody listening", func(t *testing.T) {
		l, port := listen(t, "tcp", ":0")
		_ = l.Close()
		assertListens(t, self, port, false)
	})
}

func TestHarnessPortOutside(t *testing.T) {
	for _, tc := range []struct {
		lo, hi int
		// want is the set of acceptable results, as inclusive ranges; nil
		// means only 0 (no port outside the range).
		want [][2]int
	}{
		{lo: 32768, hi: 60999, want: [][2]int{{minPickedPort, 32767}, {61000, 65535}}},
		{lo: 40000, hi: 65535, want: [][2]int{{minPickedPort, 39999}}},
		{lo: 1024, hi: 60999, want: [][2]int{{61000, 65535}}},
		{lo: 1024, hi: 65535},
		{lo: minPickedPort, hi: 65535},
	} {
		t.Run(fmt.Sprintf("%d-%d", tc.lo, tc.hi), func(t *testing.T) {
			for range 2000 {
				got := portOutside(tc.lo, tc.hi)
				ok := tc.want == nil && got == 0
				for _, r := range tc.want {
					ok = ok || (got >= r[0] && got <= r[1])
				}
				if !ok {
					t.Fatalf("portOutside(%d, %d) = %d, want one of %v (0 when none)", tc.lo, tc.hi, got, tc.want)
				}
			}
		})
	}
}

func TestHarnessReserveControllerPorts(t *testing.T) {
	env := func(kv ...string) func(string) string {
		m := map[string]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return func(key string) string { return m[key] }
	}

	t.Run("free ports outside the ephemeral range, released on return", func(t *testing.T) {
		lo, hi, err := ephemeralPortRange()
		if err != nil {
			t.Fatalf("reading the ephemeral range: %v", err)
		}
		roomOutside := portOutside(lo, hi) != 0
		for range 20 {
			metrics, health, err := reserveControllerPorts(env(), portOutside)
			if err != nil {
				t.Fatalf("reserveControllerPorts: %v", err)
			}
			if metrics == health {
				t.Fatalf("metrics and health share port %d", metrics)
			}
			for _, port := range []int{metrics, health} {
				if roomOutside && (port < minPickedPort || (port >= lo && port <= hi)) {
					t.Errorf("port %d: want one in [%d, 65535] outside the ephemeral range %d-%d", port, minPickedPort, lo, hi)
				}
				l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
				if err != nil {
					t.Errorf("port %d is still held once reserveControllerPorts returned: %v", port, err)
					continue
				}
				_ = l.Close()
			}
		}
	})

	t.Run("overrides are honoured", func(t *testing.T) {
		metrics, health, err := reserveControllerPorts(env(), portOutside)
		if err != nil {
			t.Fatalf("reserveControllerPorts: %v", err)
		}
		gotMetrics, gotHealth, err := reserveControllerPorts(env(
			"E2E_METRICS_PORT", strconv.Itoa(metrics),
			"E2E_HEALTH_PORT", strconv.Itoa(health),
		), portOutside)
		if err != nil {
			t.Fatalf("reserveControllerPorts with overrides: %v", err)
		}
		if gotMetrics != metrics || gotHealth != health {
			t.Errorf("got ports %d/%d, want the overrides %d/%d", gotMetrics, gotHealth, metrics, health)
		}
	})

	t.Run("a busy override fails, naming it", func(t *testing.T) {
		_, busy := listen(t, "tcp", ":0")
		_, _, err := reserveControllerPorts(env("E2E_HEALTH_PORT", strconv.Itoa(busy)), portOutside)
		want := fmt.Sprintf("E2E_HEALTH_PORT=%d: the port is not free", busy)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("got error %v, want one containing %q", err, want)
		}
	})

	t.Run("a pick never takes an override", func(t *testing.T) {
		// The metrics pick draws the health override's port first. Picking
		// before the override was bound took that port for metrics and
		// failed the run, claiming E2E_HEALTH_PORT was held by another
		// process.
		l, override := listen(t, "tcp", ":0")
		_ = l.Close()
		draws := 0
		pick := func(lo, hi int) int {
			draws++
			if draws == 1 {
				return override
			}
			return portOutside(lo, hi)
		}
		metrics, health, err := reserveControllerPorts(env("E2E_HEALTH_PORT", strconv.Itoa(override)), pick)
		if err != nil {
			t.Fatalf("reserveControllerPorts: %v", err)
		}
		if health != override || metrics == override {
			t.Errorf("got ports %d/%d, want the health override %d and another metrics port", metrics, health, override)
		}
		if draws < 2 {
			t.Errorf("the metrics port was drawn %d time(s), want the override's port drawn and refused first", draws)
		}
	})

	t.Run("equal overrides fail, naming both", func(t *testing.T) {
		l, port := listen(t, "tcp", ":0")
		_ = l.Close()
		_, _, err := reserveControllerPorts(env("E2E_METRICS_PORT", strconv.Itoa(port), "E2E_HEALTH_PORT", strconv.Itoa(port)), portOutside)
		want := fmt.Sprintf("E2E_HEALTH_PORT=%d is also E2E_METRICS_PORT", port)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("got error %v, want one containing %q", err, want)
		}
	})

	for _, v := range []string{"8o90", "0", "-1", "65536"} {
		t.Run("invalid override "+v, func(t *testing.T) {
			_, _, err := reserveControllerPorts(env("E2E_METRICS_PORT", v), portOutside)
			want := fmt.Sprintf("E2E_METRICS_PORT=%q is not a TCP port", v)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("got error %v, want one containing %q", err, want)
			}
		})
	}
}

func TestHarnessWatchController(t *testing.T) {
	start := func(t *testing.T, name string, args ...string) *exec.Cmd {
		t.Helper()
		cmd := exec.Command(name, args...)
		if err := cmd.Start(); err != nil {
			t.Fatalf("starting %s: %v", name, err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill() }) // reaped by the watcher
		return cmd
	}
	awaitExit := func(t *testing.T, ctrl *controller) {
		t.Helper()
		select {
		case <-ctrl.exited:
		case <-time.After(30 * time.Second):
			t.Fatal("the watcher never observed the exit")
		}
	}

	t.Run("an unexpected exit is reported, then cancels the context", func(t *testing.T) {
		cmd := start(t, "sh", "-c", "exit 3")
		reported := make(chan error, 1)
		ctrl, ctx, cancel := watchController(context.Background(), cmd, func(exitErr error) { reported <- exitErr })
		defer cancel(nil)
		awaitExit(t, ctrl)
		var exitErr error
		select {
		case exitErr = <-reported:
		default:
			t.Fatal("exited was closed before the crash was reported")
		}
		want := fmt.Sprintf("pid %d, exit status 3", cmd.Process.Pid)
		if !errors.Is(exitErr, errControllerExited) || !strings.Contains(exitErr.Error(), want) {
			t.Errorf("reported %v, want errControllerExited with %q", exitErr, want)
		}
		if !ctrl.crashed.Load() || !ctrl.dead() {
			t.Errorf("crashed=%v dead=%v, want both true", ctrl.crashed.Load(), ctrl.dead())
		}
		if cause := context.Cause(ctx); !errors.Is(cause, exitErr) {
			t.Errorf("context cause %v, want the reported exit %v", cause, exitErr)
		}
	})

	t.Run("a crash aborts a running wait, naming the exit", func(t *testing.T) {
		cmd := start(t, "sleep", "60")
		ctrl, ctx, cancel := watchController(context.Background(), cmd, func(error) {})
		defer cancel(nil)
		time.AfterFunc(200*time.Millisecond, func() { _ = cmd.Process.Kill() })
		begin := time.Now()
		err := waitUntil(ctx, 50*time.Millisecond, 5*time.Minute, func(context.Context) (bool, string) { return false, "pending" })
		if elapsed := time.Since(begin); elapsed > time.Minute {
			t.Errorf("the wait ran %s after the crash, want it aborted within one poll", elapsed)
		}
		if !errors.Is(err, errControllerExited) {
			t.Errorf("wait error %v, want it to wrap errControllerExited", err)
		}
		for _, want := range []string{"aborted after", "signal: killed", "(last: pending)"} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("wait error %v, want it to contain %q", err, want)
			}
		}
		awaitExit(t, ctrl)
	})

	t.Run("a requested stop is not a crash", func(t *testing.T) {
		cmd := start(t, "sleep", "60")
		ctrl, ctx, cancel := watchController(context.Background(), cmd, func(exitErr error) {
			t.Errorf("a requested stop was reported as a crash: %v", exitErr)
		})
		defer cancel(nil)
		ctrl.stopping.Store(true)
		_ = cmd.Process.Kill()
		awaitExit(t, ctrl)
		if ctrl.crashed.Load() {
			t.Error("crashed is set after a requested stop")
		}
		if err := ctx.Err(); err != nil {
			t.Errorf("the context was cancelled by a requested stop: %v", err)
		}
	})
}

func TestHarnessWaitUntil(t *testing.T) {
	pending := func(context.Context) (bool, string) { return false, "pending" }

	t.Run("done", func(t *testing.T) {
		calls := 0
		err := waitUntil(context.Background(), 10*time.Millisecond, time.Minute, func(context.Context) (bool, string) {
			calls++
			return calls == 3, "counting"
		})
		if err != nil {
			t.Errorf("got %v, want nil once cond is done", err)
		}
	})

	t.Run("its own timeout", func(t *testing.T) {
		err := waitUntil(context.Background(), 10*time.Millisecond, 50*time.Millisecond, pending)
		want := "not reached within 50ms (last: pending)"
		if err == nil || err.Error() != want {
			t.Errorf("got %v, want %q", err, want)
		}
	})

	t.Run("the suite deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := waitUntil(ctx, 10*time.Millisecond, time.Minute, pending)
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "aborted after") {
			t.Errorf("got %v, want an abort naming the deadline", err)
		}
	})
}

// recorder stands in for the suite's *testing.T in the cleanup tests: it
// records what the cleanup reports instead of failing the test with it.
// Anything else (Fatalf) still reaches the real test.
type recorder struct {
	testing.TB
	errors, logs []string
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func (r *recorder) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

func (r *recorder) assertErrors(t *testing.T, want ...string) {
	t.Helper()
	if len(r.errors) != len(want) {
		t.Fatalf("the cleanup reported %d error(s), want %d: %q", len(r.errors), len(want), r.errors)
	}
	for i, w := range want {
		if !strings.Contains(r.errors[i], w) {
			t.Errorf("error %d = %q, want it to contain %q", i, r.errors[i], w)
		}
	}
}

func (r *recorder) assertLogged(t *testing.T, want string) {
	t.Helper()
	for _, l := range r.logs {
		if strings.Contains(l, want) {
			return
		}
	}
	t.Errorf("nothing logged contains %q: %q", want, r.logs)
}

const cleanupRun = "c1ea70"

// cleanupFramework builds a framework of run cleanupRun over the fake
// client, with a live controller and no re-check.
func cleanupFramework(t *testing.T, kubeClient client.Client) (*framework, *recorder) {
	t.Helper()
	t.Setenv("E2E_KEEP", "")
	rec := &recorder{TB: t}
	return &framework{
		t:         rec,
		client:    kubeClient,
		runID:     cleanupRun,
		prefix:    "e2e-" + cleanupRun,
		namespace: "e2e-" + cleanupRun,
		ctrl:      &controller{exited: make(chan struct{})},
	}, rec
}

func fakeClient(objs ...client.Object) *fake.ClientBuilder {
	return fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...)
}

// cleanupGroup builds a live NodeGroup named name, owned by NodeClaim claim
// unless claim is empty. Its finalizer stands for the platform's: a delete
// leaves the group terminating, as on CKE.
func cleanupGroup(name string, labels map[string]string, claim string) *ngv1.NodeGroup {
	ng := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			UID:               types.UID("uid-" + name),
			Labels:            labels,
			CreationTimestamp: metav1.NewTime(time.Date(2026, 9, 2, 10, 6, 0, 0, time.UTC)),
			Finalizers:        []string{"test.finalizer/keep"},
		},
		Spec: ngv1.NodeGroupSpec{Flavor: "2XS", NodeCount: 1},
	}
	if claim != "" {
		ng.OwnerReferences = []metav1.OwnerReference{
			{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: claim, UID: types.UID("uid-" + claim)},
		}
	}
	return ng
}

// runCleanup runs f.cleanupAll, failing the test instead of sitting out the
// graceful wait's 15-minute budget when a group it must not wait on holds it.
func runCleanup(t *testing.T, f *framework) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.cleanupAll()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cleanupAll still running after 10s: something holds the graceful wait")
	}
}

func terminating(ng *ngv1.NodeGroup) *ngv1.NodeGroup {
	now := metav1.Now()
	ng.DeletionTimestamp = &now
	return ng
}

// assertDeleted reports whether the named group is terminating (deleted) or
// still live, against want.
func assertDeleted(t *testing.T, kubeClient client.Client, name string, want bool) {
	t.Helper()
	ng := &ngv1.NodeGroup{}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: name}, ng); err != nil {
		t.Fatalf("getting nodegroup %s: %v", name, err)
	}
	if got := !ng.DeletionTimestamp.IsZero(); got != want {
		t.Errorf("nodegroup %s deleted = %v, want %v", name, got, want)
	}
}

// TestHarnessCleanupOwnerless pins how the cleanup treats a NodeGroup of the
// run without a NodeClaim owner: karpenter launches none, so apart from the
// garbage collection decoy it is a group the platform re-created, and the
// cleanup fails the run for it wherever it meets it, instead of deleting it
// with a log line.
func TestHarnessCleanupOwnerless(t *testing.T) {
	prefix := "e2e-" + cleanupRun
	runLabel := map[string]string{runLabelKey: cleanupRun}

	t.Run("the decoy is deleted without failing the run", func(t *testing.T) {
		c := fakeClient(cleanupGroup(prefix+"-decoy", runLabel, "")).Build()
		f, rec := cleanupFramework(t, c)
		runCleanup(t, f)
		rec.assertErrors(t)
		rec.assertLogged(t, "deleting the garbage collection decoy "+prefix+"-decoy")
		assertDeleted(t, c, prefix+"-decoy", true)
	})

	t.Run("a re-created group fails the run and is deleted", func(t *testing.T) {
		c := fakeClient(cleanupGroup(prefix+"-default-x7k2q", nil, "")).Build()
		f, rec := cleanupFramework(t, c)
		runCleanup(t, f)
		rec.assertErrors(t, "nodegroup "+prefix+"-default-x7k2q has no NodeClaim owner (uid uid-"+prefix+"-default-x7k2q, created 2026-09-02T10:06:00Z, labels map[])")
		assertDeleted(t, c, prefix+"-default-x7k2q", true)
	})

	t.Run("a re-created decoy fails the run", func(t *testing.T) {
		// Deleted by its scenario, re-created without the run label.
		c := fakeClient(cleanupGroup(prefix+"-decoy", nil, "")).Build()
		f, rec := cleanupFramework(t, c)
		runCleanup(t, f)
		rec.assertErrors(t, "nodegroup "+prefix+"-decoy has no NodeClaim owner")
		assertDeleted(t, c, prefix+"-decoy", true)
	})

	t.Run("terminating groups and groups of other runs are left alone", func(t *testing.T) {
		c := fakeClient(
			terminating(cleanupGroup(prefix+"-default-b4m9z", runLabel, prefix+"-default-b4m9z")),
			terminating(cleanupGroup(prefix+"-default-k8p2w", nil, "")),
			cleanupGroup("e2e-0d4e5f-default-q2w3e", nil, ""),
		).Build()
		f, rec := cleanupFramework(t, c)
		runCleanup(t, f)
		rec.assertErrors(t)
		assertDeleted(t, c, "e2e-0d4e5f-default-q2w3e", false)
	})

	t.Run("the final sweep classifies a group the first sweep missed", func(t *testing.T) {
		// The first listing fails and the controller is dead, so the final
		// sweep is the first step to meet the group: it reports it as
		// re-created, not as one more group the dead controller left.
		failed := false
		c := fakeClient(cleanupGroup(prefix+"-default-x7k2q", nil, "")).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*ngv1.NodeGroupList); ok && !failed {
						failed = true
						return errors.New("connection refused")
					}
					return c.List(ctx, list, opts...)
				},
			}).Build()
		f, rec := cleanupFramework(t, c)
		close(f.ctrl.exited)
		runCleanup(t, f)
		rec.assertErrors(t,
			"listing nodegroups for the ownerless sweep: connection refused",
			"the controller is dead",
			"nodegroup "+prefix+"-default-x7k2q has no NodeClaim owner",
		)
		assertDeleted(t, c, prefix+"-default-x7k2q", true)
	})
}

// TestHarnessCleanupDrain pins the graceful wait's condition: claim-backed
// groups and claims hold it, an ownerless group never does.
func TestHarnessCleanupDrain(t *testing.T) {
	prefix := "e2e-" + cleanupRun
	ctx := context.Background()

	t.Run("a group re-created during the wait is deleted, not waited on", func(t *testing.T) {
		c := fakeClient(cleanupGroup(prefix+"-default-x7k2q", nil, "")).Build()
		f, rec := cleanupFramework(t, c)
		if done, err := f.drainDone(ctx, map[types.UID]bool{}); !done || err != nil {
			t.Errorf("drainDone = %v, %v; want done", done, err)
		}
		rec.assertErrors(t, "nodegroup "+prefix+"-default-x7k2q has no NodeClaim owner")
		assertDeleted(t, c, prefix+"-default-x7k2q", true)
	})

	t.Run("one whose delete fails is reported once and not waited on", func(t *testing.T) {
		// The final sweep retries the delete; the wait must not re-report
		// the group at every poll, nor wait on it.
		c := fakeClient(cleanupGroup(prefix+"-default-x7k2q", nil, "")).
			WithInterceptorFuncs(interceptor.Funcs{
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
					return errors.New("connection refused")
				},
			}).Build()
		f, rec := cleanupFramework(t, c)
		ownerless := map[types.UID]bool{}
		for range 2 {
			if done, err := f.drainDone(ctx, ownerless); !done || err != nil {
				t.Fatalf("drainDone = %v, %v; want done", done, err)
			}
		}
		rec.assertErrors(t,
			"nodegroup "+prefix+"-default-x7k2q has no NodeClaim owner",
			"deleting ownerless nodegroup "+prefix+"-default-x7k2q: connection refused",
		)
	})

	t.Run("a live claim-backed group holds it", func(t *testing.T) {
		c := fakeClient(cleanupGroup(prefix+"-default-b4m9z", nil, prefix+"-default-b4m9z")).Build()
		f, rec := cleanupFramework(t, c)
		if done, err := f.drainDone(ctx, map[types.UID]bool{}); done || err != nil {
			t.Errorf("drainDone = %v, %v; want not done", done, err)
		}
		rec.assertErrors(t)
		assertDeleted(t, c, prefix+"-default-b4m9z", false)
	})

	t.Run("a claim of the run holds it", func(t *testing.T) {
		c := fakeClient(&karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: prefix + "-default-b4m9z"}}).Build()
		f, _ := cleanupFramework(t, c)
		if done, err := f.drainDone(ctx, map[types.UID]bool{}); done || err != nil {
			t.Errorf("drainDone = %v, %v; want not done", done, err)
		}
	})

	t.Run("a dead controller ends it", func(t *testing.T) {
		f, _ := cleanupFramework(t, fakeClient().Build())
		close(f.ctrl.exited)
		if _, err := f.drainDone(ctx, map[types.UID]bool{}); !errors.Is(err, errControllerExited) {
			t.Errorf("drainDone error %v, want errControllerExited", err)
		}
	})
}

// TestHarnessCleanupRecheck pins what the re-check says of a group of the
// run it finds after the final sweep.
func TestHarnessCleanupRecheck(t *testing.T) {
	name := "e2e-" + cleanupRun + "-default-x7k2q"
	uid := types.UID("uid-" + name)
	for _, tc := range []struct {
		name    string
		group   *ngv1.NodeGroup
		seen    map[types.UID]bool
		swept   bool
		wantErr string // empty: only logged, left alone
	}{
		{name: "still terminating", group: terminating(cleanupGroup(name, nil, "")), swept: true},
		{name: "reappeared", group: cleanupGroup(name, nil, ""), swept: true, wantErr: "REAPPEARED after the cleanup (uid " + string(uid)},
		{name: "survived the direct delete", group: cleanupGroup(name, nil, ""), seen: map[types.UID]bool{uid: true}, swept: true, wantErr: "survived the cleanup's direct delete"},
		{name: "final sweep could not list", group: cleanupGroup(name, nil, ""), wantErr: "whether it survived the cleanup or reappeared is unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fakeClient(tc.group).Build()
			f, rec := cleanupFramework(t, c)
			f.recheckAfter = time.Millisecond
			f.recheckNodeGroups(tc.seen, tc.swept)
			if tc.wantErr == "" {
				rec.assertErrors(t)
				rec.assertLogged(t, "nodegroup "+name+" is still terminating")
				return
			}
			rec.assertErrors(t, tc.wantErr)
			assertDeleted(t, c, name, true)
		})
	}
}
