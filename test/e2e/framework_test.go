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

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

const (
	// suiteLabelKey marks every object the suite creates, so the fallback
	// cleanup (hack/e2e-cleanup.sh) can sweep leftovers from any run.
	suiteLabelKey = "e2e.karpenter.clever-cloud.com/suite"
	// runLabelKey scopes objects to one suite invocation.
	runLabelKey = "e2e.karpenter.clever-cloud.com/run"
)

// errControllerExited is the cause of the scenario context's cancellation
// when the controller subprocess exits without the suite asking it to.
var errControllerExited = errors.New("the controller exited unexpectedly")

// framework carries everything one suite run needs: a pinned kubeconfig, a
// direct (uncached) client, the controller subprocess and unique names.
type framework struct {
	// t is the (sub)test's *testing.T in the suite; the harness tests record
	// what the cleanup reports through it.
	t              testing.TB
	client         client.Client
	clientset      *kubernetes.Clientset
	restCfg        *rest.Config
	kubeconfigPath string

	runID     string
	prefix    string // "e2e-<runID>", prefixes every cluster-scoped object name
	namespace string

	metricsPort int // set by startController
	healthPort  int // set by startController
	// recheckAfter is how long after the cleanup the run's NodeGroups are
	// looked for once more (E2E_CLEANUP_RECHECK); 0 skips the re-check.
	recheckAfter time.Duration
	artifacts    string
	logPath      string

	// ctrl is the controller subprocess, set by startController. A pointer:
	// withT copies the framework, and every copy must see the same process.
	ctrl *controller
}

// controller tracks the out-of-cluster controller subprocess.
type controller struct {
	pid int
	// exited is closed once the process has been reaped, after an unexpected
	// exit has been reported.
	exited chan struct{}
	// stopping is set before the suite stops the process on purpose: an exit
	// after it is expected.
	stopping atomic.Bool
	// crashed is set when the process exited while the suite still needed it.
	crashed atomic.Bool
}

// dead reports whether the controller process has exited.
func (c *controller) dead() bool {
	select {
	case <-c.exited:
		return true
	default:
		return false
	}
}

func newFramework(t *testing.T) *framework {
	t.Helper()

	kubeContext := os.Getenv("E2E_CONTEXT")
	if kubeContext == "" {
		t.Fatal("E2E_CONTEXT is required: the suite refuses to run against an implicit current-context " +
			"(it creates and deletes real VMs). Set it to the kubeconfig context of the dedicated test cluster.")
	}

	buf := make([]byte, 3)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("generating run id: %v", err)
	}
	runID := hex.EncodeToString(buf)

	f := &framework{
		t:            t,
		runID:        runID,
		prefix:       "e2e-" + runID,
		namespace:    "e2e-" + runID,
		recheckAfter: 2 * time.Minute,
		artifacts:    envString("E2E_ARTIFACTS", filepath.Join(os.TempDir(), "karpenter-e2e")),
	}
	if v := os.Getenv("E2E_CLEANUP_RECHECK"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			t.Fatalf("E2E_CLEANUP_RECHECK=%q: want a duration such as 7m, or 0 to skip the re-check", v)
		}
		f.recheckAfter = d
	}
	if err := os.MkdirAll(f.artifacts, 0o755); err != nil {
		t.Fatalf("creating artifacts dir: %v", err)
	}
	f.logPath = filepath.Join(f.artifacts, fmt.Sprintf("controller-%s.log", runID))

	// Pin the requested context into a private kubeconfig copy: the user's
	// file may have its current-context rotated externally, and both the test
	// client and the controller subprocess must agree on the target cluster.
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rawCfg, err := rules.Load()
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	if _, ok := rawCfg.Contexts[kubeContext]; !ok {
		t.Fatalf("context %q not found in kubeconfig", kubeContext)
	}
	rawCfg.CurrentContext = kubeContext
	f.kubeconfigPath = filepath.Join(f.artifacts, fmt.Sprintf("kubeconfig-%s", runID))
	if err := clientcmd.WriteToFile(*rawCfg, f.kubeconfigPath); err != nil {
		t.Fatalf("writing pinned kubeconfig: %v", err)
	}

	f.restCfg, err = clientcmd.BuildConfigFromFlags("", f.kubeconfigPath)
	if err != nil {
		t.Fatalf("building rest config: %v", err)
	}
	// scheme.Scheme carries corev1/appsv1 plus karpenter.sh and both provider
	// groups (registered by package inits on import).
	f.client, err = client.New(f.restCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatalf("building client: %v", err)
	}
	f.clientset, err = kubernetes.NewForConfig(f.restCfg)
	if err != nil {
		t.Fatalf("building clientset: %v", err)
	}
	return f
}

// withT rebinds the framework to a subtest's *testing.T: Fatalf must be
// called from the goroutine running the current (sub)test, never the parent.
func (f *framework) withT(t *testing.T) *framework {
	c := *f
	c.t = t
	return &c
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// minPickedPort is the lowest port reserveControllerPorts picks on its own,
// clear of the ports local services are commonly configured on.
const minPickedPort = 10000

// reserveControllerPorts returns the controller's metrics and health ports:
// E2E_METRICS_PORT / E2E_HEALTH_PORT (read through getenv) when set, free
// ports drawn by pick (portOutside in the suite) otherwise, so two suites
// (against sibling clusters) never compete for a fixed default. A picked port
// lies outside the kernel's ephemeral range: the kernel draws the source port
// of every outbound connection from that range (the suite's readiness dials,
// the controller's own apiserver connections), and one of them could take an
// ephemeral port between its release here and the controller's bind,
// crashing the controller for nothing. Each port is bound here the way the
// controller binds it (every interface): a port another process holds is an
// error now, naming it, rather than a controller panic. Every override is
// bound before any port is picked, so a pick that draws an override's port
// finds it taken and draws again, instead of taking it and failing the
// override as if another process held it. The binds are released on return,
// right before the controller starts; startController then checks the
// controller really holds both.
func reserveControllerPorts(getenv func(string) string, pick func(lo, hi int) int) (metrics, health int, err error) {
	var listeners []net.Listener
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	lo, hi, err := ephemeralPortRange()
	if err != nil {
		return 0, 0, err
	}
	keys := [2]string{"E2E_METRICS_PORT", "E2E_HEALTH_PORT"}
	var ports [2]int
	for i, key := range keys {
		v := getenv(key)
		if v == "" {
			continue
		}
		port, err := strconv.Atoi(v)
		if err != nil || port < 1 || port > 65535 {
			return 0, 0, fmt.Errorf("%s=%q is not a TCP port (1-65535)", key, v)
		}
		if port == ports[0] {
			return 0, 0, fmt.Errorf("%s=%d is also %s: the metrics and health ports must differ", key, port, keys[0])
		}
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return 0, 0, fmt.Errorf("%s=%d: the port is not free: %w", key, port, err)
		}
		listeners = append(listeners, l)
		ports[i] = port
	}
	for i, key := range keys {
		if ports[i] != 0 {
			continue
		}
		for range 64 {
			// 0 (the kernel picks an ephemeral port) only when the
			// ephemeral range leaves no port to pick outside it.
			port := pick(lo, hi)
			l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
			if err != nil {
				if port == 0 {
					return 0, 0, fmt.Errorf("reserving a port for %s: %w", key, err)
				}
				continue // taken, by another process or an override: draw again
			}
			listeners = append(listeners, l)
			ports[i] = l.Addr().(*net.TCPAddr).Port
			break
		}
		if ports[i] == 0 {
			return 0, 0, fmt.Errorf("reserving a port for %s: no free port found outside the ephemeral range %d-%d; set %s", key, lo, hi, key)
		}
	}
	return ports[0], ports[1], nil
}

// ephemeralPortRange returns the kernel's ephemeral port range
// (net.ipv4.ip_local_port_range, which IPv6 shares): the ports it hands out
// as the source port of an outbound connection and to a bind on port 0.
func ephemeralPortRange() (lo, hi int, err error) {
	const path = "/proc/sys/net/ipv4/ip_local_port_range"
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(raw))
	if len(fields) == 2 {
		lo, errLo := strconv.Atoi(fields[0])
		hi, errHi := strconv.Atoi(fields[1])
		if errLo == nil && errHi == nil && lo <= hi {
			return lo, hi, nil
		}
	}
	return 0, 0, fmt.Errorf("%s: unexpected content %q", path, raw)
}

// portOutside returns a random port in [minPickedPort, 65535] outside
// [lo, hi], or 0 when there is none.
func portOutside(lo, hi int) int {
	below := max(0, lo-minPickedPort)      // minPickedPort .. lo-1
	aboveStart := max(hi+1, minPickedPort) // aboveStart .. 65535
	above := max(0, 65535-aboveStart+1)
	if below+above == 0 {
		return 0
	}
	n := mrand.IntN(below + above)
	if n < below {
		return minPickedPort + n
	}
	return aboveStart + n - below
}

// listensOn reports whether process pid holds a listening TCP socket on
// port. The suite already requires Linux (Pdeathsig), so it reads /proc: the
// kernel's socket tables give the inode of every listener on the port, and
// the process's file descriptors say whether one of them is its own.
func listensOn(pid, port int) (bool, error) {
	inodes := map[string]bool{}
	for _, table := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		raw, err := os.ReadFile(table)
		if errors.Is(err, fs.ErrNotExist) {
			continue // IPv6 disabled on this host
		}
		if err != nil {
			return false, err
		}
		for _, line := range strings.Split(string(raw), "\n")[1:] {
			// sl local_address rem_address st tx:rx tr:when retrnsmt uid timeout inode ...
			fields := strings.Fields(line)
			if len(fields) < 10 || fields[3] != "0A" { // 0A: TCP_LISTEN
				continue
			}
			_, hexPort, ok := strings.Cut(fields[1], ":")
			if !ok {
				continue
			}
			if p, err := strconv.ParseUint(hexPort, 16, 16); err == nil && int(p) == port {
				inodes[fields[9]] = true
			}
		}
	}
	if len(inodes) == 0 {
		return false, nil
	}
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	fds, err := os.ReadDir(fdDir)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil // the process is gone; its exit is reported by startController's watcher
	}
	if err != nil {
		return false, err
	}
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
		if err != nil {
			continue // closed since the listing
		}
		if inode, ok := strings.CutPrefix(target, "socket:["); ok && inodes[strings.TrimSuffix(inode, "]")] {
			return true, nil
		}
	}
	return false, nil
}

// checkCluster refuses to run against anything that does not look like the
// dedicated CKE test cluster, and reports pre-existing suite leftovers.
func (f *framework) checkCluster(ctx context.Context) {
	f.t.Helper()
	if err := f.client.List(ctx, &ngv1.NodeGroupList{}, client.Limit(1)); err != nil {
		f.t.Fatalf("cluster does not serve nodegroups.api.clever-cloud.com — not a CKE cluster? %v", err)
	}
	groups := &ngv1.NodeGroupList{}
	if err := f.client.List(ctx, groups); err != nil {
		f.t.Fatalf("listing nodegroups: %v", err)
	}
	for i := range groups.Items {
		ng := &groups.Items[i]
		if strings.HasPrefix(ng.Name, "e2e-") {
			f.t.Logf("WARNING: leftover nodegroup %s from a previous run is still present (billing!) — hack/e2e-cleanup.sh removes them", ng.Name)
		} else if nodegroup.IsManaged(ng) {
			f.t.Logf("WARNING: pre-existing managed nodegroup %s — quota headroom for this run is reduced", ng.Name)
		}
	}
}

// applyCRDs server-side-applies every CRD under deploy/crds. The suite owns
// no CRD lifecycle beyond that: they stay installed on the test cluster.
func (f *framework) applyCRDs(ctx context.Context) {
	f.t.Helper()
	root := repoRoot(f.t)
	entries, err := filepath.Glob(filepath.Join(root, "deploy", "crds", "*.yaml"))
	if err != nil || len(entries) == 0 {
		f.t.Fatalf("locating deploy/crds: %v (found %d)", err, len(entries))
	}
	for _, path := range entries {
		raw, err := os.ReadFile(path)
		if err != nil {
			f.t.Fatalf("reading %s: %v", path, err)
		}
		obj := &unstructured.Unstructured{}
		if err := yaml.Unmarshal(raw, &obj.Object); err != nil {
			f.t.Fatalf("decoding %s: %v", path, err)
		}
		if err := f.client.Apply(ctx, client.ApplyConfigurationFromUnstructured(obj), client.FieldOwner("karpenter-e2e"), client.ForceOwnership); err != nil {
			f.t.Fatalf("applying CRD %s: %v", filepath.Base(path), err)
		}
	}
	f.t.Logf("applied %d CRDs", len(entries))
}

func repoRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// startController builds the controller from the working tree and runs it
// out-of-cluster against the pinned kubeconfig — the `make run` shape used by
// every manual validation run. It returns a context derived from ctx that is
// cancelled, with the exit as its cause, the moment the controller exits on
// its own: the scenarios run on it, so a dead controller aborts them at once
// instead of leaving each wait to time out against a cluster nobody
// reconciles. The stop function it also returns must run after cleanup
// (cleanup needs a live controller to drain NodeClaims).
func (f *framework) startController(ctx context.Context) (context.Context, func()) {
	f.t.Helper()
	root := repoRoot(f.t)
	bin := filepath.Join(f.artifacts, fmt.Sprintf("controller-%s", f.runID))

	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/controller")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		f.t.Fatalf("building controller: %v\n%s", err, out)
	}

	logFile, err := os.Create(f.logPath)
	if err != nil {
		f.t.Fatalf("creating controller log file: %v", err)
	}

	f.metricsPort, f.healthPort, err = reserveControllerPorts(os.Getenv, portOutside)
	if err != nil {
		_ = logFile.Close()
		f.t.Fatalf("reserving the controller ports: %v", err)
	}
	cmd := exec.Command(bin)
	cmd.Dir = root
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// If the test process dies hard (go test watchdog, runner eviction), the
	// controller must die with it — an orphaned controller would keep
	// reconciling the cluster behind the fallback cleanup's back.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.Env = append(os.Environ(),
		"KUBECONFIG="+f.kubeconfigPath,
		"DISABLE_LEADER_ELECTION=true",
		"LOG_LEVEL=debug",
		fmt.Sprintf("METRICS_PORT=%d", f.metricsPort),
		fmt.Sprintf("HEALTH_PROBE_PORT=%d", f.healthPort),
	)
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		f.t.Fatalf("starting controller: %v", err)
	}
	// An exit the suite did not ask for (a panic, a port another process
	// took before the controller bound it) fails the suite from the watcher,
	// as it happens, with the log tail: Errorf and Logf are safe from any
	// goroutine while the test runs.
	ctrl, ctx, cancel := watchController(ctx, cmd, func(exitErr error) {
		f.t.Errorf("%v: failing the suite now", exitErr)
		f.dumpControllerLog()
	})
	f.ctrl = ctrl
	f.t.Logf("controller started (pid %d, metrics port %d, health port %d, log %s)", ctrl.pid, f.metricsPort, f.healthPort, f.logPath)

	// A healthy answer alone proves nothing: another local process may
	// listen on the port, and the controller only binds its ports once it
	// runs its manager. The controller must be the one listening on both.
	healthURL := fmt.Sprintf("http://127.0.0.1:%d/healthz", f.healthPort)
	var last string
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			return false, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			last = err.Error()
			return false, nil
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			last = "GET /healthz: " + resp.Status
			return false, nil
		}
		for _, port := range []int{f.healthPort, f.metricsPort} {
			owned, err := listensOn(ctrl.pid, port)
			if err != nil {
				return false, fmt.Errorf("checking which process listens on port %d: %w", port, err)
			}
			if !owned {
				last = fmt.Sprintf("port %d is not held by the controller (pid %d): another process listens on it, or the controller has not bound it yet", port, ctrl.pid)
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		ctrl.stopping.Store(true)
		_ = cmd.Process.Kill()
		<-ctrl.exited
		cause := context.Cause(ctx)
		cancel(nil)
		_ = logFile.Close()
		if ctrl.crashed.Load() {
			// The watcher already failed the suite with the log tail.
			f.t.Fatalf("controller never became healthy: %v", cause)
		}
		f.dumpControllerLog()
		f.t.Fatalf("controller never became healthy on %s: %v (last: %s)", healthURL, err, last)
	}

	return ctx, func() {
		ctrl.stopping.Store(true)
		// SIGINT lets the manager stop cleanly; escalate if it lingers.
		_ = cmd.Process.Signal(syscall.SIGINT)
		select {
		case <-ctrl.exited:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			<-ctrl.exited
		}
		cancel(nil)
		_ = logFile.Close()
		// A crash printed the tail when it happened.
		if f.t.Failed() && !ctrl.crashed.Load() {
			f.dumpControllerLog()
		}
	}
}

// watchController runs the only Wait of the started process cmd. It returns
// the controller handle and a context derived from ctx that, when the
// process exits before stopping is set, is cancelled with the exit as its
// cause, right after onCrash has reported that exit. exited is closed only
// once onCrash has returned, so a stop that waits on it never races a crash
// report.
func watchController(ctx context.Context, cmd *exec.Cmd, onCrash func(exitErr error)) (*controller, context.Context, context.CancelCauseFunc) {
	ctrl := &controller{pid: cmd.Process.Pid, exited: make(chan struct{})}
	ctx, cancel := context.WithCancelCause(ctx)
	go func() {
		_ = cmd.Wait()
		if !ctrl.stopping.Load() {
			ctrl.crashed.Store(true)
			exitErr := fmt.Errorf("%w (pid %d, %s)", errControllerExited, ctrl.pid, cmd.ProcessState)
			onCrash(exitErr)
			cancel(exitErr)
		}
		close(ctrl.exited)
	}()
	return ctrl, ctx, cancel
}

// dumpControllerLog surfaces the tail of the controller log into the test
// output so a red run is diagnosable from the terminal alone.
func (f *framework) dumpControllerLog() {
	file, err := os.Open(f.logPath)
	if err != nil {
		return
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > 120 {
			lines = lines[1:]
		}
	}
	f.t.Logf("---- controller log tail (%s) ----\n%s\n---- end controller log ----", f.logPath, strings.Join(lines, "\n"))
}

// eventually polls cond until it reports done or the timeout elapses; the
// last message is included in the failure. All waits go through this so a
// suite-context deadline or the controller's death aborts them promptly,
// naming which, and cleanup still runs.
func (f *framework) eventually(ctx context.Context, timeout time.Duration, what string, cond func(ctx context.Context) (bool, string)) {
	f.t.Helper()
	start := time.Now()
	if err := waitUntil(ctx, 5*time.Second, timeout, cond); err != nil {
		f.t.Fatalf("%s: %v", what, err)
	}
	f.t.Logf("%s: reached in %s", what, time.Since(start).Round(time.Second))
}

// waitUntil polls cond every interval until it reports done (nil) or the
// wait ends. The error then says why, with cond's last message: the cause of
// ctx's cancellation (the controller's exit, the suite deadline) when ctx
// ended it, the timeout otherwise.
func waitUntil(ctx context.Context, interval, timeout time.Duration, cond func(ctx context.Context) (bool, string)) error {
	start := time.Now()
	var last string
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		var done bool
		done, last = cond(ctx)
		return done, nil
	})
	if err == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return fmt.Errorf("aborted after %s: %w (last: %s)", time.Since(start).Round(time.Second), cause, last)
	}
	return fmt.Errorf("not reached within %s (last: %s)", timeout, last)
}

// metric returns the summed value of a provider metric across its label
// variants, scraped from the out-of-cluster controller's endpoint — 0 when the
// scrape fails or the series is absent, which suits the polling callers that
// retry. A check that asserts 0 must use scrapeMetric, or a dead endpoint
// passes it.
func (f *framework) metric(name string) float64 {
	f.t.Helper()
	total, _, _ := f.scrapeMetric(name)
	return total
}

// scrapeMetric is metric with its failure modes: an error when the endpoint
// cannot be scraped, and found=false when it exposes no series of name.
func (f *framework) scrapeMetric(name string) (total float64, found bool, err error) {
	f.t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", f.metricsPort))
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("GET /metrics: %s", resp.Status)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, name) {
			continue
		}
		rest := line[len(name):]
		if rest != "" && rest[0] != ' ' && rest[0] != '{' {
			continue // e.g. name is a prefix of another metric
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err == nil {
			total += v
			found = true
		}
	}
	return total, found, scanner.Err()
}

// hasEvent reports whether an event with the given reason exists for the
// named involved object (any namespace — events for cluster-scoped objects
// land in "default").
func (f *framework) hasEvent(ctx context.Context, objectName, reason string) bool {
	f.t.Helper()
	events, err := f.clientset.CoreV1().Events(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: "involvedObject.name=" + objectName + ",reason=" + reason,
	})
	if err != nil {
		return false
	}
	return len(events.Items) > 0
}

func (f *framework) labels() map[string]string {
	return map[string]string{suiteLabelKey: "true", runLabelKey: f.runID}
}

func (f *framework) nodeClass(name string, extraLabels map[string]string) *v1alpha1.CleverNodeClass {
	return &v1alpha1.CleverNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: f.labels()},
		Spec:       v1alpha1.CleverNodeClassSpec{Labels: extraLabels},
	}
}

func (f *framework) nodePool(name, nodeClassName string, flavors []string, cpuLimit string) *karpv1.NodePool {
	return &karpv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: f.labels()},
		Spec: karpv1.NodePoolSpec{
			Template: karpv1.NodeClaimTemplate{
				Spec: karpv1.NodeClaimTemplateSpec{
					NodeClassRef: &karpv1.NodeClassReference{
						Group: "karpenter.clever-cloud.com",
						Kind:  "CleverNodeClass",
						Name:  nodeClassName,
					},
					Requirements: []karpv1.NodeSelectorRequirementWithMinValues{{
						Key:      corev1.LabelInstanceTypeStable,
						Operator: corev1.NodeSelectorOpIn,
						Values:   flavors,
					}},
					ExpireAfter: karpv1.MustParseNillableDuration("Never"),
				},
			},
			Limits: karpv1.Limits{corev1.ResourceCPU: resource.MustParse(cpuLimit)},
			Disruption: karpv1.Disruption{
				ConsolidationPolicy: karpv1.ConsolidationPolicyWhenEmptyOrUnderutilized,
				ConsolidateAfter:    karpv1.MustParseNillableDuration("30s"),
				// Explicit permissive budget: the default 10% floors to zero
				// allowed disruptions on the 1-2 node pools this suite runs,
				// which would park the drift roll forever.
				Budgets: []karpv1.Budget{{Nodes: "10"}},
			},
		},
	}
}

func (f *framework) deployment(name string, replicas int32, cpu, memory string, onePodPerNode bool) *appsv1.Deployment {
	podLabels := map[string]string{"app": name}
	spec := corev1.PodSpec{
		TerminationGracePeriodSeconds: ptrInt64(0),
		// Every scenario asserts that a pending pod produces a NodeClaim, so
		// the workload must be unschedulable on anything Karpenter did not
		// create. Selecting `cluster-node-role=worker` only achieved that on
		// ALL_IN_ONE, where the sole non-Karpenter node is the control plane:
		// on DEDICATED_COMPUTE and DISTRIBUTED the pre-existing pool is made
		// of worker nodes too, so the pods would land there, no claim would be
		// created, and the suite would fail on a perfectly healthy cluster.
		// Requiring karpenter.sh/nodepool to EXIST is the topology-independent
		// form: karpenter-core stamps it on every node it provisions and on no
		// other.
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      karpv1.NodePoolLabelKey,
							Operator: corev1.NodeSelectorOpExists,
						}},
					}},
				},
			},
		},
		// Short not-ready/unreachable tolerations: when a scenario kills a
		// node out from under a pod (the GC reap), the default 300s eviction
		// delay would dominate the wait bounds.
		Tolerations: []corev1.Toleration{
			{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptrInt64(30)},
			{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptrInt64(30)},
		},
		Containers: []corev1.Container{{
			Name:  "inflate",
			Image: "registry.k8s.io/pause:3.10",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(memory),
			}},
		}},
	}
	if onePodPerNode {
		// Add to the node affinity above, never replace it: dropping it would
		// let the pods schedule on the pre-existing pool and silently stop
		// exercising Karpenter.
		spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: podLabels},
				TopologyKey:   corev1.LabelHostname,
			}},
		}
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: f.namespace, Labels: f.labels()},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: podLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec:       spec,
			},
		},
	}
}

func ptrInt64(v int64) *int64 { return &v }

func (f *framework) scaleDeployment(ctx context.Context, name string, replicas int32) {
	f.t.Helper()
	deploy := &appsv1.Deployment{}
	if err := f.client.Get(ctx, types.NamespacedName{Namespace: f.namespace, Name: name}, deploy); err != nil {
		f.t.Fatalf("getting deployment %s: %v", name, err)
	}
	stored := deploy.DeepCopy()
	deploy.Spec.Replicas = &replicas
	if err := f.client.Patch(ctx, deploy, client.MergeFrom(stored)); err != nil {
		f.t.Fatalf("scaling deployment %s to %d: %v", name, replicas, err)
	}
}

// claimsOfPool lists the NodeClaims labeled for one NodePool.
func (f *framework) claimsOfPool(ctx context.Context, pool string) []karpv1.NodeClaim {
	f.t.Helper()
	claims := &karpv1.NodeClaimList{}
	if err := f.client.List(ctx, claims, client.MatchingLabels{karpv1.NodePoolLabelKey: pool}); err != nil {
		f.t.Fatalf("listing nodeclaims of pool %s: %v", pool, err)
	}
	return claims.Items
}

// decoyName names the garbage collection scenario's hand-made NodeGroup.
func (f *framework) decoyName() string { return f.prefix + "-decoy" }

// runNodeGroups lists NodeGroups whose name carries this run's prefix,
// failing the test on error — for scenario assertions.
func (f *framework) runNodeGroups(ctx context.Context) []ngv1.NodeGroup {
	f.t.Helper()
	out, err := f.tryRunNodeGroups(ctx)
	if err != nil {
		f.t.Fatalf("listing nodegroups: %v", err)
	}
	return out
}

// tryRunNodeGroups is the non-fatal variant for the cleanup path: cleanup is
// the last line of defense against leaked VMs and must never abort itself on
// a transient List error.
func (f *framework) tryRunNodeGroups(ctx context.Context) ([]ngv1.NodeGroup, error) {
	groups := &ngv1.NodeGroupList{}
	if err := f.client.List(ctx, groups); err != nil {
		return nil, err
	}
	var out []ngv1.NodeGroup
	for _, ng := range groups.Items {
		if strings.HasPrefix(ng.Name, f.prefix) {
			out = append(out, ng)
		}
	}
	return out, nil
}

// cleanupAll tears down everything the run created, in an order that works
// with the controller still alive: workloads first (pods release nodes),
// ownerless NodeGroups (nothing can drain those), then NodePools (karpenter
// drains claims and deletes NodeGroups), then NodeClasses (their finalizer
// waits on the claims), then a direct sweep of any NodeGroup left carrying
// the run prefix, and recheckAfter later a second look for one. An ownerless
// group met at any of these steps goes through deleteOwnerless, which fails
// the run for any but the decoy. It uses a fresh context so it still runs
// after the suite deadline expired, and never aborts on a transient API
// error — this is the last line of defense against VMs that bill hourly.
func (f *framework) cleanupAll() {
	f.t.Helper()
	if os.Getenv("E2E_KEEP") != "" {
		f.t.Logf("E2E_KEEP set: skipping cleanup (namespace %s, prefix %s)", f.namespace, f.prefix)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.namespace}}
	if err := f.client.Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
		f.t.Errorf("cleanup: deleting namespace %s: %v", f.namespace, err)
	}

	// Groups without a NodeClaim owner (the GC decoy, a group the platform
	// re-created) can only be deleted directly — karpenter never drains
	// them, and waiting on them would burn the whole graceful budget below.
	// ownerless holds the uids already dealt with, so no step reports or
	// waits on one twice.
	ownerless := map[types.UID]bool{}
	if groups, err := f.tryRunNodeGroups(ctx); err != nil {
		f.t.Errorf("cleanup: listing nodegroups for the ownerless sweep: %v", err)
	} else {
		f.sweepOwnerless(ctx, groups, ownerless)
	}

	pools := &karpv1.NodePoolList{}
	if err := f.client.List(ctx, pools, client.MatchingLabels{runLabelKey: f.runID}); err != nil {
		f.t.Errorf("cleanup: listing nodepools: %v", err)
	} else {
		for i := range pools.Items {
			if err := f.client.Delete(ctx, &pools.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				f.t.Errorf("cleanup: deleting nodepool %s: %v", pools.Items[i].Name, err)
			}
		}
	}

	// Wait for karpenter to drain every claim of this run — this is the
	// normal, graceful path that also deletes the NodeGroups (and VMs).
	deadline := 15 * time.Minute
	err := wait.PollUntilContextTimeout(ctx, 10*time.Second, deadline, true, func(ctx context.Context) (bool, error) {
		return f.drainDone(ctx, ownerless)
	})
	drained := !errors.Is(err, errControllerExited)
	switch {
	case !drained:
		f.t.Errorf("cleanup: the controller is dead, so nothing drains the claims of run %s: deleting their nodegroups directly — "+
			"run hack/e2e-cleanup.sh afterwards for the claims, nodes and nodeclasses left on their finalizers", f.runID)
	case err != nil:
		f.t.Errorf("cleanup: claims/nodegroups of run %s still present after %s", f.runID, deadline)
	}

	classes := &v1alpha1.CleverNodeClassList{}
	if err := f.client.List(ctx, classes, client.MatchingLabels{runLabelKey: f.runID}); err != nil {
		f.t.Errorf("cleanup: listing nodeclasses: %v", err)
	} else {
		for i := range classes.Items {
			if err := f.client.Delete(ctx, &classes.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				f.t.Errorf("cleanup: deleting nodeclass %s: %v", classes.Items[i].Name, err)
			}
		}
	}

	// Last-resort direct sweep: anything still carrying the run prefix. The
	// uids it sees tell the re-check a group that survived from one that
	// reappeared — as long as one of its two listings succeeded (swept).
	seen := map[types.UID]bool{}
	swept := false
	groups, err2 := f.tryRunNodeGroups(ctx)
	if err2 != nil {
		f.t.Errorf("cleanup: listing nodegroups for the final sweep: %v — LEFTOVERS MAY BILL HOURLY, run hack/e2e-cleanup.sh", err2)
	} else {
		swept = true
	}
	for i := range groups {
		ng := &groups[i]
		seen[ng.UID] = true
		if !ng.DeletionTimestamp.IsZero() {
			continue
		}
		if len(nodegroup.NodeClaimOwners(ng)) == 0 && !ownerless[ng.UID] {
			f.deleteOwnerless(ctx, ng, ownerless) // appeared after the graceful wait
			continue
		}
		if drained {
			f.t.Errorf("cleanup: nodegroup %s survived the graceful path, deleting directly (check why!)", ng.Name)
		} else {
			// Expected: the graceful path was skipped, the failure is already reported.
			f.t.Logf("cleanup: deleting nodegroup %s directly (the controller is dead)", ng.Name)
		}
		if err := f.client.Delete(ctx, ng); err != nil && !apierrors.IsNotFound(err) {
			f.t.Errorf("cleanup: direct delete of nodegroup %s failed: %v — DELETE IT MANUALLY, IT BILLS HOURLY", ng.Name, err)
		}
	}
	if leftovers, err := f.tryRunNodeGroups(ctx); err != nil {
		f.t.Logf("cleanup: listing nodegroups to verify the final sweep: %v", err)
	} else {
		swept = true
		var live []string
		for _, ng := range leftovers {
			seen[ng.UID] = true
			if ng.DeletionTimestamp.IsZero() {
				live = append(live, ng.Name)
			}
		}
		if len(live) > 0 {
			f.t.Errorf("cleanup: NODEGROUPS STILL PRESENT (billing hourly): %s — run hack/e2e-cleanup.sh", strings.Join(live, ", "))
		}
	}

	// Node objects can outlive their VM when a scenario bypassed the normal
	// termination flow (the GC reap deletes the group, nobody deletes the
	// node) — purely cosmetic, but don't litter the shared test cluster.
	nodes := &corev1.NodeList{}
	if err := f.client.List(ctx, nodes); err == nil {
		for i := range nodes.Items {
			if strings.HasPrefix(nodes.Items[i].Name, f.prefix) {
				if err := f.client.Delete(ctx, &nodes.Items[i]); err != nil && !apierrors.IsNotFound(err) {
					f.t.Errorf("cleanup: deleting orphaned node object %s: %v", nodes.Items[i].Name, err)
				}
			}
		}
	}

	f.recheckNodeGroups(seen, swept)
}

// drainDone is the graceful wait's condition: no NodeClaim of the run left
// and no live NodeGroup of the run backed by one. Groups already carrying a
// deletion timestamp are the platform's to finish, and an ownerless group is
// never karpenter's to drain: it goes through deleteOwnerless at once, so a
// group re-created during the wait is reported and deleted instead of holding
// the wait to its deadline. Nothing drains once the controller is dead:
// waiting would only delay the direct sweep by the whole budget.
func (f *framework) drainDone(ctx context.Context, ownerless map[types.UID]bool) (bool, error) {
	f.t.Helper()
	if f.ctrl.dead() {
		return false, errControllerExited
	}
	groups, err := f.tryRunNodeGroups(ctx)
	if err != nil {
		return false, nil
	}
	if f.sweepOwnerless(ctx, groups, ownerless) {
		return false, nil
	}
	claims := &karpv1.NodeClaimList{}
	if err := f.client.List(ctx, claims); err != nil {
		return false, nil
	}
	for _, c := range claims.Items {
		if strings.HasPrefix(c.Name, f.prefix) {
			return false, nil
		}
	}
	return true, nil
}

// sweepOwnerless hands every live group of groups that has no NodeClaim
// owner, and that no earlier step dealt with, to deleteOwnerless. It reports
// whether a live group backed by a NodeClaim remains: karpenter drains those.
func (f *framework) sweepOwnerless(ctx context.Context, groups []ngv1.NodeGroup, ownerless map[types.UID]bool) (claimBacked bool) {
	f.t.Helper()
	for i := range groups {
		ng := &groups[i]
		if !ng.DeletionTimestamp.IsZero() || ownerless[ng.UID] {
			continue // terminating, or dealt with (the final sweep retries a failed delete)
		}
		if len(nodegroup.NodeClaimOwners(ng)) > 0 {
			claimBacked = true
			continue
		}
		f.deleteOwnerless(ctx, ng, ownerless)
	}
	return claimBacked
}

// deleteOwnerless deletes a live NodeGroup of the run that has no NodeClaim
// owner, and records its uid in ownerless. Every group karpenter launches
// carries the owner reference of its NodeClaim, and the only ownerless group
// the suite makes is the garbage collection decoy, which carries the run
// label. Any other is a group the platform re-created after a deletion (seen
// once, about 6 minutes after it, during an upstream incident: same name, a
// new uid, no labels or owner references, a fresh VM): no provider path acts
// on it, so it fails the run, logged with what tells it apart.
func (f *framework) deleteOwnerless(ctx context.Context, ng *ngv1.NodeGroup, ownerless map[types.UID]bool) {
	f.t.Helper()
	ownerless[ng.UID] = true
	if ng.Name == f.decoyName() && ng.Labels[runLabelKey] == f.runID {
		f.t.Logf("cleanup: deleting the garbage collection decoy %s directly", ng.Name)
	} else {
		f.t.Errorf("cleanup: nodegroup %s has no NodeClaim owner (uid %s, created %s, labels %v), deleting it — "+
			"the run makes no ownerless group but its decoy, so the platform most likely re-created a group deleted earlier in the run",
			ng.Name, ng.UID, ng.CreationTimestamp.UTC().Format(time.RFC3339), ng.Labels)
	}
	if err := f.client.Delete(ctx, ng); err != nil && !apierrors.IsNotFound(err) {
		f.t.Errorf("cleanup: deleting ownerless nodegroup %s: %v", ng.Name, err)
	}
}

// recheckNodeGroups looks for the run's NodeGroups once more, recheckAfter
// the cleanup, which only checked their absence at one instant. The platform
// was once seen re-creating a deleted NodeGroup about 6 minutes after its
// deletion, during an upstream incident: same name, a new uid, no labels or
// owner references, and a fresh VM. No provider path acts on a group without
// the managed label, so nothing would ever delete it: a live group of the run
// at this point fails the run and is deleted again. seen holds the uids the
// cleanup's final sweep listed; swept is false when neither of its listings
// succeeded, and seen then tells nothing.
func (f *framework) recheckNodeGroups(seen map[types.UID]bool, swept bool) {
	f.t.Helper()
	if f.recheckAfter <= 0 {
		return
	}
	f.t.Logf("cleanup: looking again for nodegroups of run %s in %s (E2E_CLEANUP_RECHECK)", f.runID, f.recheckAfter)
	time.Sleep(f.recheckAfter)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	groups, err := f.tryRunNodeGroups(ctx)
	if err != nil {
		f.t.Errorf("cleanup re-check: listing nodegroups: %v — run hack/e2e-cleanup.sh", err)
		return
	}
	for i := range groups {
		ng := &groups[i]
		switch {
		case !ng.DeletionTimestamp.IsZero():
			f.t.Logf("cleanup re-check: nodegroup %s is still terminating; its VM bills until the platform finishes the teardown (hack/e2e-cleanup.sh waits for it)", ng.Name)
			continue
		case !swept:
			f.t.Errorf("cleanup re-check: nodegroup %s is still present (uid %s, created %s, labels %v, nodeclaim owners %v), deleting it — "+
				"the final sweep could not list nodegroups, so whether it survived the cleanup or reappeared is unknown",
				ng.Name, ng.UID, ng.CreationTimestamp.UTC().Format(time.RFC3339), ng.Labels, nodegroup.NodeClaimOwners(ng))
		case seen[ng.UID]:
			f.t.Errorf("cleanup re-check: nodegroup %s (uid %s) survived the cleanup's direct delete, trying again", ng.Name, ng.UID)
		default:
			f.t.Errorf("cleanup re-check: nodegroup %s REAPPEARED after the cleanup (uid %s, created %s, labels %v, nodeclaim owners %v), deleting it again — "+
				"a group the platform re-created carries neither labels nor owners", ng.Name, ng.UID, ng.CreationTimestamp.UTC().Format(time.RFC3339), ng.Labels, nodegroup.NodeClaimOwners(ng))
		}
		if err := f.client.Delete(ctx, ng); err != nil && !apierrors.IsNotFound(err) {
			f.t.Errorf("cleanup re-check: deleting nodegroup %s failed: %v — DELETE IT MANUALLY, IT BILLS HOURLY", ng.Name, err)
		}
	}
}
