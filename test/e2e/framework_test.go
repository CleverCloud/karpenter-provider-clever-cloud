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
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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

// framework carries everything one suite run needs: a pinned kubeconfig, a
// direct (uncached) client, the controller subprocess and unique names.
type framework struct {
	t              *testing.T
	client         client.Client
	clientset      *kubernetes.Clientset
	restCfg        *rest.Config
	kubeconfigPath string

	runID     string
	prefix    string // "e2e-<runID>", prefixes every cluster-scoped object name
	namespace string

	metricsPort int
	healthPort  int
	artifacts   string
	logPath     string
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
		t:           t,
		runID:       runID,
		prefix:      "e2e-" + runID,
		namespace:   "e2e-" + runID,
		metricsPort: envInt("E2E_METRICS_PORT", 8090),
		healthPort:  envInt("E2E_HEALTH_PORT", 8091),
		artifacts:   envString("E2E_ARTIFACTS", filepath.Join(os.TempDir(), "karpenter-e2e")),
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

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
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

func repoRoot(t *testing.T) string {
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
// every manual validation run. It returns a stop function that must run after
// cleanup (cleanup needs a live controller to drain NodeClaims).
func (f *framework) startController(ctx context.Context) func() {
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
		// Deterministic catalog: the suite tests the provider, not the public
		// pricing API's availability.
		"PRICING_REFRESH_ENABLED=false",
		"LOG_LEVEL=debug",
		fmt.Sprintf("METRICS_PORT=%d", f.metricsPort),
		fmt.Sprintf("HEALTH_PROBE_PORT=%d", f.healthPort),
	)
	if err := cmd.Start(); err != nil {
		f.t.Fatalf("starting controller: %v", err)
	}
	f.t.Logf("controller started (pid %d, log %s)", cmd.Process.Pid, f.logPath)

	healthURL := fmt.Sprintf("http://127.0.0.1:%d/healthz", f.healthPort)
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		resp, err := http.Get(healthURL)
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK, nil
	}); err != nil {
		_ = cmd.Process.Kill()
		f.dumpControllerLog()
		f.t.Fatalf("controller never became healthy on %s: %v", healthURL, err)
	}

	return func() {
		// SIGINT lets the manager stop cleanly; escalate if it lingers.
		_ = cmd.Process.Signal(syscall.SIGINT)
		done := make(chan struct{})
		go func() { _, _ = cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
		}
		_ = logFile.Close()
		if f.t.Failed() {
			f.dumpControllerLog()
		}
	}
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
// suite-context deadline aborts them promptly and cleanup still runs.
func (f *framework) eventually(ctx context.Context, timeout time.Duration, what string, cond func(ctx context.Context) (bool, string)) {
	f.t.Helper()
	start := time.Now()
	var last string
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var done bool
		done, last = cond(ctx)
		return done, nil
	})
	if err != nil {
		f.t.Fatalf("%s: not reached within %s (last: %s)", what, timeout, last)
	}
	f.t.Logf("%s: reached in %s", what, time.Since(start).Round(time.Second))
}

// metric returns the summed value of a provider metric across its label
// variants, scraped from the out-of-cluster controller's endpoint.
func (f *framework) metric(name string) float64 {
	f.t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", f.metricsPort))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var total float64
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
		}
	}
	return total
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
		NodeSelector:                  map[string]string{v1alpha1.NodeRoleLabelKey: v1alpha1.NodeRoleWorker},
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
		spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: podLabels},
				TopologyKey:   corev1.LabelHostname,
			}},
		}}
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
// the run prefix. It uses a fresh context so it still runs after the suite
// deadline expired, and never aborts on a transient API error — this is the
// last line of defense against VMs that bill hourly.
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

	// Hand-made groups without a NodeClaim owner (the GC decoy) can only be
	// deleted directly — karpenter never drains them, and waiting on them
	// would burn the whole graceful budget below.
	if groups, err := f.tryRunNodeGroups(ctx); err != nil {
		f.t.Errorf("cleanup: listing nodegroups for the ownerless sweep: %v", err)
	} else {
		for i := range groups {
			ng := &groups[i]
			if len(nodegroup.NodeClaimOwners(ng)) == 0 && ng.DeletionTimestamp.IsZero() {
				f.t.Logf("cleanup: deleting ownerless nodegroup %s directly", ng.Name)
				if err := f.client.Delete(ctx, ng); err != nil && !apierrors.IsNotFound(err) {
					f.t.Errorf("cleanup: deleting ownerless nodegroup %s: %v", ng.Name, err)
				}
			}
		}
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
	// Groups already carrying a deletion timestamp are the platform's to
	// finish; only claim-backed live groups are worth the graceful wait.
	deadline := 15 * time.Minute
	err := wait.PollUntilContextTimeout(ctx, 10*time.Second, deadline, true, func(ctx context.Context) (bool, error) {
		claims := &karpv1.NodeClaimList{}
		if err := f.client.List(ctx, claims); err != nil {
			return false, nil
		}
		for _, c := range claims.Items {
			if strings.HasPrefix(c.Name, f.prefix) {
				return false, nil
			}
		}
		groups, err := f.tryRunNodeGroups(ctx)
		if err != nil {
			return false, nil
		}
		for _, ng := range groups {
			if ng.DeletionTimestamp.IsZero() {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
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

	// Last-resort direct sweep: anything still carrying the run prefix.
	groups, err2 := f.tryRunNodeGroups(ctx)
	if err2 != nil {
		f.t.Errorf("cleanup: listing nodegroups for the final sweep: %v — LEFTOVERS MAY BILL HOURLY, run hack/e2e-cleanup.sh", err2)
	}
	for i := range groups {
		ng := &groups[i]
		if !ng.DeletionTimestamp.IsZero() {
			continue
		}
		f.t.Errorf("cleanup: nodegroup %s survived the graceful path, deleting directly (check why!)", ng.Name)
		if err := f.client.Delete(ctx, ng); err != nil && !apierrors.IsNotFound(err) {
			f.t.Errorf("cleanup: direct delete of nodegroup %s failed: %v — DELETE IT MANUALLY, IT BILLS HOURLY", ng.Name, err)
		}
	}
	if leftovers, err := f.tryRunNodeGroups(ctx); err == nil {
		var live []string
		for _, ng := range leftovers {
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
}
