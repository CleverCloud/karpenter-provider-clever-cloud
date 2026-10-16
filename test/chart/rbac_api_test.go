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

package chart

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	sigsyaml "sigs.k8s.io/yaml"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// chartServiceAccount is the identity `helm template` without --namespace
// binds the chart's RBAC to.
const chartServiceAccount = "system:serviceaccount:default:karpenter"

// applyChartRBAC creates every ClusterRole, ClusterRoleBinding, Role and
// RoleBinding the chart renders with its default values.
func applyChartRBAC(ctx context.Context, t *testing.T, kubeClient client.Client) {
	t.Helper()
	reader := yaml.NewYAMLReader(bufio.NewReader(bytes.NewReader([]byte(helmTemplate(t)))))
	applied := 0
	for {
		doc, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("splitting rendered manifest: %v", err)
		}
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := sigsyaml.Unmarshal(doc, &meta); err != nil {
			continue
		}
		var obj client.Object
		switch meta.Kind {
		case "ClusterRole":
			obj = &rbacv1.ClusterRole{}
		case "ClusterRoleBinding":
			obj = &rbacv1.ClusterRoleBinding{}
		case "Role":
			obj = &rbacv1.Role{}
		case "RoleBinding":
			obj = &rbacv1.RoleBinding{}
		default:
			continue
		}
		if err := sigsyaml.Unmarshal(doc, obj); err != nil {
			t.Fatalf("decoding %s: %v", meta.Kind, err)
		}
		if ns := obj.GetNamespace(); ns != "" {
			if err := kubeClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil && !apierrors.IsAlreadyExists(err) {
				t.Fatal(err)
			}
		}
		if err := kubeClient.Create(ctx, obj); err != nil {
			t.Fatalf("creating %s %s: %v", meta.Kind, obj.GetName(), err)
		}
		applied++
	}
	if applied == 0 {
		t.Fatal("the chart rendered no RBAC object")
	}
}

// TestChartRBACLetsCoreDeduceDefaultSpreadSelectors runs, under the chart's
// RBAC, the read karpenter-core 1.15 added for SCHEDULER_CONFIG: pods with no
// topology spread constraint of their own get the configured defaults, with a
// selector deduced from the Services selecting them — listed through the
// manager's informer cache, like every core read. The cache waits for the
// informer to sync before answering, so a ClusterRole without list/watch on
// services does not fail the read: it blocks it, and with it the provisioning
// and consolidation passes that run the injection, cluster-wide (reproduced
// live with the chart of v0.14.0).
func TestChartRBACLetsCoreDeduceDefaultSpreadSelectors(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run with make test-envtest")
	}
	environment := &envtest.Environment{}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})
	admin, err := client.New(config, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	applyChartRBAC(ctx, t, admin)
	user, err := environment.AddUser(envtest.User{
		Name:   chartServiceAccount,
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web", "tier": "front"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "web"}}},
			},
		},
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "db"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "db", "tier": "front"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "db", Image: "db"}}},
			},
		},
	}
	replicationController := &corev1.ReplicationController{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default"},
		Spec: corev1.ReplicationControllerSpec{
			Selector: map[string]string{"app": "legacy"},
			Template: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "legacy", "tier": "front"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "legacy", Image: "legacy"}}},
			},
		},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Selector: map[string]string{"tier": "front"}, Ports: []corev1.ServicePort{{Port: 80}}},
	}
	for _, obj := range []client.Object{replicaSet, statefulSet, replicationController, service} {
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}

	informers, err := cache.New(user.Config(), cache.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	cacheCtx, stopCache := context.WithCancel(ctx)
	defer stopCache()
	go func() { _ = informers.Start(cacheCtx) }()
	// A read before the cache has started fails at once, and the injector
	// swallows the error: wait, so the test measures the reads, not the race.
	if !informers.WaitForCacheSync(cacheCtx) {
		t.Fatal("the informer cache did not start")
	}
	kubeClient, err := client.New(user.Config(), client.Options{Scheme: scheme.Scheme, Cache: &client.CacheOptions{Reader: informers}})
	if err != nil {
		t.Fatal(err)
	}

	schedulerConfig, err := options.ParseSchedulerConfiguration(`{"podTopologySpread":{"defaultConstraints":[{"maxSkew":1,"topologyKey":"kubernetes.io/hostname","whenUnsatisfiable":"ScheduleAnyway"}]}}`)
	if err != nil {
		t.Fatal(err)
	}
	const deadline = 15 * time.Second
	injectCtx, stopInject := context.WithTimeout(options.ToContext(ctx, &options.Options{SchedulerConfig: schedulerConfig}), deadline)
	defer stopInject()
	// One pod per kind of owner the injector reads a selector from.
	owned := func(name, app, apiVersion, kind string, owner client.Object) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", Labels: map[string]string{"app": app, "tier": "front"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: apiVersion, Kind: kind, Name: owner.GetName(), UID: owner.GetUID(), Controller: new(true)}},
		}}
	}
	pods := []*corev1.Pod{
		owned("web-1", "web", "apps/v1", "ReplicaSet", replicaSet),
		owned("db-0", "db", "apps/v1", "StatefulSet", statefulSet),
		owned("legacy-1", "legacy", "v1", "ReplicationController", replicationController),
	}
	start := time.Now()
	scheduling.NewDefaultTopologySpreadInjector(kubeClient).Inject(injectCtx, pods)
	if elapsed := time.Since(start); elapsed > deadline/2 {
		t.Fatalf("the injection blocked for %s under the chart's RBAC: a cache read it needs never syncs", elapsed.Round(time.Second))
	}
	for _, pod := range pods {
		if len(pod.Spec.TopologySpreadConstraints) != 1 {
			t.Errorf("%s: no default constraint injected", pod.Name)
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(pod.Spec.TopologySpreadConstraints[0].LabelSelector)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case !selector.Matches(labels.Set(pod.Labels)):
			t.Errorf("%s: selector %s does not select the pod itself", pod.Name, selector)
		case selector.Matches(labels.Set{"app": pod.Labels["app"]}):
			t.Errorf("%s: selector %s ignores the Service's tier=front: the Services were not read", pod.Name, selector)
		case selector.Matches(labels.Set{"app": "other", "tier": "front"}):
			t.Errorf("%s: selector %s ignores its owner's selector: the owner was not read", pod.Name, selector)
		}
	}
}

// TestControllerBinaryRunsUnderTheChartRBAC starts the real controller binary
// authenticated as the chart's ServiceAccount, bound to nothing but the RBAC
// the chart renders, with leader election on as in the cluster: every core and
// provider controller must start, which needs every informer it waits on to
// sync, and no read may be refused. envtest and `make e2e` otherwise run the
// binary with admin credentials, so a rule a karpenter-core bump needs would
// only show in production. The node repair configurations also prove that
// main.go's repair wiring starts: core panics at startup on an invalid policy
// set or a cluster state built without the matcher. IGNORE_DRA_REQUESTS=false
// makes core watch the resource.k8s.io objects.
func TestControllerBinaryRunsUnderTheChartRBAC(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run with make test-envtest")
	}
	environment := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(repoRoot(t), "deploy", "crds"), filepath.Join(repoRoot(t), "test", "envtest", "testdata")},
		ErrorIfCRDPathMissing: true,
	}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})
	admin, err := client.New(config, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	applyChartRBAC(ctx, t, admin)
	user, err := environment.AddUser(envtest.User{
		Name:   chartServiceAccount,
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	kubeconfig := filepath.Join(dir, "kubeconfig")
	userConfig := user.Config()
	if err := clientcmd.WriteToFile(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"test": {Server: userConfig.Host, CertificateAuthorityData: userConfig.CAData}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"test": {ClientCertificateData: userConfig.CertData, ClientKeyData: userConfig.KeyData}},
		Contexts:       map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test"}},
		CurrentContext: "test",
	}, kubeconfig); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "controller")
	if out, err := exec.Command("go", "build", "-o", binary, filepath.Join(repoRoot(t), "cmd", "controller")).CombinedOutput(); err != nil {
		t.Fatalf("building controller: %v\n%s", err, out)
	}

	for _, tc := range []struct {
		name string
		env  []string
	}{
		{"chart defaults", []string{"FEATURE_GATES=NodeRepair=false"}},
		{"node repair", []string{"FEATURE_GATES=NodeRepair=true"}},
		{"legacy node repair", []string{"FEATURE_GATES=NodeRepair=true", "LEGACY_NODE_REPAIR=true"}},
		{"dynamic resource allocation", []string{"FEATURE_GATES=NodeRepair=false", "IGNORE_DRA_REQUESTS=false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Each run must win the lease its predecessor held.
			lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "karpenter-leader-election", Namespace: "default"}}
			if err := admin.Delete(ctx, lease); err != nil && !apierrors.IsNotFound(err) {
				t.Fatal(err)
			}
			log := runControllerAs(t, binary, kubeconfig, tc.env)
			var started int
			deadline := time.Now().Add(2 * time.Minute)
			for stable := 0; stable < 3; time.Sleep(time.Second) {
				data := log()
				controllers := strings.Count(data, `"message":"Starting Controller"`)
				workers := strings.Count(data, `"message":"Starting workers"`)
				if refused := strings.Contains(strings.ToLower(data), "forbidden"); refused {
					t.Fatal("the controller was refused a request under the chart's RBAC")
				}
				switch {
				case strings.Contains(data, "Successfully acquired lease") && controllers > 0 && workers == controllers && controllers == started:
					stable++
				default:
					stable = 0
				}
				started = controllers
				if time.Now().After(deadline) {
					t.Fatalf("controllers did not all start: %d started, %d running workers (a cache sync blocked on a read the chart does not grant?)", controllers, workers)
				}
			}
			time.Sleep(5 * time.Second)
			if strings.Contains(strings.ToLower(log()), "forbidden") {
				t.Fatal("the controller was refused a request under the chart's RBAC")
			}
		})
	}
}

// runControllerAs runs the controller binary with the given extra environment
// until the test ends, failing it if the process exits on its own. It returns
// a reader of the process's log.
func runControllerAs(t *testing.T, binary, kubeconfig string, env []string) func() string {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "controller.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	read := func() string {
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	cmd := exec.Command(binary)
	cmd.Env = append(append(os.Environ(), "KUBECONFIG="+kubeconfig, "NODE_NAME=", "FLAVORS_CONFIG_PATH=", "SCHEDULER_CONFIG=",
		"SYSTEM_NAMESPACE=default", "LEADER_ELECTION_NAMESPACE=default", "DISABLE_LEADER_ELECTION=false",
		"METRICS_PORT=0", "HEALTH_PROBE_PORT=0"), env...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var exitErr error
	go func() { exitErr = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		defer func() { _ = logFile.Close() }()
		select {
		case <-done:
			t.Errorf("controller exited unexpectedly: %v", exitErr)
		default:
			_ = cmd.Process.Signal(os.Interrupt)
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Error("controller did not stop after interrupt")
			}
		}
		if t.Failed() {
			t.Logf("controller log:\n%s", read())
		}
	})
	return func() string {
		select {
		case <-done:
			t.Fatalf("controller exited: %v\n%s", exitErr, read())
		default:
		}
		return read()
	}
}
