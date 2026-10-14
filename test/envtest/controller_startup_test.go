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
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Exercise main, rather than a helper that main could accidentally stop using.
// The real binary runs on an isolated apiserver so its core controllers cannot
// interfere with the fixtures of the provider-controller integration suite.
func TestControllerBinaryUsesConfiguredDiskReference(t *testing.T) {
	controlPlane := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../deploy/crds", "testdata"},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := controlPlane.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := controlPlane.Stop(); err != nil {
			t.Error(err)
		}
	})
	kube, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "binary-disk-reference"}}
	if err := kube.Create(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	node.Status.Capacity = corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("23Gi")}
	node.Status.Allocatable = corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("19Gi")}
	if err := kube.Status().Update(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	kubeconfig := filepath.Join(dir, "kubeconfig")
	if err := clientcmd.WriteToFile(clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{"test": {Server: cfg.Host, CertificateAuthorityData: cfg.CAData}},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{"test": {
			ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData,
		}},
		Contexts: map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test"}}, CurrentContext: "test",
	}, kubeconfig); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "controller")
	build := exec.Command("go", "build", "-o", binary, "../../cmd/controller")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building controller: %v\n%s", err, out)
	}
	for _, reference := range []string{"", node.Name} {
		name := "static"
		if reference != "" {
			name = "named-node"
		}
		t.Run(name, func(t *testing.T) {
			metricsURL := runControllerBinary(t, binary, kubeconfig, reference)
			eventually(t, 45*time.Second, func(ctx context.Context) error {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
				if err != nil {
					return err
				}
				response, err := (&http.Client{Timeout: time.Second}).Do(req)
				if err != nil {
					return err
				}
				defer response.Body.Close()
				data, err := io.ReadAll(response.Body)
				if err != nil {
					return err
				}
				if response.StatusCode != http.StatusOK || !strings.Contains(string(data), "karpenter_build_info") {
					return fmt.Errorf("controller metrics are not ready")
				}
				prefix := "karpenter_clevercloud_instancetype_host_ephemeral_storage_"
				if reference == "" {
					if strings.Contains(string(data), prefix) {
						return fmt.Errorf("static mode initialized the reference controller")
					}
					return nil
				}
				for suffix, want := range map[string]float64{"ready": 1, "capacity_bytes": 23 * (1 << 30), "allocatable_bytes": 19 * (1 << 30)} {
					found := false
					for _, line := range strings.Split(string(data), "\n") {
						fields := strings.Fields(line)
						if len(fields) == 2 && fields[0] == prefix+suffix {
							got, err := strconv.ParseFloat(fields[1], 64)
							found = err == nil && got == want
						}
					}
					if !found {
						return fmt.Errorf("%s%s has not reached %g", prefix, suffix, want)
					}
				}
				return nil
			})
		})
	}
}

func runControllerBinary(t *testing.T, binary, kubeconfig, nodeName string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(t.TempDir(), "controller.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = logFile.Close()
		if t.Failed() {
			if data, err := os.ReadFile(logPath); err == nil {
				t.Logf("controller log:\n%s", data)
			}
		}
	})
	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+kubeconfig, "NODE_NAME="+nodeName,
		"FLAVORS_CONFIG_PATH=", "DISABLE_LEADER_ELECTION=true", "SYSTEM_NAMESPACE=default",
		"METRICS_PORT="+strconv.Itoa(port), "HEALTH_PROBE_PORT=0")
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var exitErr error
	go func() { exitErr = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		select {
		case <-done:
			t.Errorf("controller exited unexpectedly: %v", exitErr)
			return
		default:
		}
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
			if exitErr != nil {
				t.Errorf("controller shutdown: %v", exitErr)
			}
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("controller did not stop after interrupt")
		}
	})
	return fmt.Sprintf("http://127.0.0.1:%d/metrics", port)
}
