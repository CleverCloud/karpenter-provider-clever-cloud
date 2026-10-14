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

package garbagecollection

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/karpenter/pkg/events"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics/metricstest"
)

func deletionSnapshot(deletedAt time.Time) ngv1.NodeGroup {
	return ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{
		Name:              "deletion-probe",
		UID:               "uid-original",
		DeletionTimestamp: &metav1.Time{Time: deletedAt},
		Finalizers:        []string{"test.example.com/cleanup"},
	}}
}

// The sweep already passes one observation time to the reporting helper.
// Exercise that boundary directly, without sleeps or changing GC scheduling.
func TestDeletionOverdueStartsAtFiveMinutes(t *testing.T) {
	deletedAt := time.Date(2026, time.October, 14, 10, 0, 0, 0, time.UTC)
	groups := []ngv1.NodeGroup{deletionSnapshot(deletedAt)}
	recorded := record.NewFakeRecorder(10)
	ctrl := &Controller{recorder: events.NewRecorder(recorded)}
	for _, test := range []struct {
		age        time.Duration
		wantGauge  float64
		wantEvents int
	}{
		{40 * time.Second, 0, 0},
		{4*time.Minute + 59*time.Second, 0, 0},
		{5 * time.Minute, 1, 1},
		{7 * time.Minute, 1, 1}, // changing age must not bypass event deduplication
	} {
		ctrl.reportOverdueDeletions(context.Background(), groups, deletedAt.Add(test.age))
		if got := metricstest.Value(t, "karpenter_clevercloud_nodegroup_deletion_overdue"); got != test.wantGauge {
			t.Errorf("at age %s, gauge = %v, want %v", test.age, got, test.wantGauge)
		}
		if got := len(recorded.Events); got != test.wantEvents {
			t.Fatalf("at age %s, events = %d, want %d", test.age, got, test.wantEvents)
		}
	}
	if event := <-recorded.Events; !strings.Contains(event, "Warning NodeGroupDeletionOverdue") || !strings.Contains(event, "Observed NodeGroup deletion-probe still present 5m0s") {
		t.Errorf("unexpected event at the threshold: %s", event)
	}
}

func TestDeletionOverdueReportsAReplacementUID(t *testing.T) {
	now := time.Date(2026, time.October, 14, 10, 0, 0, 0, time.UTC)
	groups := []ngv1.NodeGroup{deletionSnapshot(now.Add(-6 * time.Minute))}
	recorded := record.NewFakeRecorder(10)
	ctrl := &Controller{recorder: events.NewRecorder(recorded)}
	logs := 0
	ctx := log.IntoContext(context.Background(), funcr.New(func(_, line string) {
		if strings.Contains(line, "nodegroup deletion remains overdue") {
			logs++
		}
	}, funcr.Options{}))
	// Consecutive successful lists can contain two incarnations of the same
	// name without an intervening list pruning the old log state. The real
	// recorder must publish for the new UID inside its hourly dedupe window.
	for _, test := range []struct {
		uid  types.UID
		want int
	}{{"uid-original", 1}, {"uid-original", 1}, {"uid-replacement", 2}, {"uid-replacement", 2}} {
		groups[0].UID = test.uid
		ctrl.reportOverdueDeletions(ctx, groups, now)
		if got := len(recorded.Events); got != test.want || logs != test.want {
			t.Fatalf("UID %s: events/logs = %d/%d, want %d/%d", test.uid, got, logs, test.want, test.want)
		}
		if got := metricstest.Value(t, "karpenter_clevercloud_nodegroup_deletion_overdue"); got != 1 {
			t.Fatalf("replacement changed overdue count to %v, want 1", got)
		}
	}
}

func TestDeletionOverdueDiagnosticDescribesAnObservation(t *testing.T) {
	now := time.Date(2026, time.October, 14, 10, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name       string
		finalizers []string
		wantAction string
		wantCKE    bool
	}{
		{"platform and another controller", []string{ckeFinalizer, "test.example.com/cleanup"}, "also check the CKE cluster state", true},
		{"another controller", []string{"test.example.com/cleanup"}, "controllers responsible for the remaining finalizers", false},
		{"unknown finalizer", []string{"future-clever-nodegroup-operator"}, "controllers responsible for the remaining finalizers", false},
		{"no remaining finalizers", nil, "No finalizers remain in this observation", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ng := deletionSnapshot(now.Add(-7*time.Minute - 12*time.Second))
			ng.Finalizers = test.finalizers
			ng.Status.Phase = "DeletionRequested"
			ng.Status.Conditions = []ngv1.NodeGroupCondition{
				{Type: ngv1.ConditionTypeReady, Status: corev1.ConditionTrue, Reason: "Synced"},
				{Type: "Terminating", Status: corev1.ConditionTrue, Reason: "DeletionRequested"},
				{Type: ngv1.ConditionTypeReconcileFailed, Status: corev1.ConditionFalse, Reason: "InactiveFailure"},
			}
			recorded := record.NewFakeRecorder(1)
			ctrl := &Controller{recorder: events.NewRecorder(recorded)}
			ctrl.reportOverdueDeletions(context.Background(), []ngv1.NodeGroup{ng}, now)
			if len(recorded.Events) != 1 {
				t.Fatal("expected one deletion diagnostic")
			}
			event := <-recorded.Events
			for _, detail := range []string{"Observed NodeGroup deletion-probe still present 7m12s", `last reported phase "DeletionRequested"`, "Ready (Synced)", "Terminating (DeletionRequested)", test.wantAction, "kubectl get nodegroup deletion-probe -o yaml", "VM deletion is not confirmed"} {
				if !strings.Contains(event, detail) {
					t.Errorf("diagnostic lacks %q: %s", detail, event)
				}
			}
			if strings.Contains(event, "InactiveFailure") || strings.Contains(event, "Clever Cloud support") != test.wantCKE {
				t.Errorf("unexpected failure or platform-support diagnostic: %s", event)
			}
			if test.wantCKE && !strings.Contains(event, "presence does not establish the cause") {
				t.Errorf("platform finalizer must not be presented as proof of the cause: %s", event)
			}
		})
	}
}
