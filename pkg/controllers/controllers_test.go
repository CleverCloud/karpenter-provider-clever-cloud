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

package controllers_test

import (
	"fmt"
	"maps"
	"testing"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/karpenter/pkg/events"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

type noopRecorder struct{}

func (noopRecorder) Publish(...events.Event) {}

// TestNewControllersWiresEveryProviderController pins the list main.go
// registers. Each provider controller exists for a CKE quirk, and one dropped
// from this list still compiles and passes its own package's tests while its
// duty silently stops: the GC's safety net against orphaned VMs, the
// providerID stamp core matches nodes by, the NodeGroup status watch, the
// NodeClass finalizer, the observed-capacity feedback.
func TestNewControllersWiresEveryProviderController(t *testing.T) {
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	instanceTypeProvider := instancetype.NewProvider("par", nil, nil)
	nodeGroupProvider := nodegroup.NewProvider(kubeClient, noopRecorder{}, instanceTypeProvider, clock.RealClock{})

	got := map[string]int{}
	for _, c := range controllers.NewControllers(kubeClient, kubeClient, noopRecorder{}, nodeGroupProvider, instanceTypeProvider) {
		got[fmt.Sprintf("%T", c)]++
	}
	want := map[string]int{
		"*garbagecollection.Controller":    1,
		"*instancetypecapacity.Controller": 1,
		"*nodeclass.Controller":            1,
		"*nodegroupstatus.Controller":      1,
		"*providerid.Controller":           1,
	}
	if !maps.Equal(got, want) {
		t.Errorf("NewControllers wired %v, want %v", got, want)
	}
}
