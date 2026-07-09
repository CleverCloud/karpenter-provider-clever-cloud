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

// Package controllers wires the Clever Cloud specific controllers that run
// alongside the karpenter-core controllers.
package controllers

import (
	"github.com/awslabs/operatorpkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/karpenter/pkg/events"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/garbagecollection"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/instancetypecapacity"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/nodeclass"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/pricing"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/providerid"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

// NewControllers wires the Clever Cloud controllers. uncached reads straight
// from the API server (the GC confirms destructive decisions with it);
// pricingController is optional (nil when the dynamic price refresher is
// disabled).
func NewControllers(kubeClient client.Client, uncached client.Reader, recorder events.Recorder, nodeGroupProvider *nodegroup.Provider, instanceTypeProvider *instancetype.Provider, pricingController *pricing.Controller) []controller.Controller {
	controllers := []controller.Controller{
		providerid.NewController(kubeClient),
		garbagecollection.NewController(kubeClient, uncached, nodeGroupProvider, recorder),
		nodeclass.NewController(kubeClient),
		instancetypecapacity.NewController(kubeClient, instanceTypeProvider),
	}
	if pricingController != nil {
		controllers = append(controllers, pricingController)
	}
	return controllers
}
