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

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/garbagecollection"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/instancetypecapacity"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/nodeclass"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/providerid"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

func NewControllers(kubeClient client.Client, nodeGroupProvider *nodegroup.Provider, instanceTypeProvider *instancetype.Provider) []controller.Controller {
	return []controller.Controller{
		providerid.NewController(kubeClient),
		garbagecollection.NewController(kubeClient, nodeGroupProvider),
		nodeclass.NewController(kubeClient),
		instancetypecapacity.NewController(kubeClient, instanceTypeProvider),
	}
}
