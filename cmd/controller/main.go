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

package main

import (
	"os"

	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/karpenter/pkg/utils/env"

	"sigs.k8s.io/karpenter/pkg/cloudprovider/overlay"
	corecontrollers "sigs.k8s.io/karpenter/pkg/controllers"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator"

	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

func main() {
	ctx, op := operator.NewOperator()

	region := env.WithDefaultString("CLEVER_CLOUD_REGION", "par")

	// FLAVORS_CONFIG_PATH points at a YAML flavor catalog (mounted from the
	// chart's ConfigMap). When unset, the built-in DefaultFlavors are used.
	var flavors []instancetype.Flavor
	if flavorsPath := env.WithDefaultString("FLAVORS_CONFIG_PATH", ""); flavorsPath != "" {
		var err error
		if flavors, err = instancetype.LoadFlavorsFromFile(flavorsPath); err != nil {
			log.FromContext(ctx).Error(err, "loading flavor catalog")
			os.Exit(1)
		}
	}

	instanceTypeProvider := instancetype.NewProvider(region, flavors)
	nodeGroupProvider := nodegroup.NewProvider(op.GetClient())
	cleverCloudProvider := cloudprovider.New(op.GetClient(), instanceTypeProvider, nodeGroupProvider)
	decoratedCloudProvider := overlay.Decorate(cleverCloudProvider, op.GetClient(), op.InstanceTypeStore)
	clusterState := state.NewCluster(op.Clock, op.GetClient(), decoratedCloudProvider)

	op.
		WithControllers(ctx, corecontrollers.NewControllers(
			ctx,
			op.Manager,
			op.Clock,
			op.GetClient(),
			op.EventRecorder,
			decoratedCloudProvider,
			cleverCloudProvider,
			clusterState,
			op.InstanceTypeStore,
		)...).
		WithControllers(ctx, controllers.NewControllers(
			op.GetClient(),
			nodeGroupProvider,
			instanceTypeProvider,
		)...).
		Start(ctx)
}
