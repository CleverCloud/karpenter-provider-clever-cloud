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
	pricingcontroller "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers/pricing"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
	pricingprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/pricing"
)

func main() {
	ctx, op := operator.NewOperator()

	region := env.WithDefaultString("CLEVER_CLOUD_REGION", "par")

	// FLAVORS_CONFIG_PATH points at a YAML list of per-flavor overrides (mounted
	// from the chart's ConfigMap). They overlay the base catalog — the dynamic
	// refresher's result, or the built-in DefaultFlavors — and always win.
	var overrides []instancetype.FlavorOverride
	if flavorsPath := env.WithDefaultString("FLAVORS_CONFIG_PATH", ""); flavorsPath != "" {
		var err error
		if overrides, err = instancetype.LoadFlavorsFromFile(flavorsPath); err != nil {
			log.FromContext(ctx).Error(err, "loading flavor overrides")
			os.Exit(1)
		}
	}

	instanceTypeProvider := instancetype.NewProvider(region, nil, overrides)

	// Optional dynamic price/flavor refresher: opt-in via PRICING_REFRESH_ENABLED.
	// It updates the base catalog every PRICING_REFRESH_PERIOD from Clever Cloud's
	// public API; the overrides above are re-applied on top of every refresh.
	var pricingCtrl *pricingcontroller.Controller
	if env.WithDefaultBool("PRICING_REFRESH_ENABLED", false) {
		resolver := pricingprovider.NewProvider(pricingprovider.Options{
			BaseURL:        env.WithDefaultString("PRICING_API_URL", pricingprovider.DefaultBaseURL),
			ProductURL:     env.WithDefaultString("PRICING_PRODUCT_URL", ""),
			PriceSystemURL: env.WithDefaultString("PRICING_PRICE_SYSTEM_URL", ""),
			Region:         region, // price-system zone_id
			Topology:       env.WithDefaultString("CLEVER_CLOUD_TOPOLOGY", pricingprovider.DefaultTopology),
		})
		period := env.WithDefaultDuration("PRICING_REFRESH_PERIOD", pricingcontroller.DefaultRefreshPeriod)
		pricingCtrl = pricingcontroller.NewController(resolver, instanceTypeProvider, period)
	}

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
			pricingCtrl,
		)...).
		Start(ctx)
}
