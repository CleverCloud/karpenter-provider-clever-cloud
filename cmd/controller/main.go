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
	"os/signal"
	"syscall"

	"github.com/samber/lo"
	"sigs.k8s.io/karpenter/pkg/utils/env"

	coremetrics "sigs.k8s.io/karpenter/pkg/cloudprovider/metrics"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/overlay"
	corecontrollers "sigs.k8s.io/karpenter/pkg/controllers"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator"

	cloudprovider "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/cloudprovider"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/controllers"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/nodegroup"
)

func main() {
	ctx, op := operator.NewOperator()

	// karpenter-core installs no signal handler: operator.NewOperator() roots
	// its context in context.Background(), and nothing in the module calls
	// signal.Notify. SIGTERM therefore takes Go's default disposition and kills
	// the process outright — controller-runtime never runs its shutdown
	// sequence, so the LeaderElectionReleaseOnCancel the operator asks for
	// never fires and the replacement pod waits out the full lease on every
	// rollout. In-flight work is cut mid-flight too: a NodeGroup Create can die
	// between the API call and its acceptance poll.
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stop()

	region := env.WithDefaultString("CLEVER_CLOUD_REGION", "par")

	// FLAVORS_CONFIG_PATH points at a YAML list of per-flavor overrides (mounted
	// from the chart's ConfigMap). They overlay the built-in DefaultFlavors and
	// always win. An invalid file degrades to the built-in catalogue instead of
	// crashlooping the controller (surfaced via the flavors_config_invalid
	// gauge).
	overrides := instancetype.LoadFlavorsOrDegrade(env.WithDefaultString("FLAVORS_CONFIG_PATH", ""))

	instanceTypeProvider := instancetype.NewProvider(region, nil, overrides)

	// The catalogue sizes the flavors a quota rejection makes unavailable: the
	// rejected one and every flavor at least as large.
	nodeGroupProvider := nodegroup.NewProvider(op.GetClient(), op.EventRecorder, instanceTypeProvider, op.Clock)
	cleverCloudProvider := cloudprovider.New(op.GetClient(), op.GetAPIReader(), instanceTypeProvider, nodeGroupProvider)
	// Innermost decorator so durations/errors measure the actual provider
	// calls (karpenter_cloudprovider_* series), then the NodeOverlay layer.
	metricsCloudProvider := coremetrics.Decorate(cleverCloudProvider)
	decoratedCloudProvider := overlay.Decorate(metricsCloudProvider, op.GetClient(), op.InstanceTypeStore)
	// Node repair (the NodeRepair feature gate) runs inside core's disruption
	// controller, which matches the provider's RepairPolicies through cluster
	// state: with the gate on, core panics at startup on a cluster built
	// without the matcher. The matcher is nil while the gate is off or
	// LEGACY_NODE_REPAIR runs the standalone controller of earlier releases
	// instead; an invalid policy set fails startup too.
	repairPolicyMatcher := lo.Must(health.NewRepairPolicyMatcher(ctx, decoratedCloudProvider))
	clusterState := state.NewCluster(op.Clock, op.GetClient(), decoratedCloudProvider, state.WithRepairPolicyMatcher(repairPolicyMatcher))

	op.
		WithControllers(ctx, corecontrollers.NewControllers(
			ctx,
			op.Manager,
			op.Clock,
			op.GetClient(),
			op.EventRecorder,
			decoratedCloudProvider,
			metricsCloudProvider,
			clusterState,
			op.InstanceTypeStore,
			op.PredictionStore,
		)...).
		WithControllers(ctx, controllers.NewControllers(
			op.GetClient(),
			op.Manager.GetAPIReader(),
			op.EventRecorder,
			nodeGroupProvider,
			instanceTypeProvider,
			env.WithDefaultString("NODE_NAME", ""),
		)...).
		Start(ctx)
}
