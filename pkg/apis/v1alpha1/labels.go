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

package v1alpha1

import (
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis"
)

const (
	// Labels that can be selected on in NodePool requirements and that are
	// propagated to nodes.
	InstanceCPULabelKey    = apis.Group + "/instance-cpu"
	InstanceMemoryLabelKey = apis.Group + "/instance-memory"

	// FlavorLabelKey mirrors the label Clever Cloud sets on every worker node.
	FlavorLabelKey = "clever-cloud.com/flavor"

	// Labels set by Clever Cloud on worker nodes (read-only for us).
	NodeGroupNodeLabelKey = "clever-cloud.com/nodegroup"
	NodeRoleLabelKey      = "clever-cloud.com/cluster-node-role"
	NodeRoleWorker        = "worker"

	// Labels and annotations applied by the provider on NodeGroups it manages.
	ManagedLabelKey       = apis.Group + "/managed"
	NodeClaimLabelKey     = apis.Group + "/nodeclaim"
	NodeClassLabelKey     = apis.Group + "/nodeclass"
	NodePoolLabelKey      = apis.Group + "/nodepool"
	NodeClassHashLabelKey = apis.Group + "/clevernodeclass-hash"

	// TerminationFinalizer protects CleverNodeClasses that still back NodeClaims.
	TerminationFinalizer = apis.Group + "/termination"
)

func init() {
	v1.RestrictedLabelDomains = v1.RestrictedLabelDomains.Insert(apis.Group)
	v1.WellKnownLabels = v1.WellKnownLabels.Insert(
		InstanceCPULabelKey,
		InstanceMemoryLabelKey,
		FlavorLabelKey,
		NodeRoleLabelKey,
	)
}
