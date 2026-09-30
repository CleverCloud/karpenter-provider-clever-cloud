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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	coreapis "sigs.k8s.io/karpenter/pkg/apis"
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
	// NodeClassHashVersionAnnotationKey records which generation of Hash()
	// produced NodeClassHashLabelKey (absent: v1). The stamp is only ever
	// compared with what its own generation computes for the current spec
	// (CleverNodeClass.HashMatches): without it, any change to Hash() or to
	// CleverNodeClassSpec silently reads as drift on every existing NodeGroup
	// and replaces the whole fleet.
	NodeClassHashVersionAnnotationKey = apis.Group + "/clevernodeclass-hash-version"

	// TerminationFinalizer protects CleverNodeClasses that still back NodeClaims.
	TerminationFinalizer = apis.Group + "/termination"
)

// ValidateNodeClassLabel is the single owner of the rule for what a
// CleverNodeClass label may contain. The NodeGroup payload is the ONLY path a
// NodeClass label takes to the node — karpenter-core knows nothing of NodeClass
// labels, so unlike NodeClaim labels there is no registration-sync fallback. A
// key the NodeGroup filter drops, or a value the apiserver refuses on the Node,
// would otherwise validate cleanly, let the NodeClass go Ready, and never
// appear on any node. Three call sites share it: the CRD CEL rule mirrors the
// prefix checks at admission, the nodeclass controller surfaces the full rule
// as ValidationSucceeded=False, and the NodeGroup label filter admits exactly
// what it accepts.
func ValidateNodeClassLabel(key, value string) error {
	// Any kubernetes.io/ domain — bare, node.kubernetes.io/, or subdomained
	// like app.kubernetes.io/ and topology.kubernetes.io/ — is kept out of the
	// NodeGroup payload (reserved or owned by Karpenter's registration sync),
	// so for a NodeClass label it would silently never reach a node.
	if strings.Contains(key, "kubernetes.io/") {
		return fmt.Errorf("label key %q uses the kubernetes.io/ domain, which is filtered from the NodeGroup payload — a NodeClass label has no other path to the node", key)
	}
	if strings.HasPrefix(key, "clever-cloud.com/") {
		return fmt.Errorf("label key %q uses reserved prefix %q (rejected by the Clever Cloud API)", key, "clever-cloud.com/")
	}
	// The karpenter.sh domain, bare or subdomained, is karpenter-core's: it
	// reads those keys on Nodes and applies its own at registration. On the
	// NodeGroup they would reach EVERY node of the group — spec.labels is
	// immutable and applied to all of them, including nodes karpenter never
	// registers (the extra node of an externally resized group) — and core's
	// cluster state ignores a node carrying karpenter.sh/nodepool without a
	// provider ID; its first sync after a restart waits for every node, so one
	// such node stops provisioning and disruption cluster-wide. From a
	// NodeClass, karpenter.sh/registered or /initialized would reach the node
	// at join and misreport its lifecycle to core. Spelled the way the CEL rule
	// has to: for a valid key (a single "/") it means exactly "the prefix is
	// karpenter.sh or a subdomain of it", and any other key it catches is
	// invalid anyway.
	if strings.HasPrefix(key, coreapis.Group+"/") || strings.Contains(key, "."+coreapis.Group+"/") {
		return fmt.Errorf("label key %q uses the %s domain, which karpenter-core owns — it applies its own keys to the node at registration", key, coreapis.Group)
	}
	if errs := validation.IsQualifiedName(key); len(errs) > 0 {
		return fmt.Errorf("label key %q is not a valid label key: %s", key, strings.Join(errs, "; "))
	}
	if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
		return fmt.Errorf("label %q value %q is not a valid label value: %s", key, value, strings.Join(errs, "; "))
	}
	return nil
}

func init() {
	v1.RestrictedLabelDomains = v1.RestrictedLabelDomains.Insert(apis.Group)
	v1.WellKnownLabels = v1.WellKnownLabels.Insert(
		InstanceCPULabelKey,
		InstanceMemoryLabelKey,
		FlavorLabelKey,
		NodeRoleLabelKey,
	)
}
