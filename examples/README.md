# Examples

This directory contains example NodePool / CleverNodeClass specs and demo
workloads for Karpenter on Clever Kubernetes Engine. Each file under `v1/`
is self-contained (its own NodePool and CleverNodeClass) so you can apply
and remove them independently:

```sh
kubectl apply -f examples/v1/general-purpose.yaml
kubectl apply -f examples/workloads/inflate.yaml
kubectl scale deployment inflate --replicas=3
```

## NodePools (`v1/`)

| Example | Demonstrates |
|---|---|
| [general-purpose.yaml](v1/general-purpose.yaml) | Whole flavor catalog, cheapest-fit, consolidation |
| [small-flavors.yaml](v1/small-flavors.yaml) | Restricting to the smallest flavors (2XS, XS) |
| [large-instances.yaml](v1/large-instances.yaml) | Restricting to large flavors (M+); quota caveats |
| [flavor-pinned.yaml](v1/flavor-pinned.yaml) | Pinning a pool to one flavor via `clever-cloud.com/flavor` |
| [cpu-limit.yaml](v1/cpu-limit.yaml) | Hard cap on the total CPU/memory a pool may provision |
| [team-dedicated.yaml](v1/team-dedicated.yaml) | Dedicated tainted+labeled nodes for one team |
| [weighted-nodepools.yaml](v1/weighted-nodepools.yaml) | Preferred pool with fallback through weights |
| [min-values.yaml](v1/min-values.yaml) | Requiring flavor diversity with `minValues` |
| [max-node-lifetime.yaml](v1/max-node-lifetime.yaml) | Rolling node refresh with `expireAfter` |
| [disruption-budgets.yaml](v1/disruption-budgets.yaml) | Throttling voluntary disruptions, maintenance windows |

## Workloads (`workloads/`)

| Example | Demonstrates |
|---|---|
| [inflate.yaml](workloads/inflate.yaml) | Basic scale-up/scale-down driver |
| [one-pod-per-node.yaml](workloads/one-pod-per-node.yaml) | One replica per node via required pod anti-affinity |
| [do-not-disrupt.yaml](workloads/do-not-disrupt.yaml) | Opting a pod out of voluntary disruption |
| [flavor-selector.yaml](workloads/flavor-selector.yaml) | Targeting a flavor with a nodeSelector |
| [prefer-small.yaml](workloads/prefer-small.yaml) | Preferred (soft) affinity to the cheapest flavor |
| [team-workload.yaml](workloads/team-workload.yaml) | Toleration + selector for dedicated team nodes |
| [disruption-budget.yaml](workloads/disruption-budget.yaml) | PodDisruptionBudget bounding node drains |

All workloads start at `replicas: 0`: scale them up to trigger provisioning.
They pin pods to `clever-cloud.com/cluster-node-role: worker` because CKE
control-plane nodes are schedulable and would otherwise absorb small pods.

Mind the organisation quota (default 40 vCPU / 40 GB RAM **including the
control plane**): apply one NodePool example at a time when experimenting on
a small org, and keep `limits` conservative.
