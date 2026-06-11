# karpenter

Helm chart for the [Karpenter provider for Clever Kubernetes Engine](https://github.com/CleverCloud/karpenter-provider-clever-cloud).

The chart installs the controller (Deployment pinned to control-plane
nodes, RBAC, leader-election roles, metrics Service, PDB) and carries the
required CRDs in its `crds/` directory: `nodepools.karpenter.sh`,
`nodeclaims.karpenter.sh`, `nodeoverlays.karpenter.sh` and
`clevernodeclasses.karpenter.clever-cloud.com`. Helm installs missing CRDs
on first install, skips existing ones, and never deletes them on
uninstall — your NodePools and NodeClasses survive chart removal.

Because Helm never **upgrades** CRDs shipped in `crds/`, the companion
[karpenter-crd](../karpenter-crd/README.md) chart
manages the same CRDs as regular templates — install it alongside this
chart to upgrade CRDs through Helm.

## Install

Each release publishes the chart to ghcr.io as an OCI artifact, versioned on
the release tag without the `v` prefix (release `v0.1.0` → chart version
`0.1.0`). No image settings are needed — the chart's `appVersion` pins the
matching published image:

```sh
helm install karpenter \
  oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/charts/karpenter \
  --version <version> --namespace karpenter --create-namespace
```

To run your own build instead, push the controller image to a registry your
cluster can pull from, then install the chart from the repo checkout:

```sh
make image IMAGE=<registry>/karpenter-clevercloud TAG=v0.1.0
docker push <registry>/karpenter-clevercloud:v0.1.0

helm install karpenter charts/karpenter \
  --namespace karpenter --create-namespace \
  --set image.repository=<registry>/karpenter-clevercloud \
  --set image.tag=v0.1.0
```

Then create a NodePool and a CleverNodeClass (see
[examples/](../../examples/README.md)):

```sh
kubectl apply -f examples/v1/general-purpose.yaml
```

## Values worth knowing

| Key | Default | Description |
|---|---|---|
| `image.repository` / `image.tag` / `image.digest` | ghcr.io/clevercloud/karpenter-provider-clever-cloud | Controller image (digest wins over tag) |
| `replicas` | `1` | Leader election keeps a single active controller |
| `nodeSelector` | control-plane role | Karpenter must not run on nodes it manages |
| `settings.region` | `par` | Zone advertised on instance types |
| `settings.logLevel` | `info` | debug / info / error |
| `settings.disableLeaderElection` | `false` | For single-replica dev setups |
| `settings.batchMaxDuration` / `batchIdleDuration` | `10s` / `1s` | Pod batching windows |
| `settings.featureGates.nodeRepair` | `false` | Enable node auto-repair |
| `controller.resources` | 200m/256Mi, limit 512Mi | Controller container resources |
| `controller.env` | `[]` | Extra environment variables |
| `service.enabled` | `true` | ClusterIP service exposing `/metrics` |
| `podDisruptionBudget.enabled` | `true` | maxUnavailable: 1 |

## Upgrade & uninstall

```sh
# CRDs first (if installed)
helm upgrade karpenter-crd \
  oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/charts/karpenter-crd \
  --version <version> -n karpenter
helm upgrade karpenter \
  oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/charts/karpenter \
  --version <version> -n karpenter --reuse-values
helm uninstall karpenter -n karpenter   # CRDs, NodePools and NodeClasses are kept
```

Before uninstalling for good, scale your Karpenter-backed workloads down
(or delete the NodePools) so the provisioned NodeGroups are cleaned up
while the controller still runs.
