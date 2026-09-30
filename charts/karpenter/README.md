# karpenter

Helm chart for the [Karpenter provider for Clever Kubernetes Engine](https://github.com/CleverCloud/karpenter-provider-clever-cloud).

The chart installs the controller (Deployment kept off Karpenter-managed
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
the release tag without the `v` prefix (release `v0.12.0` → chart version
`0.12.0`). No image settings are needed — the chart's `appVersion` pins the
matching published image:

```sh
helm install karpenter \
  oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/karpenter \
  --version <version> --namespace karpenter --create-namespace
```

To run your own build instead, push the controller image to a registry your
cluster can pull from, then install the chart from the repo checkout:

```sh
make image IMAGE=<registry>/karpenter-clevercloud TAG=v0.12.0
docker push <registry>/karpenter-clevercloud:v0.12.0

helm install karpenter charts/karpenter \
  --namespace karpenter --create-namespace \
  --set image.repository=<registry>/karpenter-clevercloud \
  --set image.tag=v0.12.0
```

Then create a NodePool and a CleverNodeClass (see
[examples/](../../examples/README.md)):

```sh
kubectl apply -f examples/v1/general-purpose.yaml
```

## Values worth knowing

| Key | Default | Description |
|---|---|---|
| `image.repository` / `image.tag` / `image.digest` | ghcr.io/clevercloud/karpenter | Controller image (digest wins over tag) |
| `replicas` | `1` | Leader election keeps a single active controller |
| `nodeSelector` | `{}` | Extra pinning, ANDed with `affinity` |
| `affinity` | require `karpenter.sh/nodepool` absent | Karpenter must not run on nodes it manages — holds on every CKE topology |
| `tolerations` | control-plane `NoSchedule` | No-op today (no CKE node role is tainted); future-proofs placement. Never tolerate `karpenter.sh/unregistered` or everything: until a node registers (and for good on a resized NodeGroup's extra node) that taint is all that keeps the controller off it |
| `settings.region` | `par` | Zone advertised on instance types |
| `settings.logLevel` | `info` | debug / info / error |
| `settings.disableLeaderElection` | `false` | For single-replica dev setups |
| `settings.batchMaxDuration` / `batchIdleDuration` | `10s` / `1s` | Pod batching windows |
| `settings.featureGates.nodeRepair` | `false` | Enable node auto-repair |
| `settings.flavors` | `[]` | Per-flavor overrides overlaid on the built-in catalogue (`name` required; `cpu`/`memoryKi` optional). No price field: prices are derived from cpu and memoryKi, so pin every flavor the same way or none, and a leftover `priceHourly` fails schema validation. Empty = built-in catalogue unchanged. Mounted via a ConfigMap; overrides always win |
| `controller.resources` | 200m/256Mi, limit 512Mi | Controller container resources |
| `controller.env` | `[]` | Extra environment variables |
| `service.enabled` | `true` | ClusterIP service exposing `/metrics` |
| `podDisruptionBudget.enabled` | `true` | maxUnavailable: 1 |

## Upgrade & uninstall

```sh
# CRDs first (if installed)
helm upgrade karpenter-crd \
  oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/karpenter-crd \
  --version <version> -n karpenter
helm upgrade karpenter \
  oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/karpenter \
  --version <version> -n karpenter --reuse-values
helm uninstall karpenter -n karpenter   # CRDs, NodePools and NodeClasses are kept
```

The controller talks to nothing but the cluster's Kubernetes API: it needs no
Clever Cloud API token and no egress beyond the API server. Earlier releases
had a `settings.pricing` block (a dynamic pricing refresher, since removed); a
`--reuse-values` upgrade that still carries it renders fine and the values are
ignored. The opposite holds for `priceHourly`, which `settings.flavors`
entries no longer accept: a reused entry that still carries it makes the
upgrade above fail schema validation (`additional properties 'priceHourly' not
allowed`). Pass the corrected list explicitly, since a list given with `-f` or
`--set-json` replaces the reused one: `--set-json 'settings.flavors=[]'` drops
the overrides (see the
[upgrade notes](../../docs/getting-started/installation.md#upgrading)).

Before uninstalling for good, scale your Karpenter-backed workloads down
(or delete the NodePools) so the provisioned NodeGroups are cleaned up
while the controller still runs.
