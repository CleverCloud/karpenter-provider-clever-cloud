# Installation

This guide walks you through deploying the Karpenter provider for Clever Cloud on a CKE cluster using Helm.

## Prerequisites

- A running [CKE cluster](https://www.clever.cloud/developers/doc/kubernetes/) (Kubernetes ≥ 1.34)
- `kubectl` pointing at your cluster, with cluster-admin access
- `helm` v3.x

> **Note:** No Clever Cloud API token or credentials are required. The provider drives the in-cluster NodeGroup API (`nodegroups.api.clever-cloud.com/v1`) that every CKE cluster serves; the Clever Cloud operator upstream turns NodeGroups into VMs.

Each release publishes the controller image and both Helm charts to ghcr.io, so the default path below needs no local build. Chart versions follow the release tags without the `v` prefix: release `v0.11.0` publishes chart version `0.11.0` and image tag `v0.11.0`. To build and deploy your own image instead, see [Installing from source](#installing-from-source).

## Step 1 — Install the CRDs

Four CRDs are required: `nodepools.karpenter.sh`, `nodeclaims.karpenter.sh`, `nodeoverlays.karpenter.sh` and `clevernodeclasses.karpenter.clever-cloud.com`. The NodeGroup CRD is owned by Clever Cloud and already present on every CKE cluster.

The recommended way is the dedicated [karpenter-crd](../../charts/karpenter-crd/README.md) chart, which manages the CRDs as regular Helm resources so later `helm upgrade` runs keep them in sync:

```sh
helm upgrade --install karpenter-crd \
  oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/karpenter-crd \
  --version <version> --namespace karpenter --create-namespace
```

Alternatively you can skip this step: the main chart carries the same CRDs in its `crds/` directory, so a **first install** picks them up automatically. Helm never upgrades or deletes CRDs installed that way, so on **upgrades** you would have to apply them by hand from a checkout of the matching release tag (`kubectl apply -f deploy/crds/`) — or adopt them into the CRD chart later (see [its README](../../charts/karpenter-crd/README.md)).

## Step 2 — Install Karpenter with Helm

```sh
helm upgrade --install karpenter \
  oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/karpenter \
  --version <version> --namespace karpenter --create-namespace --wait
```

No image settings are needed: the chart's `appVersion` pins the matching published image (`ghcr.io/clevercloud/karpenter:v<version>`).

The controller is pinned to control-plane nodes by default (`nodeSelector: clever-cloud.com/cluster-node-role: control-plane`) — Karpenter must never run on a node it can deprovision, and CKE control-plane nodes are schedulable. See [the chart values](../../charts/karpenter/README.md) for everything you can tune.

> **Warning:** Do not enable CKE's own `autoscalingEnabled` on the cluster alongside this provider — two autoscalers will fight over the same NodeGroups.

## Step 3 — Verify

```sh
kubectl get pods -n karpenter
```

The controller pod should reach `Running` state. Check the logs if it doesn't:

```sh
kubectl logs -n karpenter deployment/karpenter
```

On a healthy start-up the controller wins leader election and logs each controller starting; image-pull or RBAC problems show up here.

Confirm the CRDs are established and the API groups respond:

```sh
kubectl get crd | grep karpenter
kubectl get nodepools,nodeclaims,clevernodeclasses
```

`No resources found` is the expected answer at this point — the API works, nothing is deployed yet.

## Installing from source

To run your own build instead of the published artifacts, you additionally need `docker` and a container registry your cluster can pull from. Build and push the controller image:

```sh
export IMAGE=<registry>/karpenter-clevercloud
export TAG=v0.11.0

make image IMAGE=$IMAGE TAG=$TAG
docker push $IMAGE:$TAG
```

Then install the charts from the repo checkout, pointing the controller at your image:

```sh
helm upgrade --install karpenter-crd charts/karpenter-crd \
  --namespace karpenter --create-namespace

helm upgrade --install karpenter charts/karpenter \
  --namespace karpenter --create-namespace \
  --set image.repository=$IMAGE \
  --set image.tag=$TAG \
  --wait
```

## Upgrading

Upgrade the CRDs first, then the controller release:

```sh
helm upgrade karpenter-crd \
  oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/karpenter-crd \
  --version <version> --namespace karpenter
helm upgrade karpenter \
  oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/karpenter \
  --version <version> --namespace karpenter --reuse-values
```

If you did not install the CRD chart, apply the CRDs by hand instead (Helm does not upgrade CRDs shipped in the main chart's `crds/` directory), from a checkout of the matching release tag: `kubectl apply -f deploy/crds/`.

> **Note:** Releases installed from the pre-rename chart (`helm install karpenter-clevercloud charts/karpenter-clevercloud`) cannot be upgraded in place: the chart rename changes the Deployment's immutable selector labels. Uninstall the old release first, then install fresh under the new name — the CRDs and your NodePools/NodeClasses are untouched by that operation.

## Uninstalling

Delete the NodePools first and wait for the provisioned nodes to drain **while the controller is still running**, so the backing NodeGroups are cleaned up:

```sh
kubectl delete nodepools --all
kubectl wait --for=delete nodeclaims --all --timeout=10m

helm uninstall karpenter --namespace karpenter
kubectl delete namespace karpenter
```

> **Note:** Uninstalling the main chart never deletes the CRDs — NodePool, NodeClaim and CleverNodeClass definitions (and any remaining objects) survive its removal. Uninstalling the **karpenter-crd** chart, however, deletes the CRDs and every object of those kinds with them — only do that once the cluster has no Karpenter-managed nodes left.

## Next steps

- [Examples](../../examples/README.md) — a catalog of NodePool + CleverNodeClass use cases, each validated on a live CKE cluster. Trigger your first provisioning event:

  ```sh
  kubectl apply -f examples/v1/general-purpose.yaml
  ```
