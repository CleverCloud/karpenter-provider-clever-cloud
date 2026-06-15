# Karpenter provider for Clever Cloud

[![Continuous integration](https://github.com/CleverCloud/karpenter-provider-clever-cloud/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/CleverCloud/karpenter-provider-clever-cloud/actions/workflows/ci.yaml)

> A [Karpenter](https://karpenter.sh) cloud provider that autoscales [Clever Kubernetes Engine (CKE)](https://www.clever.cloud/developers/doc/kubernetes/) clusters through Clever Cloud's NodeGroup custom resources

## How it works

This project implements Karpenter's [`CloudProvider` interface](https://karpenter.sh/docs/) on top of the Clever Cloud
**NodeGroup** API (`nodegroups.api.clever-cloud.com/v1`) that every CKE cluster serves. Karpenter observes pending pods,
provisions exactly the nodes they need — one NodeGroup per node — then consolidates the cluster to keep costs down. The
Clever Cloud operator upstream turns those NodeGroups into VMs.

Everything goes through the cluster's own Kubernetes API: there is no Clever Cloud HTTP client in the provider.

## Status

The provider is under development: you can use it, but it may have bugs or unimplemented features. Its behavior
(provisioning times, quota handling, instance capacities) has been validated end-to-end on a live CKE cluster — see
[docs/E2E-RESULTS.md](docs/E2E-RESULTS.md). Each release publishes the controller image and both Helm charts to
ghcr.io.

## Install

To deploy the provider you will need a running CKE cluster (Kubernetes ≥ 1.34), the `kubectl` command with
cluster-admin access and `helm`. The step-by-step
[installation guide](docs/getting-started/installation.md) covers CRD handling, verification, upgrades and uninstall.

> **Warning:** Do not enable CKE's own `autoscalingEnabled` on the cluster alongside this provider — two autoscalers
> will fight over the same NodeGroups.

### From the published charts

Each release publishes the [karpenter-crd](charts/karpenter-crd/README.md) and [karpenter](charts/karpenter/README.md)
charts to ghcr.io as OCI artifacts, versioned on the release tag without the `v` prefix (release `v0.9.2` → chart
version `0.9.2`). Install the CRDs first, then the controller — no image settings are needed, the chart pulls the
matching published image by default:

```
$ helm upgrade --install karpenter-crd \
    oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/karpenter-crd \
    --version <version> --namespace karpenter --create-namespace

$ helm upgrade --install karpenter \
    oci://ghcr.io/clevercloud/karpenter-provider-clever-cloud/karpenter \
    --version <version> --namespace karpenter --create-namespace --wait
```

Finally, create a NodePool and a CleverNodeClass to start provisioning nodes — see the
[examples/](examples/README.md) catalog.

### From source

You will need some tools on your computer to build the provider, at least the `git`, `go` and `docker` commands. So,
firstly, retrieve the source from [GitHub](https://github.com/CleverCloud/karpenter-provider-clever-cloud) using the
following command.

```
$ git clone https://github.com/CleverCloud/karpenter-provider-clever-cloud.git
```
or
```
$ gh repo clone CleverCloud/karpenter-provider-clever-cloud
```

Then, go into the newly created folder where the source code is located.

```
$ cd karpenter-provider-clever-cloud
```

At this step, you can choose to build the binary and run it directly, or build the docker image, push it to your
registry and deploy the provider into your kubernetes cluster through the helm charts.

#### Build the binary

To build the binary, you can use the following command:

```
$ make build
```

The controller binary will be located at `bin/karpenter-clevercloud`. You can run it locally against your current
kubeconfig (leader election disabled):

```
$ make run
```

#### Build the docker image

To build the docker image and push it to your registry, you can use the following commands:

```
$ make image IMAGE=<your-registry>/karpenter-clevercloud TAG=v0.9.2
$ docker push <your-registry>/karpenter-clevercloud:v0.9.2
```

#### From the helm charts

Two charts are located under [charts/](charts/): [karpenter-crd](charts/karpenter-crd/README.md) installs and upgrades
the CustomResourceDefinitions, and [karpenter](charts/karpenter/README.md) installs the controller stack (Deployment
pinned to control-plane nodes, RBAC, metrics Service, PodDisruptionBudget). Install the CRDs first, then the
controller, pointing it at the image you pushed:

```
$ helm upgrade --install karpenter-crd charts/karpenter-crd \
    --namespace karpenter --create-namespace

$ helm upgrade --install karpenter charts/karpenter \
    --namespace karpenter --create-namespace \
    --set image.repository=<your-registry>/karpenter-clevercloud \
    --set image.tag=v0.9.2 \
    --wait
```

Finally, create a NodePool and a CleverNodeClass to start provisioning nodes. The
[examples/](examples/README.md) catalog covers the common use cases, each validated on a live CKE cluster.

```
$ kubectl apply -f examples/v1/general-purpose.yaml
```

## Credentials

No Clever Cloud API token or credentials are required. The provider drives the in-cluster NodeGroup API that every CKE
cluster serves; the Clever Cloud operator upstream reconciles NodeGroups into VMs with its own credentials.

## Configuration

### Global

The controller is configured through environment variables, all set by the helm chart from its
[values](charts/karpenter/README.md):

| Name                      | Kind       | Default            | Required | Description                                                  |
| ------------------------- | ---------- | ------------------ | -------- | ------------------------------------------------------------ |
| `CLEVER_CLOUD_REGION`     | `String`   | `par`              | no       | Region/zone advertised on instance types (CKE is Paris-only today) |
| `LOG_LEVEL`               | `String`   | `info`             | no       | `debug`, `info` or `error`                                   |
| `METRICS_PORT`            | `Integer`  | `8080`             | no       | Port of the `/metrics` endpoint                              |
| `HEALTH_PROBE_PORT`       | `Integer`  | `8081`             | no       | Port of the liveness/readiness probes                        |
| `DISABLE_LEADER_ELECTION` | `Boolean`  | `false`            | no       | Useful for single-replica dev setups                         |
| `BATCH_MAX_DURATION`      | `Duration` | `10s`              | no       | Maximum pod batching window before provisioning              |
| `BATCH_IDLE_DURATION`     | `Duration` | `1s`               | no       | Idle pod batching window before provisioning                 |
| `FEATURE_GATES`           | `String`   | `NodeRepair=false` | no       | Karpenter feature gates                                      |
| `FLAVORS_CONFIG_PATH`     | `String`   | _(unset)_          | no       | Path to a YAML flavor catalogue; set by the chart when `settings.flavors` is non-empty |

### Flavor catalogue

By default the controller ships a built-in catalogue (`2XS`…`XL`) with measured/estimated
capacities and the documented public-beta prices. You can override it entirely through
`settings.flavors` in the chart values — the chart renders it into a ConfigMap mounted at
`/etc/karpenter/flavors/flavors.yaml` and points `FLAVORS_CONFIG_PATH` at it. A non-empty
list **replaces** the built-in catalogue: only the flavors you list are offered to Karpenter.

```yaml
settings:
  flavors:
    - name: M            # as accepted by the Clever Cloud NodeGroup API (uppercase)
      cpu: 10            # vCPUs
      memoryKi: 15988992 # kernel-visible memory, KiB
      priceHourly: 0.1167 # EUR/hour
```

`cpu`/`memoryKi` self-correct at runtime from observed node capacity, so they only need to be
close enough for the scheduler to pick a flavor; prices are used as-is for cost-based
consolidation. Leave `settings.flavors` empty to keep the built-in catalogue.

### NodePool

A minimal NodePool targeting the CleverNodeClass below:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: default
spec:
  template:
    spec:
      nodeClassRef:
        group: karpenter.clever-cloud.com
        kind: CleverNodeClass
        name: default
      requirements:
        - key: node.kubernetes.io/instance-type
          operator: In
          values: ["2XS", "XS", "S", "M"]
      expireAfter: Never
  limits:
    cpu: "16"
    memory: 16Gi
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 30s
```

Size `limits` against your organisation quota: the default org quota is 40 vCPU / 40 GB RAM **including the control
plane** (a 3-node `S` control plane consumes 24 GB of it).

### CleverNodeClass

```yaml
apiVersion: karpenter.clever-cloud.com/v1alpha1
kind: CleverNodeClass
metadata:
  name: default
spec:
  labels:            # extra node labels applied at the NodeGroup level,
    team: platform   # visible before Karpenter registration completes
```

Changing a NodeClass marks the NodeClaims built from it as drifted; Karpenter then replaces those nodes rolling-style.

### Targeting Karpenter nodes

Control-plane nodes in CKE are schedulable. To steer a workload onto (auto-scaled) workers:

```yaml
nodeSelector:
  clever-cloud.com/cluster-node-role: worker
```

## License

See the [license](LICENSE).

## Getting in touch

- Open an [issue](https://github.com/CleverCloud/karpenter-provider-clever-cloud/issues) for bugs or feature requests
