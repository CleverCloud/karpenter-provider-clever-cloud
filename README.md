# Karpenter provider for Clever Cloud

[![Continuous integration](https://github.com/CleverCloud/karpenter-provider-clever-cloud/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/CleverCloud/karpenter-provider-clever-cloud/actions/workflows/ci.yaml)

> A [Karpenter](https://karpenter.sh) cloud provider that autoscales [Clever Kubernetes Engine (CKE)](https://www.clever.cloud/developers/doc/kubernetes/) clusters through Clever Cloud's NodeGroup custom resources

## How it works

This project implements Karpenter's [`CloudProvider` interface](https://karpenter.sh/docs/) on top of the Clever Cloud
**NodeGroup** API (`nodegroups.api.clever-cloud.com/v1`) that every CKE cluster serves. Karpenter observes pending pods,
provisions exactly the nodes they need — one NodeGroup per node — then consolidates the cluster to keep costs down. The
Clever Cloud operator upstream turns those NodeGroups into VMs.

Everything goes through the cluster's own Kubernetes API: the controller needs no Clever Cloud API token, makes no
call to any Clever Cloud HTTP endpoint, and needs no egress beyond the cluster's API server.

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
charts to ghcr.io as OCI artifacts, versioned on the release tag without the `v` prefix (release `v0.12.0` → chart
version `0.12.0`). Install the CRDs first, then the controller — no image settings are needed, the chart pulls the
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
$ make image IMAGE=<your-registry>/karpenter-clevercloud TAG=v0.12.0
$ docker push <your-registry>/karpenter-clevercloud:v0.12.0
```

#### From the helm charts

Two charts are located under [charts/](charts/): [karpenter-crd](charts/karpenter-crd/README.md) installs and upgrades
the CustomResourceDefinitions, and [karpenter](charts/karpenter/README.md) installs the controller stack (Deployment
kept off Karpenter-managed nodes, RBAC, metrics Service, PodDisruptionBudget). Install the CRDs first, then the
controller, pointing it at the image you pushed:

```
$ helm upgrade --install karpenter-crd charts/karpenter-crd \
    --namespace karpenter --create-namespace

$ helm upgrade --install karpenter charts/karpenter \
    --namespace karpenter --create-namespace \
    --set image.repository=<your-registry>/karpenter-clevercloud \
    --set image.tag=v0.12.0 \
    --wait
```

Finally, create a NodePool and a CleverNodeClass to start provisioning nodes. The
[examples/](examples/README.md) catalog covers the common use cases, each validated on a live CKE cluster.

```
$ kubectl apply -f examples/v1/general-purpose.yaml
```

## Credentials

No Clever Cloud API token or credentials are required. The provider drives the in-cluster NodeGroup API that every CKE
cluster serves; the Clever Cloud operator upstream reconciles NodeGroups into VMs with its own credentials. The
controller talks to nothing but the cluster's Kubernetes API, so it needs no egress beyond the API server either.

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
| `FLAVORS_CONFIG_PATH`     | `String`   | _(unset)_          | no       | Path to a YAML list of per-flavor overrides; set by the chart when `settings.flavors` is non-empty |

### Flavor catalogue

The controller ships a built-in catalogue (`2XS`…`XL`) with measured/estimated capacities; it
fetches nothing at runtime. `settings.flavors` lets you **overlay per-flavor overrides** on top of
that built-in catalogue — the chart renders it into a ConfigMap mounted at
`/etc/karpenter/flavors/flavors.yaml` and points `FLAVORS_CONFIG_PATH` at it. Every field except
`name` is optional: set only what you want to pin, the rest fall through to the built-in value.

```yaml
settings:
  flavors:
    # name is required, as accepted by the NodeGroup API (uppercase). This pins every flavor's
    # memoryKi to the kernel-visible memory its nodes report on the current CKE image, known
    # before one has run; cpu keeps its built-in value.
    - { name: 2XS, memoryKi: 3715344 }
    - { name: XS, memoryKi: 7553664 }
    - { name: S, memoryKi: 11385832 }
    - { name: M, memoryKi: 15229256 }
    - { name: L, memoryKi: 22896304 }
    - { name: XL, memoryKi: 30584176 }
```

A name outside the built-in catalogue adds a flavor and must set both `cpu` and `memoryKi`; the
NodeGroup API only accepts `2XS`…`XL` today, though, so in practice overrides adjust those six.
`cpu`/`memoryKi` self-correct at runtime from observed node capacity, so they only need to be
close enough for the scheduler to pick a flavor. Overrides always win. Leave `settings.flavors`
empty to use the built-in catalogue unchanged.

**Prices are relative, not a currency.** Karpenter only compares and adds up offering prices, to
launch the cheapest flavor that fits and to consolidate onto cheaper capacity, so every flavor is
priced by its size relative to the built-in `2XS`:

```text
price = 1/3 × cpu / cpu(2XS) + 2/3 × memoryKi / memoryKi(2XS)      (2XS = 1.0)
```

| Flavor | 2XS | XS | S | M | L | XL |
|---|---|---|---|---|---|---|
| Price | 1.0 | 1.8527 | 2.7044 | 3.5582 | 5.0873 | 6.783 |

Memory weighs twice as much as cpu because that is how CKE's public worker prices are built (a GB of
memory costs as much as two vCPUs). With these weights and the built-in sizing, whenever CKE bills
one set of up to four nodes less than another, its relative price is lower too, so consolidation
never swaps nodes for capacity that really costs more.

There is no price to configure: an override's price is derived from its resulting `cpu` and
`memoryKi`, so pinning a flavor's memory also reprices it, still relative to the built-in `2XS`.
The guarantee above then only holds while the pinned values stay proportionate across flavors.
Pinning all six the same way, as in the example, keeps it; pinning one alone can break it. Pinned
alone to its value above, `M` makes two `M` nodes look cheaper than an `XS` and an `L`, which CKE
bills less, and `XL` makes merging four small nodes into one `XL` look like a saving although
that `XL` really costs more. Pin every flavor the same way, or none.

The price-based figures karpenter-core reports (the `karpenter_nodepools_cost_total` metric,
`savings: $…` in disruption logs and events) use this unit too, whatever their label says, and so
does a NodeOverlay's `price` or fixed `priceAdjustment` (a percentage adjustment is unit-free).

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

Label keys may not use the `clever-cloud.com/` prefix, any `kubernetes.io/` domain, or the `karpenter.sh`
domain (Karpenter's own; it applies those keys itself at registration): the CRD rejects them at admission, and
the NodeClass reports anything else it cannot deliver as `ValidationSucceeded=False`.

Changing a NodeClass marks the NodeClaims built from it as drifted; Karpenter then replaces those nodes rolling-style.

### Targeting Karpenter nodes

A CKE cluster always has capacity Karpenter did not create — a schedulable
control-plane node on `ALL_IN_ONE`, a pre-existing node pool on the topologies whose control plane
runs outside the cluster. To steer a workload onto Karpenter's own (auto-scaled) nodes, require the
label Karpenter stamps on them and on nothing else:

```yaml
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
        - matchExpressions:
            - key: karpenter.sh/nodepool
              operator: Exists
```

(`clever-cloud.com/cluster-node-role: worker` is **not** equivalent: on the topologies where the
control plane is outside the cluster, the pre-existing pool is made of worker nodes too.)

## License

See the [license](LICENSE).

## Getting in touch

- Open an [issue](https://github.com/CleverCloud/karpenter-provider-clever-cloud/issues) for bugs or feature requests
