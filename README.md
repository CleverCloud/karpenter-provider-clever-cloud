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
cluster-admin access and `helm` ≥ 3.14. The step-by-step
[installation guide](docs/getting-started/installation.md) covers CRD handling, verification, upgrades and uninstall —
upgrade with `--reset-then-reuse-values`, never `--reuse-values`, which keeps the previous release's chart defaults, so
a default the new release changes silently never takes effect.

> **Warning:** Do not enable CKE's own `autoscalingEnabled` on the cluster alongside this provider — two autoscalers
> will fight over the same NodeGroups.

### From the published charts

Each release publishes the [karpenter-crd](charts/karpenter-crd/README.md) and [karpenter](charts/karpenter/README.md)
charts to ghcr.io as OCI artifacts, versioned on the release tag without the `v` prefix (release `v0.14.0` → chart
version `0.14.0`). Install the CRDs first, then the controller — no image settings are needed, the chart pulls the
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

This uses the static disk profile. To select a representative node for dynamic local
disk sizing, use `make run RUN_NODE_NAME=<reference-node>`.

#### Build the docker image

To build the docker image and push it to your registry, you can use the following commands:

```
$ make image IMAGE=<your-registry>/karpenter-clevercloud TAG=v0.14.0
$ docker push <your-registry>/karpenter-clevercloud:v0.14.0
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
    --set image.tag=v0.14.0 \
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

The controller is configured through environment variables set by the Helm chart
from its [values](charts/karpenter/README.md) and the pod's Downward API:

| Name                      | Kind       | Default            | Required | Description                                                  |
| ------------------------- | ---------- | ------------------ | -------- | ------------------------------------------------------------ |
| `CLEVER_CLOUD_REGION`     | `String`   | `par`              | no       | Value of the `topology.kubernetes.io/region` and `topology.kubernetes.io/zone` labels on instance types and nodes (CKE is single-zone, Paris-only today) |
| `NODE_NAME`              | `String`   | `""` | no | Reference node for local ephemeral storage. The chart controls it through `settings.ephemeralStorageReference`: empty in `static` mode, `spec.nodeName` in `host` mode, the configured name in `node` mode. |
| `LOG_LEVEL`               | `String`   | `info`             | no       | `debug`, `info` or `error`                                   |
| `METRICS_PORT`            | `Integer`  | `8080`             | no       | Port of the `/metrics` endpoint                              |
| `HEALTH_PROBE_PORT`       | `Integer`  | `8081`             | no       | Port of the liveness/readiness probes                        |
| `DISABLE_LEADER_ELECTION` | `Boolean`  | `false`            | no       | Useful for single-replica dev setups                         |
| `BATCH_MAX_DURATION`      | `Duration` | `10s`              | no       | Maximum pod batching window before provisioning              |
| `BATCH_IDLE_DURATION`     | `Duration` | `1s`               | no       | Idle pod batching window before provisioning                 |
| `FEATURE_GATES`           | `String`   | `NodeRepair=false` | no       | Karpenter feature gates                                      |
| `FLAVORS_CONFIG_PATH`     | `String`   | _(unset)_          | no       | Path to a YAML list of per-flavor overrides; set by the chart when `settings.flavors` is non-empty |

### Flavor catalogue

The controller ships a built-in catalogue (`2XS`…`XL`) with CPU and memory measured on live CKE
nodes (2026-09-30, Kubernetes 1.37, kernel 7.2.8), and disk capacity re-measured on 2026-10-14
(Kubernetes 1.36.3, kernel 7.2.10); it fetches nothing at runtime. `settings.flavors` lets
you **overlay per-flavor overrides** on top of that built-in catalogue — the chart renders it into a
ConfigMap mounted at `/etc/karpenter/flavors/flavors.yaml` and points `FLAVORS_CONFIG_PATH` at it.
Every field except `name` is optional: set only what you want to pin, the rest fall through to the
built-in value.

```yaml
settings:
  flavors:
    # name is required, as accepted by the NodeGroup API (uppercase). This pins every flavor's
    # memoryKi to the kernel-visible memory its nodes report, known before one has run; cpu keeps
    # its built-in value. The values shown are the built-in ones (measured 2026-09-30): replace
    # them with what your nodes report (status.capacity.memory) once Clever Cloud's node image moves.
    - { name: 2XS, memoryKi: 3715344 }
    - { name: XS, memoryKi: 7553664 }
    - { name: S, memoryKi: 11385832 }
    - { name: M, memoryKi: 15229256 }
    - { name: L, memoryKi: 22896304 }
    - { name: XL, memoryKi: 30584176 }
```

A name outside the built-in catalogue adds a flavor and must set both `cpu` and `memoryKi`. Names
are Clever Cloud flavor names, uppercase as the NodeGroup API takes them: a new one must follow the
platform's naming (`M`, or `S` or `L` optionally prefixed by `X` or `2X` to `9X`, such as `2XL`).
Anything else (`2xs`, `CUSTOM`) could never be created, and `helm` refuses it. An overrides file
written outside the chart that carries one is refused as a whole (the error for `2xs` asks
`did you mean "2XS"?`), and the controller runs on the built-in catalogue without any override, as
for any invalid file. The NodeGroup API only accepts `2XS`…`XL` today (the `spec.flavor` enum of
its CRD), so in practice overrides adjust those six: add a flavor only once that enum carries it.
Until then, every launch on it is refused at admission and the refused flavor is
[unavailable](docs/observability.md#unavailable-flavors) to Karpenter's scheduler 5 minutes at a
time, with a `NodeGroupRejected` event on the NodeClaim and
`karpenter_clevercloud_nodegroup_rejections_total` counting each refusal. The launch fails over to
another flavor when the claim allows one; pods that only that flavor fits stay pending.
Overrides always win over the built-in values. Leave `settings.flavors` empty to use the built-in
catalogue unchanged.

The chart defaults to **static local disk sizing**, preserving the measured profile
from v0.14.0: `102624184Ki` capacity (about 97.9 Gi), with a 10% eviction reserve,
and compatibility with old workers' `40971488Ki` disks (about 39.1 Gi). It cannot
discover a future platform disk change. Choose the source explicitly through
`settings.ephemeralStorageReference`:

| `mode` | Disk source for future workers | `nodeName` |
| --- | --- | --- |
| `static` (default) | Built-in measured profile | Empty |
| `host` | Controller pod's host, from `spec.nodeName` | Empty |
| `node` | Explicitly selected reference Node | Required |

For example, to follow a representative fixed worker independently of the
controller's placement:

```yaml
settings:
  ephemeralStorageReference:
    mode: node
    nodeName: fixed-workers-node0
```

In `host` and `node` modes, the provider reads the reference Node's
`status.capacity.ephemeral-storage` and `status.allocatable.ephemeral-storage`
and uses them for **every new worker's disk**. The chart always renders exactly
one `NODE_NAME`, including an empty value in static mode; do not set it through
`controller.env`, which rejects this reserved name.
The reference follows Node updates, with a one-minute periodic refresh; increases and
decreases replace the previous disk profile instead of accumulating a minimum.
Before the first valid observation, or if the reference node disappears, starts
terminating or reports invalid quantities, the scheduling catalogue advertises zero
local disk capacity and allocatable. CPU, memory and pod capacity stay available:
workloads whose combined `ephemeral-storage` requests are zero can still get workers.
Positive requests, including those of applicable DaemonSets, wait for a valid disk
reference. A NodePool with an explicit `spec.limits.ephemeral-storage` also waits,
because an unknown disk size cannot be counted safely against that budget. Its types
remain in the catalogue with unavailable offerings, so empty-node cleanup continues.
If the reference is lost after planning a disk-dependent claim, that unlaunched claim
is failed as insufficient capacity and re-planned; it does not block cluster-wide
synchronization while waiting for the reference. Existing-node scheduling and cleanup continue. A valid observation automatically
restores the disk capacity; transient API read failures retain the last valid
observation without a TTL. Host capacity and allocatable must be positive whole
bytes, with allocatable no greater than capacity.

**Dynamic sizing assumes the reference has the disk and reservations that new
workers will receive.** Verify both before opting in. Existing fixed workers retain
their old disks after a platform change. The control-plane disk in `ALL_IN_ONE` has
not been measured against the enlarged worker profile; being the controller's host
does not establish that it is representative. CPU, memory and pod capacity are never
copied from this reference.

Use a stable node that Karpenter cannot deprovision. In `host` mode,
`nodeSelector: {kubernetes.io/hostname: <representative-host>}` can restrict the
controller to a verified host without weakening the default placement affinity.
With several replicas, their hosts must have the same disk and reservations;
`node` mode lets every replica use one explicit reference independently of placement.
If CKE changes only future workers while the reference stays old, this mechanism
cannot discover the new size: select a representative reference, or move the
controller in host mode. Restarting it on the same old host is insufficient.

A valid reference is authoritative in both directions. A disk near the historical
39 Gi profile is not silently replaced with the larger static profile: that would
overstate capacity after a genuine platform disk decrease.

Each new NodeGroup records the resolved disk capacity and allocatable in its initial
creation annotations. Retries and controller restarts describe that group with its
original disk profile even after the host reference changes; adopting such a group
never creates a replacement if the original disappears. A launch without a valid
reference records a `0/0` snapshot: no local disk capacity was guaranteed when it was
planned. This does not mean the VM has no disk. Once the node registers, core schedules
against its actual Node status; a later reference observation does not rewrite the
launch snapshot. Groups created before this feature have no snapshot and retain the
previous catalogue-based description; they are adopted without rechecking their
original launch against the current disk reference.

Existing workers retain their actual disks. Their disk observations never change the
new-launch profile, whether they are smaller or larger, and a profile change alone
neither resizes nor drifts them. Remote PVCs provisioned through a StorageClass/CSI
are independent: a pod can use a 200Gi remote PVC on a node with a smaller local disk.
Only its separate `ephemeral-storage` request consumes this local scheduling capacity.

For local runs, use `make run RUN_NODE_NAME=<reference-node>` to enable dynamic
sizing. `make run` otherwise clears an inherited `NODE_NAME`. When starting the
binary directly, set `NODE_NAME` to the representative node; unset or empty uses
the same measured static profile as the chart's default. See the
[installation and upgrade guide](docs/getting-started/installation.md#local-disk-reference)
for choosing the reference and migrating existing environment overrides.

CPU, memory and pod capacity still follow bounded observations from managed workers:

- The node must belong to a NodeGroup this controller created and carry its immutable
  flavor. CKE names a group's nodes `<nodegroup>-node<N>`; the name must agree with
  the nodegroup label, so another kubelet cannot select an unrelated group's flavor.
- CPU must equal the catalogue entry's; memory and pod capacity/allocatable must stay
  within 10%. Allocatable cannot exceed capacity, and resource pairs must be complete.
- The smallest accepted figure per flavor wins for the process lifetime. Ordinary
  worker disks are ignored in host-reference mode, preserving useful CPU/RAM reports
  across disk generations. In static fallback mode, disk capacity, allocatable and
  their ratio must fit the current or legacy profile within 10%; legacy disks do not
  lower the new-launch profile.

A `memoryKi` override is therefore corrected by nodes within 10% of it, and served
as-is otherwise. A managed worker's kubelet can still move its flavor's learned
figures within those bounds. Refusals are counted in
`karpenter_clevercloud_instancetype_observed_capacity_rejections_total`; the host
reference has separate readiness and byte gauges (see
[docs/observability.md](docs/observability.md#controller-node-disk-reference)).
Runtime capacity corrections never change a flavor's price.

**Prices are relative, not a currency.** Karpenter only compares and adds up offering prices, to
launch the cheapest flavor that fits and to consolidate onto cheaper capacity, so every flavor is
priced by its size relative to the built-in `2XS`:

```text
price = 1/3 × cpu / cpu(2XS) + 2/3 × memoryKi / memoryKi(2XS)      (2XS = 1.0)
```

| Flavor | 2XS | XS | S | M | L | XL |
|---|---|---|---|---|---|---|
| Price | 1.0 | 1.8554 | 2.7097 | 3.566 | 5.1084 | 6.8212 |

Memory weighs twice as much as cpu because that is how CKE's public worker prices are built (a GB of
memory costs as much as two vCPUs). With these weights and the built-in sizing, whenever CKE bills
one set of up to four nodes less than another, its relative price is lower too, so consolidation
never swaps nodes for capacity that really costs more.

There is no price to configure: an override's price is derived from its resulting `cpu` and
`memoryKi`, so pinning a flavor's memory also reprices it, still relative to the built-in `2XS`.
The guarantee above then only holds while the pinned values stay proportionate across flavors.
Pinning all six the same way, as the example does, keeps it; pinning one alone can break it. Pinned
alone 5% below its built-in memory, `XL` makes merging four small nodes (three `2XS` and an `M`)
into one `XL` look like a saving although that `XL` really costs more; pinned alone to what the
previous node image reported (about 5% more), it looks dearer than an `XS` and an `L` together,
which CKE bills more, so merging those two into one `XL`, a real saving, is never made. Pin every
flavor the same way, or none.

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
plane** (a 3-node `S` control plane consumes 24 vCPU / 36 GB of it, leaving 16 vCPU / 4 GB for workers), and every other
cluster of the organisation draws on it too. When the quota rejects a node, Karpenter plans around that flavor for a
minute and around every larger one for two, and keeps launching the smaller ones, which may still fit (see
[Unavailable flavors](docs/observability.md#unavailable-flavors)).

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

v0.12.0 still admitted subdomained `kubernetes.io/` keys (such as `app.kubernetes.io/part-of`) and `karpenter.sh`
keys. A NodeClass that carries one from then (whatever its value, up to the 63 characters v0.12.0 enforced) stays
Ready and keeps provisioning: no node launched from now on gets the key, and the `LabelsIgnored` status condition
and a `LabelsIgnored` warning event name it. Until it is removed, the CRD refuses any edit of `spec.labels` that
keeps it.

Remove it only after the upgraded controller has reconciled the NodeClass. v0.12.0 hashed the key into the drift
stamp of every NodeGroup it built, so removing it drifts those nodes until the controller has re-stamped them with
a hash that leaves it out (annotation `karpenter.clever-cloud.com/clevernodeclass-hash-version: v3`). The
`LabelsIgnored` condition says when that is done: its reason is then `LegacyLabelKeys`, and its message says that
no node drifts for removing the key. While its reason is `HashMigrationPending`, its message counts the NodeGroups
that would still drift, such as one whose node already drifted for another reason and keeps its stamp until it is
replaced. Removing the key before the upgrade, in the same change as the upgrade, or while v0.12.0 still runs
drifts the nodes v0.12.0 built from the NodeClass, and Karpenter replaces them.

Changing a label the NodeClass delivers marks the NodeClaims built from it as drifted; Karpenter then replaces
those nodes rolling-style. Adding or removing a key the NodeClass does not deliver drifts nothing, except on the
nodes v0.12.0 built that the upgraded controller has not re-stamped yet (above).

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

### Topology labels

The platform puts no topology labels on its nodes. Karpenter puts both well-known ones on every node
it launches, with the value of `CLEVER_CLOUD_REGION` (`settings.region`, `par` by default):
`topology.kubernetes.io/region` and `topology.kubernetes.io/zone`. CKE is single-zone, so the zone is
the region. A pod nodeSelector or node affinity, a NodePool requirement, a StorageClass
`allowedTopologies` or a PersistentVolume node affinity on either key selects those nodes. A value
other than the configured one matches no instance type, so Karpenter provisions nothing for it.

Nodes launched by v0.12.0 or earlier carry the zone but not the region, and upgrading neither adds
it to them nor replaces them: karpenter-core takes a NodeClaim's labels from the provider once, at
launch, and copies them onto the node at registration. A pod that requires the region only lands on
nodes launched after the upgrade, Karpenter launching one for it if needed. To let it use the older
nodes right away, label them with your `CLEVER_CLOUD_REGION` (`par` below, the default: replace it
if you changed the setting):

```
$ kubectl label nodes -l 'karpenter.sh/nodepool,!topology.kubernetes.io/region' topology.kubernetes.io/region=par
```

A NodePool that requires the region replaces its older nodes through drift under its disruption
budgets, as it already did before the upgrade; their replacements carry the label and stay.

The nodes Karpenter launches after the upgrade form a single region domain, as they already formed
a single zone domain. A required pod anti-affinity keyed on `topology.kubernetes.io/region` therefore
allows one matching replica across all of them, exactly as one keyed on the zone does. Before the
upgrade no node carried the region, and a node without the topology key satisfies anti-affinity, so
such a rule limited nothing. Its replicas beyond the first keep running where they are, but once the
older nodes are gone (or labelled by hand) a replica that has to be rescheduled stays Pending, and
Karpenter cannot launch a node for it. Key such a rule on `kubernetes.io/hostname` to keep one replica
per node.

## Observability

The controller serves Prometheus metrics on `:8080/metrics`, exposed by the chart's `karpenter` Service, and publishes
Kubernetes Events on the NodeClaims and NodeGroups it acts on. The [observability runbook](docs/observability.md) lists
every provider metric and event, what each one means and what to do when it moves, with suggested alert expressions.

## License

See the [license](LICENSE).

## Getting in touch

- Open an [issue](https://github.com/CleverCloud/karpenter-provider-clever-cloud/issues) for bugs or feature requests
