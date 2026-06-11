# karpenter-crd

Helm chart that installs and **upgrades** the CustomResourceDefinitions used
by the [Karpenter provider for Clever Kubernetes
Engine](https://github.com/diodonfrost/karpenter-provider-clever-cloud) as
regular, Helm-managed resources:

- `nodepools.karpenter.sh`
- `nodeclaims.karpenter.sh`
- `nodeoverlays.karpenter.sh`
- `clevernodeclasses.karpenter.clever-cloud.com`

## Why a separate chart

The main [karpenter](../karpenter/README.md) chart
carries the same CRDs in its `crds/` directory. Helm installs those on first
install but **never upgrades or deletes** them afterwards. This chart ships
the CRDs in `templates/` instead, so `helm upgrade` keeps them in sync with
the controller version — the same split as upstream Karpenter's
`karpenter-crd` chart.

The CRD sources live in [deploy/crds/](../../deploy/crds/); `make
sync-chart-crds` regenerates both charts from them.

## Install

Each release publishes the chart to ghcr.io as an OCI artifact, versioned on
the release tag without the `v` prefix (release `v0.9.1` → chart version
`0.9.1`):

```sh
helm upgrade --install karpenter-crd \
  oci://ghcr.io/diodonfrost/karpenter-provider-clever-cloud/charts/karpenter-crd \
  --version <version> --namespace karpenter --create-namespace
```

Or, from a repo checkout:

```sh
helm upgrade --install karpenter-crd charts/karpenter-crd \
  --namespace karpenter --create-namespace
```

Then install the main chart — the copies in its `crds/` directory are
skipped because the CRDs already exist.

## Taking over CRDs installed another way

If the CRDs were first created by the main chart's `crds/` directory or with
`kubectl apply -f deploy/crds/`, they carry no Helm ownership metadata and
installing this chart fails with `invalid ownership metadata`. Adopt them
first:

```sh
for crd in nodepools.karpenter.sh nodeclaims.karpenter.sh \
           nodeoverlays.karpenter.sh clevernodeclasses.karpenter.clever-cloud.com; do
  kubectl label crd "$crd" app.kubernetes.io/managed-by=Helm --overwrite
  kubectl annotate crd "$crd" meta.helm.sh/release-name=karpenter-crd --overwrite
  kubectl annotate crd "$crd" meta.helm.sh/release-namespace=karpenter --overwrite
done
```

## Values

| Key | Default | Description |
|---|---|---|
| `additionalAnnotations` | `{}` | Extra annotations added to every CRD |

## Uninstall

> **Warning:** unlike CRDs shipped through a `crds/` directory, the CRDs in
> this chart are ordinary templates — `helm uninstall` **deletes them**, and
> with them every NodePool, NodeClaim and CleverNodeClass in the cluster.
> Never uninstall this chart while Karpenter still manages nodes.
