# Contributing

Thanks for your interest in improving the Karpenter provider for Clever Kubernetes Engine (CKE)!

This provider bridges Karpenter and Clever Cloud's in-cluster NodeGroup API. If your issue is not
specific to Clever Cloud (scheduling, consolidation, NodePool semantics, ...), it most likely
belongs upstream in [kubernetes-sigs/karpenter](https://github.com/kubernetes-sigs/karpenter).

## Development environment

You need Go (the version pinned in [go.mod](go.mod)), `make`, and — for chart or deployment work —
`helm` and `kubectl`. A ready-to-use [devcontainer](.devcontainer/) is included. Run `make help`
for the list of targets.

## Local validation chain

Before opening a pull request, the local validation chain must be green:

```sh
make vet         # go vet
make lint        # golangci-lint (see .golangci.yml)
make build
make test
make generate    # must leave the tree clean — CI fails if it produces a diff
make chart-lint  # when touching charts/ or deploy/
```

## Commit messages

Commits follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) and are
checked by CI on every pull request (no `fixup!`/`squash!` commits either — autosquash before
pushing the final revision):

```
<type>(<scope>): <imperative, lowercase summary>
```

- Types: `feat`, `fix`, `chore`, `perf`, `docs`, `test`, `ci`, `refactor`, `style`, `revert`.
  Append `!` for a backwards-incompatible change.
- Scopes name the touched area: `cloudprovider`, `nodegroup`, `instancetype`, `nodeclass`,
  `apis`, `charts`, `deploy`, `ci`, `docs`, ...
- Write the summary and body in English. Use the body to explain *why* — especially for anything
  touching the CKE quirks documented in [CLAUDE.md](CLAUDE.md) (quota handling, the
  1 NodeClaim = 1 NodeGroup invariant, ...).

## Tests land in the same changeset

A behavior change ships its tests in the same pull request — not as a follow-up. Unit tests use
the controller-runtime fake client; see
[pkg/cloudprovider/cloudprovider_test.go](pkg/cloudprovider/cloudprovider_test.go) for the
pattern (no cluster needed). Changes to provisioning, quota, or lifecycle behavior should also be
validated against a live CKE cluster and recorded in
[docs/E2E-RESULTS.md](docs/E2E-RESULTS.md), following the format used there.

## Generated files

Never hand-edit `zz_generated.deepcopy.go` files, the generated CleverNodeClass CRD in
[deploy/crds/](deploy/crds/), or the chart CRD copies under
[charts/karpenter/crds/](charts/karpenter/crds/) and
[charts/karpenter-crd/templates/](charts/karpenter-crd/templates/). Run `make generate` after any
change to `pkg/apis/` and commit the result; CI rejects drift. The `karpenter.sh_*.yaml` CRDs are
vendored from the matching karpenter-core release and only change when `sigs.k8s.io/karpenter` is
bumped (a manual operation — Dependabot deliberately ignores it).

## Docs are code

If a change alters behavior, configuration, or installation, update the README, `docs/`, or
`examples/` in the same pull request. New Go files carry the Apache 2.0 license header used by the
existing sources.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE). There is no CLA to sign.
