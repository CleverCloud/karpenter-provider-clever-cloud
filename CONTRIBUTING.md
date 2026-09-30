# Contributing

Thanks for your interest in improving the Karpenter provider for Clever Kubernetes Engine (CKE)!

This provider bridges Karpenter and Clever Cloud's in-cluster NodeGroup API. If your issue is not
specific to Clever Cloud (scheduling, consolidation, NodePool semantics, ...), it most likely
belongs upstream in [kubernetes-sigs/karpenter](https://github.com/kubernetes-sigs/karpenter).

## Development environment

You need Go (the version pinned in [go.mod](go.mod) or newer), `make`, `helm` (the chart checks
and the generated-manifest check below render the charts), and `kubectl` for deployment work. Run
`make help` for the list of targets.

The `go` directive in go.mod names one exact patch release: CI installs that release, and the
[Dockerfile](Dockerfile) builder image is pinned to the same tag, because the golang image never
switches toolchains (`GOTOOLCHAIN=local`): its tag, not go.mod, decides which standard library
ships in the image. Bump both together (Go patch releases carry the standard-library security
fixes); a unit test in `cmd/controller` fails when they diverge. Nothing bumps them automatically:
Dependabot does not raise the `go` directive for Go security releases, and a Dockerfile-only bump
would fail that test by design. The trigger is the daily Govulncheck job of the CodeQL workflow
(`make vulncheck`), which turns red as soon as a standard-library vulnerability reachable from this
code is published.
Move both pins to the latest patch release of the Go minor, not merely the first one that fixes the
reported issue.

## Local validation chain

Before opening a pull request, the local validation chain must be green. It mirrors the build,
test, lint and vulnerability gates CI runs on every pull request:

```sh
make vet
make lint          # golangci-lint (see .golangci.yml)
make build
make test
go test -race ./pkg/... ./cmd/...
make test-envtest  # controllers against a real kube-apiserver (downloads the envtest binaries)
make chart-lint
make test-chart    # go test ./test/chart/... — fails instead of skipping when helm is missing
make vulncheck     # govulncheck under the go.mod toolchain (fetches vuln.go.dev)
make generate raw-manifest && git status --porcelain  # on a committed tree: must print nothing
```

`make lint` runs golangci-lint on the Go toolchain go.mod pins — the one CI lints with — whatever
Go you have installed: a newer local Go changes the standard library the analyzers load, which
can change the findings or crash them. The first run fetches that toolchain through the module
proxy, like any module. `make generate raw-manifest` regenerates the CRDs, both chart copies and
[deploy/karpenter.yaml](deploy/karpenter.yaml) from their sources; CI fails if it leaves a diff, so
commit whatever it changes.

`make vulncheck` reads the current vulnerability database, so it can turn red on an unchanged
tree the day an advisory is published (CI also runs it daily, beside CodeQL). Fix such a finding
in its own pull request: bump the affected module or, for a standard-library advisory, the `go`
directive in [go.mod](go.mod) together with the Dockerfile builder tag (see
[Development environment](#development-environment)).

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

Never hand-edit `zz_generated.deepcopy.go` files, the CRDs in [deploy/crds/](deploy/crds/), or the
chart CRD copies under [charts/karpenter/crds/](charts/karpenter/crds/) and
[charts/karpenter-crd/templates/](charts/karpenter-crd/templates/). Run `make generate` after any
change to `pkg/apis/` and commit the result; CI rejects drift. The `karpenter.sh_*.yaml` CRDs are
synced by `make generate` from the pinned `sigs.k8s.io/karpenter` module, so they only change when
the dependency is bumped.

## Upgrading karpenter-core

`sigs.k8s.io/karpenter` is deliberately in its own Dependabot group (together with
`github.com/awslabs/operatorpkg`, which defines the `status.Object` contract karpenter's API types
implement and must move in lockstep): these bumps carry more than Go code and follow this playbook
(a Dependabot PR stays red on CI — compilation and/or the generated-files check — until step 2 is
pushed to it):

1. Check the [compatibility matrix](https://karpenter.sh/docs/upgrading/compatibility/): the
   karpenter minor must support every Kubernetes version CKE offers (the public
   `/v4/kubernetes-product` endpoint lists them). Never ship a karpenter minor whose matrix
   excludes the CKE default version.
2. Bump the module (`go get sigs.k8s.io/karpenter@vX.Y.Z && go mod tidy` — operatorpkg usually
   moves with it), fix any API breakage, then run `make generate` — it refreshes the vendored
   `karpenter.sh_*.yaml` CRDs from the module and syncs both chart copies. Review the CRD diff
   for schema changes users would see.
3. Build and fix API breakages, then run the full [local validation chain](#local-validation-chain).
4. Skim the karpenter-core release notes for behavior changes in provisioning, disruption, or the
   CloudProvider contract; call them out in the commit body.
5. Remind users in the release notes that the `karpenter-crd` chart must be upgraded before the
   controller chart (see [installation.md](docs/getting-started/installation.md)).

## Docs are code

If a change alters behavior, configuration, or installation, update the README, `docs/`, or
`examples/` in the same pull request. New Go files carry the Apache 2.0 license header used by the
existing sources.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE). There is no CLA to sign.
