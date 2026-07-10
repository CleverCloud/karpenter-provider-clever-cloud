#!/usr/bin/env bash
# Out-of-band cleanup for the e2e suite (test/e2e). The suite cleans up after
# itself, but if its process dies (go test timeout, runner eviction) the
# controller is gone with it: NodeClaims keep their termination finalizer
# forever and NodeGroups keep their VMs billing hourly. This script needs no
# controller — it deletes NodeGroups directly (the platform operator tears
# down the VMs) and strips the stuck finalizers by hand.
#
# Everything the suite creates is either labeled e2e.karpenter.clever-cloud.com/suite=true
# or named with the e2e- prefix (NodeGroups and Nodes inherit their NodeClaim's
# name). Safe to run at any time; it only ever touches those objects.
# Exits non-zero if a live e2e NodeGroup survives the sweep.
#
# Usage: KUBECONFIG=... E2E_CONTEXT=<context> hack/e2e-cleanup.sh
set -euo pipefail

if [ -z "${E2E_CONTEXT:-}" ]; then
  echo "E2E_CONTEXT is required (refusing to sweep an implicit current-context)" >&2
  exit 1
fi
KUBECTL=(kubectl --context "${E2E_CONTEXT}")

# names <kind> [extra args...] — prints e2e- resource names, one per line.
# kubectl runs as its own statement so set -e still catches its failures
# (a kubectl error inside `mapfile < <(kubectl | grep || true)` is silently
# swallowed and would turn the sweep into a green no-op).
names() {
  local kind="$1"; shift
  local out
  out="$("${KUBECTL[@]}" get "$kind" "$@" -o name)"
  grep '/e2e-' <<<"$out" || true
}

echo "==> e2e leftovers on context ${E2E_CONTEXT}"

# 1. Workloads first: deleting the namespaces releases the pods.
mapfile -t namespaces < <("${KUBECTL[@]}" get namespaces -l e2e.karpenter.clever-cloud.com/suite=true -o name)
for ns in "${namespaces[@]}"; do
  echo "deleting ${ns}"
  "${KUBECTL[@]}" delete "$ns" --ignore-not-found --wait=false
done

# 2. NodeGroups: direct deletion, the platform operator handles VM teardown.
#    Covers both provisioned groups (named after their e2e- NodeClaim) and
#    the suite's hand-made decoys.
mapfile -t groups < <(names nodegroups.api.clever-cloud.com)
for ng in "${groups[@]}"; do
  echo "deleting ${ng} (VM bills until it is gone)"
  "${KUBECTL[@]}" delete "$ng" --ignore-not-found --wait=false
done

# 3. NodeClaims of e2e pools: without the controller their termination
#    finalizer never clears — strip it, then delete.
mapfile -t claims < <(names nodeclaims.karpenter.sh)
for claim in "${claims[@]}"; do
  echo "force-deleting ${claim}"
  "${KUBECTL[@]}" patch "$claim" --type merge -p '{"metadata":{"finalizers":null}}'
  "${KUBECTL[@]}" delete "$claim" --ignore-not-found --wait=false
done

# 4. NodePools, then NodeClasses (same stuck-finalizer treatment).
"${KUBECTL[@]}" delete nodepools.karpenter.sh -l e2e.karpenter.clever-cloud.com/suite=true --ignore-not-found
mapfile -t classes < <("${KUBECTL[@]}" get clevernodeclasses.karpenter.clever-cloud.com -l e2e.karpenter.clever-cloud.com/suite=true -o name)
for class in "${classes[@]}"; do
  echo "force-deleting ${class}"
  "${KUBECTL[@]}" patch "$class" --type merge -p '{"metadata":{"finalizers":null}}'
  "${KUBECTL[@]}" delete "$class" --ignore-not-found --wait=false
done

# 5. Node objects can outlive their VM when nothing ran the normal
#    termination flow (cosmetic, but don't litter the test cluster).
mapfile -t nodes < <(names nodes)
for node in "${nodes[@]}"; do
  echo "deleting orphaned ${node}"
  "${KUBECTL[@]}" delete "$node" --ignore-not-found --wait=false
done

# 6. Assert the sweep took: a live (not-yet-deleting) e2e NodeGroup at this
#    point means VMs keep billing — fail loudly rather than reporting green.
#    deletionTimestamp is absent (renders empty) on live objects, so filter
#    with awk rather than a jsonpath field comparison.
all_groups="$("${KUBECTL[@]}" get nodegroups.api.clever-cloud.com \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.deletionTimestamp}{"\n"}{end}')"
live="$(awk '$1 ~ /^e2e-/ && $2 == "" {print $1}' <<<"$all_groups")"
if [ -n "$live" ]; then
  echo "ERROR: live e2e nodegroups survived the sweep (billing hourly):" >&2
  echo "$live" >&2
  exit 1
fi
echo "==> sweep complete; any remaining e2e nodegroup is already terminating"
