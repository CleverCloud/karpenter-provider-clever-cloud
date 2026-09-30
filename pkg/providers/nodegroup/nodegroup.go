/*
Copyright 2026 The karpenter-provider-clever-cloud Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package nodegroup manages the Clever Cloud NodeGroups backing Karpenter
// NodeClaims. The mapping is strictly one NodeClaim to one NodeGroup with
// nodeCount=1: the NodeGroup carries the NodeClaim's name, and the single
// node it produces is named "<nodegroup>-node0" by Clever Cloud.
package nodegroup

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"

	ngv1 "github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/nodegroup/v1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/apis/v1alpha1"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/metrics"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

const (
	// ProviderIDPrefix prefixes every provider ID handed to Karpenter.
	// The full form is "clevercloud://<nodegroup-name>".
	ProviderIDPrefix = "clevercloud://"

	// quotaCheckInterval is the poll period during the quota check.
	quotaCheckInterval = time.Second
)

// quotaCheckTimeout bounds how long Create waits for the Clever Cloud
// operator's decision on a new NodeGroup. The decision is the operator's
// first status write, about 1 s after the group's creation live: phase
// Creating + ReconcileInProgress=True on a group it accepted, QuotaExceeded
// written directly on one the quota engine rejects. The VM comes up far
// later (Synced at 38-58 s), after Create has returned. A timeout therefore
// means the operator did not even acknowledge the group; Create proceeds
// optimistically and lets Karpenter's registration TTL be the backstop.
// Variable only so tests can exercise the timeout path without waiting 15s.
var quotaCheckTimeout = 15 * time.Second

// QuotaBackoff is how long a quota rejection of a flavor makes that flavor
// unavailable: Create fails it fast instead of churning create/delete cycles
// against the Clever Cloud API, and the cloud provider reports its offerings
// unavailable to the scheduler. Every other flavor at least as large stays
// unavailable for twice as long, so that the rejected flavor is retried first
// (see quotaRejection.coverage). Deleting a NodeGroup that may hold capacity
// ends both; deleting one the operator refused and that never synced does not
// (see Delete).
const QuotaBackoff = time.Minute

// ErrQuotaExceeded is returned by Create when the organisation quota rejects
// the requested capacity, or a recent rejection covers it. Flavor is the
// flavor of the failed launch: the requested one, or the flavor of the group
// Create adopted.
type ErrQuotaExceeded struct {
	Flavor  string
	Message string
}

func (e *ErrQuotaExceeded) Error() string {
	if e.Message == "" {
		// Live, the operator's rejection carries no message.
		return fmt.Sprintf("clever cloud quota exceeded for flavor %s", e.Flavor)
	}
	return fmt.Sprintf("clever cloud quota exceeded for flavor %s: %s", e.Flavor, e.Message)
}

// ErrFlavorRejected is returned by Create when the upstream operator refuses
// the NodeGroup for a reason that is not the organisation quota — a flavor the
// cluster cannot provision, a spec it will not accept. It is terminal: waiting
// for a group the operator has already refused only burns karpenter's
// registration TTL, and retrying the same flavor reproduces it. A failure the
// operator retries on its own (ngv1.NodeGroup.TransientFailure, e.g. a Clever
// Cloud API error) is not a refusal and never produces this error.
type ErrFlavorRejected struct {
	Flavor  string
	Reason  string
	Message string
}

func (e *ErrFlavorRejected) Error() string {
	return fmt.Sprintf("clever cloud refused flavor %s (%s): %s", e.Flavor, e.Reason, e.Message)
}

// flavorBackoff is how long a flavor stays unavailable after the upstream
// operator refused it. Long enough that karpenter re-plans onto another flavor
// instead of looping on the refused one, short enough that a transient refusal
// or a platform-side fix is picked up without a restart.
const flavorBackoff = 5 * time.Minute

// Catalogue sizes flavors with the vCPU count and memory their catalogue
// entries advertise, the two resources the organisation quota counts.
// *instancetype.Provider implements it.
type Catalogue interface {
	Sizing(flavor string) (instancetype.Sizing, bool)
}

// Provider performs CRUD operations on Clever Cloud NodeGroups.
type Provider struct {
	kubeClient client.Client
	recorder   events.Recorder
	// catalogue sizes the flavors a quota rejection makes unavailable. Nil
	// sizes none, so that a rejection covers the rejected flavor only.
	catalogue Catalogue
	// clock times the quota backoff and the refusal hold-out.
	clock clock.PassiveClock

	// createMu serializes NodeGroup creations. Concurrent creations make the
	// upstream quota engine evaluate all in-flight groups together, rejecting
	// several at once; deleting groups while their first upstream reconcile
	// is still running has been observed to leak upstream reservations.
	// It is held until the operator has decided on the group (its first
	// status write, about 1 s after creation), by which point the quota
	// engine has evaluated it, and not until the group is Ready: waiting for
	// Ready outlasted the 15s window on every healthy launch, so launches ran
	// 15 s apart, and karpenter-core, whose cluster state stays unsynced
	// while any NodeClaim lacks a provider ID, paused disruption cluster-wide
	// for as long as the queue lasted.
	createMu sync.Mutex

	// mu guards what the provider learned from the operator's refusals, which
	// Unavailable reports. karpenter-core keeps no per-offering memory of an
	// InsufficientCapacityError: it deletes the claim and re-plans over the
	// same offerings, so this state, turned into unavailable offerings by the
	// cloud provider, is the only thing that keeps the scheduler from
	// rebuilding the claim that was just refused.
	mu sync.Mutex
	// quotaRejections remembers, per flavor, the organisation quota's last
	// rejection of it.
	quotaRejections map[string]quotaRejection
	// rejectedFlavors remembers, per flavor, the upstream operator's last
	// refusal of it for a non-quota reason.
	rejectedFlavors map[string]flavorHoldOut
	// holdOutSeq is the sequence number of the last hold-out recorded.
	holdOutSeq uint64
}

// flavorHoldOut is a flavor held out of provisioning after a refusal.
type flavorHoldOut struct {
	// at is when the refusal was recorded; the hold-out lasts flavorBackoff.
	at time.Time
	// seq orders the hold-out among all those recorded, so that an acceptance
	// releases only the ones recorded before its launch's create call (see
	// clearFlavorRejection). A sequence rather than a time: two readings of
	// the clock can be equal (a test's fake clock does not move at all), and
	// the order must be exact.
	seq uint64
}

// quotaRejection is the organisation quota's last rejection of a flavor.
type quotaRejection struct {
	at      time.Time
	message string
	// size is the rejected flavor's catalogue sizing; sized is false for a
	// flavor the catalogue cannot size, whose rejection covers only itself.
	size  instancetype.Sizing
	sized bool
}

// NewProvider builds a Provider. catalogue sizes the flavors a quota
// rejection makes unavailable (see Unavailable); nil sizes none. clk times how
// long they stay unavailable.
func NewProvider(kubeClient client.Client, recorder events.Recorder, catalogue Catalogue, clk clock.PassiveClock) *Provider {
	return &Provider{kubeClient: kubeClient, recorder: recorder, catalogue: catalogue, clock: clk}
}

// sizing returns flavor's catalogue sizing; ok is false when there is no
// catalogue or it cannot size flavor.
func (p *Provider) sizing(flavor string) (instancetype.Sizing, bool) {
	if p.catalogue == nil {
		return instancetype.Sizing{}, false
	}
	return p.catalogue.Sizing(flavor)
}

// recordQuotaRejection records that the organisation quota rejected flavor.
// Until the rejection's coverage ends (see quotaRejection.coverage) or
// capacity is freed, Create fails flavor and every flavor at least as large
// fast and Unavailable reports them, while every smaller flavor stays
// available — it may still fit.
func (p *Provider) recordQuotaRejection(flavor, message string) {
	size, sized := p.sizing(flavor)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.quotaRejections == nil {
		p.quotaRejections = map[string]quotaRejection{}
	}
	p.quotaRejections[flavor] = quotaRejection{at: p.clock.Now(), message: message, size: size, sized: sized}
}

// coverage reports until when this rejection of flavor rejected keeps flavor
// unavailable, and whether it covers flavor at all; size and sized are
// flavor's catalogue sizing.
//
// It covers rejected itself for QuotaBackoff. The quota counts vCPUs and
// memory, so capacity it cannot fit rejected into cannot fit a flavor at least
// as large either: the rejection covers each of those for twice as long. Once
// the first window ends, rejected — the cheapest probe of whether capacity came
// back — is launchable again on its own, and the larger flavors reopen a window
// later, after that probe, unless it was rejected in turn. Were they all to
// reopen together, a quota that stays exhausted would cost a walk down the
// catalogue in every window: rejecting the smallest flavor covers every other
// one, so every rejection would expire at the same moment, core would pack the
// pending pods back onto the largest claim, and each flavor size on the way
// down would cost one more real rejection upstream. A flavor the catalogue
// cannot size is covered by its own rejection only, and its rejection covers
// no other flavor.
func (r quotaRejection) coverage(rejected, flavor string, size instancetype.Sizing, sized bool) (time.Time, bool) {
	if rejected == flavor {
		return r.at.Add(QuotaBackoff), true
	}
	if sized && r.sized && size.AtLeast(r.size) {
		return r.at.Add(2 * QuotaBackoff), true
	}
	return time.Time{}, false
}

// quotaRejectionCovering returns the quota rejection in force that keeps flavor
// unavailable the longest, the flavor it rejected and when it stops covering
// flavor. p.mu must be held; size and sized are flavor's catalogue sizing.
func (p *Provider) quotaRejectionCovering(flavor string, size instancetype.Sizing, sized bool) (string, quotaRejection, time.Time, bool) {
	now := p.clock.Now()
	var (
		coveredBy string
		covering  quotaRejection
		until     time.Time
		found     bool
	)
	for rejected, r := range p.quotaRejections {
		end, covers := r.coverage(rejected, flavor, size, sized)
		if !covers || !now.Before(end) {
			continue
		}
		// Ties broken by name, so the fast-fail names the same rejection
		// whatever the map order.
		if !found || end.After(until) || (end.Equal(until) && rejected < coveredBy) {
			coveredBy, covering, until, found = rejected, r, end, true
		}
	}
	return coveredBy, covering, until, found
}

// Unavailable reports whether a launch of flavor is known to fail right now,
// which the cloud provider turns into Available=false offerings: a quota
// rejection covers it (see quotaRejection.coverage) and no capacity was freed
// since, or the operator refused flavor itself within the last flavorBackoff.
func (p *Provider) Unavailable(flavor string) bool {
	size, sized := p.sizing(flavor)
	p.mu.Lock()
	defer p.mu.Unlock()
	if hold, held := p.rejectedFlavors[flavor]; held && p.clock.Since(hold.at) < flavorBackoff {
		return true
	}
	_, _, _, covered := p.quotaRejectionCovering(flavor, size, sized)
	return covered
}

// quotaFastFail returns the error Create fails a launch of flavor with, without
// calling the API, while a quota rejection covers it; nil otherwise.
func (p *Provider) quotaFastFail(flavor string) error {
	size, sized := p.sizing(flavor)
	p.mu.Lock()
	defer p.mu.Unlock()
	rejected, r, until, covered := p.quotaRejectionCovering(flavor, size, sized)
	if !covered {
		return nil
	}
	cause := "the organisation quota rejected it"
	if rejected != flavor {
		cause = fmt.Sprintf("the organisation quota rejected %s, and it is at least as large", rejected)
	}
	if r.message != "" {
		cause += " (" + r.message + ")"
	}
	remaining := max(until.Sub(p.clock.Now()).Round(time.Second), time.Second)
	return &ErrQuotaExceeded{Flavor: flavor, Message: fmt.Sprintf("%s; it stays unavailable for another %s unless capacity is freed", cause, remaining)}
}

func (p *Provider) recordFlavorRejection(flavor string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rejectedFlavors == nil {
		p.rejectedFlavors = map[string]flavorHoldOut{}
	}
	p.holdOutSeq++
	p.rejectedFlavors[flavor] = flavorHoldOut{at: p.clock.Now(), seq: p.holdOutSeq}
}

// holdOutMark returns the sequence number of the last hold-out recorded so
// far. A launch takes it just before its create call, and its acceptance then
// releases only the hold-outs recorded up to it (clearFlavorRejection).
func (p *Provider) holdOutMark() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.holdOutSeq
}

// clearFlavorRejection forgets the refusal of a flavor the operator has just
// accepted, unless the refusal was recorded after mark, the launch's
// holdOutMark. A refusal recorded while the launch was polled is not answered
// by its acceptance: it is typically the late refusal of a group acknowledged
// earlier (RecordLateRefusal, which does not take createMu), and an
// acknowledgement is exactly what that group had before it was refused, so it
// proves nothing about the flavor. Releasing that hold-out would hand the
// flavor straight back to the next launch, and a flavor refused late on every
// group would never stay held out while launches of it kept being
// acknowledged.
func (p *Provider) clearFlavorRejection(flavor string, mark uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if hold, held := p.rejectedFlavors[flavor]; held && hold.seq <= mark {
		delete(p.rejectedFlavors, flavor)
	}
}

// clearQuotaRejections forgets every quota rejection: capacity was freed, so
// any flavor may fit again.
func (p *Provider) clearQuotaRejections() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.quotaRejections = nil
}

// ProviderID returns the provider ID for a NodeGroup name.
func ProviderID(nodeGroupName string) string {
	return ProviderIDPrefix + nodeGroupName
}

// ParseProviderID extracts the NodeGroup name from a provider ID.
func ParseProviderID(providerID string) (string, error) {
	name := strings.TrimPrefix(providerID, ProviderIDPrefix)
	if name == providerID || name == "" {
		return "", fmt.Errorf("provider id %q is not a clever cloud provider id", providerID)
	}
	return name, nil
}

// NodeGroupOfNode returns the NodeGroup a node belongs to according to its
// name: Clever Cloud names the nodes of a group "<nodegroup>-node<N>" (node0
// for the single node of a group this provider creates, node1 onwards when
// the group is resized). Unlike the clever-cloud.com/nodegroup label, which a
// kubelet can rewrite on its own Node, the name is bound to the kubelet's
// credential by the node authorizer, so it is what ties a node to a group
// when the node's word must not be taken. ok is false for a name without the
// suffix.
func NodeGroupOfNode(nodeName string) (string, bool) {
	i := strings.LastIndex(nodeName, nodeNameInfix)
	if i <= 0 {
		return "", false
	}
	index := nodeName[i+len(nodeNameInfix):]
	if index == "" || strings.Trim(index, "0123456789") != "" {
		return "", false
	}
	return nodeName[:i], true
}

// nodeNameInfix separates a NodeGroup's name from the index of its node.
const nodeNameInfix = "-node"

// IsManaged reports whether the NodeGroup was created by this provider.
func IsManaged(ng *ngv1.NodeGroup) bool {
	return ng.Labels[v1alpha1.ManagedLabelKey] == "true"
}

// NodeClaimOwners returns the names of the NodeClaim owner references stamped
// by Create. The managed label alone is forgeable — copying a karpenter-created
// manifest keeps it — so destructive paths that act on label-matched NodeGroups
// must require this ownership proof, and must key their decision on the
// referenced claim itself, never on the mutable labels.
func NodeClaimOwners(ng *ngv1.NodeGroup) []string {
	var names []string
	for _, ref := range ng.OwnerReferences {
		if ref.Kind == "NodeClaim" && strings.HasPrefix(ref.APIVersion, "karpenter.sh/") {
			names = append(names, ref.Name)
		}
	}
	return names
}

// Create creates the NodeGroup backing a NodeClaim and waits for the Clever
// Cloud operator's decision on it. It returns ErrQuotaExceeded (after cleaning
// up the NodeGroup) when the organisation quota rejects it, and
// ErrFlavorRejected on any other refusal. A group the operator acknowledged is
// returned at once, before its VM is up; the nodegroupstatus controller
// follows it from there.
// It is idempotent: an already-existing NodeGroup owned by the same NodeClaim
// is reused.
func (p *Provider) Create(ctx context.Context, nodeClaim *karpv1.NodeClaim, nodeClass *v1alpha1.CleverNodeClass, flavor string) (*ngv1.NodeGroup, error) {
	p.createMu.Lock()
	defer p.createMu.Unlock()
	// Checked under createMu: a claim resolved before a rejection that landed
	// while it waited here must not repeat it. Only the flavors the rejection
	// covers fail fast; a smaller one may fit and goes to the API. No event on
	// the fast-fail: karpenter-core already publishes an
	// InsufficientCapacityError event per attempt, and claims get fresh names
	// each retry so per-claim dedupe cannot bound the volume.
	if err := p.quotaFastFail(flavor); err != nil {
		return nil, err
	}
	ng := &ngv1.NodeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeClaim.Name,
			Labels: map[string]string{
				v1alpha1.ManagedLabelKey:   "true",
				v1alpha1.NodeClaimLabelKey: nodeClaim.Name,
				v1alpha1.NodePoolLabelKey:  nodeClaim.Labels[karpv1.NodePoolLabelKey],
				v1alpha1.NodeClassLabelKey: nodeClass.Name,
			},
			Annotations: map[string]string{
				v1alpha1.NodeClassHashLabelKey: nodeClass.Hash(),
				// Which generation of Hash() produced the value above; drift is
				// only evaluated between matching generations.
				v1alpha1.NodeClassHashVersionAnnotationKey: v1alpha1.NodeClassHashVersion,
			},
			// The NodeClaim owns the NodeGroup: if the NodeClaim disappears
			// without going through the termination flow, Kubernetes garbage
			// collection removes the NodeGroup (and Clever Cloud the VM).
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "karpenter.sh/v1",
					Kind:       "NodeClaim",
					Name:       nodeClaim.Name,
					UID:        nodeClaim.UID,
				},
			},
		},
		Spec: ngv1.NodeGroupSpec{
			Flavor:    flavor,
			NodeCount: 1,
			Labels:    nodeGroupLabels(nodeClaim, nodeClass),
			// The unregistered taint closes the race between node readiness
			// and Karpenter's label/taint sync; Karpenter removes it once
			// registration completes.
			Taints: []ngv1.NodeGroupTaint{
				{Key: karpv1.UnregisteredTaintKey, Effect: corev1.TaintEffectNoExecute},
			},
		},
	}
	// Taken just before the create call: the acceptance below releases only
	// the hold-outs recorded up to here (see clearFlavorRejection).
	mark := p.holdOutMark()
	if err := p.kubeClient.Create(ctx, ng); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("creating nodegroup, %w", err)
		}
		existing := &ngv1.NodeGroup{}
		if getErr := p.kubeClient.Get(ctx, types.NamespacedName{Name: ng.Name}, existing); getErr != nil {
			return nil, fmt.Errorf("getting existing nodegroup, %w", getErr)
		}
		if !IsManaged(existing) || existing.Labels[v1alpha1.NodeClaimLabelKey] != nodeClaim.Name ||
			!slices.Contains(NodeClaimOwners(existing), nodeClaim.Name) {
			return nil, fmt.Errorf("nodegroup %q already exists and is not managed by this nodeclaim", ng.Name)
		}
		ng = existing
	}
	accepted, retried, err := p.waitForAcceptance(ctx, ng.Name)
	// Before the error branches, not only on success: a group that vanished or
	// was refused after the operator reported a transient failure must stay
	// traceable to that platform incident.
	if retried != nil {
		p.publishTransientFailure(ctx, nodeClaim, ng.Name, retried, accepted, err)
	}
	if err != nil {
		quotaErr := &ErrQuotaExceeded{}
		if errors.As(err, &quotaErr) {
			p.publishQuotaEvent(nodeClaim, quotaErr)
		}
		rejectedErr := &ErrFlavorRejected{}
		if errors.As(err, &rejectedErr) {
			// Hold the flavor out of provisioning for a short window:
			// karpenter-core keeps no per-offering memory of an
			// InsufficientCapacityError, so without this (reported as an
			// unavailable offering) the scheduler re-picks the cheapest
			// flavor immediately and loops on the one that was just refused.
			p.recordFlavorRejection(rejectedErr.Flavor)
			p.recorder.Publish(events.Event{
				InvolvedObject: nodeClaim,
				Type:           corev1.EventTypeWarning,
				Reason:         "NodeGroupRejected",
				Message: fmt.Sprintf("Clever Cloud refused flavor %s (%s): %s — holding that flavor out of provisioning for %s so the scheduler relaxes to another one",
					rejectedErr.Flavor, rejectedErr.Reason, rejectedErr.Message, flavorBackoff),
				DedupeValues: []string{nodeClaim.Name},
			})
		}
		if errors.Is(err, ErrNodeGroupVanished) {
			metrics.NodeGroupVanished.Inc(nil)
			// The documented cause is the quota engine reclaiming an accepted
			// group: record it as a quota rejection of the group's flavor so
			// retries fail fast instead of looping create→vanish against the
			// API. Freed capacity clears it.
			p.recordQuotaRejection(ng.Spec.Flavor, "an accepted nodegroup was reclaimed upstream (vanish); capacity is likely exhausted")
			p.recorder.Publish(events.Event{
				InvolvedObject: nodeClaim,
				Type:           corev1.EventTypeWarning,
				Reason:         "NodeGroupVanished",
				Message: fmt.Sprintf("NodeGroup %s disappeared or was being deleted during the acceptance poll (usually the quota engine reclaiming an accepted group); failing the launch instead of waiting out the registration TTL, and %s",
					ng.Name, p.quotaConsequence(ng.Spec.Flavor)),
				DedupeValues: []string{nodeClaim.Name},
			})
		}
		return nil, err
	}
	if accepted == unacknowledged {
		// Optimistic-launch path. Healthy launches are acknowledged about 1 s
		// after creation, so this is the signal of an operator that is down,
		// wedged or not picking groups up. Karpenter's registration TTL
		// backstops it, and the nodegroupstatus controller reports the group
		// if it stays unsynced.
		metrics.NodeGroupAcceptanceTimeouts.Inc(nil)
		log.FromContext(ctx).WithValues("NodeGroup", ng.Name).Info(
			"nodegroup neither acknowledged nor refused by the node-group operator within the poll window; proceeding optimistically")
		p.recorder.Publish(events.Event{
			InvolvedObject: nodeClaim,
			Type:           corev1.EventTypeWarning,
			Reason:         "NodeGroupAcceptanceTimeout",
			Message:        fmt.Sprintf("The node-group operator neither acknowledged nor refused NodeGroup %s within %s; proceeding optimistically (the registration TTL is the backstop)", ng.Name, quotaCheckTimeout),
			DedupeValues:   []string{nodeClaim.Name},
		})
		return ng, nil
	}
	// Acknowledged or already Ready, the operator took the flavor: that
	// releases a hold-out recorded before the create call, never one recorded
	// while this launch was polled (see clearFlavorRejection).
	// ng.Spec.Flavor, not the requested flavor: on the AlreadyExists adoption
	// path the group that was actually accepted can carry a different one, and
	// releasing the hold on the wrong flavor would both keep a usable flavor
	// out and let a refused one back in.
	p.clearFlavorRejection(ng.Spec.Flavor, mark)
	return ng, nil
}

// publishQuotaEvent surfaces a quota rejection on the NodeClaim so users see
// it in kubectl describe, not only in controller logs and scheduler events.
func (p *Provider) publishQuotaEvent(nodeClaim *karpv1.NodeClaim, err *ErrQuotaExceeded) {
	p.recorder.Publish(events.Event{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         "NodeGroupQuotaExceeded",
		Message:        fmt.Sprintf("%v; %s", err, p.quotaConsequence(err.Flavor)),
		DedupeValues:   []string{nodeClaim.Name},
	})
}

// quotaConsequence says what a quota rejection of flavor does to the next
// launches, for the events that report one (see quotaRejection.coverage).
func (p *Provider) quotaConsequence(flavor string) string {
	if _, sized := p.sizing(flavor); !sized {
		return fmt.Sprintf("%s is unavailable to the scheduler for up to %s (freed capacity clears it); the catalogue cannot size it, so no other flavor is affected",
			flavor, QuotaBackoff)
	}
	return fmt.Sprintf("%s is unavailable to the scheduler for up to %s and every other flavor at least as large for up to %s, so that %s is retried first (freed capacity clears it); smaller flavors stay available",
		flavor, QuotaBackoff, 2*QuotaBackoff, flavor)
}

// publishTransientFailure surfaces a transient failure the operator reported
// during the acceptance poll, whatever ended the poll, Ready included; a
// controller shutdown is not an outcome and publishes nothing. Nothing was
// deleted or held out for it, but without this signal a launch riding out a
// Clever Cloud API incident would look like an ordinary slow reconcile — or,
// once the window closes, like a down operator — and a group that vanished or
// was refused afterwards could not be tied back to the incident.
//
// The wording states only that the poll saw the failure, never that it is
// still current or that it was resolved: a later poll may have seen it
// cleared, and the operator can keep it set next to Ready. The outcome is what
// ended the poll: Ready (Normal: the machine is up), acknowledged (the machine
// is still being built while the operator retries), not acknowledged in time,
// or failed.
func (p *Provider) publishTransientFailure(ctx context.Context, nodeClaim *karpv1.NodeClaim, name string, failure *transientFailure, accepted acceptance, pollErr error) {
	eventType := corev1.EventTypeWarning
	var outcome string
	switch {
	case pollErr != nil:
		outcome = fmt.Sprintf("the launch then failed: %v", pollErr)
	case accepted == ready:
		eventType = corev1.EventTypeNormal
		outcome = "the NodeGroup was Ready, so its machine is up"
	case accepted == acknowledged:
		outcome = "the operator acknowledged the NodeGroup, so the launch proceeds while its machine is built and the group is followed until it syncs (the registration TTL is the backstop)"
	default:
		outcome = fmt.Sprintf("the NodeGroup was neither acknowledged nor refused within %s, so the launch proceeds optimistically (the registration TTL is the backstop)", quotaCheckTimeout)
	}
	log.FromContext(ctx).WithValues("NodeGroup", name, "reason", failure.reason, "message", failure.message, "outcome", outcome).Info(
		"node-group operator reported a transient failure during the acceptance poll; not treated as a refusal")
	p.recorder.Publish(events.Event{
		InvolvedObject: nodeClaim,
		Type:           eventType,
		Reason:         "NodeGroupTransientFailure",
		Message: fmt.Sprintf("The node-group operator reported a transient failure on NodeGroup %s during the acceptance poll (%s: %s); it retries such failures on its own, so nothing was deleted or held out for it; %s",
			name, failure.reason, failure.message, outcome),
		DedupeValues: []string{nodeClaim.Name},
	})
}

// ErrNodeGroupVanished is returned by Create when the NodeGroup disappeared
// during the acceptance poll after having been observed once, or was seen
// terminating — the quota engine reclaiming an accepted group is the
// documented cause. Failing the launch immediately beats returning optimistic
// success and burning the registration TTL on a group that no longer exists.
// The poll ends on the operator's decision, about 1 s after the creation, so
// it only catches a vanish up to then: a group that vanishes after its
// acknowledgement is failed by the garbage collector's sweep instead, 2 to 4
// minutes after its NodeClaim's creation.
var ErrNodeGroupVanished = errors.New("nodegroup vanished during the acceptance poll")

// transientFailure is a ReconcileFailed condition the operator retries on its
// own (ngv1.NodeGroup.TransientFailure), as last seen by the acceptance poll.
type transientFailure struct {
	reason, message string
}

// acceptance is how far the node-group operator had taken a NodeGroup when
// the acceptance poll ended without a refusal.
type acceptance int

const (
	// unacknowledged: the window closed before the operator acknowledged or
	// refused the group. It wrote no status at all, or only a transient
	// failure.
	unacknowledged acceptance = iota
	// acknowledged: the operator took the group and is building it
	// (ReconcileInProgress=True, no refusal). Its machine is not up yet.
	acknowledged
	// ready: the group's machines are up (Ready=True).
	ready
)

// waitForAcceptance polls the NodeGroup until the Clever Cloud operator
// decides on it or the timeout elapses. The decision is the operator's first
// status write, about 1 s after the group's creation live, not the group
// turning Ready, which takes 38-58 s: a refusal returns its typed error, and
// an accepted group returns acknowledged, or ready when it is already Ready.
// A timeout returns (unacknowledged, _, nil): the caller treats it as
// optimistic success but must surface it — it is the only signal
// distinguishing a down operator from normal provisioning. A cancelled parent
// context (controller shutdown) is NOT a timeout and returns its error, so
// shutdowns don't fake that signal. The last transient failure the poll saw,
// if any, is returned alongside so the caller can surface it whatever ended
// the poll, Ready included: it is not a refusal, and on its own it is not a
// decision either. A shutdown returns none, for the same reason it returns no
// timeout.
func (p *Provider) waitForAcceptance(parentCtx context.Context, name string) (acceptance, *transientFailure, error) {
	ctx, cancel := context.WithTimeout(parentCtx, quotaCheckTimeout)
	defer cancel()
	seen := false
	accepted := unacknowledged
	var retried *transientFailure
	err := wait.PollUntilContextCancel(ctx, quotaCheckInterval, true, func(ctx context.Context) (bool, error) {
		ng := &ngv1.NodeGroup{}
		if err := p.kubeClient.Get(ctx, types.NamespacedName{Name: name}, ng); err != nil {
			if apierrors.IsNotFound(err) {
				// Not-seen-yet is informer lag right after Create; seen-then
				// -gone is a vanish and must fail the launch, not proceed
				// optimistically on a group that no longer exists.
				if seen {
					return false, fmt.Errorf("%w: %q", ErrNodeGroupVanished, name)
				}
				return false, nil
			}
			return false, err
		}
		seen = true
		// Recorded first, before anything below can end the poll, Ready
		// included: the operator keeps a transient failure set next to Ready
		// (live), and the caller surfaces what the poll saw whatever ended it.
		// Recorded after the Ready return, a failure first seen together with
		// Ready would end the poll with nothing to surface.
		if reason, message, transient := ng.TransientFailure(); transient {
			retried = &transientFailure{reason: reason, message: message}
		}
		// A group being deleted is going away whatever its status still says,
		// and cloudprovider.Get already reads it as gone. Accepting it would
		// hand core a launch on a group that will not exist and release the
		// hold-out of a flavor nothing runs. Reachable on the AlreadyExists
		// adoption path, where a terminating group can still carry Ready or
		// ReconcileInProgress, and on a group deleted mid-poll whose
		// finalizer holds it: either way it is on its way to vanishing, so it
		// fails the launch as a vanish does. Checked before Ready, which only
		// wins over the other conditions of a group that stays.
		if !ng.DeletionTimestamp.IsZero() {
			return false, fmt.Errorf("%w: %q is being deleted", ErrNodeGroupVanished, name)
		}
		// Ready wins over every other condition. The operator reports several
		// at once — live: Ready=True + ReconcileInProgress=True +
		// ReconcileFailed=True(UpstreamError) on a group whose machine was up
		// — and a Ready group is a booted VM. Checking a failure first would
		// delete it as "refused": on the AlreadyExists adoption path, the VM
		// this very claim launched on an earlier attempt.
		if ng.IsSynced() {
			accepted = ready
			return true, nil
		}
		if ng.IsQuotaExceeded() {
			msg := ""
			if cond := ng.GetCondition(ngv1.ConditionTypeReconcileFailed); cond != nil {
				msg = cond.Message
			}
			// Record before the cleanup delete: the rejection happened even
			// if freeing the reservation below fails. Fresh upstream
			// rejections only — backoff fast-fails don't count.
			p.recordQuotaRejection(ng.Spec.Flavor, msg)
			metrics.NodeGroupQuotaRejections.Inc(nil)
			// Free the rejected reservation immediately so it does not
			// starve other NodeGroups in the org. Deleted directly (not via
			// p.Delete) because removing a rejected group frees no real
			// capacity and must not clear the quota backoff — and best-effort,
			// never replacing the rejection: this Delete runs on the poll's
			// own 15s context, so a rejection observed late enough would fail
			// it with a wrapped context.DeadlineExceeded; wait.Interrupted
			// would then match, waitForAcceptance would return
			// (unacknowledged, _, nil), and Create would report optimistic
			// success for a group the quota engine has already rejected — the
			// claim would burn the 15-minute registration TTL with the
			// reservation never freed. The typed error wins; the leftover
			// group is reclaimed by the GC sweep.
			if err := p.kubeClient.Delete(ctx, &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil && !apierrors.IsNotFound(err) {
				log.FromContext(ctx).WithValues("NodeGroup", name).Error(err,
					"could not delete the quota-rejected nodegroup; the garbage collector will reclaim it")
			}
			return false, &ErrQuotaExceeded{Flavor: ng.Spec.Flavor, Message: msg}
		}
		// A transient failure is the operator retrying a Clever Cloud API call
		// on its own (live: UpstreamError, recovered about an hour later), not
		// a verdict on the group — so none of the refusal machinery below. The
		// same incident hits every flavor: treated as a refusal, successive
		// claims would delete one group each mid-first-reconcile (the upstream
		// reservation-leak pattern createMu guards against) and hold out the
		// whole catalogue flavor by flavor. Refusal() excludes it, so the rest
		// of the status is read as if it were absent; it was recorded above, and
		// the caller surfaces it.
		//
		// Any other ReconcileFailed is a refusal too — including a reason never
		// seen before. Previously it was indistinguishable from "still
		// reconciling": the poll timed out, Create reported optimistic success,
		// and the launch burned the full 15-minute registration TTL before
		// karpenter re-planned — onto the same flavor, forever, with the
		// operator's own explanation never surfaced anywhere.
		if reason, message, refused := ng.Refusal(); refused {
			metrics.NodeGroupRejections.Inc(nil)
			// Free the refused reservation, exactly as the quota branch does —
			// but never let its failure replace the refusal. This Delete runs on
			// the poll's own 15s context, so a refusal observed late enough
			// would fail it with a wrapped context.DeadlineExceeded;
			// wait.Interrupted would then match, waitForAcceptance would return
			// (unacknowledged, _, nil), and Create would report optimistic
			// success for a group the operator has already refused — the exact
			// 15-minute TTL burn this branch exists to prevent. The typed error
			// wins; the leftover group is reclaimed by the GC sweep.
			if err := p.kubeClient.Delete(ctx, &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil && !apierrors.IsNotFound(err) {
				log.FromContext(ctx).WithValues("NodeGroup", name).Error(err,
					"could not delete the refused nodegroup; the garbage collector will reclaim it")
			}
			return false, &ErrFlavorRejected{Flavor: ng.Spec.Flavor, Reason: reason, Message: message}
		}
		// Not refused and in progress: the operator acknowledged the group,
		// even with a transient failure alongside. That first status write is
		// its decision: live, a group the quota engine rejects gets
		// QuotaExceeded directly, never preceded by ReconcileInProgress, while
		// Ready only comes 38-58 s later. Return now, releasing createMu:
		// waiting for Ready held it for the whole window on every healthy
		// launch and counted each one as an acceptance timeout. The
		// nodegroupstatus controller follows the group until it syncs, and
		// fails the launch if the operator refuses it after all; a group that
		// vanishes from here on is failed by the garbage collector's sweep,
		// which deletes its NodeClaim 2 to 4 minutes after the claim's
		// creation.
		if ng.IsReconciling() {
			accepted = acknowledged
			return true, nil
		}
		// No status yet, or only a transient failure: no decision yet.
		return false, nil
	})
	if err == nil {
		return accepted, retried, nil
	}
	if wait.Interrupted(err) {
		if parentCtx.Err() != nil {
			// Shutdown, not an outcome: no transient failure either, or it
			// would be published as a failed launch. The claim's next attempt
			// adopts the group and its poll reads the status again.
			return unacknowledged, nil, parentCtx.Err()
		}
		return unacknowledged, retried, nil
	}
	return unacknowledged, retried, err
}

// Get fetches a NodeGroup by name.
func (p *Provider) Get(ctx context.Context, name string) (*ngv1.NodeGroup, error) {
	ng := &ngv1.NodeGroup{}
	if err := p.kubeClient.Get(ctx, types.NamespacedName{Name: name}, ng); err != nil {
		return nil, err
	}
	return ng, nil
}

// List returns all NodeGroups managed by this provider.
func (p *Provider) List(ctx context.Context) ([]ngv1.NodeGroup, error) {
	list := &ngv1.NodeGroupList{}
	if err := p.kubeClient.List(ctx, list, client.MatchingLabels{v1alpha1.ManagedLabelKey: "true"}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// Delete removes a NodeGroup; the Clever Cloud operator finalizer tears down
// the VM and the Node object (~40s observed). Deletions free quota, so every
// quota rejection is forgotten and the flavors it covered are available again
// — except when the operator refused that very group (a quota rejection or any
// other terminal refusal, ngv1.NodeGroup.IsRefused) and it never synced: its
// VM never came up, so it held no capacity, and clearing the backoff on its
// removal would send the next Create straight back into an exhausted quota —
// whichever claim's rejection armed it. The acceptance poll deletes the groups
// it sees refused directly, bypassing this method, for the same reason. This
// removal is what karpenter-core's termination runs after the nodegroupstatus
// controller fails a launch the operator refused late, and what the GC runs on
// a refused group the acceptance poll could not free. A Ready group always
// clears it: its VM holds capacity whatever its later reconciles report. A
// refusal hold-out is not a capacity verdict, so no deletion lifts it.
func (p *Provider) Delete(ctx context.Context, ng *ngv1.NodeGroup) error {
	if ng.IsSynced() || !ng.IsRefused() {
		p.clearQuotaRejections()
	}
	return p.kubeClient.Delete(ctx, &ngv1.NodeGroup{ObjectMeta: metav1.ObjectMeta{Name: ng.Name}})
}

// RecordLateRefusal records a refusal the node-group operator published after
// Create's acceptance poll had returned, the way Create records one it sees
// within the poll, so that the re-plan that follows lands somewhere else. The
// nodegroupstatus controller calls it once it has deleted the launched
// NodeClaim of the refused group. A quota rejection arms the quota backoff for
// the group's flavor and, for longer, every flavor at least as large; any
// other refusal holds the flavor out of provisioning. Either way Unavailable
// reports the flavors concerned. Each is counted, and published on the
// NodeClaim, under the same metric and event reason as a refusal seen within
// the poll. A group that is not refused is ignored.
func (p *Provider) RecordLateRefusal(nodeClaim *karpv1.NodeClaim, ng *ngv1.NodeGroup) {
	if ng.IsQuotaExceeded() {
		msg := ""
		if cond := ng.GetCondition(ngv1.ConditionTypeReconcileFailed); cond != nil {
			msg = cond.Message
		}
		p.recordQuotaRejection(ng.Spec.Flavor, msg)
		metrics.NodeGroupQuotaRejections.Inc(nil)
		p.recorder.Publish(events.Event{
			InvolvedObject: nodeClaim,
			Type:           corev1.EventTypeWarning,
			Reason:         "NodeGroupQuotaExceeded",
			Message: fmt.Sprintf("The organisation quota rejected NodeGroup %s (flavor %s) after its launch (%s); the NodeClaim was deleted so karpenter re-plans instead of waiting out the registration TTL, and %s",
				ng.Name, ng.Spec.Flavor, DescribeFailure(ngv1.ReasonQuotaExceeded, msg), p.quotaConsequence(ng.Spec.Flavor)),
			DedupeValues: []string{nodeClaim.Name},
		})
		return
	}
	reason, message, refused := ng.Refusal()
	if !refused {
		return
	}
	p.recordFlavorRejection(ng.Spec.Flavor)
	metrics.NodeGroupRejections.Inc(nil)
	p.recorder.Publish(events.Event{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         "NodeGroupRejected",
		Message: fmt.Sprintf("Clever Cloud refused flavor %s for NodeGroup %s after its launch (%s); the NodeClaim was deleted so karpenter re-plans instead of waiting out the registration TTL, and that flavor is held out of provisioning for %s so the scheduler relaxes to another one",
			ng.Spec.Flavor, ng.Name, DescribeFailure(reason, message), flavorBackoff),
		DedupeValues: []string{nodeClaim.Name},
	})
}

// DescribeFailure renders an operator condition's reason and message for an
// event or a log line; the message can be empty (live: quota rejections no
// longer carry one).
func DescribeFailure(reason, message string) string {
	if message == "" {
		return reason
	}
	return reason + ": " + message
}

// nodeGroupLabels computes the node labels carried by the NodeGroup so that
// they are present on the node as soon as it joins, before Karpenter's
// registration sync. Keys the NodeGroup payload cannot carry are filtered out:
// for NodeClaim labels Karpenter applies those at registration anyway, while
// NodeClass labels have no such fallback — which is why the nodeclass
// controller rejects up front, with the same rule, anything filtered here
// (bar the keys v0.12.0 accepted, which it reports as ignored), and why the
// NodeClass hash stamped on the group covers only what passes it.
//
// The payload is immutable and the platform applies it to every node of the
// group, not only to the one this NodeClaim is for, which is why core's
// karpenter.sh keys never go in it: karpenter.sh/nodepool on a node that never
// gets a provider ID wedges karpenter-core's cluster-state sync (see
// v1alpha1.ValidateNodeClassLabel). The registered node still receives them
// through core's registration sync, and the karpenter.sh/unregistered taint
// keeps pods off it until then.
func nodeGroupLabels(nodeClaim *karpv1.NodeClaim, nodeClass *v1alpha1.CleverNodeClass) map[string]string {
	labels := map[string]string{}
	for k, v := range nodeClass.Spec.Labels {
		if isNodeGroupLabelAllowed(k, v) {
			labels[k] = v
		}
	}
	for k, v := range nodeClaim.Labels {
		if isNodeGroupLabelAllowed(k, v) {
			labels[k] = v
		}
	}
	return labels
}

// isNodeGroupLabelAllowed admits exactly the labels the shared rule accepts
// (v1alpha1.ValidateNodeClassLabel): reserved prefixes, kubernetes.io/ domains
// owned by Karpenter's sync, the karpenter.sh domain owned by karpenter-core,
// and label syntax the apiserver would refuse on the Node. It filters rather
// than errors — NodeClaim labels carry Karpenter's own reserved keys
// (node.kubernetes.io/instance-type, karpenter.sh/nodepool, ...) by design and
// still reach the node through the registration sync. NodeClass labels have
// NO such fallback: a dropped key is simply gone, which is why the nodeclass
// controller rejects them up front with the same rule instead of ever letting
// this filter fire — except for keys v0.12.0 accepted
// (v1alpha1.IsLegacyNodeClassLabel), which it keeps Ready and reports as
// ignored, and which this filter drops.
func isNodeGroupLabelAllowed(key, value string) bool {
	return v1alpha1.ValidateNodeClassLabel(key, value) == nil
}
