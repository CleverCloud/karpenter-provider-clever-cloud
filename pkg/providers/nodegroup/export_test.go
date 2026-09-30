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

package nodegroup

import "time"

// SetQuotaCheckTimeout overrides the acceptance-poll timeout so tests can
// exercise the timeout path without waiting the production 15s. It returns
// the previous value for restoration via t.Cleanup.
func SetQuotaCheckTimeout(d time.Duration) time.Duration {
	prev := quotaCheckTimeout
	quotaCheckTimeout = d
	return prev
}

// FlavorBackoff is how long a refused flavor is held out.
const FlavorBackoff = flavorBackoff

// RejectedFlavors returns the flavors currently held out because the upstream
// operator refused them, so tests can tell a hold-out from a quota rejection,
// which Unavailable reports alike.
func (p *Provider) RejectedFlavors() map[string]struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]struct{}{}
	for flavor, hold := range p.rejectedFlavors {
		if p.clock.Since(hold.at) < flavorBackoff {
			out[flavor] = struct{}{}
		}
	}
	return out
}
