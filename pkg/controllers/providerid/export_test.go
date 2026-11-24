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

package providerid

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// UnstampedSiblings exposes the mapping of the deletion watch to tests.
func (c *Controller) UnstampedSiblings(ctx context.Context, obj client.Object) []reconcile.Request {
	return c.unstampedSiblings(ctx, obj)
}

// FreesProviderID exposes the predicate of the deletion watch to tests.
func FreesProviderID(obj client.Object) bool {
	return freesProviderID(obj)
}
