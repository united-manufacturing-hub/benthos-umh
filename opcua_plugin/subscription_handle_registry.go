// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opcua_plugin

import "sync"

// subscriptionHandleRegistry is the subscription-owned map from a
// MonitoredItem's ClientHandle to the NodeDef the handle was allocated for.
//
// ClientHandles must identify exactly one MonitoredItem within a subscription
// (OPC 10000-4 §7.21: the clientHandle is the "Client-supplied id of the
// MonitoredItem. This id is used in Notifications generated"). They must not
// be derived from a position in a node slice the subscriber does not own:
// MonitorBatched retries a filter rejection with the suffix
// nodes[failedNodeIndex:] (read_discover.go), so slice positions restart at 0
// while the earlier items of the same subscription are still live under those
// handles. The registry decouples handles from slice positions: a handle is
// allocated here as the request is built and resolved here when a notification
// arrives.
//
// Concurrency: handles are allocated by the browse goroutine building later
// batches while the benthos input goroutine resolves notifications, so the
// map is guarded by an RWMutex. The zero value is ready to use.
//
// Generation scoping: reset is called when a subscription is created and again
// on teardown where it is cancelled. reset deliberately keeps nextHandle
// monotonic: a handle from a previous subscription generation must resolve to
// "unknown" against the new one, never to a different node that happened to
// receive the same handle value.
type subscriptionHandleRegistry struct {
	mu sync.RWMutex
	// handles maps each live ClientHandle of the current subscription
	// generation to the node it was allocated for. Lazily created; nil means
	// no handle has been allocated (or the registry was reset).
	handles map[uint32]NodeDef
	// nextHandle is the next handle allocate hands out. It is monotonic
	// across resets and is never zeroed - see the type comment.
	nextHandle uint32
}

// allocate registers node under a fresh ClientHandle and returns the handle.
// It is called as the CreateMonitoredItems request is built, i.e. before the
// request is sent, so notifications that arrive before the response returns
// (OPC 10000-4 §5.13.2.1) can already be resolved.
func (r *subscriptionHandleRegistry) allocate(node NodeDef) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	handle := r.nextHandle
	r.nextHandle++
	if r.handles == nil {
		r.handles = make(map[uint32]NodeDef)
	}
	r.handles[handle] = node
	return handle
}

// lookup resolves a notification's ClientHandle to the node it was allocated
// for. The second return value is false when the handle is unknown to the
// current subscription generation: it was never allocated, or it belongs to
// a previous generation (the registry was reset since).
func (r *subscriptionHandleRegistry) lookup(handle uint32) (NodeDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.handles[handle]
	return node, ok
}

// reset drops every handle of the current subscription generation, e.g. on
// teardown where the subscription is cancelled. nextHandle keeps counting:
// resetting it to zero would let a stale handle from the cancelled generation
// resolve against a different node of the next one.
func (r *subscriptionHandleRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handles = nil
}
