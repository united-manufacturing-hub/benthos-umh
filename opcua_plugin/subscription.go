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

import (
	"context"
	"sync"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

// subscription is the part of *opcua.Subscription that OPCUAInput calls.
// Tests replace it with a fake server.
type subscription interface {
	Monitor(ctx context.Context, ts ua.TimestampsToReturn, items ...*ua.MonitoredItemCreateRequest) (*ua.CreateMonitoredItemsResponse, error)
	Cancel(ctx context.Context) error
}

var _ subscription = (*opcua.Subscription)(nil)

// handleRegistry maps the ClientHandle of each monitored item to its node
// (OPC 10000-4 §7.21). Handles are never reused, so a late value for a
// cancelled subscription cannot be published under another node. gopcua's
// monitor package does the same, but drops values when the reader is slow.
//
// The zero value is ready to use. It is safe for concurrent use.
type handleRegistry struct {
	mu    sync.RWMutex
	nodes map[uint32]NodeDef
	next  uint32
}

// allocate returns a new handle for node. Call it before the request is sent,
// because the server may send values before it answers (OPC 10000-4 §5.13.2.1).
func (r *handleRegistry) allocate(node NodeDef) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nodes == nil {
		r.nodes = make(map[uint32]NodeDef)
	}
	handle := r.next
	r.next++
	r.nodes[handle] = node
	return handle
}

// lookup returns the node that handle was allocated for, and false if the
// handle is unknown or was dropped by reset.
func (r *handleRegistry) lookup(handle uint32) (NodeDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.nodes[handle]
	return node, ok
}

// reset forgets all handles. Call it when the subscription is cancelled.
func (r *handleRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes = nil
}
