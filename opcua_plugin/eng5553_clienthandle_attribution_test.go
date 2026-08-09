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
	"fmt"
	"testing"

	"github.com/gopcua/opcua/ua"
)

// ENG-5553: after a StatusBadFilterNotAllowed retry, values are attributed to the
// wrong node.
//
// ─────────────────────────────────────────────────────────────────────────────
// READ THIS BEFORE USING THIS TEST AS A REGRESSION GUARD — IT IS NOT ONE YET.
//
// This test REIMPLEMENTS the handle arithmetic below (see handleForPosition). It
// does not call MonitorBatched. It therefore demonstrates that the arithmetic is
// wrong, but it CANNOT prove that a fix to the production code works: change
// read_discover.go however you like and this test's verdict will not move.
//
// That is the same defect the existing read_discover_recursive_retry_bug_test.go
// has — it simulates the batching logic in a local closure and asserts against its
// own simulation.
//
// Whoever fixes ENG-5553 must REPLACE this with a test that drives the real
// MonitorBatched (injecting a subscription that answers one node with
// ua.StatusBadFilterNotAllowed) and asserts that a notification carrying handle h
// resolves to the node actually monitored under h. Delete this file at that point.
// ─────────────────────────────────────────────────────────────────────────────
//
// The invariant under test: a notification carrying ClientHandle h must resolve to
// the node that was monitored under h.
//
// It breaks because ClientHandle is a position in the slice handed to
// MonitorBatched (read_discover.go:579, `batchRange.Start + pos`), while the
// consumer resolves it against the always-full g.NodeList (read.go:594). The
// filter retry recurses with a suffix (read_discover.go:649,
// `nodes[failedNodeIndex:]`), so handles restart at 0 and the two disagree.

// handleForPosition mirrors read_discover.go:579 for the first batch of whatever
// slice MonitorBatched was passed. MIRROR, not a call — see the warning above.
func handleForPosition(pos int) uint32 {
	const batchRangeStart = 0
	return uint32(batchRangeStart + pos)
}

func eng5553Nodes(n int) []NodeDef {
	nodes := make([]NodeDef, n)
	for i := range nodes {
		nodes[i] = NodeDef{
			NodeID:     ua.NewNumericNodeID(1, uint32(1000+i)),
			BrowseName: fmt.Sprintf("tag_%c", rune('A'+i)),
		}
	}
	return nodes
}

func TestClientHandleResolvesToTheMonitoredNodeAfterFilterRetry(t *testing.T) {
	// g.NodeList is the full list (read_discover.go:325).
	full := eng5553Nodes(6)

	// The server rejects the filter on index 2, so the plugin recurses with
	// nodes[2:] (read_discover.go:649).
	const failedNodeIndex = 2
	retried := full[failedNodeIndex:]

	for pos, monitored := range retried {
		handle := handleForPosition(pos)

		// read.go:594 — the consumer indexes the FULL list with that handle.
		resolved := full[handle]

		if resolved.NodeID.String() != monitored.NodeID.String() {
			t.Errorf("handle %d: server sends values for %s (%s); pipeline publishes them as %s (%s)",
				handle,
				monitored.NodeID.String(), monitored.BrowseName,
				resolved.NodeID.String(), resolved.BrowseName)
		}
	}
}
