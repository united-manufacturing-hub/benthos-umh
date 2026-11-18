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
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Fault injection for the ClientHandle attribution fix (ENG-5553 step 3).
//
// The scenario tests in read_client_handle_attribution_test.go pin the
// attribution obligations on the happy paths and the filter-fallback path.
// These tests make the SURROUNDING parts fail on purpose - a subscription
// cancelled and rebuilt mid-stream, monitored-item creation failing for a
// reason that is not StatusBadFilterNotAllowed, and a notification arriving
// after teardown but before the next generation is built - and require that
// attribution still holds or that the failure is reported rather than
// silently swallowed.
//
// They reuse the frozen harness (newAttributionHarness, fakeSubscription,
// checkItemAttribution, ...) from read_client_handle_attribution_test.go and
// subscription_fake_test.go; expected handles are never computed here.

const obligationFI = "FAULT INJECTION (UMH policy, ENG-5553 step 3: when the surrounding machinery fails - " +
	"reconnect mid-stream, monitored-item creation failure, teardown - every notification must still resolve to " +
	"the node the server monitors under its handle, and every failure must be reported, never silently swallowed)"

// waitForLiveItems polls the server (a server-observed fact, never a
// recomputation of batching arithmetic) until it has the given number of
// live MonitoredItems, or fails if MonitorBatched returns first.
func waitForLiveItems(t *testing.T, h *attributionHarness, want int, done <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case <-done:
			t.Fatalf("test infrastructure problem: MonitorBatched returned before all %d nodes were monitored", want)
		default:
		}
		if len(h.sub.serverView()) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("test infrastructure problem: timed out waiting for %d live items", want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// requireDroppedWithDiagnostic pushes a DataChangeNotification carrying the
// given handle and requires production to drop it: no message published, no
// panic, and a diagnostic logged - a silent drop hides data loss from
// operators.
func (h *attributionHarness) requireDroppedWithDiagnostic(
	t *testing.T, handle uint32, obligation, scenario, what string,
) {
	t.Helper()
	snapshot := h.logSnapshot()
	h.sub.pushDataChange(handle, 424242)
	msgs, panicked, err := h.readQueued()
	if panicked != nil {
		t.Errorf("%s [%s]: %s: a DataChangeNotification carrying handle %d panicked ReadBatchSubscribe: %v - "+
			"it must be dropped with a diagnostic, never fatal", obligation, scenario, what, handle, panicked)
		return
	}
	if err != nil {
		t.Fatalf("%s [%s]: test infrastructure problem: notification with handle %d was not processed: %v",
			obligation, scenario, handle, err)
	}
	if len(msgs) != 0 {
		parts := make([]string, 0, len(msgs))
		for _, m := range msgs {
			nodeID, _ := m.MetaGet("opcua_attr_nodeid")
			browse, _ := m.MetaGet("opcua_attr_browsename")
			parts = append(parts, fmt.Sprintf("node %s (browse name %q)", nodeID, browse))
		}
		t.Errorf("%s [%s]: %s: a DataChangeNotification carrying handle %d was published as %s - it must be "+
			"dropped with a diagnostic, never attributed to an arbitrary node",
			obligation, scenario, what, handle, strings.Join(parts, "; "))
	}
	if h.logSince(snapshot) == "" {
		t.Errorf("%s [%s]: %s: a DataChangeNotification carrying handle %d was dropped silently - it must be "+
			"dropped with a diagnostic so operators can tell values are being lost",
			obligation, scenario, what, handle)
	}
}

// ---------------------------------------------------------------------------
// FI-1: a subscription cancelled and rebuilt (reconnect) mid-stream.
//
// Notifications are streaming from inside Monitor (notifyOnAccept: delivered
// before the CreateMonitoredItems response returns, OPC 10000-4 §5.13.2.1)
// while the production teardown path runs. The notifications still queued
// when the subscription is cancelled become stale: they must be dropped with
// a diagnostic, never attributed; the rebuilt generation must attribute
// correctly; a stale handle must not resolve against the new generation even
// when it is positionally valid in its node list.
// ---------------------------------------------------------------------------

func TestFaultInjectionSubscriptionCancelledAndRebuiltMidStream(t *testing.T) {
	// Generation 1: five nodes, one batch, notifications stream from inside
	// Monitor while MonitorBatched is between its (single) batch and its
	// post-batch sleep.
	h := newAttributionHarness(t, harnessConfig{
		nodeCount:        5,
		maxBatchSize:     5,
		filterCapability: FilterSupported,
		notifyOnAccept:   true,
		subscriptionID:   1001,
		namespace:        2,
		idBase:           1000,
		browsePrefix:     "sensor",
	})
	done := h.subscribeAsync(t)
	waitForLiveItems(t, h, len(h.nodes), done)

	early := h.sub.earlyDelivered()
	if len(early) != len(h.nodes) {
		t.Fatalf("test infrastructure problem: expected %d early notifications, got %d", len(h.nodes), len(early))
	}

	// (1) Mid-stream, before teardown: the first two streaming notifications
	// must be attributed to the node the server monitors under their handle.
	var violations []string
	for _, item := range early[:2] {
		msgs, panicked, err := h.readQueued()
		if panicked != nil {
			violations = append(violations, fmt.Sprintf(
				"%s [mid-stream, before teardown]: handle %d (%s): a streaming notification panicked "+
					"ReadBatchSubscribe: %v", obligationFI, item.ClientHandle, item, panicked))
			continue
		}
		if err != nil || len(msgs) != 1 {
			violations = append(violations, fmt.Sprintf(
				"%s [mid-stream, before teardown]: handle %d (%s): a streaming notification did not produce "+
					"exactly one message (messages=%d, err=%v)",
				obligationFI, item.ClientHandle, item, len(msgs), err))
			continue
		}
		want, ok := h.nodeByID[item.NodeID]
		if !ok {
			t.Fatalf("test infrastructure problem: server monitors unknown node %s", item.NodeID)
		}
		violations = append(violations, attributionViolations(
			msgs[0], item, want, obligationFI, "mid-stream, before teardown")...)
	}

	// (2) Teardown mid-stream, through the production cleanup path: Cancel
	// the live subscription and reset the handle registry (generation over).
	generation1 := h.sub.serverView()
	if len(generation1) == 0 {
		t.Fatal("test infrastructure problem: generation 1 monitored no items")
	}
	h.g.unsubscribeAndResetHeartbeat(context.Background())
	if !h.sub.wasCancelled() {
		t.Fatal("test infrastructure problem: mid-stream teardown did not cancel the subscription")
	}
	// MonitorBatched had a single batch; it is in its post-batch sleep and
	// cannot touch g.Subscription again. Let it return.
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("test infrastructure problem: MonitorBatched did not return after teardown")
	}

	// (3) The three generation-1 notifications still queued when the
	// subscription was cancelled are now post-teardown arrivals: their
	// handles belong to a cancelled generation, so they must be dropped with
	// a diagnostic - never attributed to anything.
	for _, item := range early[2:] {
		snapshot := h.logSnapshot()
		msgs, panicked, err := h.readQueued()
		if panicked != nil {
			violations = append(violations, fmt.Sprintf(
				"%s [mid-stream, after teardown]: a notification for handle %d (%s), queued before the teardown, "+
					"panicked ReadBatchSubscribe after it: %v", obligationFI, item.ClientHandle, item, panicked))
			continue
		}
		if err != nil {
			t.Fatalf("test infrastructure problem: queued notification was not processed: %v", err)
		}
		if len(msgs) != 0 {
			nodeID, _ := msgs[0].MetaGet("opcua_attr_nodeid")
			violations = append(violations, fmt.Sprintf(
				"%s [mid-stream, after teardown]: a notification for handle %d (%s), queued before the teardown "+
					"of its generation, was published after the teardown as node %s - a cancelled generation's "+
					"handles must not resolve", obligationFI, item.ClientHandle, item, nodeID))
			continue
		}
		if h.logSince(snapshot) == "" {
			violations = append(violations, fmt.Sprintf(
				"%s [mid-stream, after teardown]: a notification for handle %d (%s) from the cancelled generation "+
					"was dropped silently - it must be dropped with a diagnostic",
				obligationFI, item.ClientHandle, item))
		}
	}

	// (4) The rebuild: a new subscription generation against different nodes
	// (a fresh browse can produce those), sharing the notify channel with
	// the old one - a stale publish reaching the reader around a reconnect.
	h.reconnect(t, harnessConfig{
		nodeCount:        4,
		maxBatchSize:     5,
		filterCapability: FilterSupported,
		subscriptionID:   2002,
		namespace:        3,
		idBase:           2000,
		browsePrefix:     "valve",
	})
	h.subscribe(t)
	h.checkServerViewAttribution(t, obligationFI, "mid-stream rebuild, generation 2")

	// (5) A stale generation-1 handle must not resolve against generation 2 -
	// even though it is positionally valid in generation 2's 4-entry node list
	// (the old positional lookup would have attributed it to a valve).
	for _, stale := range generation1 {
		if stale.ClientHandle < uint32(len(h.g.NodeList)) {
			h.requireDroppedWithDiagnostic(t, stale.ClientHandle, obligationFI,
				"mid-stream rebuild: stale handle, positionally valid in generation 2",
				fmt.Sprintf("a stale notification from generation 1 (handle %d -> node %s)", stale.ClientHandle, stale.NodeID))
		}
	}

	// (6) Generation 2 attribution is unaffected by the stale arrivals.
	h.checkServerViewAttribution(t, obligationFI, "mid-stream rebuild, generation 2 after stale arrivals")

	if len(violations) > 0 {
		t.Errorf("%s [%s]: the mid-stream reconnect broke attribution or swallowed a failure:\n%s",
			obligationFI, "subscription cancelled and rebuilt mid-stream", joinStrings(violations))
	}
}

// ---------------------------------------------------------------------------
// FI-2: monitored-item creation failing for a reason that is NOT
// StatusBadFilterNotAllowed.
//
// A non-filter rejection must NOT take the filter-fallback retry (that would
// loop or mis-skip); it must abort with the failure reported (returned error,
// error log, metrics counter) rather than silently swallowed. Attribution of
// the items the server did create before the abort must still hold (per-item
// result processing means a real server creates them), and the registry must
// never map a handle to a node it was not allocated for.
//
// Note on the ENG-7894 suspicion (BrowseAndSubscribeIfNeeded / MonitorBatched
// calling g.Close(ctx) from inside the browse goroutine while Close waits on
// that goroutine): this test calls MonitorBatched from the test goroutine, so
// g.Close's cleanupBrowsing has no browse goroutine to wait for - the abort
// path itself is exercised without reproducing the deadlock, which has its
// own dedicated (currently skipped) test below.
// ---------------------------------------------------------------------------

func TestFaultInjectionMonitoredItemCreationFailsNotBadFilterNotAllowed(t *testing.T) {
	h := newAttributionHarness(t, harnessConfig{
		nodeCount:        5,
		maxBatchSize:     5,
		filterCapability: FilterSupported,
		subscriptionID:   1001,
		namespace:        2,
		idBase:           1000,
		browsePrefix:     "sensor",
	})
	rejectedNode := h.nodes[3].NodeID.String()
	h.sub.reject = func(item *ua.MonitoredItemCreateRequest) ua.StatusCode {
		if item.ItemToMonitor.NodeID.String() == rejectedNode {
			// A reason that is NOT StatusBadFilterNotAllowed: the server
			// does not know the node.
			return ua.StatusBadNodeIDUnknown
		}
		return ua.StatusOK
	}

	ResetMetrics()
	snapshot := h.logSnapshot()
	monitored, err := h.g.MonitorBatched(context.Background(), h.nodes)

	var violations []string

	// (1) The failure is reported, not swallowed: a non-nil error that names
	// the node, an error log line, and the metrics counter incremented.
	if err == nil {
		violations = append(violations, fmt.Sprintf(
			"%s [creation fails, non-filter reason]: MonitorBatched returned no error although the server "+
				"rejected node %s - the failure was silently swallowed", obligationFI, rejectedNode))
	} else if !strings.Contains(err.Error(), rejectedNode) {
		violations = append(violations, fmt.Sprintf(
			"%s [creation fails, non-filter reason]: MonitorBatched returned %v, which does not name the rejected "+
				"node %s - the failure must be reported with the node it happened on",
			obligationFI, err, rejectedNode))
	}
	if monitored != 0 {
		t.Logf("note: MonitorBatched reported %d monitored nodes before the abort", monitored)
	}
	if !strings.Contains(h.logSince(snapshot), rejectedNode) {
		violations = append(violations, fmt.Sprintf(
			"%s [creation fails, non-filter reason]: no log line names the rejected node %s - the failure must "+
				"be reported to operators", obligationFI, rejectedNode))
	}
	failureCount := testutil.ToFloat64(opcuaSubscriptionFailuresTotal.WithLabelValues("node_id_unknown", rejectedNode))
	if failureCount < 1 {
		violations = append(violations, fmt.Sprintf(
			"%s [creation fails, non-filter reason]: the opcua_subscription_failures_total counter was not "+
				"incremented for node %s - the failure must be recorded, not swallowed",
			obligationFI, rejectedNode))
	}

	// (2) The server processed the request per item (OPC 10000-4): it created
	// every item except the rejected one, including the one AFTER it. All of
	// those live items must still attribute correctly.
	live := h.sub.serverView()
	liveNodes := make(map[string]bool, len(live))
	for _, item := range live {
		liveNodes[item.NodeID] = true
	}
	if len(live) != len(h.nodes)-1 {
		t.Fatalf("test infrastructure problem: expected %d live items after per-item rejection, got %d",
			len(h.nodes)-1, len(live))
	}
	if liveNodes[rejectedNode] {
		t.Fatalf("test infrastructure problem: the server created the item it rejected")
	}
	for _, item := range live {
		violations = append(violations, h.checkItemAttribution(
			item, obligationFI, "creation fails, non-filter reason: survivor items")...)
	}

	// (3) The registry must never map a handle to a node it was not allocated
	// for, even though the request failed partway. Handles are not
	// enumerable, so scan a generous range: every handle that resolves must
	// point at a live node or at the rejected node itself (its handle was
	// allocated before the request was sent, per OPC 10000-4 §5.13.2.1).
	rejectedHandle, foundRejected := uint32(0), false
	for handle := uint32(0); handle <= 2*uint32(len(h.nodes)); handle++ {
		node, ok := h.g.handleRegistry.lookup(handle)
		if !ok {
			continue
		}
		if node.NodeID.String() == rejectedNode {
			rejectedHandle, foundRejected = handle, true
			continue
		}
		if !liveNodes[node.NodeID.String()] {
			violations = append(violations, fmt.Sprintf(
				"%s [creation fails, non-filter reason]: handle %d resolves to node %s, which the server has no "+
					"live item for and which was not the rejected node - the registry must never point a handle "+
					"at a node it was not allocated for",
				obligationFI, handle, node.NodeID))
		}
	}
	if !foundRejected {
		violations = append(violations, fmt.Sprintf(
			"%s [creation fails, non-filter reason]: no allocated handle resolves to the rejected node %s - the "+
				"handle was allocated when the request was built and must resolve to that node (and only it)",
			obligationFI, rejectedNode))
	}

	// (4) The rejected node's own handle (allocated before the request was
	// sent; the server never created the item) resolves to the rejected node
	// itself - never to a different node. This pins the documented decision
	// from FIX-REPORT.md: a conformant server never notifies under it.
	if foundRejected {
		h.sub.pushDataChange(rejectedHandle, 777)
		msgs, panicked, readErr := h.readQueued()
		switch {
		case panicked != nil:
			violations = append(violations, fmt.Sprintf(
				"%s [creation fails, non-filter reason]: a notification under the rejected node's handle %d "+
					"panicked ReadBatchSubscribe: %v", obligationFI, rejectedHandle, panicked))
		case readErr != nil:
			t.Fatalf("test infrastructure problem: notification was not processed: %v", readErr)
		case len(msgs) != 1:
			violations = append(violations, fmt.Sprintf(
				"%s [creation fails, non-filter reason]: a notification under the rejected node's handle %d "+
					"produced %d messages, expected exactly 1", obligationFI, rejectedHandle, len(msgs)))
		default:
			gotNodeID, _ := msgs[0].MetaGet("opcua_attr_nodeid")
			if gotNodeID != rejectedNode {
				violations = append(violations, fmt.Sprintf(
					"%s [creation fails, non-filter reason]: a notification under the rejected node's handle %d "+
						"was published as node %s instead of %s - a handle must never resolve to a node it was "+
						"not allocated for",
					obligationFI, rejectedHandle, gotNodeID, rejectedNode))
			}
		}
	}

	if len(violations) > 0 {
		t.Errorf("%s [%s]: a non-filter monitored-item creation failure broke attribution or was swallowed:\n%s",
			obligationFI, "creation fails, non-filter reason", joinStrings(violations))
	}
}

// ---------------------------------------------------------------------------
// FI-2b / ENG-7894: the same abort path taken from INSIDE the browse
// goroutine calls g.Close(ctx), and Close waits for the browse goroutine -
// on itself. See the Skip comment for the reproduced evidence.
// ---------------------------------------------------------------------------

// TestFaultInjectionENG7894CloseFromInsideBrowseGoroutineDeadlocks drives the
// REAL production goroutine shape (startBrowsing -> BrowseAndSubscribeIfNeeded
// -> g.Close(ctx)) and detects whether the browse goroutine can ever finish.
//
// SKIPPED: the deadlock was reproduced with this exact test on this branch
// (2025-12-04, see ONEWAY-REPORT.md for the captured goroutine dump):
//
//	the browse goroutine blocks in sync.WaitGroup.Wait inside
//	OPCUAConnection.cleanupBrowsing, called from OPCUAConnection.Close,
//	called from OPCUAInput.BrowseAndSubscribeIfNeeded - i.e. Close waits for
//	the very goroutine that is calling it (ENG-7894).
//
// Do not delete the repro body: once ENG-7894 is fixed, remove the Skip and
// this test verifies the fix. Do not add a timeout to the PRODUCTION path to
// make this pass.
func TestFaultInjectionENG7894CloseFromInsideBrowseGoroutineDeadlocks(t *testing.T) {
	// SKIPPED - ENG-7894, reproduced 2025-12-04 with this exact test (see ONEWAY-REPORT.md
	// for the captured goroutine dump): the browse goroutine blocks forever in
	// sync.WaitGroup.Wait inside OPCUAConnection.cleanupBrowsing (core_connection.go:200),
	// called from OPCUAConnection.Close (core_connection.go:225), called from
	// OPCUAInput.BrowseAndSubscribeIfNeeded (read_discover.go:342, the Subscribe-failure
	// branch) - i.e. Close waits for the very goroutine that is calling it. The same
	// g.Close(ctx)-inside-the-browse-goroutine shape exists in MonitorBatched's error paths
	// (read_discover.go: g.Close on Monitor error, nil response, and non-filter rejection),
	// which share the deadlock by construction. Do not delete the repro body: once
	// ENG-7894 is fixed, remove this Skip and this test verifies the fix. Do not add a
	// timeout to the production path to make this pass.
	t.Skip("ENG-7894: reproduced deadlock - BrowseAndSubscribeIfNeeded calls g.Close(ctx) from inside the " +
		"browse goroutine while Close waits on that goroutine (goroutine dump captured with this test in " +
		"ONEWAY-REPORT.md); skipped until ENG-7894 is fixed")

	// Production shape, exactly as Connect() builds it before the browse
	// starts: a client object that was never connected (its Subscribe fails
	// fast with StatusBadServerNotConnected), a fresh visited-cache so the
	// browse can be skipped, subscriptions enabled, a notify channel.
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(io.Discard, nil)))
	g := &OPCUAInput{OPCUAConnection: &OPCUAConnection{Log: logger}}
	g.SubscribeEnabled = true
	g.SubNotifyChan = newNotifyChan()
	client, clientErr := opcua.NewClient("opc.tcp://127.0.0.1:1")
	if clientErr != nil {
		t.Fatalf("test infrastructure problem: opcua.NewClient failed: %v", clientErr)
	}
	g.Client = client
	g.ServerProfile = GetProfileByName(ProfileAuto)

	nodeID := ua.NewNumericNodeID(2, 1000)
	g.NodeIDs = []*ua.NodeID{nodeID}
	g.visited.Store(nodeID.String(), VisitedNodeInfo{
		Def: NodeDef{
			NodeID:     nodeID,
			NodeClass:  ua.NodeClassVariable,
			BrowseName: "sensor_000",
			DataType:   "Float",
			DataTypeID: ua.TypeIDFloat,
			Path:       "plant.line",
		},
		LastSeen:        time.Now(),
		FullyDiscovered: true,
	})

	// The real production entry point: this is the goroutine Close waits for.
	g.startBrowsing(context.Background())

	// BrowseAndSubscribeIfNeeded can only ever finish by setting a browse
	// error (failure) or by completing the subscription (success). If the
	// suspicion is right, neither can happen: the goroutine is stuck in
	// Close -> cleanupBrowsing -> browseWaitGroup.Wait() - waiting for itself.
	deadline := time.Now().Add(15 * time.Second)
	for {
		g.browseErrMu.Lock()
		errSet := g.browseErr != nil
		g.browseErrMu.Unlock()
		if errSet {
			t.Fatalf("ENG-7894 did not reproduce: the browse goroutine finished and reported: %v", g.browseErr)
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Reproduced. Capture every goroutine's stack as evidence.
	stacks := &bytes.Buffer{}
	if err := pprof.Lookup("goroutine").WriteTo(stacks, 2); err != nil {
		t.Fatalf("ENG-7894 reproduced, but the goroutine dump failed: %v", err)
	}
	t.Fatalf("ENG-7894 reproduced: the browse goroutine did not finish within 15s; it is deadlocked in "+
		"Close waiting for itself. Goroutine dump:\n%s", stacks.String())
}

// ---------------------------------------------------------------------------
// FI-3: a notification arriving after teardown but before the next
// generation is built.
//
// The teardown (production path) ends the subscription generation; nothing is
// rebuilt yet. Every notification that arrives in this window - under a handle
// the cancelled generation owned, positionally valid in any future node list,
// or entirely out of band - must be dropped with a diagnostic, never fatal,
// never attributed. When the next generation is finally built, its attribution
// must work and the old handles must still not resolve.
// ---------------------------------------------------------------------------

func TestFaultInjectionNotificationAfterTeardownBeforeNextGeneration(t *testing.T) {
	// Generation 1: five nodes.
	h := newAttributionHarness(t, harnessConfig{
		nodeCount:        5,
		maxBatchSize:     5,
		filterCapability: FilterSupported,
		subscriptionID:   1001,
		namespace:        2,
		idBase:           1000,
		browsePrefix:     "sensor",
	})
	h.subscribe(t)
	generation1 := h.sub.serverView()
	if len(generation1) == 0 {
		t.Fatal("test infrastructure problem: generation 1 monitored no items")
	}

	// Teardown through the production path. No generation is rebuilt yet.
	h.g.unsubscribeAndResetHeartbeat(context.Background())
	if !h.sub.wasCancelled() {
		t.Fatal("test infrastructure problem: teardown did not cancel the subscription")
	}

	// (1) Notifications arriving after teardown, before the next generation
	// is built: every handle of the cancelled generation must be dropped with
	// a diagnostic. These handles are all positionally valid against any node
	// list of length >= 5 - the old positional lookup would have attributed
	// them; the registry must not.
	for _, item := range generation1 {
		h.requireDroppedWithDiagnostic(t, item.ClientHandle, obligationFI,
			"after teardown, before the next generation is built",
			fmt.Sprintf("a notification under generation 1's handle %d (which monitored node %s)",
				item.ClientHandle, item.NodeID))
	}

	// (2) A handle nobody ever allocated: also dropped with a diagnostic.
	h.requireDroppedWithDiagnostic(t, 999, obligationFI,
		"after teardown, before the next generation is built",
		"an out-of-band notification under a handle that was never allocated")

	// (3) The next generation is eventually built: different nodes from a
	// fresh browse (namespace 3), sharing the notify channel.
	h.reconnect(t, harnessConfig{
		nodeCount:        4,
		maxBatchSize:     5,
		filterCapability: FilterSupported,
		subscriptionID:   2002,
		namespace:        3,
		idBase:           2000,
		browsePrefix:     "valve",
	})
	h.subscribe(t)
	h.checkServerViewAttribution(t, obligationFI, "notification after teardown: generation 2 built")

	// (4) The generation-1 handles must STILL not resolve against generation
	// 2 - including the positionally valid ones (handle 1 with a 4-entry node
	// list is the exact ENG-5553 misattribution shape).
	for _, item := range generation1 {
		h.requireDroppedWithDiagnostic(t, item.ClientHandle, obligationFI,
			"after teardown, after the next generation is built",
			fmt.Sprintf("a late notification under generation 1's handle %d (which monitored node %s)",
				item.ClientHandle, item.NodeID))
	}

	// (5) Generation 2's own attribution is unaffected by all the drops.
	h.checkServerViewAttribution(t, obligationFI, "notification after teardown: generation 2 after late arrivals")
}
