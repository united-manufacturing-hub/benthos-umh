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
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// These tests pin the obligations from ENG-5553 step 1. They call the real
// production functions - MonitorBatched (read_discover.go) and
// ReadBatchSubscribe (read.go) - and assert against the server's view of the
// world, which is recorded by fakeSubscription (subscription_fake_test.go):
// for every MonitoredItem the server accepted, the (ClientHandle, NodeID)
// pair as the server sees it. A notification carrying handle h must resolve
// to the node the server actually monitors under h.
//
// The tests deliberately never compute an expected ClientHandle from the
// batching arithmetic. The earlier tests around this area (see
// read_discover_recursive_retry_bug_test.go) reimplemented the arithmetic in
// a local closure and asserted against their own reimplementation, which is
// why they agreed with production by construction and missed this defect.
//
// Obligations under test:
//
//	B1  OPC 10000-4 §7.21: clientHandle is the "Client-supplied id of the
//	    MonitoredItem. This id is used in Notifications generated" - a handle
//	    must identify exactly one MonitoredItem within a subscription.
//	B2  OPC 10000-4 §5.13.2.1: "Clients set the ClientHandle for the
//	    MonitoredItem in the CreateMonitoredItems Request and are therefore
//	    able to process the Notifications" that arrive before the response
//	    does - attribution must already be correct for early notifications.
//	O1  UMH policy: the filter-rejection fallback preserves each value's
//	    node ID, browse name and downstream tag.
//	O3  UMH policy: an unknown or out-of-range handle is dropped with a
//	    diagnostic, never fatal.
//	O4  UMH policy: handles from one subscription generation cannot resolve
//	    against a later one after a reconnect.

const (
	obligationB1 = "B1 (OPC 10000-4 §7.21 MonitoringParameters.clientHandle: \"Client-supplied id of the " +
		"MonitoredItem. This id is used in Notifications generated\" - a ClientHandle must identify exactly " +
		"one MonitoredItem within a subscription)"
	obligationB2 = "B2 (OPC 10000-4 §5.13.2.1: \"Clients set the ClientHandle for the MonitoredItem in the " +
		"CreateMonitoredItems Request and are therefore able to process the Notifications\" that arrive before " +
		"the response does - attribution must already be correct for early notifications)"
	obligationO1 = "O1 (UMH policy, ENG-5553: the filter-rejection fallback must preserve each value's node ID, " +
		"browse name and downstream tag)"
	obligationO3 = "O3 (UMH policy, ENG-5553: an unknown or out-of-range ClientHandle must be dropped with a " +
		"diagnostic, never fatal)"
	obligationO4 = "O4 (UMH policy, ENG-5553: handles from one subscription generation must not resolve against " +
		"a later one after a reconnect)"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// harnessConfig describes one subscription scenario. The zero value is not
// usable; every test passes an explicit literal.
type harnessConfig struct {
	nodeCount        int
	maxBatchSize     int
	filterCapability FilterCapability
	reject           func(item *ua.MonitoredItemCreateRequest) ua.StatusCode
	notifyOnAccept   bool
	subscriptionID   uint32
	heartbeatIndex   int // used only when useHeartbeat is set
	useHeartbeat     bool
	namespace        uint16
	idBase           uint32
	browsePrefix     string
}

// attributionHarness wires a fakeSubscription into a real OPCUAInput so the
// production code paths (MonitorBatched, ReadBatchSubscribe,
// unsubscribeAndResetHeartbeat) run unmodified.
type attributionHarness struct {
	g        *OPCUAInput
	sub      *fakeSubscription
	nodes    []NodeDef
	nodeByID map[string]NodeDef
	logBuf   *bytes.Buffer
}

func newAttributionHarness(t *testing.T, cfg harnessConfig) *attributionHarness {
	t.Helper()

	logBuf := &bytes.Buffer{}
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	nodes := makeTestNodes(cfg)
	nodeByID := make(map[string]NodeDef, len(nodes))
	for _, n := range nodes {
		nodeByID[n.NodeID.String()] = n
	}

	notifyChan := newNotifyChan()
	sub := &fakeSubscription{
		SubscriptionID: cfg.subscriptionID,
		reject:         cfg.reject,
		notifyOnAccept: cfg.notifyOnAccept,
		notifyChan:     notifyChan,
	}

	g := &OPCUAInput{
		OPCUAConnection: &OPCUAConnection{Log: logger},
	}
	g.SubscribeEnabled = true
	g.SubNotifyChan = notifyChan
	g.Subscription = sub
	g.NodeList = append([]NodeDef(nil), nodes...)
	g.ServerProfile = ServerProfile{
		Name:             "attribution-test",
		MaxBatchSize:     cfg.maxBatchSize,
		MaxWorkers:       5,
		MinWorkers:       1,
		FilterCapability: cfg.filterCapability,
	}
	// Fresh connection state: no capability trial has happened yet on this
	// connection.
	g.ServerCapabilities = &ServerCapabilities{}
	g.DeadbandType = "absolute"
	g.DeadbandValue = 1.0
	g.QueueSize = 10
	g.SamplingInterval = 1000.0
	if cfg.useHeartbeat {
		g.UseHeartbeat = true
		g.HeartbeatNodeId = nodes[cfg.heartbeatIndex].NodeID
	}

	return &attributionHarness{
		g:        g,
		sub:      sub,
		nodes:    nodes,
		nodeByID: nodeByID,
		logBuf:   logBuf,
	}
}

func makeTestNodes(cfg harnessConfig) []NodeDef {
	nodes := make([]NodeDef, cfg.nodeCount)
	for i := range nodes {
		nodes[i] = NodeDef{
			NodeID:     ua.NewNumericNodeID(cfg.namespace, cfg.idBase+uint32(i)),
			NodeClass:  ua.NodeClassVariable,
			BrowseName: fmt.Sprintf("%s_%03d", cfg.browsePrefix, i),
			DataType:   "Float",
			DataTypeID: ua.TypeIDFloat, // numeric: the deadband filter is applied when the server supports it
			Path:       "plant.line",
		}
	}
	return nodes
}

// subscribe runs the production MonitorBatched against the harness nodes,
// exactly as BrowseAndSubscribeIfNeeded does after a browse.
func (h *attributionHarness) subscribe(t *testing.T) {
	t.Helper()
	if _, err := h.g.MonitorBatched(context.Background(), h.nodes); err != nil {
		t.Fatalf("MonitorBatched failed (test infrastructure problem): %v", err)
	}
}

// subscribeAsync runs MonitorBatched on a background goroutine. Used by the
// B2 tests: the fake queues notifications from inside Monitor, i.e. before
// the CreateMonitoredItems response returns, and the test processes them
// while the response is still outstanding.
func (h *attributionHarness) subscribeAsync(t *testing.T) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := h.g.MonitorBatched(context.Background(), h.nodes); err != nil {
			t.Errorf("MonitorBatched failed (test infrastructure problem): %v", err)
		}
	}()
	return done
}

// reconnect simulates a reconnect through the production cleanup path:
// the old subscription is cancelled via unsubscribeAndResetHeartbeat, then a
// new subscription generation is created with fresh connection state
// (ServerCapabilities) against the same notify channel.
//
// The notify channel is deliberately shared across generations: it models a
// stale Publish notification from the previous generation reaching the reader
// after the reconnect. The server the harness reconnects to is described by
// cfg (it may expose different nodes in a different namespace).
func (h *attributionHarness) reconnect(t *testing.T, cfg harnessConfig) *fakeSubscription {
	t.Helper()
	oldSub := h.sub

	// Production cleanup: Cancel the live subscription and reset heartbeat state.
	h.g.unsubscribeAndResetHeartbeat(context.Background())
	if !oldSub.wasCancelled() {
		t.Fatalf("test infrastructure problem: reconnect did not cancel the previous subscription")
	}

	nodes := makeTestNodes(cfg)
	nodeByID := make(map[string]NodeDef, len(nodes))
	for _, n := range nodes {
		nodeByID[n.NodeID.String()] = n
	}

	newSub := &fakeSubscription{
		SubscriptionID: cfg.subscriptionID,
		reject:         cfg.reject,
		notifyOnAccept: cfg.notifyOnAccept,
		notifyChan:     h.g.SubNotifyChan, // shared across generations, see comment above
	}
	h.sub = newSub
	h.nodes = nodes
	h.nodeByID = nodeByID
	h.g.Subscription = newSub
	h.g.NodeList = append([]NodeDef(nil), nodes...)
	h.g.ServerProfile = ServerProfile{
		Name:             "attribution-test",
		MaxBatchSize:     cfg.maxBatchSize,
		MaxWorkers:       5,
		MinWorkers:       1,
		FilterCapability: cfg.filterCapability,
	}
	// A reconnect establishes fresh connection state: the capability trial
	// starts over on the new connection.
	h.g.ServerCapabilities = &ServerCapabilities{}
	if cfg.useHeartbeat {
		h.g.UseHeartbeat = true
		h.g.HeartbeatNodeId = nodes[cfg.heartbeatIndex].NodeID
	}
	return oldSub
}

// readQueued drains exactly one queued notification through the production
// ReadBatchSubscribe. Panics are recovered and returned so callers can
// report fatality as an obligation violation instead of crashing the test.
func (h *attributionHarness) readQueued() (msgs service.MessageBatch, panicked any, err error) {
	defer func() { panicked = recover() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msgs, _, err = h.g.ReadBatchSubscribe(ctx)
	return
}

// readQueuedFor pushes a DataChangeNotification carrying the item's handle
// (value = the item's server-side MonitoredItemID) and drains it through
// production.
func (h *attributionHarness) readQueuedFor(item fakeMonitoredItem) (service.MessageBatch, any, error) {
	h.sub.pushDataChange(item.ClientHandle, float64(item.MonitoredItemID))
	return h.readQueued()
}

// checkItemAttribution delivers a DataChangeNotification for one monitored
// item (the server's record of it) and asserts the published message carries
// exactly that item's node identity. It returns one violation string per
// broken obligation instance, or nil if the item is handled correctly.
func (h *attributionHarness) checkItemAttribution(item fakeMonitoredItem, obligation, scenario string) []string {
	want, ok := h.nodeByID[item.NodeID]
	if !ok {
		return []string{fmt.Sprintf(
			"test infrastructure problem: the server monitors node %s, which is not in the discovered node list",
			item.NodeID)}
	}

	msgs, panicked, err := h.readQueuedFor(item)
	if panicked != nil {
		return []string{fmt.Sprintf(
			"%s [%s]: handle %d: the server monitors node %s under this handle (%s), but processing its "+
				"DataChangeNotification panicked: %v - this must never be fatal",
			obligation, scenario, item.ClientHandle, item.NodeID, item, panicked)}
	}
	if err != nil {
		return []string{fmt.Sprintf(
			"%s [%s]: handle %d: the server monitors node %s under this handle (%s), but its DataChangeNotification "+
				"was not processed: %v",
			obligation, scenario, item.ClientHandle, item.NodeID, item, err)}
	}
	if len(msgs) == 0 {
		return []string{fmt.Sprintf(
			"%s [%s]: handle %d: the server monitors node %s under this handle (%s), but the notification's value "+
				"was never published (dropped without a message)",
			obligation, scenario, item.ClientHandle, item.NodeID, item)}
	}
	if len(msgs) > 1 {
		return []string{fmt.Sprintf(
			"%s [%s]: handle %d: the server monitors node %s under this handle (%s), but a single notification "+
				"produced %d messages",
			obligation, scenario, item.ClientHandle, item.NodeID, item, len(msgs))}
	}

	return attributionViolations(msgs[0], item, want, obligation, scenario)
}

// attributionViolations compares one published message against the node the
// server actually monitors under the item's handle.
func attributionViolations(m *service.Message, item fakeMonitoredItem, want NodeDef, obligation, scenario string) []string {
	gotNodeID, _ := m.MetaGet("opcua_attr_nodeid")
	gotBrowse, _ := m.MetaGet("opcua_attr_browsename")
	gotTagName, _ := m.MetaGet("opcua_tag_name")
	gotTagGroup, _ := m.MetaGet("opcua_tag_group")
	gotTagPath, _ := m.MetaGet("opcua_tag_path")
	payloadBytes, err := m.AsBytes()
	if err != nil {
		return []string{fmt.Sprintf("test infrastructure problem: message payload unreadable: %v", err)}
	}
	payload := string(payloadBytes)

	wantTagName := sanitize(want.BrowseName)
	wantTagGroup := expectedTagGroup(want)
	wantTagPath := wantTagGroup
	if wantTagGroup == wantTagName {
		wantTagPath = ""
	}

	var mismatches []string
	if gotNodeID != want.NodeID.String() {
		mismatches = append(mismatches, fmt.Sprintf("node ID: published %s, expected %s", gotNodeID, want.NodeID))
	}
	if gotBrowse != want.BrowseName {
		mismatches = append(mismatches, fmt.Sprintf("browse name: published %q, expected %q", gotBrowse, want.BrowseName))
	}
	if gotTagName != wantTagName {
		mismatches = append(mismatches, fmt.Sprintf("tag name: published %q, expected %q", gotTagName, wantTagName))
	}
	if gotTagGroup != wantTagGroup {
		mismatches = append(mismatches, fmt.Sprintf("tag group: published %q, expected %q", gotTagGroup, wantTagGroup))
	}
	if gotTagPath != wantTagPath {
		mismatches = append(mismatches, fmt.Sprintf("tag path: published %q, expected %q", gotTagPath, wantTagPath))
	}
	if payload != payloadForItem(item) {
		mismatches = append(mismatches, fmt.Sprintf("payload: published %q, expected %q", payload, payloadForItem(item)))
	}
	if len(mismatches) == 0 {
		return nil
	}

	return []string{fmt.Sprintf(
		"%s [%s]: handle %d: the server monitors node %s (browse name %q, tag %s/%s) under this handle via %s, "+
			"but the notification's value was published as node %s (browse name %q, tag %s/%s, payload %q); "+
			"mismatched: %s",
		obligation, scenario, item.ClientHandle, want.NodeID, want.BrowseName, wantTagGroup, wantTagName, item,
		gotNodeID, gotBrowse, gotTagGroup, gotTagName, payload, strings.Join(mismatches, "; "))}
}

// expectedTagGroup mirrors how createMessageFromValue derives the tag group
// from a NodeDef (Path with the browse name removed). Needed so the tests
// can assert the downstream tag of the node the server actually monitors.
func expectedTagGroup(node NodeDef) string {
	group := strings.Replace(node.Path, node.BrowseName, "", 1)
	group = strings.TrimSuffix(group, ".")
	if group == "" {
		return sanitize(node.BrowseName)
	}
	return group
}

// checkServerViewAttribution replays, for every MonitoredItem the server has
// live, a DataChangeNotification carrying that item's ClientHandle, and
// asserts production attributes the value to exactly the node the server
// monitors under that handle. Expected handles are never computed here; they
// come from the server's record of what it accepted.
func (h *attributionHarness) checkServerViewAttribution(t *testing.T, obligation, scenario string) {
	t.Helper()
	items := h.sub.serverView()
	if len(items) == 0 {
		t.Fatalf("[%s] test infrastructure problem: the server has no live monitored items", scenario)
	}
	var violations []string
	for _, item := range items {
		violations = append(violations, h.checkItemAttribution(item, obligation, scenario)...)
	}
	if len(violations) > 0 {
		t.Errorf("%s [%s]: %d of %d live MonitoredItems were attributed to the wrong node:\n%s",
			obligation, scenario, len(violations), len(items), strings.Join(violations, "\n"))
	}
}

// checkHandleUniqueness asserts B1: within the (single) subscription, every
// ClientHandle the server accepted identifies exactly one live MonitoredItem.
func (h *attributionHarness) checkHandleUniqueness(t *testing.T, obligation, scenario string) {
	t.Helper()
	byHandle := h.sub.itemsByHandle()
	handles := make([]int, 0, len(byHandle))
	for handle := range byHandle {
		handles = append(handles, int(handle))
	}
	sort.Ints(handles)

	var violations []string
	for _, handle := range handles {
		items := byHandle[uint32(handle)]
		if len(items) > 1 {
			violations = append(violations, fmt.Sprintf(
				"handle %d identifies %d live MonitoredItems: %s",
				handle, len(items), itemDescriptions(items)))
		}
	}
	if len(violations) > 0 {
		t.Errorf("%s [%s]: %d handles identify more than one MonitoredItem:\n%s",
			obligation, scenario, len(violations), strings.Join(violations, "\n"))
	}
}

func itemDescriptions(items []fakeMonitoredItem) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, item.String())
	}
	return strings.Join(parts, "; ")
}

// checkHeartbeatIdentity asserts that every live MonitoredItem watching the
// heartbeat node publishes under the heartbeat node's identity, carrying the
// heartbeat marker downstream consumers rely on.
func (h *attributionHarness) checkHeartbeatIdentity(t *testing.T, obligation, scenario string) {
	t.Helper()
	heartbeatNodeID := h.g.HeartbeatNodeId.String()
	var heartbeatItems []fakeMonitoredItem
	for _, item := range h.sub.serverView() {
		if item.NodeID == heartbeatNodeID {
			heartbeatItems = append(heartbeatItems, item)
		}
	}
	if len(heartbeatItems) == 0 {
		t.Fatalf("[%s] test infrastructure problem: the server monitors no item for heartbeat node %s",
			scenario, heartbeatNodeID)
	}

	var violations []string
	for _, item := range heartbeatItems {
		msgs, panicked, err := h.readQueuedFor(item)
		if panicked != nil {
			violations = append(violations, fmt.Sprintf(
				"%s [%s]: the heartbeat node %s is monitored under handle %d (%s), but processing its "+
					"DataChangeNotification panicked: %v",
				obligation, scenario, heartbeatNodeID, item.ClientHandle, item, panicked))
			continue
		}
		if err != nil || len(msgs) != 1 {
			violations = append(violations, fmt.Sprintf(
				"%s [%s]: the heartbeat node %s is monitored under handle %d (%s), but its notification did not "+
					"produce exactly one message (messages=%d, err=%v)",
				obligation, scenario, heartbeatNodeID, item.ClientHandle, item, len(msgs), err))
			continue
		}
		gotNodeID, _ := msgs[0].MetaGet("opcua_attr_nodeid")
		marker, hasMarker := msgs[0].MetaGet("opcua_heartbeat_message")
		if gotNodeID != heartbeatNodeID || !hasMarker || marker != "true" {
			violations = append(violations, fmt.Sprintf(
				"%s [%s]: the heartbeat node %s is monitored under handle %d (%s), but its value was published "+
					"as node %s with heartbeat marker %q (marker present=%v)",
				obligation, scenario, heartbeatNodeID, item.ClientHandle, item, gotNodeID, marker, hasMarker))
		}
	}
	if len(violations) > 0 {
		t.Errorf("%s [%s]: the heartbeat node lost its identity:\n%s",
			obligation, scenario, strings.Join(violations, "\n"))
	}
}

func (h *attributionHarness) logSnapshot() string {
	return h.logBuf.String()
}

func (h *attributionHarness) logSince(snapshot string) string {
	return strings.TrimPrefix(h.logBuf.String(), snapshot)
}

// ---------------------------------------------------------------------------
// Server-side rejection predicates (what kind of server are we talking to?)
// ---------------------------------------------------------------------------

// rejectFilteredFor models a server that refuses the DataChangeFilter for
// exactly the given nodes but accepts it elsewhere. This is realistic: a
// percent deadband needs the node's EURange (OPC 10000-4), and only some
// nodes of a server may lack it.
func rejectFilteredFor(nodeIDs ...string) func(item *ua.MonitoredItemCreateRequest) ua.StatusCode {
	rejected := make(map[string]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		rejected[id] = true
	}
	return func(item *ua.MonitoredItemCreateRequest) ua.StatusCode {
		if item.RequestedParameters.Filter != nil && rejected[item.ItemToMonitor.NodeID.String()] {
			return ua.StatusBadFilterNotAllowed
		}
		return ua.StatusOK
	}
}

// rejectAllFiltered models a server without any DataChangeFilter support
// (e.g. the Micro Embedded Device profile of an S7-1200): every request that
// carries a filter is rejected per item.
func rejectAllFiltered(item *ua.MonitoredItemCreateRequest) ua.StatusCode {
	if item.RequestedParameters.Filter != nil {
		return ua.StatusBadFilterNotAllowed
	}
	return ua.StatusOK
}

// ---------------------------------------------------------------------------
// B1 - OPC 10000-4 §7.21: a ClientHandle identifies exactly one MonitoredItem
// within a subscription.
// ---------------------------------------------------------------------------

func TestB1ClientHandleIdentifiesExactlyOneMonitoredItem(t *testing.T) {
	t.Run("accepted filter path assigns one handle per item", func(t *testing.T) {
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
		h.checkHandleUniqueness(t, obligationB1, "accepted filter path")
	})

	t.Run("filter fallback at a nonzero index reuses handles for different nodes", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		// The server rejects the filter for the third node only.
		h.sub.reject = rejectFilteredFor(h.nodes[2].NodeID.String())
		h.subscribe(t)
		h.checkHandleUniqueness(t, obligationB1, "filter fallback at a nonzero index")
	})

	t.Run("filter fallback in a later batch reuses handles for different nodes", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        10,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		// The server rejects the filter for the eighth node (index 7), which
		// lives in the second batch.
		h.sub.reject = rejectFilteredFor(h.nodes[7].NodeID.String())
		h.subscribe(t)
		h.checkHandleUniqueness(t, obligationB1, "filter fallback in a later batch")
	})

	t.Run("filter fallback at index zero against a filterless server keeps handles unique", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			reject:           rejectAllFiltered,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		// A server with no DataChangeFilter support at all rejects every
		// filtered request, so production's retry starts at index zero.
		h.subscribe(t)
		h.checkHandleUniqueness(t, obligationB1, "filter fallback at index zero against a filterless server")
	})

	t.Run("rejection of only the first node's filter re-subscribes the survivors under duplicate handles", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		// The server rejects the filter for the first node only; nodes 1-4
		// are created by the first request before production retries.
		h.sub.reject = rejectFilteredFor(h.nodes[0].NodeID.String())
		h.subscribe(t)
		h.checkHandleUniqueness(t, obligationB1, "rejection of only the first node's filter")
	})
}

// ---------------------------------------------------------------------------
// B2 - OPC 10000-4 §5.13.2.1: notifications can arrive before the
// CreateMonitoredItems response returns, so attribution must already be
// correct when they do.
// ---------------------------------------------------------------------------

func TestB2EarlyNotificationsAreAttributedCorrectly(t *testing.T) {
	// runEarlyNotificationScenario subscribes with notifyOnAccept=true: the
	// fake queues a DataChangeNotification for each accepted item from inside
	// Monitor, before the CreateMonitoredItems response returns to
	// MonitorBatched. The test then processes those queued notifications
	// while the response is still outstanding.
	runEarlyNotificationScenario := func(t *testing.T, cfg harnessConfig, scenario string, configureServer func(h *attributionHarness)) {
		t.Helper()
		h := newAttributionHarness(t, cfg)
		if configureServer != nil {
			configureServer(h)
		}
		done := h.subscribeAsync(t)

		// Wait until the server has live items for every discovered node (a
		// server-observed fact, not a recomputation of batching arithmetic).
		// MonitorBatched sleeps 1s after each batch, so once the items exist
		// there is a long window in which the response is still outstanding.
		deadline := time.Now().Add(15 * time.Second)
		for len(h.sub.serverView()) < len(h.nodes) {
			select {
			case <-done:
				t.Fatalf("test infrastructure problem: MonitorBatched returned before all %d nodes were monitored", len(h.nodes))
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("test infrastructure problem: timed out waiting for monitored items")
			}
			time.Sleep(5 * time.Millisecond)
		}

		// Process every early notification - these were queued before the
		// CreateMonitoredItems response returned.
		early := h.sub.earlyDelivered()
		var violations []string
		for _, item := range early {
			msgs, panicked, err := h.readQueued()
			if panicked != nil {
				violations = append(violations, fmt.Sprintf(
					"%s [%s]: handle %d (%s): an early notification panicked ReadBatchSubscribe: %v",
					obligationB2, scenario, item.ClientHandle, item, panicked))
				continue
			}
			if err != nil {
				violations = append(violations, fmt.Sprintf(
					"%s [%s]: handle %d (%s): an early notification was not processed: %v",
					obligationB2, scenario, item.ClientHandle, item, err))
				continue
			}
			if len(msgs) != 1 {
				violations = append(violations, fmt.Sprintf(
					"%s [%s]: handle %d (%s): an early notification produced %d messages, expected exactly 1",
					obligationB2, scenario, item.ClientHandle, item, len(msgs)))
				continue
			}
			want, ok := h.nodeByID[item.NodeID]
			if !ok {
				t.Fatalf("test infrastructure problem: server monitors unknown node %s", item.NodeID)
			}
			violations = append(violations, attributionViolations(msgs[0], item, want, obligationB2, scenario)...)
		}

		// Evidence that the notifications above were processed while the
		// CreateMonitoredItems response was still outstanding. (Soft check:
		// the 1s post-batch sleep in MonitorBatched makes the window large,
		// but a slow environment losing the race does not invalidate the
		// attribution assertions above - the notifications were still queued
		// before the response returned.)
		select {
		case <-done:
			t.Logf("note: MonitorBatched returned before the early notifications were processed")
		default:
		}

		if len(violations) > 0 {
			t.Errorf("%s [%s]: %d of %d early notifications (delivered before the CreateMonitoredItems "+
				"response returned) were attributed to the wrong node:\n%s",
				obligationB2, scenario, len(violations), len(early), strings.Join(violations, "\n"))
		}
	}

	t.Run("accepted filter path", func(t *testing.T) {
		runEarlyNotificationScenario(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterSupported,
			notifyOnAccept:   true,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		}, "accepted filter path", nil)
	})

	t.Run("filter fallback at a nonzero index", func(t *testing.T) {
		runEarlyNotificationScenario(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			notifyOnAccept:   true,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		}, "filter fallback at a nonzero index", func(h *attributionHarness) {
			// The server rejects the filter for the third node only, so the
			// recursive retry delivers its early notifications while the
			// outer CreateMonitoredItems response is still outstanding.
			h.sub.reject = rejectFilteredFor(h.nodes[2].NodeID.String())
		})
	})
}

// ---------------------------------------------------------------------------
// O1 - the filter-rejection fallback preserves each value's node ID, browse
// name and downstream tag.
// ---------------------------------------------------------------------------

func TestO1FilterRejectionFallbackPreservesNodeIdentity(t *testing.T) {
	t.Run("rejection at a nonzero index", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		h.sub.reject = rejectFilteredFor(h.nodes[2].NodeID.String())
		h.subscribe(t)
		h.checkServerViewAttribution(t, obligationO1, "rejection at a nonzero index")
	})

	t.Run("rejection at index zero against a filterless server", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			reject:           rejectAllFiltered,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		h.subscribe(t)
		h.checkServerViewAttribution(t, obligationO1, "rejection at index zero against a filterless server")
	})

	t.Run("rejection in a later batch", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        10,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		h.sub.reject = rejectFilteredFor(h.nodes[7].NodeID.String())
		h.subscribe(t)
		h.checkServerViewAttribution(t, obligationO1, "rejection in a later batch")
	})

	t.Run("repeated fallback across two subscription generations", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		h.sub.reject = rejectFilteredFor(h.nodes[2].NodeID.String())
		h.subscribe(t)
		h.checkServerViewAttribution(t, obligationO1, "repeated fallback, generation 1")

		// Reconnect (same server, same nodes, fresh connection state): the
		// capability trial runs again and the fallback fires a second time.
		h.reconnect(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			subscriptionID:   2002,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		h.sub.reject = rejectFilteredFor(h.nodes[2].NodeID.String())
		h.subscribe(t)
		h.checkServerViewAttribution(t, obligationO1, "repeated fallback, generation 2")
	})

	t.Run("accepted filter path (no fallback)", func(t *testing.T) {
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
		h.checkServerViewAttribution(t, obligationO1, "accepted filter path")
	})

	t.Run("no-filter profile path", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterUnsupported,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		h.subscribe(t)
		h.checkServerViewAttribution(t, obligationO1, "no-filter profile path")
	})

	t.Run("heartbeat node keeps its identity through the fallback", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterUnknown,
			subscriptionID:   1001,
			useHeartbeat:     true,
			heartbeatIndex:   4,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		// Reject a node before the heartbeat node in the list, so the
		// heartbeat node ends up in the retried suffix.
		h.sub.reject = rejectFilteredFor(h.nodes[2].NodeID.String())
		h.subscribe(t)
		h.checkHeartbeatIdentity(t, obligationO1, "heartbeat node through the fallback")
	})

	t.Run("heartbeat node keeps its identity on the accepted filter path", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterSupported,
			subscriptionID:   1001,
			useHeartbeat:     true,
			heartbeatIndex:   4,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		h.subscribe(t)
		h.checkHeartbeatIdentity(t, obligationO1, "heartbeat node on the accepted filter path")
	})
}

// ---------------------------------------------------------------------------
// O3 - an unknown or out-of-range handle is dropped with a diagnostic, never
// fatal.
// ---------------------------------------------------------------------------

func TestO3UnknownOrOutOfRangeHandleIsDroppedWithDiagnosticNeverFatal(t *testing.T) {
	t.Run("handle equal to the node list length must not be fatal", func(t *testing.T) {
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

		outOfRangeHandle := uint32(len(h.g.NodeList)) // == 5: one past the last valid index
		h.sub.pushDataChange(outOfRangeHandle, 424242)
		msgs, panicked, err := h.readQueued()

		if panicked != nil {
			t.Errorf("%s: a DataChangeNotification carrying handle %d (the node list has %d entries, so this "+
				"handle is out of range) panicked ReadBatchSubscribe: %v - an out-of-range handle must be "+
				"dropped with a diagnostic, never fatal",
				obligationO3, outOfRangeHandle, len(h.g.NodeList), panicked)
			return
		}
		if err != nil {
			t.Fatalf("test infrastructure problem: notification was not processed: %v", err)
		}
		if len(msgs) != 0 {
			t.Errorf("%s: a DataChangeNotification carrying out-of-range handle %d was published as %d message(s) "+
				"instead of being dropped", obligationO3, outOfRangeHandle, len(msgs))
		}
	})

	t.Run("handle beyond the node list length is dropped but must be diagnosed", func(t *testing.T) {
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

		farOutOfRangeHandle := uint32(100)
		snapshot := h.logSnapshot()
		h.sub.pushDataChange(farOutOfRangeHandle, 424242)
		msgs, panicked, err := h.readQueued()

		if panicked != nil {
			t.Errorf("%s: a DataChangeNotification carrying handle %d panicked ReadBatchSubscribe: %v",
				obligationO3, farOutOfRangeHandle, panicked)
			return
		}
		if err != nil {
			t.Fatalf("test infrastructure problem: notification was not processed: %v", err)
		}
		if len(msgs) != 0 {
			t.Errorf("%s: a DataChangeNotification carrying out-of-range handle %d was published as %d message(s) "+
				"instead of being dropped", obligationO3, farOutOfRangeHandle, len(msgs))
		}
		if newLogs := h.logSince(snapshot); newLogs == "" {
			t.Errorf("%s: a DataChangeNotification carrying out-of-range handle %d (the node list has %d entries) "+
				"was dropped silently - it must be dropped with a diagnostic so operators can tell values are "+
				"being lost", obligationO3, farOutOfRangeHandle, len(h.g.NodeList))
		}
	})

	t.Run("unknown in-range handle the server has no item for must be dropped", func(t *testing.T) {
		h := newAttributionHarness(t, harnessConfig{
			nodeCount:        5,
			maxBatchSize:     5,
			filterCapability: FilterSupported,
			subscriptionID:   1001,
			namespace:        2,
			idBase:           1000,
			browsePrefix:     "sensor",
		})
		// No MonitorBatched call: the server has created no MonitoredItem at
		// all, so no handle is known. Such a notification reaches the reader
		// when a stale generation's publish is still in flight (see the O4
		// tests) or a server misbehaves.
		unknownHandle := uint32(2)
		snapshot := h.logSnapshot()
		h.sub.pushDataChange(unknownHandle, 99)
		msgs, panicked, err := h.readQueued()

		if panicked != nil {
			t.Errorf("%s: a DataChangeNotification carrying unknown handle %d panicked ReadBatchSubscribe: %v",
				obligationO3, unknownHandle, panicked)
			return
		}
		if err != nil {
			t.Fatalf("test infrastructure problem: notification was not processed: %v", err)
		}
		if len(msgs) != 0 {
			gotNodeID, _ := msgs[0].MetaGet("opcua_attr_nodeid")
			gotBrowse, _ := msgs[0].MetaGet("opcua_attr_browsename")
			t.Errorf("%s: the server has no MonitoredItem under handle %d (it has no live items at all), yet the "+
				"notification's value was published under node %s (browse name %q) - an unknown handle must be "+
				"dropped with a diagnostic, never attributed to an arbitrary node",
				obligationO3, unknownHandle, gotNodeID, gotBrowse)
		}
		if newLogs := h.logSince(snapshot); newLogs == "" {
			t.Errorf("%s: a DataChangeNotification carrying unknown handle %d (the server has no item under it) "+
				"was dropped silently - it must be dropped with a diagnostic", obligationO3, unknownHandle)
		}
	})
}

// ---------------------------------------------------------------------------
// O4 - handles from one subscription generation cannot resolve against a
// later one after a reconnect.
// ---------------------------------------------------------------------------

func TestO4HandlesFromOneGenerationDoNotResolveAfterReconnect(t *testing.T) {
	// Generation 1: five nodes in namespace 2, filter accepted.
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

	// Reconnect: the production cleanup cancels generation 1; generation 2
	// subscribes to a different node list (namespace 3), as a fresh browse of
	// the same server can produce.
	h.reconnect(t, harnessConfig{
		nodeCount:        3,
		maxBatchSize:     5,
		filterCapability: FilterSupported,
		subscriptionID:   2002,
		namespace:        3,
		idBase:           2000,
		browsePrefix:     "valve",
	})

	var violations []string

	// (a) A stale generation-1 notification that was already in flight when
	// the reconnect happened sits in the channel while generation 2 sets up.
	stale := generation1[2] // handle 2 -> a generation-1 node in namespace 2
	h.sub.pushDataChange(stale.ClientHandle, float64(stale.MonitoredItemID))
	h.subscribe(t)

	msgs, panicked, err := h.readQueued()
	if panicked != nil {
		violations = append(violations, fmt.Sprintf(
			"%s: a stale notification from subscription generation 1 (subscription ID 1001) carrying handle %d, "+
				"which generation 1's server monitored as node %s, panicked ReadBatchSubscribe after the "+
				"reconnect: %v",
			obligationO4, stale.ClientHandle, stale.NodeID, panicked))
	} else if err != nil {
		t.Fatalf("test infrastructure problem: notification was not processed: %v", err)
	} else if len(msgs) != 0 {
		describe := func(m *service.Message) string {
			nodeID, _ := m.MetaGet("opcua_attr_nodeid")
			browse, _ := m.MetaGet("opcua_attr_browsename")
			return fmt.Sprintf("node %s (browse name %q)", nodeID, browse)
		}
		parts := make([]string, 0, len(msgs))
		for _, m := range msgs {
			parts = append(parts, describe(m))
		}
		violations = append(violations, fmt.Sprintf(
			"%s: a stale notification from subscription generation 1 (subscription ID 1001) carrying handle %d, "+
				"which generation 1's server monitored as node %s, was published after the reconnect as: %s - "+
				"a handle from a previous generation must not resolve against the new node list",
			obligationO4, stale.ClientHandle, stale.NodeID, strings.Join(parts, "; ")))
	}

	// (b) A stale generation-1 notification arriving even later, after
	// generation 2 is fully set up. Handle 3 is past the end of generation
	// 2's 3-entry node list.
	lateStale := generation1[3]
	snapshot := h.logSnapshot()
	h.sub.pushDataChange(lateStale.ClientHandle, float64(lateStale.MonitoredItemID))
	msgs, panicked, err = h.readQueued()
	if panicked != nil {
		violations = append(violations, fmt.Sprintf(
			"%s: a late stale notification from subscription generation 1 (subscription ID 1001) carrying handle "+
				"%d, which generation 1's server monitored as node %s, panicked ReadBatchSubscribe after the "+
				"reconnect: %v",
			obligationO4, lateStale.ClientHandle, lateStale.NodeID, panicked))
	} else if err != nil {
		t.Fatalf("test infrastructure problem: notification was not processed: %v", err)
	} else if len(msgs) != 0 {
		gotNodeID, _ := msgs[0].MetaGet("opcua_attr_nodeid")
		violations = append(violations, fmt.Sprintf(
			"%s: a late stale notification from subscription generation 1 (subscription ID 1001) carrying handle "+
				"%d, which generation 1's server monitored as node %s, was published after the reconnect as node %s",
			obligationO4, lateStale.ClientHandle, lateStale.NodeID, gotNodeID))
	} else if newLogs := h.logSince(snapshot); newLogs == "" {
		violations = append(violations, fmt.Sprintf(
			"%s: a late stale notification carrying handle %d from generation 1 was dropped silently - it must "+
				"be dropped with a diagnostic", obligationO4, lateStale.ClientHandle))
	}

	if len(violations) > 0 {
		t.Errorf("%s: stale handles from a previous subscription generation resolved against the new one:\n%s",
			obligationO4, strings.Join(violations, "\n"))
	}
}
