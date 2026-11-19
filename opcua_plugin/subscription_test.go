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
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// subscriptionHarness runs the production MonitorBatched and
// ReadBatchSubscribe against a fakeSubscription.
type subscriptionHarness struct {
	g     *OPCUAInput
	sub   *fakeSubscription
	nodes map[string]NodeDef // by node ID, across every subscription
	log   *bytes.Buffer
}

func newSubscriptionHarness(nodes []NodeDef, batchSize int, filter FilterCapability) *subscriptionHarness {
	log := &bytes.Buffer{}
	g := &OPCUAInput{OPCUAConnection: &OPCUAConnection{
		Log: service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug}))),
	}}
	g.SubscribeEnabled = true
	g.SubNotifyChan = make(chan *opcua.PublishNotificationData, 1024)
	g.ServerProfile = ServerProfile{Name: "test", MaxBatchSize: batchSize, MaxWorkers: 5, MinWorkers: 1, FilterCapability: filter}
	g.DeadbandType = "absolute"
	g.DeadbandValue = 1.0
	g.QueueSize = 10
	g.SamplingInterval = 1000.0

	h := &subscriptionHarness{g: g, nodes: map[string]NodeDef{}, log: log}
	h.connect(nodes)
	return h
}

// connect starts a new subscription for nodes, as a fresh connection does.
// It shares the notification channel with the previous subscription, so
// values still queued for that one reach the reader afterwards.
func (h *subscriptionHarness) connect(nodes []NodeDef) {
	h.sub = &fakeSubscription{notify: h.g.SubNotifyChan}
	h.g.Subscription = h.sub
	h.g.ServerCapabilities = &ServerCapabilities{}
	h.g.NodeList = nodes
	for _, n := range nodes {
		h.nodes[n.NodeID.String()] = n
	}
}

func (h *subscriptionHarness) monitor() {
	_, err := h.g.MonitorBatched(context.Background(), h.g.NodeList)
	Expect(err).NotTo(HaveOccurred())
}

// readQueued reads the next queued notification through ReadBatchSubscribe.
func (h *subscriptionHarness) readQueued() service.MessageBatch {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msgs, _, err := h.g.ReadBatchSubscribe(ctx)
	Expect(err).NotTo(HaveOccurred())
	return msgs
}

// expectPublishedAs checks that msgs is the one value the server sent for item.
func (h *subscriptionHarness) expectPublishedAs(msgs service.MessageBatch, item fakeItem) {
	GinkgoHelper()
	want := h.nodes[item.nodeID]
	Expect(msgs).To(HaveLen(1), "handle %d of node %s", item.handle, item.nodeID)
	nodeID, _ := msgs[0].MetaGet("opcua_attr_nodeid")
	browseName, _ := msgs[0].MetaGet("opcua_attr_browsename")
	tagName, _ := msgs[0].MetaGet("opcua_tag_name")
	payload, err := msgs[0].AsBytes()
	Expect(err).NotTo(HaveOccurred())
	Expect([]string{nodeID, browseName, tagName, string(payload)}).To(Equal(
		[]string{item.nodeID, want.BrowseName, want.BrowseName, strconv.Itoa(int(item.id))}),
		"handle %d is monitored as node %s", item.handle, item.nodeID)
}

// expectEveryItemPublishedAsItsNode sends a value for every live item and
// checks that each one is published under the node the server monitors.
func (h *subscriptionHarness) expectEveryItemPublishedAsItsNode() {
	GinkgoHelper()
	items := h.sub.items()
	Expect(items).NotTo(BeEmpty())
	handles := map[uint32]fakeItem{}
	for _, item := range items {
		Expect(handles).NotTo(HaveKey(item.handle), "handle %d is used by two monitored items", item.handle)
		handles[item.handle] = item

		h.sub.publish(item.handle, float64(item.id))
		h.expectPublishedAs(h.readQueued(), item)
	}
}

// expectDropped sends a value under handle and checks that it is logged
// and not published.
func (h *subscriptionHarness) expectDropped(handle uint32) {
	GinkgoHelper()
	before := h.log.Len()
	h.sub.publish(handle, 42)
	Expect(h.readQueued()).To(BeEmpty(), "value under handle %d was published", handle)
	logged := h.log.String()[before:]
	Expect(logged).To(ContainSubstring("Dropped 1 of 1 values"),
		"the drop must be reported, not swallowed")
	Expect(logged).To(ContainSubstring("first was %d", handle),
		"the report must name the handle so an operator can tell which values were lost")
}

// expectDroppedInBulk sends many unknown handles in ONE notification and
// checks that the report is a single line naming the count, not a line per
// item. The server decides how many items a notification carries, so a line
// per item is a log flood it can trigger at will.
func (h *subscriptionHarness) expectDroppedInBulk(handles []uint32) {
	GinkgoHelper()
	before := h.log.Len()
	h.sub.publishMany(handles, 42)
	Expect(h.readQueued()).To(BeEmpty(), "values under unknown handles were published")

	logged := h.log.String()[before:]
	Expect(strings.Count(logged, "\n")).To(Equal(1),
		"expected one summary line for %d dropped values, got:\n%s", len(handles), logged)
	Expect(logged).To(ContainSubstring(fmt.Sprintf("Dropped %d of %d values", len(handles), len(handles))))
}

func testNodes(namespace uint16, prefix string, count int) []NodeDef {
	nodes := make([]NodeDef, count)
	for i := range nodes {
		nodes[i] = NodeDef{
			NodeID:     ua.NewNumericNodeID(namespace, uint32(1000+i)),
			NodeClass:  ua.NodeClassVariable,
			BrowseName: fmt.Sprintf("%s_%d", prefix, i),
			DataType:   "Float",
			DataTypeID: ua.TypeIDFloat, // numeric, so it gets the deadband filter
			Path:       "plant.line",
		}
	}
	return nodes
}

// rejectFilterFor models a server that rejects the deadband filter for the
// given nodes, for example because they have no EURange.
func rejectFilterFor(nodes ...NodeDef) func(*ua.MonitoredItemCreateRequest) ua.StatusCode {
	rejected := map[string]bool{}
	for _, n := range nodes {
		rejected[n.NodeID.String()] = true
	}
	return func(req *ua.MonitoredItemCreateRequest) ua.StatusCode {
		if req.RequestedParameters.Filter != nil && rejected[req.ItemToMonitor.NodeID.String()] {
			return ua.StatusBadFilterNotAllowed
		}
		return ua.StatusOK
	}
}

// rejectAllFilters models a server without filter support, such as an S7-1200.
func rejectAllFilters(req *ua.MonitoredItemCreateRequest) ua.StatusCode {
	if req.RequestedParameters.Filter != nil {
		return ua.StatusBadFilterNotAllowed
	}
	return ua.StatusOK
}

// filterScenario is a server's answer to one subscription: which nodes have
// their filter rejected, and how many items fit in one request.
type filterScenario struct {
	nodes      int
	batchSize  int
	capability FilterCapability
	rejected   []int // indices of the nodes whose filter is rejected
	rejectAll  bool
}

func (s filterScenario) String() string {
	return fmt.Sprintf("%d nodes, batch size %d, filter rejected for %v (all: %v)", s.nodes, s.batchSize, s.rejected, s.rejectAll)
}

// randomFilterScenario covers shapes the fixed entries do not. The seed
// reproduces it.
func randomFilterScenario(seed int64) filterScenario {
	rng := rand.New(rand.NewSource(seed))
	s := filterScenario{nodes: 1 + rng.Intn(8), batchSize: 1 + rng.Intn(8), capability: FilterUnknown}
	for i := 0; i < s.nodes; i++ {
		if rng.Intn(3) == 0 {
			s.rejected = append(s.rejected, i)
		}
	}
	return s
}

func filterScenarioEntries() []TableEntry {
	entries := []TableEntry{
		Entry("filter accepted", filterScenario{nodes: 5, batchSize: 5, capability: FilterSupported}),
		Entry("profile without filter support", filterScenario{nodes: 5, batchSize: 5, capability: FilterUnsupported}),
		Entry("filter rejected for the first node", filterScenario{nodes: 5, batchSize: 5, capability: FilterUnknown, rejected: []int{0}}),
		Entry("filter rejected for a middle node", filterScenario{nodes: 5, batchSize: 5, capability: FilterUnknown, rejected: []int{2}}),
		Entry("filter rejected for the last node", filterScenario{nodes: 5, batchSize: 5, capability: FilterUnknown, rejected: []int{4}}),
		Entry("filter rejected in a later batch", filterScenario{nodes: 10, batchSize: 5, capability: FilterUnknown, rejected: []int{7}}),
		Entry("filter rejected in several batches", filterScenario{nodes: 8, batchSize: 3, capability: FilterUnknown, rejected: []int{0, 3, 7}}),
		Entry("server without filter support", filterScenario{nodes: 5, batchSize: 5, capability: FilterUnknown, rejectAll: true}),
		Entry("batch size 1", filterScenario{nodes: 3, batchSize: 1, capability: FilterUnknown, rejected: []int{1}}),
		Entry("batch larger than the node list", filterScenario{nodes: 3, batchSize: 10, capability: FilterUnknown, rejected: []int{0}}),
		Entry("single node, filter rejected", filterScenario{nodes: 1, batchSize: 1, capability: FilterUnknown, rejected: []int{0}}),
	}
	for seed := int64(1); seed <= 8; seed++ {
		s := randomFilterScenario(seed)
		entries = append(entries, Entry(fmt.Sprintf("seed %d: %s", seed, s), s))
	}
	return entries
}

// newUnconnectedInput builds an input the way Connect does, with a client
// that was never connected, so Subscribe fails. The cached browse result
// lets BrowseAndSubscribeIfNeeded skip the browse.
func newUnconnectedInput() *OPCUAInput {
	g := &OPCUAInput{OPCUAConnection: &OPCUAConnection{Log: service.MockResources().Logger()}}
	g.cleanup_func = g.unsubscribeAndResetHeartbeat
	g.SubscribeEnabled = true
	g.SubNotifyChan = make(chan *opcua.PublishNotificationData, 1)
	client, err := opcua.NewClient("opc.tcp://127.0.0.1:1")
	Expect(err).NotTo(HaveOccurred())
	g.Client = client
	g.ServerProfile = GetProfileByName(ProfileAuto)

	node := testNodes(2, "sensor", 1)[0]
	g.NodeIDs = []*ua.NodeID{node.NodeID}
	g.visited.Store(node.NodeID.String(), VisitedNodeInfo{Def: node, LastSeen: time.Now(), FullyDiscovered: true})
	return g
}

var _ = Describe("OPC UA subscription ClientHandles", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	DescribeTable("publishes every value under the node the server monitors under its handle",
		func(s filterScenario) {
			nodes := testNodes(2, "sensor", s.nodes)
			h := newSubscriptionHarness(nodes, s.batchSize, s.capability)
			var rejected []NodeDef
			for _, i := range s.rejected {
				rejected = append(rejected, nodes[i])
			}
			switch {
			case s.rejectAll:
				h.sub.reject = rejectAllFilters
			case len(rejected) > 0:
				h.sub.reject = rejectFilterFor(rejected...)
			}

			h.monitor()
			h.expectEveryItemPublishedAsItsNode()
		},
		filterScenarioEntries(),
	)

	It("publishes values the server sends before it answers CreateMonitoredItems under the right node", func() {
		nodes := testNodes(2, "sensor", 5)
		h := newSubscriptionHarness(nodes, 5, FilterUnknown)
		h.sub.reject = rejectFilterFor(nodes[2])
		created := 0
		h.sub.onCreate = func(item fakeItem) {
			created++
			h.sub.publish(item.handle, float64(item.id))
			h.expectPublishedAs(h.readQueued(), item)
		}

		h.monitor()
		Expect(created).To(Equal(len(h.sub.items())))
	})

	It("keeps the heartbeat node's identity through the filter fallback", func() {
		nodes := testNodes(2, "sensor", 5)
		h := newSubscriptionHarness(nodes, 5, FilterUnknown)
		h.g.UseHeartbeat = true
		h.g.HeartbeatNodeId = nodes[4].NodeID
		h.sub.reject = rejectFilterFor(nodes[2])
		h.monitor()

		for _, item := range h.sub.items() {
			h.sub.publish(item.handle, float64(item.id))
			msgs := h.readQueued()
			h.expectPublishedAs(msgs, item)
			_, isHeartbeat := msgs[0].MetaGet("opcua_heartbeat_message")
			Expect(isHeartbeat).To(Equal(item.nodeID == nodes[4].NodeID.String()))
		}
	})

	DescribeTable("drops and logs a value whose handle belongs to no monitored item",
		func(monitorFirst bool, handle uint32) {
			h := newSubscriptionHarness(testNodes(2, "sensor", 5), 5, FilterSupported)
			if monitorFirst {
				h.monitor()
			}
			h.expectDropped(handle)
		},
		// Handles start at firstHandle, so an index-sized value is never one
		// the registry handed out. These pin that: the old code read such a
		// value as a position in the node list.
		Entry("handle equal to the number of nodes", true, uint32(5)),
		Entry("handle one past the last allocated one", true, firstHandle+uint32(5)),
		Entry("handle far beyond anything allocated", true, firstHandle+uint32(10000)),
		Entry("no monitored item created yet", false, uint32(2)),
	)

	It("survives a server that returns more results than items requested", func() {
		nodes := testNodes(2, "sensor", 3)
		h := newSubscriptionHarness(nodes, 3, FilterSupported)
		// The extra results carry a failure status at an index past the end
		// of the request, which is what reaches for batch[i] out of range.
		h.sub.extraResults = 4

		Expect(func() { h.monitor() }).NotTo(Panic(),
			"a server over-answering CreateMonitoredItems must not panic the input")

		// The nodes we did ask for still resolve to themselves.
		h.expectEveryItemPublishedAsItsNode()
	})

	It("reports many unknown handles in one notification as a single line", func() {
		h := newSubscriptionHarness(testNodes(2, "sensor", 5), 5, FilterSupported)
		h.monitor()

		// A server that sends handles we never issued decides the item count.
		// One log line per item would let it fill the disk of the device we
		// run on, so the report is per notification.
		handles := make([]uint32, 2000)
		for i := range handles {
			handles[i] = firstHandle + uint32(100000+i)
		}
		h.expectDroppedInBulk(handles)
	})

	It("drops values for a cancelled subscription, also after the next one is set up", func() {
		first := testNodes(2, "sensor", 5)
		h := newSubscriptionHarness(first, 5, FilterUnknown)
		h.sub.reject = rejectFilterFor(first[2])
		h.monitor()
		cancelled := h.sub.items()

		h.g.unsubscribeAndResetHeartbeat(context.Background())
		Expect(h.sub.wasCancelled()).To(BeTrue())
		for _, item := range cancelled {
			h.expectDropped(item.handle)
		}

		// A fresh browse found fewer nodes, so the old handles would fit
		// inside the new node list.
		second := testNodes(3, "valve", 3)
		h.connect(second)
		h.sub.reject = rejectFilterFor(second[1])
		h.monitor()
		h.expectEveryItemPublishedAsItsNode()
		for _, item := range cancelled {
			h.expectDropped(item.handle)
		}
	})

	It("reports a monitored item the server refuses for a reason other than the filter", func() {
		nodes := testNodes(2, "sensor", 5)
		refused := nodes[3].NodeID.String()
		h := newSubscriptionHarness(nodes, 5, FilterSupported)
		h.sub.reject = func(req *ua.MonitoredItemCreateRequest) ua.StatusCode {
			if req.ItemToMonitor.NodeID.String() == refused {
				return ua.StatusBadNodeIDUnknown
			}
			return ua.StatusOK
		}
		ResetMetrics()

		_, err := h.g.MonitorBatched(context.Background(), nodes)

		Expect(err).To(MatchError(ContainSubstring(refused)))
		Expect(h.log.String()).To(ContainSubstring("Failed to monitor node " + refused))
		Expect(testutil.ToFloat64(opcuaSubscriptionFailuresTotal.WithLabelValues("node_id_unknown", refused))).To(Equal(1.0))
	})

	It("leaves no subscription behind when Subscribe fails", func() {
		g := newUnconnectedInput()

		// If the failed result were stored, Close would call Cancel on a nil
		// *opcua.Subscription and panic here.
		Expect(g.BrowseAndSubscribeIfNeeded(context.Background())).To(HaveOccurred())
		// Not Expect(...).To(BeNil()): Gomega's BeNil accepts an interface
		// holding a typed-nil pointer, which is exactly the bug being guarded.
		Expect(g.Subscription == nil).To(BeTrue(),
			"a failed Subscribe must leave the field truly nil, not an interface wrapping a nil pointer")
	})

	// Pending until ENG-7894: the browse goroutine calls Close, which waits for
	// the browse goroutine.
	PIt("reports a failed Subscribe from the browse goroutine", func() {
		g := newUnconnectedInput()
		g.startBrowsing(context.Background())
		Eventually(func() error {
			g.browseErrMu.Lock()
			defer g.browseErrMu.Unlock()
			return g.browseErr
		}, 15*time.Second).Should(HaveOccurred())
	})
})

var _ = Describe("handleRegistry", func() {
	It("hands out distinct handles when several goroutines allocate and look up at once", func() {
		var r handleRegistry
		const goroutines, allocations = 4, 200
		allocated := make([][]uint32, goroutines)

		var wg sync.WaitGroup
		for w := 0; w < goroutines; w++ {
			wg.Add(2)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < allocations; i++ {
					allocated[w] = append(allocated[w], r.allocate(NodeDef{BrowseName: fmt.Sprintf("w%d-%d", w, i)}))
				}
			}(w)
			go func() {
				defer wg.Done()
				for i := 0; i < allocations; i++ {
					r.lookup(uint32(i))
				}
			}()
		}
		wg.Wait()

		seen := map[uint32]bool{}
		for w, handles := range allocated {
			for i, handle := range handles {
				Expect(seen).NotTo(HaveKey(handle))
				seen[handle] = true
				node, ok := r.lookup(handle)
				Expect(ok).To(BeTrue())
				Expect(node.BrowseName).To(Equal(fmt.Sprintf("w%d-%d", w, i)))
			}
		}
	})

	It("never hands out a handle that could also be a node list index", func() {
		var r handleRegistry

		// A handle that is also a valid index is how ENG-5553 stayed hidden:
		// reading a handle as a position still found a real node, just the
		// wrong one. Assert against the largest node list the browser will
		// ever produce, not against a number that happens to be convenient:
		// a 200-node server is ordinary, so a spec using 64 would pass while
		// leaving real deployments unseparated.
		const nodes = MaxTagsToBrowse

		// Allocating 100k nodes here would only restate the arithmetic; the
		// property is about where the range starts.
		first := r.allocate(NodeDef{NodeID: ua.NewStringNodeID(1, "first")})
		Expect(first).To(BeNumerically(">=", uint32(nodes)),
			"handle %d is also a valid index into a %d-node list", first, nodes)

		second := r.allocate(NodeDef{NodeID: ua.NewStringNodeID(1, "second")})
		Expect(second).To(BeNumerically(">", first), "handles must keep increasing")

		r.reset()
		afterReset := r.allocate(NodeDef{NodeID: ua.NewStringNodeID(1, "after-reset")})
		Expect(afterReset).To(BeNumerically(">", second),
			"reset must not send handles back to the start of the range")
	})

	It("forgets handles on reset and never hands them out again", func() {
		var r handleRegistry
		old := r.allocate(NodeDef{BrowseName: "old"})
		r.reset()

		_, ok := r.lookup(old)
		Expect(ok).To(BeFalse())
		Expect(r.allocate(NodeDef{BrowseName: "new"})).NotTo(Equal(old))
	})
})
