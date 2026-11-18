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
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

// newNotifyChan returns the Publish notification channel shared between the
// fake subscription and the production OPCUAInput (production: SubNotifyChan).
func newNotifyChan() chan *opcua.PublishNotificationData {
	return make(chan *opcua.PublishNotificationData, 4096)
}

// fakeMonitoredItem is one MonitoredItem exactly as the OPC UA server sees it:
// the server-assigned MonitoredItemID, the client-supplied ClientHandle, and
// the node the server actually monitors under that handle.
type fakeMonitoredItem struct {
	MonitoredItemID uint32
	ClientHandle    uint32
	NodeID          string
}

func (i fakeMonitoredItem) String() string {
	return fmt.Sprintf("item %d (handle %d -> node %s)", i.MonitoredItemID, i.ClientHandle, i.NodeID)
}

// fakeSubscription is a test double for SubscriptionAPI that plays the OPC UA
// server's side of the CreateMonitoredItems / Publish exchange.
//
// Its one job is to record, for every MonitoredItem it accepts, the
// (ClientHandle, NodeID) pair exactly as the server sees it, so tests can
// assert that a notification carrying handle h is attributed to the node the
// server actually monitors under h. Tests must never recompute an expected
// handle from the batching arithmetic; the fake is the authority.
//
// Server-faithful behaviors (OPC 10000-4):
//   - Each item in a CreateMonitoredItems request is processed independently:
//     items before AND after a rejected item in the same request are created,
//     exactly as a real server would.
//   - ClientHandle values are echoed as supplied. A real server does not
//     enforce their uniqueness - OPC 10000-4 §7.21 makes uniqueness the
//     client's obligation - so the fake records duplicate handles as separate
//     live items, which is how misattribution becomes observable.
//   - Cancel discards the subscription and every MonitoredItem in it.
//   - When notifyOnAccept is set, a DataChangeNotification for each accepted
//     item is delivered on the notify channel from inside Monitor, before the
//     CreateMonitoredItems response returns to the caller. This models OPC
//     10000-4 §5.13.2.1: notifications can arrive before the response does.
type fakeSubscription struct {
	// SubscriptionID is stamped on every notification this subscription
	// delivers. Real servers assign one per subscription; tests use distinct
	// values per subscription generation.
	SubscriptionID uint32

	// reject decides, per requested item, whether the server refuses to
	// create it (e.g. ua.StatusBadFilterNotAllowed). nil accepts everything.
	reject func(item *ua.MonitoredItemCreateRequest) ua.StatusCode

	notifyOnAccept bool
	notifyChan     chan *opcua.PublishNotificationData

	mu           sync.Mutex
	nextItemID   uint32
	live         []fakeMonitoredItem
	early        []fakeMonitoredItem // items whose notification was delivered before the response returned
	cancelled    bool
	monitorCalls int
}

// Compile-time proof that the fake plugs into the seam production uses.
var _ SubscriptionAPI = (*fakeSubscription)(nil)

func (f *fakeSubscription) Monitor(
	ctx context.Context,
	ts ua.TimestampsToReturn,
	items ...*ua.MonitoredItemCreateRequest,
) (*ua.CreateMonitoredItemsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.monitorCalls++
	resp := &ua.CreateMonitoredItemsResponse{
		Results: make([]*ua.MonitoredItemCreateResult, len(items)),
	}
	accepted := make([]fakeMonitoredItem, 0, len(items))

	for i, item := range items {
		status := ua.StatusOK
		if f.reject != nil {
			status = f.reject(item)
		}
		if status == ua.StatusOK {
			// A real server assigns its own MonitoredItemID and remembers the
			// client handle to stamp on every future notification for the item.
			f.nextItemID++
			rec := fakeMonitoredItem{
				MonitoredItemID: f.nextItemID,
				ClientHandle:    item.RequestedParameters.ClientHandle,
				NodeID:          item.ItemToMonitor.NodeID.String(),
			}
			f.live = append(f.live, rec)
			accepted = append(accepted, rec)
			resp.Results[i] = &ua.MonitoredItemCreateResult{
				StatusCode:       ua.StatusOK,
				MonitoredItemID:  rec.MonitoredItemID,
				RevisedQueueSize: 1,
			}
		} else {
			resp.Results[i] = &ua.MonitoredItemCreateResult{StatusCode: status}
		}
	}

	for _, rec := range accepted {
		f.early = append(f.early, rec)
		if f.notifyOnAccept {
			// OPC 10000-4 §5.13.2.1: deliver before the response returns.
			f.notifyChan <- notificationFor(f.SubscriptionID, rec)
		}
	}
	return resp, nil
}

func (f *fakeSubscription) Cancel(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = true
	f.live = nil
	return nil
}

// serverView returns the live MonitoredItems as the server sees them.
func (f *fakeSubscription) serverView() []fakeMonitoredItem {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeMonitoredItem(nil), f.live...)
}

// itemsByHandle groups the live MonitoredItems by ClientHandle.
func (f *fakeSubscription) itemsByHandle() map[uint32][]fakeMonitoredItem {
	byHandle := make(map[uint32][]fakeMonitoredItem)
	for _, item := range f.serverView() {
		byHandle[item.ClientHandle] = append(byHandle[item.ClientHandle], item)
	}
	return byHandle
}

// earlyDelivered returns, in delivery order, the items whose notifications
// were delivered before the CreateMonitoredItems response returned.
func (f *fakeSubscription) earlyDelivered() []fakeMonitoredItem {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeMonitoredItem(nil), f.early...)
}

// wasCancelled reports whether the subscription was cancelled.
func (f *fakeSubscription) wasCancelled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancelled
}

// notificationFor builds a DataChangeNotification (OPC 10000-4 §7.25.2: it
// carries clientHandle and Value, nothing else) for one monitored item.
// The value encodes the item's server-side MonitoredItemID so a published
// message can be traced back to the exact server item it came from.
func notificationFor(subscriptionID uint32, item fakeMonitoredItem) *opcua.PublishNotificationData {
	variant, err := ua.NewVariant(float64(item.MonitoredItemID))
	if err != nil {
		// A float64 variant cannot fail to encode; anything else is a bug in
		// the test double itself.
		panic(fmt.Sprintf("fakeSubscription: ua.NewVariant failed: %v", err))
	}
	return &opcua.PublishNotificationData{
		SubscriptionID: subscriptionID,
		Value: &ua.DataChangeNotification{
			MonitoredItems: []*ua.MonitoredItemNotification{{
				ClientHandle: item.ClientHandle,
				Value: &ua.DataValue{
					Value:           variant,
					Status:          ua.StatusOK,
					SourceTimestamp: time.Now(),
					ServerTimestamp: time.Now(),
				},
			}},
		},
	}
}

// pushDataChange injects a DataChangeNotification carrying the given handle
// and a caller-chosen float value, as if the server had published it.
func (f *fakeSubscription) pushDataChange(handle uint32, value float64) {
	variant, err := ua.NewVariant(value)
	if err != nil {
		panic(fmt.Sprintf("fakeSubscription: ua.NewVariant failed: %v", err))
	}
	f.notifyChan <- &opcua.PublishNotificationData{
		SubscriptionID: f.SubscriptionID,
		Value: &ua.DataChangeNotification{
			MonitoredItems: []*ua.MonitoredItemNotification{{
				ClientHandle: handle,
				Value: &ua.DataValue{
					Value:           variant,
					Status:          ua.StatusOK,
					SourceTimestamp: time.Now(),
					ServerTimestamp: time.Now(),
				},
			}},
		},
	}
}

// payloadForItem is the byte payload createMessageFromValue produces for the
// notification value of the given server item (float64 -> FormatFloat).
func payloadForItem(item fakeMonitoredItem) string {
	return strconv.FormatFloat(float64(item.MonitoredItemID), 'f', -1, 64)
}
