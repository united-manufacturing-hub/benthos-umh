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

// This file implements a fake OPC UA server for the subscription tests.

import (
	"context"
	"sync"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

// fakeItem is a monitored item as the server sees it. Tests take the expected
// node of a handle from here, never from the batching arithmetic.
type fakeItem struct {
	id     uint32 // assigned by the server; also the value it publishes
	handle uint32 // supplied by the client
	nodeID string
}

// fakeSubscription plays the server side of CreateMonitoredItems and Publish.
// Like a real server, it creates each requested item on its own, so items
// after a rejected one are still created, and it does not check that
// handles are unique.
type fakeSubscription struct {
	// reject returns the status for one requested item; nil accepts all.
	reject func(item *ua.MonitoredItemCreateRequest) ua.StatusCode
	// onCreate runs for every created item before Monitor returns. A server
	// may send values that early (OPC 10000-4 §5.13.2.1).
	onCreate func(item fakeItem)
	notify   chan *opcua.PublishNotificationData

	mu        sync.Mutex
	lastID    uint32
	live      []fakeItem
	cancelled bool
}

var _ subscription = (*fakeSubscription)(nil)

func (f *fakeSubscription) Monitor(_ context.Context, _ ua.TimestampsToReturn, items ...*ua.MonitoredItemCreateRequest) (*ua.CreateMonitoredItemsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	resp := &ua.CreateMonitoredItemsResponse{Results: make([]*ua.MonitoredItemCreateResult, len(items))}
	for i, req := range items {
		status := ua.StatusOK
		if f.reject != nil {
			status = f.reject(req)
		}
		if status != ua.StatusOK {
			resp.Results[i] = &ua.MonitoredItemCreateResult{StatusCode: status}
			continue
		}
		f.lastID++
		item := fakeItem{id: f.lastID, handle: req.RequestedParameters.ClientHandle, nodeID: req.ItemToMonitor.NodeID.String()}
		f.live = append(f.live, item)
		resp.Results[i] = &ua.MonitoredItemCreateResult{StatusCode: ua.StatusOK, MonitoredItemID: item.id}
		if f.onCreate != nil {
			f.onCreate(item)
		}
	}
	return resp, nil
}

func (f *fakeSubscription) Cancel(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = true
	f.live = nil
	return nil
}

// items returns the live monitored items in the order they were created.
func (f *fakeSubscription) items() []fakeItem {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeItem(nil), f.live...)
}

func (f *fakeSubscription) wasCancelled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancelled
}

// publish sends a value under handle, as the server would.
func (f *fakeSubscription) publish(handle uint32, value float64) {
	f.notify <- dataChange(handle, value)
}

// publishMany sends one notification carrying a value for every handle. A
// real server decides how many items a notification holds, so this is the
// shape a server uses to send many values at once - including many the
// client never asked for.
func (f *fakeSubscription) publishMany(handles []uint32, value float64) {
	items := make([]*ua.MonitoredItemNotification, 0, len(handles))
	for _, h := range handles {
		items = append(items, dataChange(h, value).Value.(*ua.DataChangeNotification).MonitoredItems[0])
	}
	f.notify <- &opcua.PublishNotificationData{
		Value: &ua.DataChangeNotification{MonitoredItems: items},
	}
}

func dataChange(handle uint32, value float64) *opcua.PublishNotificationData {
	return &opcua.PublishNotificationData{
		Value: &ua.DataChangeNotification{
			MonitoredItems: []*ua.MonitoredItemNotification{{
				ClientHandle: handle,
				Value: &ua.DataValue{
					Value:           ua.MustVariant(value),
					Status:          ua.StatusOK,
					SourceTimestamp: time.Now(),
					ServerTimestamp: time.Now(),
				},
			}},
		},
	}
}
