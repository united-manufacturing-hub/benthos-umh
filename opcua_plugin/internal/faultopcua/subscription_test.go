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

package faultopcua_test

import (
	"context"
	"testing"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

// TestFault_CountsAWriteToAMonitoredNodeOnce pins that one request makes one
// firing when the node it touches is also watched by a subscription.
//
// The server announces every stored value to the node's monitored items by
// reading the node back. That read is part of the write: if it met the fault
// again, an AnswerLate write would count twice and be late twice, and a test
// asserting how often the machine misbehaved would be told about a request
// that never happened.
func TestFault_CountsAWriteToAMonitoredNodeOnce(t *testing.T) {
	srv, client, v := startServer(t, "Setpoint", int32(7))
	id := nodeID(t, v)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	notify := make(chan *opcua.PublishNotificationData, 16)
	sub, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 50 * time.Millisecond}, notify)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	req := opcua.NewMonitoredItemCreateRequestWithDefaults(id, ua.AttributeIDValue, 1)
	if _, err := sub.Monitor(ctx, ua.TimestampsToReturnBoth, req); err != nil {
		t.Fatalf("monitor: %v", err)
	}
	// The item's first value comes from its own read, before anything is
	// armed, so it is not counted below.
	waitForValue(t, notify, int32(7))

	f := srv.AnswerLate(v, 100*time.Millisecond)
	if st := writeValue(t, client, id, int32(4200)); st != ua.StatusOK {
		t.Fatalf("the write answered %v, want OK: AnswerLate is slow, not refusing", st)
	}

	// The subscriber still hears about the write.
	waitForValue(t, notify, int32(4200))
	if got := f.Count(); got != 1 {
		t.Errorf("one write took effect %d times, want 1: the change notification met the fault again", got)
	}
}

// waitForValue waits for a data change carrying want, and fails the test if
// none arrives.
func waitForValue(t *testing.T, notify <-chan *opcua.PublishNotificationData, want any) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case n := <-notify:
			if n.Error != nil {
				t.Fatalf("subscription: %v", n.Error)
			}
			dc, ok := n.Value.(*ua.DataChangeNotification)
			if !ok {
				continue
			}
			for _, item := range dc.MonitoredItems {
				if item.Value != nil && item.Value.Value != nil && item.Value.Value.Value() == want {
					return
				}
			}
		case <-deadline:
			t.Fatalf("no data change carrying %v arrived within 5s", want)
		}
	}
}
