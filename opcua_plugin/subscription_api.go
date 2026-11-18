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

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

// SubscriptionAPI is the seam between OPCUAInput and the OPC UA subscription
// object it drives. Production always assigns a *opcua.Subscription (see
// BrowseAndSubscribeIfNeeded in read_discover.go); the interface exists so
// tests can substitute a recording fake without standing up a gopcua client
// stack.
//
// It carries exactly the two methods production calls on the subscription:
//
//   - Monitor, called from MonitorBatched (read_discover.go) to create
//     MonitoredItems, and
//   - Cancel, called from unsubscribeAndResetHeartbeat (read.go) on cleanup.
//
// Do not widen this interface: every additional method makes the fake (and
// every future test that needs only these two call sites) more expensive.
type SubscriptionAPI interface {
	Monitor(ctx context.Context, ts ua.TimestampsToReturn, items ...*ua.MonitoredItemCreateRequest) (*ua.CreateMonitoredItemsResponse, error)
	Cancel(ctx context.Context) error
}

// Compile-time proof that the concrete production type satisfies the seam,
// so that every production use site compiles unchanged against the interface.
var _ SubscriptionAPI = (*opcua.Subscription)(nil)
