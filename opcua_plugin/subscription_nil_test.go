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
	"io"
	"log/slog"
	"testing"

	"github.com/gopcua/opcua"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestTeardownSurvivesANilSubscriptionPointer pins the one hazard that comes
// with holding the subscription behind an interface rather than a pointer.
//
// Client.Subscribe returns a nil *opcua.Subscription alongside its error. When
// the field was a *opcua.Subscription, storing that nil and testing it with
// != nil skipped the teardown correctly. Stored in an interface it becomes a
// typed nil: the interface itself is not nil, so a plain != nil admits it and
// Cancel runs on a nil receiver, which dereferences it and panics the input.
//
// Any subscription-creation failure reaches this teardown through Close, so
// the crash needs nothing more exotic than a server declining a subscription.
func TestTeardownSurvivesANilSubscriptionPointer(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(io.Discard, nil)))

	g := &OPCUAInput{OPCUAConnection: &OPCUAConnection{Log: logger}}
	g.SubscribeEnabled = true

	// Exactly what `g.Subscription, err = g.Client.Subscribe(...)` leaves behind
	// when Subscribe fails.
	var failed *opcua.Subscription
	g.Subscription = failed

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("teardown panicked on a nil subscription pointer: %v - "+
				"a failed Subscribe must not crash the input on cleanup", r)
		}
	}()

	g.unsubscribeAndResetHeartbeat(context.Background())

	if g.Subscription != nil {
		t.Errorf("teardown left a non-nil Subscription behind: %#v", g.Subscription)
	}
}
