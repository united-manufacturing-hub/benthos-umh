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
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/opcua_plugin/internal/faultopcua"
)

// TestAutoReconnect_FalseActuallyStopsTheClientReconnecting
//
// autoReconnect is documented as defaulting to false, and setting it false
// did nothing: gopcua's own default is AutoReconnect(true), and we only ever
// passed the option when our field was true, so the field was one-way.
//
// Nothing caught it because nothing had ever killed a connection underneath
// a running client. This kills one, and then counts the dials the server
// saw. A reconnect is otherwise invisible -- the session is rebuilt, the
// next request works, nothing is logged -- so the dial count is the whole
// of the evidence.
func TestAutoReconnect_FalseActuallyStopsTheClientReconnecting(t *testing.T) {
	ctx := context.Background()
	srv, err := faultopcua.New()
	if err != nil {
		t.Fatalf("fault server: %v", err)
	}
	srv.AddNode("Flag", true)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start fault server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	conn := &OPCUAConnection{
		Endpoint:      srv.Endpoint(),
		Insecure:      true,
		DirectConnect: true,
		AutoReconnect: false, // the documented default, and the thing under test
		Log:           service.MockResources().Logger(),
	}
	if err := conn.connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	dialsAfterConnect := srv.Dials()
	if dialsAfterConnect == 0 {
		t.Fatal("the server saw no connection at all, so this test proves nothing")
	}

	// Kill the session underneath the client.
	srv.Inject(faultopcua.Fault{Node: "Flag", Kind: faultopcua.DropSession})
	id, err := ua.ParseNodeID(srv.NodeID("Flag"))
	if err != nil {
		t.Fatalf("node id: %v", err)
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, _ = conn.Client.Read(readCtx, &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{{NodeID: id, AttributeID: ua.AttributeIDValue}},
	})
	cancel()
	if srv.Cuts() == 0 {
		t.Fatal("no connection was closed, so nothing was there to reconnect from")
	}

	// Give a reconnecting client time to come back. gopcua's monitor redials
	// well inside this.
	time.Sleep(1500 * time.Millisecond)

	if got := srv.Dials(); got > dialsAfterConnect {
		t.Errorf("the client reconnected %d time(s) after the drop although autoReconnect is false",
			got-dialsAfterConnect)
	}
}
