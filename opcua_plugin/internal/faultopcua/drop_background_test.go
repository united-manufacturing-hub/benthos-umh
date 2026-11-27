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
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

// dropCaseEnv runs the monitored-item scenario in a child of this same test
// binary. The scenario ends with the test failing -- the harness refuses it
// loudly -- so it cannot run inline: the parent asserts the child died with
// the refusal, exactly as TestArming_RefusesWhatItCannotArm does for the
// arm-time guards.
const dropCaseEnv = "FAULTOPCUA_DROP_CASE"

// TestDropSession_RefusesToFireWithoutAConnectionToCut pins the one invariant
// the evidence machinery stands on: a fault that reports it fired must have
// done what firing means.
//
// gopcua answers a monitored item from a background goroutine, calling
// Attribute with no request behind it. The read and write handlers are the
// only places that tell the namespace whose connection is calling, so the
// background read finds reqPort at zero, and DropSession firing there used to
// report a firing -- and hand the client a ConnectionClosed -- while cutting
// nothing: Cuts() stayed 0 and the session lived. A fault that reports it
// fired while closing nothing is the one thing this package must never do,
// so when there is no connection to cut, the harness fails the test rather
// than returning quietly.
func TestDropSession_RefusesToFireWithoutAConnectionToCut(t *testing.T) {
	if _, ok := os.LookupEnv(dropCaseEnv); ok {
		// The child: arm DropSession, then watch it through a monitored
		// item, the way a client with a subscription watches a node.
		srv, client, v := startServer(t, "Setpoint", int32(7))
		f := srv.DropSession(v)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		notify := make(chan *opcua.PublishNotificationData, 16)
		sub, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{
			Interval: 500 * time.Millisecond,
		}, notify)
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		req := opcua.NewMonitoredItemCreateRequestWithDefaults(nodeID(t, v), ua.AttributeIDValue, 1)
		if _, err := sub.Monitor(ctx, ua.TimestampsToReturnBoth, req); err != nil {
			t.Fatalf("monitor: %v", err)
		}

		// Creating the item makes gopcua read the node from a background
		// goroutine for the first notification, which is where the fault
		// fires with no request carrying it.
		deadline := time.Now().Add(5 * time.Second)
		for !f.Fired() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if !f.Fired() {
			t.Fatal("the monitored item never fired the fault, so this scenario proved nothing")
		}

		// The invariant, with no arm-time guard able to reach it: a fault
		// that fired must have closed something. Before the guard existed
		// this read fired quietly and the session lived on.
		if srv.Cuts() == 0 {
			t.Errorf("DropSession reported fired %d times while closing nothing: Cuts() == 0 and the session lives", f.Count())
		}
		return
	}

	// The parent: the child must die refusing, naming the connection it
	// could not find.
	cmd := exec.Command(os.Args[0], "-test.run", "^TestDropSession_RefusesToFireWithoutAConnectionToCut$", "-test.count=1")
	cmd.Env = append(os.Environ(), dropCaseEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the fault fired with no connection to cut and nobody failed: the scenario ran to completion; output:\n%s", out)
	}
	if !strings.Contains(string(out), "no connection to cut") {
		t.Errorf("the child failed (%v) but not by refusing to fire without a connection; output:\n%s", err, out)
	}
}
