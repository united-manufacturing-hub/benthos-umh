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
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"

	"github.com/united-manufacturing-hub/benthos-umh/opcua_plugin/internal/faultopcua"
)

// A Ginkgo spec builds its server with GinkgoT(), so the interface New takes
// must be one GinkgoT() satisfies, beside *testing.T.
var (
	_ faultopcua.TB = (*testing.T)(nil)
	_ faultopcua.TB = ginkgo.FullGinkgoTInterface(nil)
)

// gopcuaLoops are the goroutines gopcua's server starts in Start and stops
// only when the context it was started with ends.
var gopcuaLoops = []string{
	"opcua/server.(*Server).monitorConnections",
	"opcua/server.(*Server).acceptAndRegister",
}

// countGopcuaLoops counts the goroutines running one of gopcuaLoops.
func countGopcuaLoops() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		for _, fn := range gopcuaLoops {
			if strings.Contains(g, fn) {
				count++
				break
			}
		}
	}
	return count
}

// TestClose_StopsTheServersGoroutines pins that a closed server leaves nothing
// running. gopcua's server runs its accept and service loops until the context
// handed to Start ends, and its own Close does not end it, so a server that
// did not cancel that context would leave both loops behind for the rest of
// the test binary, one pair per test.
func TestClose_StopsTheServersGoroutines(t *testing.T) {
	// The servers of earlier tests have been closed but may still be on
	// their way out, so settle first and count only what this one adds.
	settle := time.Now().Add(5 * time.Second)
	for countGopcuaLoops() > 0 && time.Now().Before(settle) {
		time.Sleep(10 * time.Millisecond)
	}
	before := countGopcuaLoops()

	// A client is connected, so the service loop has served requests and
	// the shutdown has a live channel to tear down.
	srv, client, v := startServer(t, "Flag", true)
	if got := readValue(t, client, nodeID(t, v)); got != true {
		t.Fatalf("read back %v, want true", got)
	}
	if running := countGopcuaLoops() - before; running < len(gopcuaLoops) {
		t.Fatalf("%d of gopcua's server loops are running after Start, want %d: this test cannot see them, so it proves nothing",
			running, len(gopcuaLoops))
	}

	_ = srv.Close()

	deadline := time.Now().Add(5 * time.Second)
	for countGopcuaLoops() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if left := countGopcuaLoops() - before; left > 0 {
		t.Errorf("%d of gopcua's server loops are still running 5s after Close: the server's context was never cancelled", left)
	}
}
