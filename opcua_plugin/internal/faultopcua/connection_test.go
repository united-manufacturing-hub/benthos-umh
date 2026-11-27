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

package faultopcua

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
)

// recordingTB is a TB that keeps what the server reports instead of failing,
// so a test can assert that the server refused something.
type recordingTB struct {
	*testing.T

	mu     sync.Mutex
	errors []string
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func (r *recordingTB) reported() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.errors...)
}

// dropRecorder stands in for the proxy: it records the port of every cut and
// closes one connection for any port a client could have arrived on.
type dropRecorder struct {
	mu    sync.Mutex
	ports []int
}

func (d *dropRecorder) cut(port int) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ports = append(d.ports, port)
	if port == noConnection {
		return 0
	}
	return 1
}

func (d *dropRecorder) cuts() []int {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := append([]int(nil), d.ports...)
	sort.Ints(out)
	return out
}

// TestDropSession_ABackgroundReadCutsNoOnesConnection pins that a monitored
// item's read, which gopcua makes with no request behind it, never takes the
// connection of a request that happens to be in flight at the same moment.
//
// The service loop is held inside a read from the connection on port 4242,
// and a monitored item's read meets DropSession meanwhile. Had the two shared
// one record of whose request is running, the background read would cut the
// connection on 4242: a client that never touched the dropped node.
func TestDropSession_ABackgroundReadCutsNoOnesConnection(t *testing.T) {
	rec := &recordingTB{T: t}
	srv := New(rec)
	drops := &dropRecorder{}
	srv.ns.onDrop = drops.cut

	slow := srv.AddVariable("Slow", int32(1))
	doomed := srv.AddVariable("Doomed", int32(2))
	srv.AnswerLate(slow, 5*time.Second)
	f := srv.DropSession(doomed)

	served := make(chan struct{})
	go func() {
		defer close(served)
		srv.ns.read(&request{port: 4242}, slow.id, ua.AttributeIDValue)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for slow.Reads() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if slow.Reads() == 0 {
		t.Fatal("the request on port 4242 never reached its node, so nothing was in flight")
	}

	srv.ns.Attribute(doomed.id, ua.AttributeIDValue)

	_ = srv.Close() // lets the held read go
	<-served

	if !f.Fired() {
		t.Fatal("the background read never met the fault, so this test proved nothing")
	}
	if got := drops.cuts(); len(got) != 1 || got[0] != noConnection {
		t.Errorf("the background read cut the connections on ports %v, want only the empty port %d: "+
			"it took the connection of the request in flight", got, noConnection)
	}
	reported := rec.reported()
	if len(reported) != 1 || !strings.Contains(reported[0], "no connection to cut") {
		t.Errorf("the server reported %q, want one refusal to fire with no connection to cut", reported)
	}
}

// TestDropSession_ConcurrentDropsCutTheirOwnConnections pins that drops in
// flight at the same time each cut the connection their own request arrived
// on, however their steps interleave.
func TestDropSession_ConcurrentDropsCutTheirOwnConnections(t *testing.T) {
	rec := &recordingTB{T: t}
	srv := New(rec)
	drops := &dropRecorder{}
	srv.ns.onDrop = drops.cut

	const n = 32
	var want []int
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		port := 5000 + i
		want = append(want, port)
		v := srv.AddVariable(fmt.Sprintf("Doomed%d", i), int32(i))
		srv.DropSession(v)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				srv.ns.read(&request{port: port}, v.id, ua.AttributeIDValue)
			} else {
				srv.ns.write(&request{port: port}, v.id, ua.AttributeIDValue, dataValueOf(int32(-1)))
			}
		}()
	}
	close(start)
	wg.Wait()

	got := drops.cuts()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("the drops cut the connections on ports %v, want each its own, %v", got, want)
	}
	if reported := rec.reported(); len(reported) != 0 {
		t.Errorf("the server reported %q, want nothing: every drop had a connection to cut", reported)
	}
}

func dataValueOf(v any) *ua.DataValue {
	return &ua.DataValue{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(v)}
}
