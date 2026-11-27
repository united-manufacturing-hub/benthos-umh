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
	"net"
	"testing"
	"time"
)

// blockingConn stands still inside its own Close, which is where the race
// lives: the socket is dying, the client already knows, and the server has
// not yet written down that it did it.
type blockingConn struct {
	net.Conn
	started chan struct{}
	release chan struct{}
}

func (c *blockingConn) Close() error {
	close(c.started)
	<-c.release
	return nil
}

// A DropSession test learns that the drop happened from the client, whose
// write fails the instant the socket closes, and then asks the server for
// its evidence. Those two facts travel by different routes, so the count
// has to be settled before the close is observable -- otherwise the test
// reads zero and reports that nothing was closed, which is a lie about a
// connection that WAS closed. Measured on the failing shape: the conn
// closed at 14:54:03.287670, the count landed at .287675, and the assertion
// ran in between, four times in fourteen runs.
func TestProxy_TheCountIsSettledBeforeACloseIsObservable(t *testing.T) {
	p := &proxy{pairs: map[int]*connPair{}}
	c := &blockingConn{started: make(chan struct{}), release: make(chan struct{})}
	p.pairs[2048] = &connPair{client: c, backend: &fakeConn{}}

	cutDone := make(chan int, 1)
	go func() { cutDone <- p.cutPort(2048) }()

	<-c.started // the socket is dying; a client would see EOF about now

	read := make(chan int, 1)
	go func() { read <- p.cutCount() }()

	select {
	case n := <-read:
		t.Fatalf("cutCount returned %d while the close that it must count was still in flight: a test asking here is told nothing was closed", n)
	case <-time.After(100 * time.Millisecond):
		// Correct: the reader waits for the cut to finish.
	}

	close(c.release)
	if n := <-cutDone; n != 1 {
		t.Fatalf("cut closed %d connections, want 1", n)
	}
	if n := <-read; n != 1 {
		t.Fatalf("cutCount reported %d after the cut finished, want 1", n)
	}
}

// The count accumulates across cuts, counts only the socket underneath the
// client, and survives a connection whose Close fails, which is the property
// that makes it evidence rather than an intention: a cut that closed nothing
// must report nothing.
func TestProxy_TheCountRecordsClosesThatSucceeded(t *testing.T) {
	p := &proxy{pairs: map[int]*connPair{}}
	if n := p.cutCount(); n != 0 {
		t.Fatalf("a proxy that has cut nothing reports %d", n)
	}

	// A client socket that will not close is not evidence.
	p.pairs[1] = &connPair{client: &fakeConn{closeErr: net.ErrClosed}, backend: &fakeConn{}}
	if n := p.cutPort(1); n != 0 {
		t.Fatalf("cut reported %d, want 0 (the failing close is not evidence)", n)
	}
	if n := p.cutCount(); n != 0 {
		t.Fatalf("cutCount reported %d, want 0", n)
	}

	// A healthy pair counts once, and the count accumulates.
	p.pairs[2] = &connPair{client: &fakeConn{}, backend: &fakeConn{}}
	if n := p.cutPort(2); n != 1 {
		t.Fatalf("cut reported %d, want 1", n)
	}
	if n := p.cutCount(); n != 1 {
		t.Fatalf("cutCount reported %d, want 1", n)
	}

	// Cutting a port nobody arrived on closes nothing.
	if n := p.cutPort(3); n != 0 {
		t.Fatalf("cut reported %d for a port with no connection, want 0", n)
	}
	if n := p.cutCount(); n != 1 {
		t.Fatalf("cutCount reported %d after cutting a dead port, want 1", n)
	}
}

// Shutting the proxy down tears its connections away but counts nothing,
// because a server closing is not a session drop: Cuts is evidence about a
// fault, and closing the server is not a fault. Until the rewrite, Close ran
// the same cut a drop runs, and every teardown with a live client
// manufactured drop evidence.
func TestProxy_ShuttingDownIsNotACut(t *testing.T) {
	p := &proxy{pairs: map[int]*connPair{}}
	p.pairs[1] = &connPair{client: &fakeConn{}, backend: &fakeConn{}}

	p.shutdown()

	if n := p.cutCount(); n != 0 {
		t.Fatalf("shutdown counted %d cuts, want 0: a server closing is not a session drop", n)
	}
	// And the connection really was torn down, so nothing leaks past the
	// shutdown and the proxy stops tracking it.
	p.mu.Lock()
	remaining := len(p.pairs)
	done := p.done
	p.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("%d connections were still tracked after shutdown", remaining)
	}
	if !done {
		t.Fatal("shutdown left the proxy accepting connections")
	}
}

type fakeConn struct {
	net.Conn
	closeErr error
}

func (c *fakeConn) Close() error { return c.closeErr }
