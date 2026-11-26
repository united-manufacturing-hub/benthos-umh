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
	p := &proxy{}
	c := &blockingConn{started: make(chan struct{}), release: make(chan struct{})}
	p.conns = []net.Conn{c}

	cutDone := make(chan int, 1)
	go func() { cutDone <- p.cut() }()

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

// The count accumulates across cuts and survives a connection whose Close
// fails, which is the property that makes it evidence rather than an
// intention: a cut that closed nothing must report nothing.
func TestProxy_TheCountRecordsClosesThatSucceeded(t *testing.T) {
	p := &proxy{}
	if n := p.cutCount(); n != 0 {
		t.Fatalf("a proxy that has cut nothing reports %d", n)
	}

	ok1, ok2 := &fakeConn{}, &fakeConn{}
	bad := &fakeConn{closeErr: net.ErrClosed}
	p.conns = []net.Conn{ok1, bad, ok2}
	if n := p.cut(); n != 2 {
		t.Fatalf("cut reported %d, want 2 (the failing close is not evidence)", n)
	}
	if n := p.cutCount(); n != 2 {
		t.Fatalf("cutCount reported %d, want 2", n)
	}

	p.conns = []net.Conn{&fakeConn{}}
	_ = p.cut()
	if n := p.cutCount(); n != 3 {
		t.Fatalf("cutCount reported %d after a second cut, want 3", n)
	}
}

type fakeConn struct {
	net.Conn
	closeErr error
}

func (c *fakeConn) Close() error { return c.closeErr }
