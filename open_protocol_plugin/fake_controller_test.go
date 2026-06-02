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

package open_protocol_plugin_test

import (
	"net"
	"sync"
	"time"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

// fakeController is an in-process Open Protocol controller (TCP server) used to
// drive the session layer in unit tests. It listens on a loopback port and runs
// a per-connection handler script. Every telegram it receives is recorded so
// tests can assert on the MIDs the session sent (login, subscribe, ack,
// keep-alive). It reuses the production FrameReader/BuildMessage so the test
// exercises the real wire format.
type fakeController struct {
	ln net.Listener

	mu       sync.Mutex
	received []op.Header
	connObs  int // number of accepted connections

	// perConnReceived tracks received MIDs broken down by connection index (1-based).
	perConnReceived map[int][]int

	handler func(fc *fakeController, conn net.Conn)
}

// newFakeController starts a listener on 127.0.0.1:0 and serves connections
// with handler until closed.
func newFakeController(handler func(fc *fakeController, conn net.Conn)) (*fakeController, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	fc := &fakeController{
		ln:              ln,
		handler:         handler,
		perConnReceived: make(map[int][]int),
	}
	go fc.acceptLoop()
	return fc, nil
}

func (fc *fakeController) acceptLoop() {
	for {
		conn, err := fc.ln.Accept()
		if err != nil {
			return // listener closed
		}
		fc.mu.Lock()
		fc.connObs++
		connIdx := fc.connObs
		fc.mu.Unlock()
		go fc.handler(fc, connWithIndex{Conn: conn, fc: fc, idx: connIdx})
	}
}

func (fc *fakeController) addr() string { return fc.ln.Addr().String() }

func (fc *fakeController) close() { _ = fc.ln.Close() }

// record appends a received header (thread-safe).
func (fc *fakeController) record(h op.Header, connIdx int) {
	fc.mu.Lock()
	fc.received = append(fc.received, h)
	fc.perConnReceived[connIdx] = append(fc.perConnReceived[connIdx], h.MID)
	fc.mu.Unlock()
}

// receivedMIDs returns the MIDs received so far, in order.
func (fc *fakeController) receivedMIDs() []int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	out := make([]int, len(fc.received))
	for i, h := range fc.received {
		out[i] = h.MID
	}
	return out
}

// countMID returns how many telegrams with the given MID have been received.
func (fc *fakeController) countMID(mid int) int {
	n := 0
	for _, m := range fc.receivedMIDs() {
		if m == mid {
			n++
		}
	}
	return n
}

// firstReceivedHeader returns the first Header recorded for the given MID, and
// whether any such header was found. Used by tests that need to inspect fields
// (e.g. Revision) of a specific telegram type sent by the client.
func (fc *fakeController) firstReceivedHeader(mid int) (op.Header, bool) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for _, h := range fc.received {
		if h.MID == mid {
			return h, true
		}
	}
	return op.Header{}, false
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// connWithIndex wraps a net.Conn so the handler can call readTelegram and have
// the telegram recorded against the right connection index.
type connWithIndex struct {
	net.Conn
	fc  *fakeController
	idx int
}

// --- handler building blocks ---

// readTelegram reads and records one telegram from the connection.
func (fc *fakeController) readTelegram(fr *op.FrameReader) (op.Telegram, error) {
	frame, err := fr.ReadFrame()
	if err != nil {
		return op.Telegram{}, err
	}
	tel, err := op.ParseTelegram(frame)
	if err != nil {
		return op.Telegram{}, err
	}
	// When called from a connWithIndex handler the fc.record call happens inside
	// the typed handler below; when called directly (legacy handlers), use
	// connIdx=0 as a sentinel.
	fc.record(tel.Header, 0)
	return tel, nil
}

// readTelegramConn reads and records one telegram, tagging it with connIdx.
func (fc *fakeController) readTelegramConn(fr *op.FrameReader, connIdx int) (op.Telegram, error) {
	frame, err := fr.ReadFrame()
	if err != nil {
		return op.Telegram{}, err
	}
	tel, err := op.ParseTelegram(frame)
	if err != nil {
		return op.Telegram{}, err
	}
	fc.record(tel.Header, connIdx)
	return tel, nil
}

func sendFrame(conn net.Conn, mid, rev int, data []byte) error {
	_, err := conn.Write(op.BuildMessage(mid, rev, data))
	return err
}

// goodHandler is the default cooperative controller: it accepts login, replies
// MID 0002, and on a last-tightening/alarm subscription (MID 0060/0070) first
// replies MID 0005 (command accepted) to confirm the subscription, then pushes
// the supplied MID 0061 results. The session's Connect handshake expects this
// confirmation.
func goodHandler(results [][]byte) func(fc *fakeController, conn net.Conn) {
	return func(fc *fakeController, conn net.Conn) {
		defer conn.Close()
		// Unwrap connWithIndex if present so we can get the connIdx for recording.
		var connIdx int
		if cwi, ok := conn.(connWithIndex); ok {
			connIdx = cwi.idx
		}
		fr := op.NewFrameReader(conn)
		for {
			var tel op.Telegram
			var err error
			if connIdx != 0 {
				tel, err = fc.readTelegramConn(fr, connIdx)
			} else {
				tel, err = fc.readTelegram(fr)
			}
			if err != nil {
				return
			}
			switch tel.Header.MID {
			case op.MIDCommunicationStart:
				ack := "01" + "0001" + "02" + "01" + "03" + padRight("UMHTestSim", 25)
				_ = sendFrame(conn, op.MIDCommunicationStartAck, 1, []byte(ack))
			case op.MIDLastTighteningSub:
				// Confirm subscription first (MID 0005), then push results.
				_ = sendFrame(conn, op.MIDCommandAccepted, 1, nil)
				for _, r := range results {
					_ = sendFrame(conn, op.MIDLastTightening, 1, r)
				}
			case op.MIDAlarmSub:
				// Confirm subscription first (MID 0005).
				_ = sendFrame(conn, op.MIDCommandAccepted, 1, nil)
			case op.MIDKeepAlive, op.MIDLastTighteningAck, op.MIDAlarmAck:
				// accepted / ignored
			}
		}
	}
}

// goodHandlerPerConn is like goodHandler but only sends results on the FIRST
// connection, so we can test stale ack behaviour on the second connection.
// It tracks which connection is which by connection index.
func goodHandlerPerConn(firstConnResults [][]byte) func(fc *fakeController, conn net.Conn) {
	return func(fc *fakeController, conn net.Conn) {
		defer conn.Close()
		var connIdx int
		if cwi, ok := conn.(connWithIndex); ok {
			connIdx = cwi.idx
		}
		fr := op.NewFrameReader(conn)
		for {
			var tel op.Telegram
			var err error
			if connIdx != 0 {
				tel, err = fc.readTelegramConn(fr, connIdx)
			} else {
				tel, err = fc.readTelegram(fr)
			}
			if err != nil {
				return
			}
			switch tel.Header.MID {
			case op.MIDCommunicationStart:
				ack := "01" + "0001" + "02" + "01" + "03" + padRight("UMHTestSim", 25)
				_ = sendFrame(conn, op.MIDCommunicationStartAck, 1, []byte(ack))
			case op.MIDLastTighteningSub:
				// Always confirm subscription with MID 0005.
				_ = sendFrame(conn, op.MIDCommandAccepted, 1, nil)
				// Only push results on the first connection.
				if connIdx == 1 {
					for _, r := range firstConnResults {
						_ = sendFrame(conn, op.MIDLastTightening, 1, r)
					}
				}
			case op.MIDAlarmSub:
				_ = sendFrame(conn, op.MIDCommandAccepted, 1, nil)
			case op.MIDKeepAlive, op.MIDLastTighteningAck, op.MIDAlarmAck:
				// accepted / ignored
			}
		}
	}
}
