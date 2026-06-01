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

	handler func(fc *fakeController, conn net.Conn)
}

// newFakeController starts a listener on 127.0.0.1:0 and serves connections
// with handler until closed.
func newFakeController(handler func(fc *fakeController, conn net.Conn)) (*fakeController, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	fc := &fakeController{ln: ln, handler: handler}
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
		fc.mu.Unlock()
		go fc.handler(fc, conn)
	}
}

func (fc *fakeController) addr() string { return fc.ln.Addr().String() }

func (fc *fakeController) close() { _ = fc.ln.Close() }

// record appends a received header (thread-safe).
func (fc *fakeController) record(h op.Header) {
	fc.mu.Lock()
	fc.received = append(fc.received, h)
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

// connectionCount returns the number of connections accepted so far.
func (fc *fakeController) connectionCount() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.connObs
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
	fc.record(tel.Header)
	return tel, nil
}

func sendFrame(conn net.Conn, mid, rev int, data []byte) error {
	_, err := conn.Write(op.BuildMessage(mid, rev, data))
	return err
}

// goodHandler is the default cooperative controller: it accepts login, replies
// MID 0002, and on a last-tightening subscription (MID 0060) pushes the
// supplied MID 0061 results, expecting a MID 0062 ack between each.
func goodHandler(results [][]byte) func(fc *fakeController, conn net.Conn) {
	return func(fc *fakeController, conn net.Conn) {
		defer conn.Close()
		fr := op.NewFrameReader(conn)
		for {
			tel, err := fc.readTelegram(fr)
			if err != nil {
				return
			}
			switch tel.Header.MID {
			case op.MIDCommunicationStart:
				ack := "01" + "0001" + "02" + "01" + "03" + padRight("UMHTestSim", 25)
				_ = sendFrame(conn, op.MIDCommunicationStartAck, 1, []byte(ack))
			case op.MIDLastTighteningSub:
				for _, r := range results {
					_ = sendFrame(conn, op.MIDLastTightening, 1, r)
				}
			case op.MIDAlarmSub, op.MIDKeepAlive, op.MIDLastTighteningAck, op.MIDAlarmAck:
				// accepted / ignored
			}
		}
	}
}
