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

package open_protocol_plugin

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// scriptedConn is a fault-injecting net.Conn whose Write fails the first
// failWrites calls, then succeeds. Successful writes are counted so a test can
// assert exactly how many telegrams reached the wire.
type scriptedConn struct {
	mu         sync.Mutex
	failWrites int // remaining writes that should fail
	successes  int // number of successful writes recorded
}

func (c *scriptedConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failWrites > 0 {
		c.failWrites--
		return 0, fmt.Errorf("scriptedConn: injected write failure")
	}
	c.successes++
	return len(b), nil
}

func (c *scriptedConn) successCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.successes
}

func (c *scriptedConn) Read(_ []byte) (int, error)       { return 0, nil }
func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return nil }
func (c *scriptedConn) RemoteAddr() net.Addr             { return nil }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

// TestMakeAckRetriesAfterFailedWrite proves the idempotency guard latches only
// on a SUCCESSFUL 0062 write: a transient write failure must NOT suppress a
// later retry. This fails against the old sync.Once implementation (which
// latched even on failure) and passes against the sent-bool implementation.
func TestMakeAckRetriesAfterFailedWrite(t *testing.T) {
	s := NewSession(SessionConfig{}, nil)
	s.mu.Lock()
	s.generation = 5
	s.mu.Unlock()

	conn := &scriptedConn{failWrites: 1} // first Write fails, then succeeds
	ack := s.makeAck(5, conn, MIDLastTighteningAck, true)

	// First call: the write fails, so nothing is recorded and the guard must
	// NOT latch.
	ack()
	if got := conn.successCount(); got != 0 {
		t.Fatalf("after failed write, want 0 successful writes, got %d", got)
	}

	// Second call: the failure did not latch, so it retries and succeeds.
	ack()
	if got := conn.successCount(); got != 1 {
		t.Fatalf("after retry, want exactly 1 successful write, got %d", got)
	}

	// Third call: now latched on the successful send; must not re-send.
	ack()
	if got := conn.successCount(); got != 1 {
		t.Fatalf("after latch, want still exactly 1 successful write, got %d", got)
	}
}

// TestMakeAckSuppressesStaleGenerationOnWritableConn proves the stale-ack
// suppression is driven by the GENERATION check, not by a dead connection: the
// conn is perfectly writable, yet a stale-generation ack records zero writes.
func TestMakeAckSuppressesStaleGenerationOnWritableConn(t *testing.T) {
	s := NewSession(SessionConfig{}, nil)
	s.mu.Lock()
	s.generation = 1
	s.mu.Unlock()

	conn := &scriptedConn{} // always writable
	ack := s.makeAck(1, conn, MIDLastTighteningAck, true)

	// Bump generation so the ack closure is now stale.
	s.mu.Lock()
	s.generation = 2
	s.mu.Unlock()

	ack()
	if got := conn.successCount(); got != 0 {
		t.Fatalf("stale-generation ack on a writable conn should write nothing, got %d writes", got)
	}
}

// --- FIX 5: ack contract at the ReadBatch boundary ---

// ackTestController is a minimal in-process Open Protocol controller used by the
// ReadBatch-boundary tests. It accepts login, confirms a last_tightening
// subscription with MID 0005, pushes one rev-1 MID 0061, and records every MID
// it receives (so the test can count 0062 acks).
type ackTestController struct {
	ln net.Listener

	mu       sync.Mutex
	received []int
}

func newAckTestController(t *testing.T, result []byte) *ackTestController {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	c := &ackTestController{ln: ln}
	go c.serve(result)
	return c
}

func (c *ackTestController) addr() string { return c.ln.Addr().String() }
func (c *ackTestController) close()       { _ = c.ln.Close() }

func (c *ackTestController) countMID(mid int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.received {
		if m == mid {
			n++
		}
	}
	return n
}

func (c *ackTestController) serve(result []byte) {
	conn, err := c.ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	fr := NewFrameReader(conn)
	for {
		frame, err := fr.ReadFrame()
		if err != nil {
			return
		}
		tel, err := ParseTelegram(frame)
		if err != nil {
			return
		}
		c.mu.Lock()
		c.received = append(c.received, tel.Header.MID)
		c.mu.Unlock()
		switch tel.Header.MID {
		case MIDCommunicationStart:
			ack := "01" + "0001" + "02" + "01" + "03" + "UMHTestSim" + "               " // padded controller name
			_, _ = conn.Write(BuildMessage(MIDCommunicationStartAck, 1, []byte(ack)))
		case MIDLastTighteningSub:
			_, _ = conn.Write(BuildMessage(MIDCommandAccepted, 1, nil))
			_, _ = conn.Write(BuildMessage(MIDLastTightening, 1, result))
		}
	}
}

func waitForCond(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// build0061RevisionOneData returns a valid MID 0061 rev-1 (last tightening)
// parameter-ID data field, mirroring the spec/emulator layout.
func build0061RevisionOneData() []byte {
	pad := func(s string, n int) string {
		for len(s) < n {
			s += " "
		}
		return s[:n]
	}
	fields := "" +
		"01" + "0001" +
		"02" + "01" +
		"03" + pad("UMHTestSim", 25) +
		"04" + pad("VIN123", 25) +
		"05" + "00" +
		"06" + "001" +
		"07" + "0005" +
		"08" + "0002" +
		"09" + "1" +
		"10" + "1" +
		"11" + "1" +
		"12" + "004700" +
		"13" + "005300" +
		"14" + "005000" +
		"15" + "005012" +
		"16" + "00080" +
		"17" + "00100" +
		"18" + "00090" +
		"19" + "00092" +
		"20" + "2026-06-01:12:00:00" +
		"21" + "2026-06-01:11:00:00" +
		"22" + "0" +
		"23" + "0007"
	return []byte(fields)
}

// newInputForTest constructs a real openProtocolInput pointed at endpoint by
// parsing a YAML config through the production config spec, exercising the same
// ReadBatch/AckFunc path Benthos drives in production.
func newInputForTest(t *testing.T, endpoint string) *openProtocolInput {
	t.Helper()
	yaml := fmt.Sprintf(`
endpoint: "%s"
subscribe: [last_tightening]
keepalive_interval: 50ms
request_timeout: 2s
`, endpoint)
	conf, err := configSpec().ParseYAML(yaml, nil)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	in, err := newOpenProtocolInput(conf, service.MockResources())
	if err != nil {
		t.Fatalf("new input: %v", err)
	}
	return in
}

// readUntilAck drives ReadBatch until it returns a batch (the rev-1 0061
// fan-out), returning the batch's AckFunc.
func readUntilAck(t *testing.T, in *openProtocolInput) service.AckFunc {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		batch, ackFn, err := in.ReadBatch(ctx)
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		if len(batch) > 0 {
			return ackFn
		}
	}
}

// TestReadBatchOneBatchOneAck proves the real ReadBatch AckFunc sends EXACTLY
// one MID 0062 for one tightening result (not one per fanned-out message).
func TestReadBatchOneBatchOneAck(t *testing.T) {
	fc := newAckTestController(t, build0061RevisionOneData())
	defer fc.close()

	in := newInputForTest(t, fc.addr())
	if err := in.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer in.Close(context.Background())

	ackFn := readUntilAck(t, in)
	if err := ackFn(context.Background(), nil); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if !waitForCond(2*time.Second, func() bool { return fc.countMID(MIDLastTighteningAck) == 1 }) {
		t.Fatalf("want exactly 1 MID 0062, got %d", fc.countMID(MIDLastTighteningAck))
	}
}

// TestReadBatchNoAckOnDownstreamFailure proves the AckFunc withholds the 0062
// when downstream delivery fails (non-nil error), then sends exactly one when
// later called with nil (controller re-push / retry semantics).
func TestReadBatchNoAckOnDownstreamFailure(t *testing.T) {
	fc := newAckTestController(t, build0061RevisionOneData())
	defer fc.close()

	in := newInputForTest(t, fc.addr())
	if err := in.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer in.Close(context.Background())

	ackFn := readUntilAck(t, in)

	// Downstream delivery failed: no 0062 must be sent.
	if err := ackFn(context.Background(), fmt.Errorf("downstream boom")); err != nil {
		t.Fatalf("ack(err): %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := fc.countMID(MIDLastTighteningAck); got != 0 {
		t.Fatalf("on downstream failure want 0 MID 0062, got %d", got)
	}

	// Retry succeeds: exactly one 0062 now.
	if err := ackFn(context.Background(), nil); err != nil {
		t.Fatalf("ack(nil): %v", err)
	}
	if !waitForCond(2*time.Second, func() bool { return fc.countMID(MIDLastTighteningAck) == 1 }) {
		t.Fatalf("after successful retry want exactly 1 MID 0062, got %d", fc.countMID(MIDLastTighteningAck))
	}
}
