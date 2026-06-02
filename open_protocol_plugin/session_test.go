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
	"context"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

// keepAliveFloodHandler sends a burst of MID 9999 keep-alives after accepting
// the TCP connection, then never sends MID 0002 (login accept). This simulates
// a buggy controller that floods keep-alives during the login window.
func keepAliveFloodHandler(burstSize int) func(fc *fakeController, conn net.Conn) {
	return func(fc *fakeController, conn net.Conn) {
		defer conn.Close()
		for i := 0; i < burstSize; i++ {
			if err := sendFrame(conn, op.MIDKeepAlive, 1, nil); err != nil {
				return
			}
		}
		// Never send login ack — block until the connection is torn down.
		buf := make([]byte, 1)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}
}

func newTestSession(endpoint string, subs []string) *op.Session {
	return op.NewSession(op.SessionConfig{
		Endpoint:          endpoint,
		Subscriptions:     subs,
		Revision:          1,
		KeepAliveInterval: 50 * time.Millisecond,
		RequestTimeout:    2 * time.Second,
		ReadTimeout:       2 * time.Second,
	}, nil) // nil logger -> no-op
}

func newTestSessionWithReadTimeout(endpoint string, subs []string, readTimeout time.Duration) *op.Session {
	return op.NewSession(op.SessionConfig{
		Endpoint:          endpoint,
		Subscriptions:     subs,
		Revision:          1,
		KeepAliveInterval: 50 * time.Millisecond,
		RequestTimeout:    2 * time.Second,
		ReadTimeout:       readTimeout,
	}, nil)
}

var _ = Describe("Session FSM", func() {
	var ctx context.Context
	var cancel context.CancelFunc

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
	})
	AfterEach(func() {
		cancel()
	})

	It("logs in, subscribes, receives a pushed result and acknowledges it", func() {
		result := []byte(build0061Data(true))
		fc, err := newFakeController(goodHandler([][]byte{result}))
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})
		Expect(s.Connect(ctx)).To(Succeed())
		defer s.Close()

		// Read the result.
		res, err := s.Read(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Telegram.Header.MID).To(Equal(op.MIDLastTightening))

		lt, err := op.ParseLastTightening(res.Telegram)
		Expect(err).NotTo(HaveOccurred())
		Expect(lt.TighteningOK).To(BeTrue())

		// Ack it — the controller must see MID 0062.
		res.Ack()

		// The controller must have seen login, subscribe and an ack.
		Expect(waitFor(2*time.Second, func() bool {
			return fc.countMID(op.MIDLastTighteningAck) >= 1
		})).To(BeTrue(), "expected a MID 0062 ack")
		Expect(fc.receivedMIDs()).To(ContainElement(op.MIDCommunicationStart))
		Expect(fc.receivedMIDs()).To(ContainElement(op.MIDLastTighteningSub))
	})

	It("returns an error from Connect when the controller rejects login (MID 0004)", func() {
		fc, err := newFakeController(func(fc *fakeController, conn net.Conn) {
			defer conn.Close()
			fr := op.NewFrameReader(conn)
			tel, err := fc.readTelegram(fr)
			if err != nil {
				return
			}
			if tel.Header.MID == op.MIDCommunicationStart {
				_ = sendFrame(conn, op.MIDCommandError, 1, []byte("000197")) // unsupported revision
			}
		})
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})
		err = s.Connect(ctx)
		Expect(err).To(HaveOccurred())
	})

	It("sends keep-alive telegrams (MID 9999) on the configured interval", func() {
		fc, err := newFakeController(goodHandler(nil))
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})
		Expect(s.Connect(ctx)).To(Succeed())
		defer s.Close()

		Expect(waitFor(2*time.Second, func() bool {
			return fc.countMID(op.MIDKeepAlive) >= 2
		})).To(BeTrue(), "expected at least two keep-alive telegrams")
	})

	// New tests required by the spec:

	It("connects and bumps generation", func() {
		fc, err := newFakeController(goodHandler(nil))
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})

		Expect(s.Connect(ctx)).To(Succeed())
		Expect(s.Generation()).To(Equal(uint64(1)))

		Expect(s.Connect(ctx)).To(Succeed())
		Expect(s.Generation()).To(Equal(uint64(2)))

		_ = s.Close()
	})

	It("sends MID 0062 only after Ack() (not during Read)", func() {
		result := []byte(build0061Data(true))
		fc, err := newFakeController(goodHandler([][]byte{result}))
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})
		Expect(s.Connect(ctx)).To(Succeed())
		defer s.Close()

		res, err := s.Read(ctx)
		Expect(err).NotTo(HaveOccurred())

		// No ack yet for 200ms.
		Consistently(func() int {
			return fc.countMID(op.MIDLastTighteningAck)
		}, 200*time.Millisecond, 10*time.Millisecond).Should(Equal(0))

		// Now ack.
		res.Ack()

		// Ack must arrive.
		Eventually(func() int {
			return fc.countMID(op.MIDLastTighteningAck)
		}, 2*time.Second, 10*time.Millisecond).Should(Equal(1))
	})

	It("at most one 0062 even if Ack() called twice", func() {
		result := []byte(build0061Data(true))
		fc, err := newFakeController(goodHandler([][]byte{result}))
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})
		Expect(s.Connect(ctx)).To(Succeed())
		defer s.Close()

		res, err := s.Read(ctx)
		Expect(err).NotTo(HaveOccurred())

		res.Ack()
		res.Ack()

		// Allow time for any duplicate to arrive.
		time.Sleep(200 * time.Millisecond)
		Expect(fc.countMID(op.MIDLastTighteningAck)).To(Equal(1))
	})

	It("stale Ack() after re-Connect is a no-op on the new connection", func() {
		result := []byte(build0061Data(true))

		// First connection: serves a result, then we re-Connect.
		// Second connection: goodHandler with no results — we just check it
		// receives NO 0062.
		fc, err := newFakeController(goodHandlerPerConn([][]byte{result}))
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})

		// Gen 1 connect + read.
		Expect(s.Connect(ctx)).To(Succeed())
		res, err := s.Read(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Telegram.Header.MID).To(Equal(op.MIDLastTightening))

		// Re-connect (gen 2). The old result's Ack closure is now stale.
		Expect(s.Connect(ctx)).To(Succeed())
		defer s.Close()

		// Count 0062 received on all connections before the stale ack.
		before := fc.countMID(op.MIDLastTighteningAck)

		// Fire the stale ack — must be a no-op (goes to nobody or dropped).
		res.Ack()

		// Give it time to potentially propagate.
		time.Sleep(200 * time.Millisecond)

		// The NEW connection (connection 2) must not have received a 0062
		// attributable to the stale ack. Total count must not increase.
		Expect(fc.countMID(op.MIDLastTighteningAck)).To(Equal(before))
	})

	It("fails Connect when subscribe is rejected (MID 0004)", func() {
		fc, err := newFakeController(func(fc *fakeController, conn net.Conn) {
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
					// Reject the subscription.
					_ = sendFrame(conn, op.MIDCommandError, 1, []byte("006097"))
				}
			}
		})
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})
		err = s.Connect(ctx)
		Expect(err).To(HaveOccurred())
	})

	It("sends the login telegram (MID 0001) with Revision == 1 on the wire", func() {
		// This kills the mutant that sends the wrong revision in connectAndHandshake.
		// newTestSession sets Revision: 1; goodHandler records every received header
		// in fc.received so we can inspect the exact revision carried on the wire.
		fc, err := newFakeController(goodHandler(nil))
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})
		Expect(s.Connect(ctx)).To(Succeed())
		defer s.Close()

		Expect(waitFor(2*time.Second, func() bool {
			_, ok := fc.firstReceivedHeader(op.MIDCommunicationStart)
			return ok
		})).To(BeTrue(), "expected to receive a login telegram")

		h, ok := fc.firstReceivedHeader(op.MIDCommunicationStart)
		Expect(ok).To(BeTrue(), "MIDCommunicationStart header not found")
		Expect(h.Revision).To(Equal(1), "login telegram must carry Revision == 1 on the wire")
	})

	// FIX 2 — ctx cancellation during a keep-alive flood in the login window.
	//
	// A controller that sends many MID 9999 keep-alives before ever sending MID
	// 0002 must not hold Connect open past ctx cancellation. Without the fix the
	// login loop would iterate burstSize times × RequestTimeout before stopping.
	It("returns promptly when ctx is cancelled during a login keep-alive flood", func() {
		const burstSize = 20
		// Use a request_timeout that would cause a noticeable hang if we iterated
		// through all keep-alives without checking ctx (burstSize × 200ms = 4s).
		fc, err := newFakeController(keepAliveFloodHandler(burstSize))
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		// session with a longer request_timeout to make the hang detectable.
		s := op.NewSession(op.SessionConfig{
			Endpoint:          fc.addr(),
			Subscriptions:     []string{"last_tightening"},
			Revision:          1,
			KeepAliveInterval: 50 * time.Millisecond,
			RequestTimeout:    200 * time.Millisecond,
			ReadTimeout:       2 * time.Second,
		}, nil)

		// Cancel the context after a short delay — well before the flood would exhaust.
		connectCtx, connectCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer connectCancel()

		start := time.Now()
		err = s.Connect(connectCtx)
		elapsed := time.Since(start)

		Expect(err).To(HaveOccurred(), "Connect must fail when ctx is cancelled")
		// Must return well under the full flood duration (burstSize × RequestTimeout = 4s).
		Expect(elapsed).To(BeNumerically("<", 2*time.Second),
			"Connect must return promptly after ctx cancellation, not wait through all keep-alives")
	})

	It("returns a Read error when no telegram arrives within read_timeout", func() {
		// Handler: accepts login+subscribe (replies 0005), then goes silent.
		fc, err := newFakeController(func(fc *fakeController, conn net.Conn) {
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
					// Confirm subscription with MID 0005.
					_ = sendFrame(conn, op.MIDCommandAccepted, 1, nil)
					// Then go silent (no more writes).
					// Block forever until the connection is closed.
					for {
						if _, err := fr.ReadFrame(); err != nil {
							return
						}
					}
				}
			}
		})
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		// Small read timeout so the test doesn't hang.
		s := newTestSessionWithReadTimeout(fc.addr(), []string{"last_tightening"}, 150*time.Millisecond)
		Expect(s.Connect(ctx)).To(Succeed())
		defer s.Close()

		_, err = s.Read(ctx)
		Expect(err).To(HaveOccurred())
	})
})
