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
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

func newTestSession(endpoint string, subs []string) *op.Session {
	return op.NewSession(op.SessionConfig{
		Endpoint:          endpoint,
		Subscriptions:     subs,
		Revision:          1,
		KeepAliveInterval: 50 * time.Millisecond,
		RequestTimeout:    2 * time.Second,
		MaxBackoff:        100 * time.Millisecond,
	}, nil) // nil logger -> no-op
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
		Expect(s.Start(ctx)).To(Succeed())
		defer s.Stop()

		// A telegram should be forwarded on the output channel.
		var tel op.Telegram
		Eventually(s.Telegrams(), 2*time.Second).Should(Receive(&tel))
		Expect(tel.Header.MID).To(Equal(op.MIDLastTightening))

		lt, err := op.ParseLastTightening(tel)
		Expect(err).NotTo(HaveOccurred())
		Expect(lt.TighteningOK).To(BeTrue())

		// The controller must have seen login, subscribe and an ack.
		Expect(waitFor(2*time.Second, func() bool {
			return fc.countMID(op.MIDLastTighteningAck) >= 1
		})).To(BeTrue(), "expected a MID 0062 ack")
		Expect(fc.receivedMIDs()).To(ContainElement(op.MIDCommunicationStart))
		Expect(fc.receivedMIDs()).To(ContainElement(op.MIDLastTighteningSub))
	})

	It("returns an error from Start when the controller rejects login (MID 0004)", func() {
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
		err = s.Start(ctx)
		Expect(err).To(HaveOccurred())
	})

	It("sends keep-alive telegrams (MID 9999) on the configured interval", func() {
		fc, err := newFakeController(goodHandler(nil))
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})
		Expect(s.Start(ctx)).To(Succeed())
		defer s.Stop()

		Expect(waitFor(2*time.Second, func() bool {
			return fc.countMID(op.MIDKeepAlive) >= 2
		})).To(BeTrue(), "expected at least two keep-alive telegrams")
	})

	It("reconnects and replays login + subscriptions after the connection drops", func() {
		var conns int32
		result := []byte(build0061Data(true))

		fc, err := newFakeController(func(fc *fakeController, conn net.Conn) {
			n := atomic.AddInt32(&conns, 1)
			fr := op.NewFrameReader(conn)
			for {
				tel, err := fc.readTelegram(fr)
				if err != nil {
					conn.Close()
					return
				}
				switch tel.Header.MID {
				case op.MIDCommunicationStart:
					ack := "01" + "0001" + "02" + "01" + "03" + padRight("UMHTestSim", 25)
					_ = sendFrame(conn, op.MIDCommunicationStartAck, 1, []byte(ack))
				case op.MIDLastTighteningSub:
					_ = sendFrame(conn, op.MIDLastTightening, 1, result)
					if n == 1 {
						// Drop the first connection right after the first result,
						// forcing the session to reconnect.
						conn.Close()
						return
					}
				}
			}
		})
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		s := newTestSession(fc.addr(), []string{"last_tightening"})
		Expect(s.Start(ctx)).To(Succeed())
		defer s.Stop()

		// Drain results; we should receive at least two across the reconnect.
		got := 0
		Eventually(func() int {
			select {
			case <-s.Telegrams():
				got++
			default:
			}
			return got
		}, 3*time.Second, 10*time.Millisecond).Should(BeNumerically(">=", 2))

		// The controller saw a second connection with a fresh login + subscribe.
		Expect(waitFor(3*time.Second, func() bool {
			return fc.connectionCount() >= 2 &&
				fc.countMID(op.MIDCommunicationStart) >= 2 &&
				fc.countMID(op.MIDLastTighteningSub) >= 2
		})).To(BeTrue(), "expected login+subscribe to be replayed on reconnect")
	})
})
