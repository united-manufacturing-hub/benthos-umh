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
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Register Benthos default components (e.g. the "none" tracer/metrics) so
	// the StreamBuilder can build a stream in the test binary.
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

type capturedMsg struct {
	mid        string
	structured any
	raw        []byte
}

var _ = Describe("open_protocol Benthos input", func() {

	It("registers under the name 'open_protocol' with a valid config spec", func() {
		env := service.NewEnvironment()
		builder := env.NewStreamBuilder()
		err := builder.AddInputYAML(`
open_protocol:
  endpoint: "127.0.0.1:4545"
  subscribe: [last_tightening, alarms]
`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("streams a parsed MID 0061 result end-to-end with routing metadata", func() {
		result := []byte(build0061Data(true))
		fc, err := newFakeController(goodHandler([][]byte{result, result}))
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var mu sync.Mutex
		var captured []capturedMsg

		builder := service.NewStreamBuilder()
		Expect(builder.AddInputYAML(fmt.Sprintf(`
open_protocol:
  endpoint: "%s"
  subscribe: [last_tightening]
  keepalive_interval: 50ms
  request_timeout: 2s
`, fc.addr()))).To(Succeed())

		Expect(builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
			mid, _ := m.MetaGet("op_mid")
			structured, _ := m.AsStructured()
			raw, _ := m.AsBytes()
			mu.Lock()
			captured = append(captured, capturedMsg{mid: mid, structured: structured, raw: append([]byte{}, raw...)})
			mu.Unlock()
			return nil
		})).To(Succeed())

		stream, err := builder.Build()
		Expect(err).NotTo(HaveOccurred())

		go func() {
			defer GinkgoRecover()
			_ = stream.Run(ctx)
		}()

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(captured)
		}, 5*time.Second, 20*time.Millisecond).Should(BeNumerically(">=", 1))

		mu.Lock()
		first := captured[0]
		mu.Unlock()

		Expect(first.mid).To(Equal("0061"))
		obj, ok := first.structured.(map[string]any)
		Expect(ok).To(BeTrue(), "MID 0061 payload should be structured JSON")
		Expect(obj).To(HaveKeyWithValue("tightening_ok", true))
		torque, ok := obj["torque_actual"].(json.Number)
		Expect(ok).To(BeTrue(), "torque_actual should be a JSON number")
		tf, err := torque.Float64()
		Expect(err).NotTo(HaveOccurred())
		Expect(tf).To(BeNumerically("~", 50.12, 0.001))

		_ = stream.StopWithin(3 * time.Second)
	})

	It("emits the raw data field for non-natively-decoded MIDs (alarms)", func() {
		// A controller that, on alarm subscription, pushes a raw MID 0071.
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
				case op.MIDAlarmSub:
					_ = sendFrame(conn, op.MIDAlarm, 1, []byte("ALARMPAYLOAD"))
				}
			}
		})
		Expect(err).NotTo(HaveOccurred())
		defer fc.close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var mu sync.Mutex
		var captured []capturedMsg

		builder := service.NewStreamBuilder()
		Expect(builder.AddInputYAML(fmt.Sprintf(`
open_protocol:
  endpoint: "%s"
  subscribe: [alarms]
  keepalive_interval: 50ms
  request_timeout: 2s
`, fc.addr()))).To(Succeed())
		Expect(builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
			mid, _ := m.MetaGet("op_mid")
			raw, _ := m.AsBytes()
			mu.Lock()
			captured = append(captured, capturedMsg{mid: mid, raw: append([]byte{}, raw...)})
			mu.Unlock()
			return nil
		})).To(Succeed())

		stream, err := builder.Build()
		Expect(err).NotTo(HaveOccurred())
		go func() {
			defer GinkgoRecover()
			_ = stream.Run(ctx)
		}()

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(captured)
		}, 5*time.Second, 20*time.Millisecond).Should(BeNumerically(">=", 1))

		mu.Lock()
		first := captured[0]
		mu.Unlock()
		Expect(first.mid).To(Equal("0071"))
		Expect(string(first.raw)).To(Equal("ALARMPAYLOAD"))

		_ = stream.StopWithin(3 * time.Second)
	})
})
