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
	"fmt"
	"net"
	"strconv"
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

// capturedMsg holds the fields we want to inspect from a received message.
type capturedMsg struct {
	tagName     string
	mid         string
	revision    string
	stationID   string
	spindleID   string
	endpoint    string
	timestampMs string
	structured  any
	raw         []byte
	// full metadata snapshot for checking absence of keys
	allMeta map[string]string
}

// captureMsg snapshots all the metadata fields we care about from a message.
func captureMsg(m *service.Message) capturedMsg {
	tagName, _ := m.MetaGet("open_protocol_tag_name")
	mid, _ := m.MetaGet("open_protocol_mid")
	revision, _ := m.MetaGet("open_protocol_revision")
	stationID, _ := m.MetaGet("open_protocol_station_id")
	spindleID, _ := m.MetaGet("open_protocol_spindle_id")
	endpoint, _ := m.MetaGet("open_protocol_endpoint")
	tsMs, _ := m.MetaGet("timestamp_ms")
	structured, _ := m.AsStructured()
	raw, _ := m.AsBytes()

	// Collect all meta keys for absence assertions.
	all := map[string]string{}
	_ = m.MetaWalk(func(k, v string) error {
		all[k] = v
		return nil
	})

	return capturedMsg{
		tagName:     tagName,
		mid:         mid,
		revision:    revision,
		stationID:   stationID,
		spindleID:   spindleID,
		endpoint:    endpoint,
		timestampMs: tsMs,
		structured:  structured,
		raw:         append([]byte{}, raw...),
		allMeta:     all,
	}
}

// build0061DataWithTimestamp returns a MID 0061 rev-1 data field using the
// supplied timestamp string (19 chars, YYYY-MM-DD:HH:MM:SS) for param-20.
func build0061DataWithTimestamp(okStatus bool, ts string) string {
	f := defaultFixture()
	f.TighteningOK = "0"
	if okStatus {
		f.TighteningOK = "1"
	}
	f.Timestamp = ts
	return buildMID0061Data(f)
}

var _ = Describe("open_protocol Benthos input", func() {

	// -----------------------------------------------------------------------
	// Registration
	// -----------------------------------------------------------------------
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

	// -----------------------------------------------------------------------
	// Task 10 — config validation
	// -----------------------------------------------------------------------
	Describe("config validation", func() {
		// runStreamUntilErr builds a stream, starts Run in background, and
		// returns the first non-nil error that Run produces (within 3s).
		// The constructor errors surface at Run time in Benthos StreamBuilder.
		runStreamUntilErr := func(yaml string) error {
			builder := service.NewStreamBuilder()
			if err := builder.AddInputYAML(yaml); err != nil {
				return err
			}
			stream, err := builder.Build()
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			return stream.Run(ctx)
		}

		It("rejects an unknown timezone ('Mars/Phobos')", func() {
			err := runStreamUntilErr(`
open_protocol:
  endpoint: "127.0.0.1:4545"
  timezone: "Mars/Phobos"
`)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Mars/Phobos"))
		})

		It("rejects read_timeout < 2x keepalive_interval", func() {
			err := runStreamUntilErr(`
open_protocol:
  endpoint: "127.0.0.1:4545"
  read_timeout: 5s
  keepalive_interval: 10s
`)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("read_timeout"))
		})

		It("accepts read_timeout == 2x keepalive_interval (boundary is valid)", func() {
			// This will fail to connect (no server) but should NOT error on the
			// config itself. The Run context times out → context.DeadlineExceeded,
			// which is NOT a config error.
			err := runStreamUntilErr(`
open_protocol:
  endpoint: "127.0.0.1:1"
  read_timeout: 20s
  keepalive_interval: 10s
`)
			// Context deadline exceeded is fine — config was accepted.
			// A config-related error would mention "read_timeout" or "timezone".
			if err != nil {
				Expect(err.Error()).NotTo(ContainSubstring("read_timeout"))
				Expect(err.Error()).NotTo(ContainSubstring("timezone"))
			}
		})
	})

	// -----------------------------------------------------------------------
	// Task 11 + 12 — fan-out (rev-1 MID 0061 → 18 messages)
	// -----------------------------------------------------------------------
	Describe("fan-out for rev-1 MID 0061", func() {
		// Use timestamp "2026-06-02:14:30:15" so we can assert the exact epoch ms.
		const testTimestamp = "2026-06-02:14:30:15"

		var (
			fc       *fakeController
			captured []capturedMsg
			mu       sync.Mutex
		)

		BeforeEach(func() {
			result := []byte(build0061DataWithTimestamp(true, testTimestamp))
			var err error
			fc, err = newFakeController(goodHandler([][]byte{result}))
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			DeferCleanup(func() {
				cancel()
				fc.close()
			})

			builder := service.NewStreamBuilder()
			Expect(builder.AddInputYAML(fmt.Sprintf(`
open_protocol:
  endpoint: "%s"
  subscribe: [last_tightening]
  keepalive_interval: 50ms
  request_timeout: 2s
`, fc.addr()))).To(Succeed())

			Expect(builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
				cm := captureMsg(m)
				mu.Lock()
				captured = append(captured, cm)
				mu.Unlock()
				return nil
			})).To(Succeed())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			go func() {
				defer GinkgoRecover()
				_ = stream.Run(ctx)
			}()

			// Wait until we have exactly 18 messages (one full fan-out batch).
			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(captured)
			}, 5*time.Second, 20*time.Millisecond).Should(BeNumerically(">=", 18))

			_ = stream.StopWithin(3 * time.Second)
		})

		It("emits exactly 18 messages for one tightening result", func() {
			mu.Lock()
			defer mu.Unlock()
			Expect(captured).To(HaveLen(18))
		})

		It("all 18 messages share open_protocol_mid == '0061'", func() {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range captured {
				Expect(m.mid).To(Equal("0061"), "mid mismatch in message %+v", m)
			}
		})

		It("all 18 messages share open_protocol_revision == '1'", func() {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range captured {
				Expect(m.revision).To(Equal("1"), "revision mismatch in message %+v", m)
			}
		})

		It("all 18 messages share an identical non-empty timestamp_ms", func() {
			mu.Lock()
			defer mu.Unlock()
			tsSet := map[string]struct{}{}
			for _, m := range captured {
				Expect(m.timestampMs).NotTo(BeEmpty(), "timestamp_ms should not be empty")
				tsSet[m.timestampMs] = struct{}{}
			}
			Expect(tsSet).To(HaveLen(1), "all messages must share the same timestamp_ms")
		})

		It("timestamp_ms equals the UTC epoch-ms of the controller timestamp", func() {
			// "2026-06-02:14:30:15" UTC → compute expected
			t, err := time.Parse("2006-01-02:15:04:05", testTimestamp)
			Expect(err).NotTo(HaveOccurred())
			expected := strconv.FormatInt(t.UnixMilli(), 10)

			mu.Lock()
			defer mu.Unlock()
			Expect(captured[0].timestampMs).To(Equal(expected))
		})

		It("the set of open_protocol_tag_name values includes the expected tags", func() {
			mu.Lock()
			defer mu.Unlock()
			names := map[string]struct{}{}
			for _, m := range captured {
				names[m.tagName] = struct{}{}
			}
			for _, want := range []string{
				"torque_actual", "angle_actual", "tightening_ok",
				"vin", "tightening_id",
			} {
				Expect(names).To(HaveKey(want), "expected tag name %q in fan-out", want)
			}
		})

		It("torque_actual payload ≈ 50.12", func() {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range captured {
				if m.tagName == "torque_actual" {
					f, ok := m.structured.(float64)
					Expect(ok).To(BeTrue(), "torque_actual value should be float64, got %T", m.structured)
					Expect(f).To(BeNumerically("~", 50.12, 0.001))
					return
				}
			}
			Fail("torque_actual tag not found in fan-out")
		})

		It("tightening_ok payload == true", func() {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range captured {
				if m.tagName == "tightening_ok" {
					b, ok := m.structured.(bool)
					Expect(ok).To(BeTrue(), "tightening_ok value should be bool, got %T", m.structured)
					Expect(b).To(BeTrue())
					return
				}
			}
			Fail("tightening_ok tag not found in fan-out")
		})

		It("vin appears as a tag (open_protocol_tag_name), NOT as a metadata key", func() {
			mu.Lock()
			defer mu.Unlock()
			foundTag := false
			for _, m := range captured {
				if m.tagName == "vin" {
					foundTag = true
				}
				// vin must not appear as a separate metadata key
				_, hasVinMeta := m.allMeta["open_protocol_vin"]
				Expect(hasVinMeta).To(BeFalse(), "vin should not be a metadata key")
			}
			Expect(foundTag).To(BeTrue(), "vin should appear as an open_protocol_tag_name")
		})

		It("tightening_id appears as a tag, NOT as a metadata key", func() {
			mu.Lock()
			defer mu.Unlock()
			foundTag := false
			for _, m := range captured {
				if m.tagName == "tightening_id" {
					foundTag = true
				}
				_, hasMeta := m.allMeta["open_protocol_tightening_id"]
				Expect(hasMeta).To(BeFalse(), "tightening_id should not be a metadata key")
			}
			Expect(foundTag).To(BeTrue(), "tightening_id should appear as an open_protocol_tag_name")
		})

		It("no op_* metadata keys remain on any message", func() {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range captured {
				for k := range m.allMeta {
					Expect(k).NotTo(HavePrefix("op_"), "old op_* metadata key %q found", k)
				}
			}
		})
	})

	// -----------------------------------------------------------------------
	// Raw passthrough — non-0061 MID (alarm MID 0071)
	// -----------------------------------------------------------------------
	Describe("raw passthrough for non-0061 MIDs (alarms)", func() {
		It("emits exactly ONE raw message for MID 0071, no open_protocol_tag_name", func() {
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
				cm := captureMsg(m)
				mu.Lock()
				captured = append(captured, cm)
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

			_ = stream.StopWithin(3 * time.Second)

			mu.Lock()
			first := captured[0]
			mu.Unlock()

			Expect(first.mid).To(Equal("0071"))
			Expect(string(first.raw)).To(Equal("ALARMPAYLOAD"))
			Expect(first.tagName).To(BeEmpty(), "non-0061 should have no open_protocol_tag_name")
		})
	})

	// -----------------------------------------------------------------------
	// Raw passthrough — rev-2 MID 0061 (not rev-1 → one raw message)
	// -----------------------------------------------------------------------
	Describe("raw passthrough for non-rev-1 MID 0061", func() {
		It("emits ONE raw message for 0061 with revision 2, no tag_name", func() {
			result := []byte(build0061Data(true))
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
						// Confirm subscription, then push a 0061 with revision=2.
						_ = sendFrame(conn, op.MIDCommandAccepted, 1, nil)
						_ = sendFrame(conn, op.MIDLastTightening, 2, result)
					case op.MIDKeepAlive, op.MIDLastTighteningAck:
						// ignored
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
  subscribe: [last_tightening]
  keepalive_interval: 50ms
  request_timeout: 2s
`, fc.addr()))).To(Succeed())
			Expect(builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
				cm := captureMsg(m)
				mu.Lock()
				captured = append(captured, cm)
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

			_ = stream.StopWithin(3 * time.Second)

			mu.Lock()
			first := captured[0]
			// Ensure we got exactly one message (not 18).
			count := len(captured)
			mu.Unlock()

			Expect(first.mid).To(Equal("0061"))
			Expect(first.revision).To(Equal("2"))
			Expect(first.tagName).To(BeEmpty(), "non-rev-1 0061 should have no open_protocol_tag_name")
			Expect(count).To(Equal(1), "non-rev-1 0061 should emit exactly one message")
		})
	})
})
