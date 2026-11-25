// Copyright 2026 UMH Systems GmbH
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

package s7comm_plugin_test

// These tests pin what the input does today, so that moving it onto the shared
// Client changes nothing the user can see. They pass on the code before the
// move and after it. A few of them pin behavior that looks like a bug: each
// says so, and the fix belongs in its own change.

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure" // the stream builder needs the core components
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/robinson/gos7"

	"github.com/united-manufacturing-hub/benthos-umh/s7comm_plugin"
)

// readRecorder stands in for the gos7 client. It records every read request
// and fills the items from a queue of values, one per item in request order.
type readRecorder struct {
	gos7.Client // a nil client: any call the test does not expect panics

	calls   [][]gos7.S7DataItem
	values  [][]byte
	failOn  int // 1-based request number that fails, 0 for none
	perItem string
}

func (r *readRecorder) AGReadMulti(items []gos7.S7DataItem, count int) error {
	Expect(count).To(Equal(len(items)), "the input passes the item count it sends")

	sent := make([]gos7.S7DataItem, len(items))
	for index, item := range items {
		sent[index] = item
		sent[index].Data = make([]byte, len(item.Data))
	}
	r.calls = append(r.calls, sent)

	if r.failOn == len(r.calls) {
		return errors.New("connection reset by peer")
	}
	for index := range items {
		if len(r.values) > 0 {
			copy(items[index].Data, r.values[0])
			r.values = r.values[1:]
		}
	}
	if r.perItem != "" && len(items) > 0 {
		items[0].Error = r.perItem
	}
	return nil
}

func mustHex(text string) []byte {
	decoded, err := hex.DecodeString(text)
	Expect(err).NotTo(HaveOccurred())
	return decoded
}

func newCharacterizedInput(addresses []string, recorder *readRecorder) *s7comm_plugin.S7CommInput {
	parsed, err := s7comm_plugin.ParseAddresses(addresses)
	Expect(err).NotTo(HaveOccurred())
	batches, err := s7comm_plugin.BuildBatches(parsed, 240)
	Expect(err).NotTo(HaveOccurred())

	return &s7comm_plugin.S7CommInput{
		Client:           s7comm_plugin.NewGos7ClientForTest(recorder, 240),
		Log:              service.MockResources().Logger(),
		ParsedAddresses:  parsed,
		Batches:          batches,
		TimeBetweenReads: time.Millisecond,
	}
}

var _ = Describe("S7Comm input behavior that must not change", func() {
	BeforeEach(func() {
		testActive, ok := os.LookupEnv("TEST_S7COMM_UNITTEST")
		if !ok || strings.ToLower(testActive) == "false" {
			Skip("Skipping test: TEST_S7COMM_UNITTEST env variable is not set or is FALSE")
		}
	})

	type parsedItem struct {
		area, wordLen, dbNumber, start, amount, bit, dataLength int
	}

	DescribeTable("an address becomes the item that is read",
		func(address string, want parsedItem) {
			parsed, err := s7comm_plugin.ParseAddresses([]string{address})
			Expect(err).NotTo(HaveOccurred())
			Expect(parsed).To(HaveLen(1))

			item := parsed[0].Item
			got := parsedItem{item.Area, item.WordLength, item.DBNumber, item.ByteOffset, item.Amount, item.Bit, len(item.Data)}
			Expect(got).To(Equal(want))
			Expect(parsed[0].Address).To(Equal(address))
		},
		Entry("bit", "DB1.X0.3", parsedItem{0x84, 0x01, 1, 0, 1, 3, 1}),
		Entry("byte", "DB1.B7", parsedItem{0x84, 0x02, 1, 7, 1, 0, 1}),
		Entry("char", "DB1.C9", parsedItem{0x84, 0x03, 1, 9, 1, 0, 1}),
		Entry("word", "DB2.W10", parsedItem{0x84, 0x04, 2, 10, 1, 0, 2}),
		Entry("int", "DB2.I12", parsedItem{0x84, 0x05, 2, 12, 1, 0, 2}),
		Entry("double word", "DB3.DW4", parsedItem{0x84, 0x06, 3, 4, 1, 0, 4}),
		Entry("double int", "DB3.DI8", parsedItem{0x84, 0x07, 3, 8, 1, 0, 4}),
		Entry("real", "DB1.R8", parsedItem{0x84, 0x08, 1, 8, 1, 0, 4}),
		Entry("date and time", "DB1.DT0", parsedItem{0x84, 0x0F, 1, 0, 1, 0, 8}),
		Entry("string of 30 characters", "DB4.S20.30", parsedItem{0x84, 0x03, 4, 20, 30, 0, 32}),
		Entry("merker", "MK.W6", parsedItem{0x83, 0x04, 0, 6, 1, 0, 2}),
		Entry("process input", "PE.X0.1", parsedItem{0x81, 0x01, 0, 0, 1, 1, 1}),
		Entry("process output", "PA.B2", parsedItem{0x82, 0x02, 0, 2, 1, 0, 1}),
		Entry("counter", "C.W0", parsedItem{0x1C, 0x04, 0, 0, 1, 0, 2}),
		Entry("timer", "T.W0", parsedItem{0x1D, 0x04, 0, 0, 1, 0, 2}),
		Entry("block number on a non-DB area is ignored (deprecated format)", "MK1.W6", parsedItem{0x83, 0x04, 0, 6, 1, 0, 2}),
	)

	DescribeTable("an address that is rejected",
		func(address, reason string) {
			_, err := s7comm_plugin.ParseAddresses([]string{address})
			Expect(err).To(MatchError(ContainSubstring(reason)))
		},
		Entry("DB without a block number", "DB.W0", "DB requires a block number"),
		Entry("no start offset", "DB1.W", "invalid address"),
		Entry("lowercase", "db1.w0", "invalid address"),
		Entry("bit without a bit number", "DB1.X0", "extra parameter required"),
		Entry("bit number above 7", "DB1.X0.8", "out of range"),
		Entry("string without a length", "DB1.S0", "extra parameter required"),
		Entry("string of length 0", "DB1.S0.0", "invalid extra parameter"),
		Entry("a bit number on a word", "DB1.W0.1", "extra parameter specified but not used"),
		Entry("unknown area", "XX1.W0", "invalid area"),
		Entry("unknown type", "DB1.Q0", "unknown data type"),
		Entry("a block number too large for an int", "DB99999999999999999999.W0", "invalid DB number"),
		Entry("a start offset too large for an int", "DB1.W99999999999999999999", "invalid start address"),
		Entry("a string length too large for an int", "DB1.S0.99999999999999999999", "invalid extra parameter"),
		Entry("a bit number too large for an int", "DB1.X0.99999999999999999999", "invalid extra parameter"),
	)

	DescribeTable("the bytes of a read become a value",
		func(address, bytesHex string, want any) {
			parsed, err := s7comm_plugin.ParseAddresses([]string{address})
			Expect(err).NotTo(HaveOccurred())
			Expect(parsed[0].ConverterFunc(mustHex(bytesHex))).To(Equal(want))
		},
		Entry("bit set", "DB1.X0.0", "01", true),
		Entry("bit clear", "DB1.X0.0", "00", false),
		Entry("byte", "DB1.B0", "ab", byte(0xab)),
		Entry("char", "DB1.C0", "41", "A"),
		Entry("string", "DB1.S0.10", "0a0568656c6c6f0000000000", "hello"),
		Entry("string clipped to its actual length", "DB1.S0.10", "0a0368656c6c6f0000000000", "hel"),
		Entry("word", "DB1.W0", "1234", uint16(0x1234)),
		Entry("int", "DB1.I0", "fffe", int16(-2)),
		Entry("double word", "DB1.DW0", "00010000", uint32(65536)),
		Entry("double int", "DB1.DI0", "fffffffe", int32(-2)),
		Entry("real", "DB1.R0", "41200000", float32(10)),
	)

	Describe("reading through the gos7 client", func() {
		var recorder *readRecorder

		BeforeEach(func() {
			recorder = &readRecorder{}
		})

		It("returns one message per address in address order, with the value and the configured address", func() {
			addresses := []string{"DB1.W0", "DB1.X1.2", "DB1.R4", "DB2.S0.5"}
			recorder.values = [][]byte{mustHex("0102"), mustHex("01"), mustHex("41200000"), mustHex("05036162630000")}
			input := newCharacterizedInput(addresses, recorder)

			messages, _, err := input.ReadBatch(context.Background())
			Expect(err).NotTo(HaveOccurred())

			gotPayloads := make([]string, 0, len(messages))
			gotAddresses := make([]string, 0, len(messages))
			for _, message := range messages {
				payload, err := message.AsBytes()
				Expect(err).NotTo(HaveOccurred())
				address, found := message.MetaGet("s7_address")
				Expect(found).To(BeTrue())
				gotPayloads = append(gotPayloads, string(payload))
				gotAddresses = append(gotAddresses, address)
			}
			Expect(gotPayloads).To(Equal([]string{"258", "true", "10", "abc"}))
			Expect(gotAddresses).To(Equal(addresses))
		})

		It("hands back an acknowledgement that always succeeds", func() {
			input := newCharacterizedInput([]string{"DB1.W0"}, recorder)

			_, ack, err := input.ReadBatch(context.Background())

			Expect(err).NotTo(HaveOccurred())
			Expect(ack(context.Background(), nil)).To(Succeed())
			Expect(ack(context.Background(), errors.New("downstream failed"))).To(Succeed())
		})

		It("sends one request per batch and at most 20 items per request", func() {
			addresses := make([]string, 0, 30)
			for offset := range 30 {
				addresses = append(addresses, "DB1.B"+strconv.Itoa(offset))
			}
			input := newCharacterizedInput(addresses, recorder)

			messages, _, err := input.ReadBatch(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(messages).To(HaveLen(30))

			Expect(recorder.calls).To(HaveLen(len(input.Batches)))
			total := 0
			for index, call := range recorder.calls {
				Expect(call).To(HaveLen(len(input.Batches[index])))
				Expect(len(call)).To(BeNumerically("<=", 20))
				total += len(call)
			}
			Expect(total).To(Equal(30))
			Expect(len(recorder.calls)).To(BeNumerically(">", 1))
		})

		It("sends the item exactly as the address was parsed", func() {
			input := newCharacterizedInput([]string{"DB7.X3.5", "DB1.R12"}, recorder)

			_, _, err := input.ReadBatch(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Expect(recorder.calls).To(HaveLen(1))
			bit, float := recorder.calls[0][0], recorder.calls[0][1]
			Expect([]int{bit.Area, bit.WordLen, bit.DBNumber, bit.Start, bit.Bit, bit.Amount}).To(Equal([]int{0x84, 0x01, 7, 3, 5, 1}))
			Expect([]int{float.Area, float.WordLen, float.DBNumber, float.Start, float.Bit, float.Amount}).To(Equal([]int{0x84, 0x08, 1, 12, 0, 1}))
		})

		// Pinned as it is today: the request asks for the declared length, and the
		// buffer holds the two header bytes more. A full string therefore loses its
		// last two characters. A fix changes the request and gets its own change.
		It("asks for the declared string length and not for the two header bytes more (current behavior)", func() {
			input := newCharacterizedInput([]string{"DB1.S0.10"}, recorder)

			_, _, err := input.ReadBatch(context.Background())
			Expect(err).NotTo(HaveOccurred())

			item := recorder.calls[0][0]
			Expect(item.Amount).To(Equal(10))
			Expect(item.Data).To(HaveLen(12))
		})

		It("returns ErrNotConnected and no messages when a request fails", func() {
			addresses := make([]string, 0, 30)
			for offset := range 30 {
				addresses = append(addresses, "DB1.B"+strconv.Itoa(offset))
			}
			recorder.failOn = 2
			input := newCharacterizedInput(addresses, recorder)

			messages, ack, err := input.ReadBatch(context.Background())

			Expect(err).To(MatchError(service.ErrNotConnected))
			Expect(messages).To(BeNil())
			Expect(ack).To(BeNil())
		})

		// Pinned as it is today: gos7 reports a refused item in the item and not
		// as an error, the input does not look at it, and the value is read from
		// the bytes that stayed zero. A fix gets its own change.
		It("ignores the error a PLC reports for one item (current behavior)", func() {
			recorder.perItem = "Item not available"
			input := newCharacterizedInput([]string{"DB99.W0"}, recorder)

			messages, _, err := input.ReadBatch(context.Background())

			Expect(err).NotTo(HaveOccurred())
			Expect(messages).To(HaveLen(1))
			payload, err := messages[0].AsBytes()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(payload)).To(Equal("0"))
		})

		It("fails when there is no client yet", func() {
			input := &s7comm_plugin.S7CommInput{TimeBetweenReads: time.Millisecond}

			_, _, err := input.ReadBatch(context.Background())

			Expect(err).To(MatchError(ContainSubstring("not initialized")))
		})

		It("stops waiting between reads when the context ends", func() {
			input := newCharacterizedInput([]string{"DB1.W0"}, recorder)
			input.TimeBetweenReads = time.Hour
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, _, err := input.ReadBatch(ctx)

			Expect(err).To(MatchError(context.Canceled))
			Expect(recorder.calls).To(BeEmpty())
		})

		It("waits the configured time before every read", func() {
			input := newCharacterizedInput([]string{"DB1.W0"}, recorder)
			input.TimeBetweenReads = 50 * time.Millisecond

			for range 2 {
				start := time.Now()
				_, _, err := input.ReadBatch(context.Background())
				Expect(err).NotTo(HaveOccurred())
				Expect(time.Since(start)).To(BeNumerically(">=", input.TimeBetweenReads))
			}
		})

		It("closes without a connection", func() {
			input := &s7comm_plugin.S7CommInput{}

			Expect(input.Close(context.Background())).To(Succeed())
		})
	})

	Describe("the config of the input", func() {
		newBuilder := func(inputYAML string) *service.StreamBuilder {
			builder := service.NewStreamBuilder()
			Expect(builder.AddInputYAML(inputYAML)).To(Succeed())
			Expect(builder.AddConsumerFunc(func(context.Context, *service.Message) error { return nil })).To(Succeed())
			return builder
		}

		It("builds with the required fields only", func() {
			_, err := newBuilder(`
s7comm:
  tcpDevice: 192.0.2.1
  addresses: [DB1.W0]
`).Build()
			Expect(err).NotTo(HaveOccurred())
		})

		It("builds with every documented field, and the deprecated one", func() {
			_, err := newBuilder(`
s7comm:
  tcpDevice: 192.0.2.1:102
  rack: 0
  slot: 1
  timeBetweenReads: 500ms
  timeout: 5
  disableCPUInfo: true
  batchMaxSize: 5
  addresses: [DB1.W0, DB1.X1.2, DB1.S4.10, MK.W0, PE.B1]
`).Build()
			Expect(err).NotTo(HaveOccurred())
		})

		It("accepts the deprecated block number on a non-DB area", func() {
			_, err := newBuilder(`
s7comm:
  tcpDevice: 192.0.2.1
  addresses: [MK0.W6]
`).Build()
			Expect(err).NotTo(HaveOccurred())
		})

		It("refuses a field it does not know", func() {
			err := service.NewStreamBuilder().AddInputYAML(`
s7comm:
  tcpDevice: 192.0.2.1
  addresses: [DB1.W0]
  unknownField: true
`)
			Expect(err).To(MatchError(ContainSubstring("unknownField")))
		})

		It("refuses an invalid address when the input is created", func() {
			stream, err := newBuilder(`
s7comm:
  tcpDevice: 192.0.2.1
  addresses: [XX1.W0]
`).Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			Expect(stream.Run(ctx)).To(MatchError(ContainSubstring("invalid area")))
		})
	})
})

// logRecorder keeps what a stream logs, so a test can see the warnings the
// input raises while it is created.
type logRecorder struct {
	mutex    sync.Mutex
	warnings []string
}

func (r *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *logRecorder) Handle(_ context.Context, record slog.Record) error {
	if record.Level != slog.LevelWarn {
		return nil
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.warnings = append(r.warnings, record.Message)
	return nil
}

func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *logRecorder) WithGroup(string) slog.Handler      { return r }

// warningsOf creates the input from the YAML and returns the warnings it logs.
// The device is a closed port, so the stream fails to connect fast and the
// context ends it.
func warningsOf(inputYAML string) []string {
	recorder := &logRecorder{}
	builder := service.NewStreamBuilder()
	builder.SetLogger(slog.New(recorder))
	Expect(builder.AddInputYAML(inputYAML)).To(Succeed())
	Expect(builder.AddConsumerFunc(func(context.Context, *service.Message) error { return nil })).To(Succeed())
	stream, err := builder.Build()
	Expect(err).NotTo(HaveOccurred())

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = stream.Run(ctx) // ends with the context, the connection never opens

	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return append([]string(nil), recorder.warnings...)
}

func byteAddresses(count int) []string {
	addresses := make([]string, 0, count)
	for offset := range count {
		addresses = append(addresses, "DB1.B"+strconv.Itoa(offset))
	}
	return addresses
}

func stringAddresses(count int, length int) []string {
	addresses := make([]string, 0, count)
	for index := range count {
		addresses = append(addresses, "DB1.S"+strconv.Itoa(index*100)+"."+strconv.Itoa(length))
	}
	return addresses
}

var _ = Describe("S7Comm input limits and warnings that must not change", func() {
	BeforeEach(func() {
		testActive, ok := os.LookupEnv("TEST_S7COMM_UNITTEST")
		if !ok || strings.ToLower(testActive) == "false" {
			Skip("Skipping test: TEST_S7COMM_UNITTEST env variable is not set or is FALSE")
		}
	})

	// A request holds 19 bytes of header and 12 per item. A response holds 21
	// bytes of header and 4 per item plus its data, rounded up to an even length.
	DescribeTable("addresses are split into requests that fit the PDU",
		func(addresses []string, pduSize int, want []int) {
			parsed, err := s7comm_plugin.ParseAddresses(addresses)
			Expect(err).NotTo(HaveOccurred())

			batches, err := s7comm_plugin.BuildBatches(parsed, pduSize)
			Expect(err).NotTo(HaveOccurred())

			got := make([]int, 0, len(batches))
			for _, batch := range batches {
				got = append(got, len(batch))
			}
			Expect(got).To(Equal(want))
		},
		Entry("nothing to read", []string{}, 240, []int{}),
		Entry("20 items fit one request", byteAddresses(20), 960, []int{20}),
		Entry("the 21st item starts a new request", byteAddresses(21), 960, []int{20, 1}),
		Entry("the request fills the PDU to the byte: 3 items need 55 bytes", byteAddresses(7), 55, []int{3, 3, 1}),
		Entry("the response fills the PDU to the byte: 2 strings of 20 need 73 bytes", stringAddresses(5, 20), 73, []int{2, 2, 1}),
		Entry("an odd data length counts as the next even one: strings of 21 do not fit twice in 76 bytes", stringAddresses(4, 21), 76, []int{1, 1, 1, 1}),
		Entry("the smallest PDU that holds one item", byteAddresses(1), 31, []int{1}),
		Entry("a string whose response fills the PDU to the byte", stringAddresses(1, 100), 127, []int{1}),
	)

	DescribeTable("an address that cannot fit one PDU is refused",
		func(addresses []string, pduSize int) {
			parsed, err := s7comm_plugin.ParseAddresses(addresses)
			Expect(err).NotTo(HaveOccurred())

			_, err = s7comm_plugin.BuildBatches(parsed, pduSize)

			Expect(err).To(MatchError(ContainSubstring("exceeds PDU size")))
		},
		Entry("the request is one byte too big", byteAddresses(1), 30),
		Entry("the response is one byte too big", stringAddresses(1, 100), 126),
	)

	DescribeTable("an address at the edge of what is valid",
		func(address string, want parsedEdge) {
			parsed, err := s7comm_plugin.ParseAddresses([]string{address})
			Expect(err).NotTo(HaveOccurred())

			item := parsed[0].Item
			Expect(parsedEdge{item.WordLength, item.Bit, item.Amount, len(item.Data)}).To(Equal(want))
		},
		Entry("the highest bit", "DB1.X0.7", parsedEdge{0x01, 7, 1, 1}),
		Entry("the lowest bit", "DB1.X0.0", parsedEdge{0x01, 0, 1, 1}),
		Entry("a string of one character", "DB1.S0.1", parsedEdge{0x03, 0, 1, 3}),
	)

	DescribeTable("the bytes of a string read, at the edges",
		func(bytesHex string, want string) {
			parsed, err := s7comm_plugin.ParseAddresses([]string{"DB1.S0.10"})
			Expect(err).NotTo(HaveOccurred())
			Expect(parsed[0].ConverterFunc(mustHex(bytesHex))).To(Equal(want))
		},
		Entry("only the two header bytes", "0a00", ""),
		Entry("one character", "0a0141", "A"),
		Entry("a declared length longer than the buffer holds", "0a0a616263", "abc"),
		Entry("a declared length just above what the buffer holds", "0a05616263", "abc"),
		Entry("trailing zero bytes are cut", "0a0461620000", "ab"),
	)

	Describe("what the input warns about when it is created", func() {
		const device = "tcpDevice: 127.0.0.1:1\n"

		It("warns about a block number on a non-DB area, and suggests the address without it", func() {
			warnings := warningsOf("s7comm:\n  " + device + "  addresses: [MK0.W6]")

			Expect(warnings).To(ContainElement(And(ContainSubstring("deprecated format"), ContainSubstring(`"MK.W6"`))))
		})

		It("keeps the extra part of the address in the suggestion", func() {
			warnings := warningsOf("s7comm:\n  " + device + "  addresses: [PE1.X0.1]")

			Expect(warnings).To(ContainElement(ContainSubstring(`"PE.X0.1"`)))
		})

		It("does not warn about an address it cannot read, the parser refuses it", func() {
			warnings := warningsOf("s7comm:\n  " + device + "  addresses: [garbage]")

			Expect(warnings).NotTo(ContainElement(ContainSubstring("deprecated")))
		})

		It("does not warn about a DB address or an address without a block number", func() {
			warnings := warningsOf("s7comm:\n  " + device + "  addresses: [DB1.W0, MK.W6]")

			Expect(warnings).NotTo(ContainElement(ContainSubstring("deprecated")))
		})

		It("warns that batchMaxSize is ignored", func() {
			warnings := warningsOf("s7comm:\n  " + device + "  batchMaxSize: 5\n  addresses: [DB1.W0]")

			Expect(warnings).To(ContainElement(ContainSubstring("batchMaxSize")))
		})

		It("does not mention batchMaxSize when it is not set", func() {
			warnings := warningsOf("s7comm:\n  " + device + "  addresses: [DB1.W0]")

			Expect(warnings).NotTo(ContainElement(ContainSubstring("batchMaxSize")))
		})

		It("replaces a timeBetweenReads of zero", func() {
			warnings := warningsOf("s7comm:\n  " + device + "  timeBetweenReads: 0s\n  addresses: [DB1.W0]")

			Expect(warnings).To(ContainElement(ContainSubstring("invalid timeBetweenReads")))
		})

		It("accepts the smallest positive timeBetweenReads", func() {
			warnings := warningsOf("s7comm:\n  " + device + "  timeBetweenReads: 1ms\n  addresses: [DB1.W0]")

			Expect(warnings).NotTo(ContainElement(ContainSubstring("invalid timeBetweenReads")))
		})
	})

	It("returns an empty batch when there is nothing to read", func() {
		input := newCharacterizedInput([]string{}, &readRecorder{})

		messages, _, err := input.ReadBatch(context.Background())

		Expect(err).NotTo(HaveOccurred())
		Expect(messages).To(BeEmpty())
	})
})

type parsedEdge struct {
	wordLen, bit, amount, dataLength int
}

// fakeClient is a connected Client that records how it is used.
type fakeClient struct {
	pduLength int
	cpuInfo   string
	cpuErr    error
	cpuCalls  int
	closed    int
}

func (c *fakeClient) ReadItems(context.Context, []s7comm_plugin.Item) error { return nil }
func (c *fakeClient) PDULength() int                                        { return c.pduLength }
func (c *fakeClient) Close() error {
	c.closed++
	return nil
}

func (c *fakeClient) CPUInfo() (string, error) {
	c.cpuCalls++
	return c.cpuInfo, c.cpuErr
}

var _ = Describe("S7Comm input connection that must not change", func() {
	BeforeEach(func() {
		testActive, ok := os.LookupEnv("TEST_S7COMM_UNITTEST")
		if !ok || strings.ToLower(testActive) == "false" {
			Skip("Skipping test: TEST_S7COMM_UNITTEST env variable is not set or is FALSE")
		}
	})

	newInput := func(addresses []string, client *fakeClient) *s7comm_plugin.S7CommInput {
		parsed, err := s7comm_plugin.ParseAddresses(addresses)
		Expect(err).NotTo(HaveOccurred())

		input := &s7comm_plugin.S7CommInput{
			TcpDevice:        "plc.local:102",
			Rack:             2,
			Slot:             3,
			Timeout:          7 * time.Second,
			TimeBetweenReads: time.Millisecond,
			ParsedAddresses:  parsed,
			Log:              service.MockResources().Logger(),
		}
		input.UseClientFactoryForTest(func(string, int, int, time.Duration) (s7comm_plugin.Client, error) { return client, nil })

		return input
	}

	It("opens the connection with the configured device, rack, slot and timeout", func() {
		var got struct {
			device       string
			rack, slot   int
			timeout      time.Duration
			factoryCalls int
		}
		input := newInput([]string{"DB1.W0"}, &fakeClient{pduLength: 240})
		input.UseClientFactoryForTest(func(device string, rack int, slot int, timeout time.Duration) (s7comm_plugin.Client, error) {
			got.device, got.rack, got.slot, got.timeout = device, rack, slot, timeout
			got.factoryCalls++
			return &fakeClient{pduLength: 240}, nil
		})

		Expect(input.Connect(context.Background())).To(Succeed())

		Expect(got.factoryCalls).To(Equal(1))
		Expect(got.device).To(Equal("plc.local:102"))
		Expect([]int{got.rack, got.slot}).To(Equal([]int{2, 3}))
		Expect(got.timeout).To(Equal(7 * time.Second))
	})

	It("splits the addresses by the PDU size the PLC negotiated", func() {
		input := newInput(byteAddresses(30), &fakeClient{pduLength: 55})

		Expect(input.Connect(context.Background())).To(Succeed())

		Expect(input.Batches).To(HaveLen(10))
		for _, batch := range input.Batches {
			Expect(batch).To(HaveLen(3))
		}
	})

	It("returns the error of a failed connect unchanged and stays unconnected", func() {
		refused := errors.New("dial tcp: connection refused")
		input := newInput([]string{"DB1.W0"}, nil)
		input.UseClientFactoryForTest(func(string, int, int, time.Duration) (s7comm_plugin.Client, error) { return nil, refused })

		err := input.Connect(context.Background())

		Expect(err).To(MatchError(refused))
		_, _, readErr := input.ReadBatch(context.Background())
		Expect(readErr).To(MatchError(ContainSubstring("not initialized")))
	})

	It("fails when an address does not fit the negotiated PDU", func() {
		input := newInput(stringAddresses(1, 100), &fakeClient{pduLength: 126})

		err := input.Connect(context.Background())

		Expect(err).To(MatchError(ContainSubstring("failed to build batches")))
	})

	It("asks the CPU for its information, and a failure does not stop the connect", func() {
		client := &fakeClient{pduLength: 240, cpuErr: errors.New("function refused by CPU")}
		input := newInput([]string{"DB1.W0"}, client)

		Expect(input.Connect(context.Background())).To(Succeed())

		Expect(client.cpuCalls).To(Equal(1))
	})

	It("does not ask the CPU when the user disabled it", func() {
		client := &fakeClient{pduLength: 240}
		input := newInput([]string{"DB1.W0"}, client)
		input.DisableCPUInfo = true

		Expect(input.Connect(context.Background())).To(Succeed())

		Expect(client.cpuCalls).To(BeZero())
	})

	It("closes the connection once, and reading afterwards fails", func() {
		client := &fakeClient{pduLength: 240}
		input := newInput([]string{"DB1.W0"}, client)
		Expect(input.Connect(context.Background())).To(Succeed())

		Expect(input.Close(context.Background())).To(Succeed())
		Expect(input.Close(context.Background())).To(Succeed())

		Expect(client.closed).To(Equal(1))
		_, _, err := input.ReadBatch(context.Background())
		Expect(err).To(MatchError(ContainSubstring("not initialized")))
	})

	It("reads the connection fields of the config, with their defaults", func() {
		conf, err := s7comm_plugin.S7CommConfigSpec.ParseYAML("tcpDevice: plc.local\naddresses: [DB1.W0]", nil)
		Expect(err).NotTo(HaveOccurred())

		connection, err := s7comm_plugin.ParseConnectionForTest(conf)

		Expect(err).NotTo(HaveOccurred())
		Expect(connection).To(Equal(s7comm_plugin.ConnectionForTest{TCPDevice: "plc.local", Rack: 0, Slot: 1, Timeout: 10 * time.Second}))
	})

	It("reads the timeout of the config in seconds", func() {
		conf, err := s7comm_plugin.S7CommConfigSpec.ParseYAML("tcpDevice: plc.local\nrack: 1\nslot: 4\ntimeout: 5\naddresses: [DB1.W0]", nil)
		Expect(err).NotTo(HaveOccurred())

		connection, err := s7comm_plugin.ParseConnectionForTest(conf)

		Expect(err).NotTo(HaveOccurred())
		Expect(connection).To(Equal(s7comm_plugin.ConnectionForTest{TCPDevice: "plc.local", Rack: 1, Slot: 4, Timeout: 5 * time.Second}))
	})
})
