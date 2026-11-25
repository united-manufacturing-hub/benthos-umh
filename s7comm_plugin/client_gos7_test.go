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

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/robinson/gos7"

	"github.com/united-manufacturing-hub/benthos-umh/s7comm_plugin"
)

// gos7Stub stands in for the gos7 client behind the adapter.
type gos7Stub struct {
	readErr  error
	refused  map[int]string // item index to the text gos7 reports for it
	cpuInfo  gos7.S7CpuInfo
	cpuErr   error
	received []gos7.S7DataItem
}

func (g *gos7Stub) AGReadMulti(items []gos7.S7DataItem, count int) error {
	Expect(count).To(Equal(len(items)))
	g.received = append(g.received, items...)
	if g.readErr != nil {
		return g.readErr
	}
	for index := range items {
		copy(items[index].Data, []byte{0xAB, 0xCD, 0xEF, 0x01}[:len(items[index].Data)])
		items[index].Error = g.refused[index]
	}

	return nil
}

func (g *gos7Stub) GetCPUInfo() (gos7.S7CpuInfo, error) { return g.cpuInfo, g.cpuErr }

var _ = Describe("S7Comm gos7 adapter", func() {
	BeforeEach(func() {
		testActive, ok := os.LookupEnv("TEST_S7COMM_UNITTEST")
		if !ok || strings.ToLower(testActive) == "false" {
			Skip("Skipping test: TEST_S7COMM_UNITTEST env variable is not set or is FALSE")
		}
	})

	It("sends every field of an item to gos7 and takes the bytes it filled in", func() {
		stub := &gos7Stub{}
		client := s7comm_plugin.NewGos7ClientForTest(stub, 240)
		items := []s7comm_plugin.Item{{Area: 0x84, WordLength: 0x01, DBNumber: 7, ByteOffset: 3, Bit: 5, Amount: 1, Data: make([]byte, 1)}}

		Expect(client.ReadItems(context.Background(), items)).To(Succeed())

		Expect(stub.received).To(HaveLen(1))
		sent := stub.received[0]
		Expect([]int{sent.Area, sent.WordLen, sent.DBNumber, sent.Start, sent.Bit, sent.Amount}).To(Equal([]int{0x84, 0x01, 7, 3, 5, 1}))
		Expect(items[0].Data).To(Equal([]byte{0xAB}))
		Expect(items[0].Err).NotTo(HaveOccurred())
	})

	It("reports an item the PLC refused in that item and not as an error", func() {
		stub := &gos7Stub{refused: map[int]string{1: "Item not available"}}
		client := s7comm_plugin.NewGos7ClientForTest(stub, 240)
		items := []s7comm_plugin.Item{
			{Area: 0x84, WordLength: 0x04, Amount: 1, Data: make([]byte, 2)},
			{Area: 0x84, WordLength: 0x04, DBNumber: 99, Amount: 1, Data: make([]byte, 2)},
		}

		Expect(client.ReadItems(context.Background(), items)).To(Succeed())

		Expect(items[0].Err).NotTo(HaveOccurred())
		Expect(items[1].Err).To(MatchError("Item not available"))
	})

	It("returns the error of a failed request unchanged", func() {
		reset := errors.New("connection reset by peer")
		client := s7comm_plugin.NewGos7ClientForTest(&gos7Stub{readErr: reset}, 240)

		err := client.ReadItems(context.Background(), []s7comm_plugin.Item{{Area: 0x84, WordLength: 0x04, Amount: 1, Data: make([]byte, 2)}})

		Expect(err).To(MatchError(reset))
	})

	It("reports the PDU size it negotiated", func() {
		Expect(s7comm_plugin.NewGos7ClientForTest(&gos7Stub{}, 480).PDULength()).To(Equal(480))
	})

	It("describes the CPU the way the log has always shown it", func() {
		stub := &gos7Stub{cpuInfo: gos7.S7CpuInfo{ModuleTypeName: "CPU 1211C", SerialNumber: "S1", ASName: "PLC_1", Copyright: "Siemens", ModuleName: "CPU"}}

		info, err := s7comm_plugin.NewGos7ClientForTest(stub, 240).CPUInfo()

		Expect(err).NotTo(HaveOccurred())
		Expect(info).To(Equal("{CPU 1211C S1 PLC_1 Siemens CPU}"))
	})

	It("returns the error of a CPU information request", func() {
		refused := errors.New("function refused by CPU")

		_, err := s7comm_plugin.NewGos7ClientForTest(&gos7Stub{cpuErr: refused}, 240).CPUInfo()

		Expect(err).To(MatchError(refused))
	})

	It("closes through the function it was given, and closing without one is fine", func() {
		closed := 0
		client := s7comm_plugin.NewGos7ClientWithCloseForTest(&gos7Stub{}, 240, func() error {
			closed++
			return nil
		})

		Expect(client.Close()).To(Succeed())

		Expect(closed).To(Equal(1))
		Expect(s7comm_plugin.NewGos7ClientForTest(&gos7Stub{}, 240).Close()).To(Succeed())
	})

	It("returns the error of a connection that cannot be opened unchanged", func() {
		_, err := s7comm_plugin.ConnectGos7ForTest(context.Background(), "127.0.0.1:1", 0, 1, time.Second)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).NotTo(ContainSubstring("connecting to S7 PLC"))
	})
})
