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

package s7comm_plugin_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/s7comm_plugin"
)

var _ = Describe("S7Comm Plugin Unittests", func() {
	BeforeEach(func() {
		testActive := os.Getenv("TEST_S7COMM_UNITTEST")

		// Check if environment variables are set
		if testActive == "" {
			Skip("Skipping test: environment variables are not set")
			return
		}
	})

	Describe("Parsing Addresses", func() {
		type testCase struct {
			address            string
			inputBytesHex      string
			expectedConversion interface{}
		}

		It("correctly parses addresses and converts input bytes", func() {
			tests := []testCase{
				{"DB2.W0", "0000", uint16(0)},
				{"DB2.W1", "0001", uint16(1)},
				// Regression: DT requires an 8-byte buffer.
				{"DB1.DT0", "2401151030451230",
					time.Date(2024, 1, 15, 10, 30, 45, 123*1000000, time.UTC).UnixNano()},
			}

			for _, tc := range tests {
				By("Testing address "+tc.address+" with bytes "+tc.inputBytesHex, func() {
					addresses := []string{tc.address}

					parsedAddresses, err := s7comm_plugin.ParseAddresses(addresses)
					Expect(err).NotTo(HaveOccurred())
					Expect(parsedAddresses).To(HaveLen(1))

					converterFunc := parsedAddresses[0].ConverterFunc

					inputBytes, err := hex.DecodeString(tc.inputBytesHex)
					Expect(err).NotTo(HaveOccurred())

					actualConversionResult := converterFunc(inputBytes)
					Expect(actualConversionResult).To(Equal(tc.expectedConversion))
				})
			}
		})
	})

	type S7Addresses struct {
		addresses      []string
		expectedErrMsg []string
	}

	DescribeTable("Parsing duplicate Addresses", func(entries S7Addresses) {
		_, err := s7comm_plugin.ParseAddresses(entries.addresses)

		if entries.expectedErrMsg != nil {
			Expect(err).To(HaveOccurred())
			for _, containedErrStr := range entries.expectedErrMsg {
				Expect(err.Error()).To(ContainSubstring(containedErrStr))
			}
			return
		}
		Expect(err).NotTo(HaveOccurred())
	},
		Entry("same DBNumber but different Item.Start",
			S7Addresses{
				addresses:      []string{"DB2.W0", "DB2.W2"},
				expectedErrMsg: nil,
			}),
		Entry("same DBNumber but different Item.Bit",
			S7Addresses{
				addresses:      []string{"DB2.X0.0", "DB2.X0.1"},
				expectedErrMsg: nil,
			}),
		Entry("same Bit but different Item.DBNumber",
			S7Addresses{
				addresses:      []string{"DB2.X0.0", "DB3.X0.0"},
				expectedErrMsg: nil,
			}),
		Entry("same Area but different Item.Bit",
			S7Addresses{
				addresses:      []string{"PE.X0.0", "PE.X0.1"},
				expectedErrMsg: nil,
			}),
		Entry("non-DB area with block number is deprecated but still accepted",
			S7Addresses{
				addresses:      []string{"PE2.X0.0"},
				expectedErrMsg: nil,
			}),
		Entry("same DBNumber and same Item.Area",
			S7Addresses{
				addresses:      []string{"DB2.W0", "DB2.W2", "DB2.W0"},
				expectedErrMsg: []string{"duplicate address", "DB2.W0"},
			}),
		Entry("same DBNumber and same Item.Bit",
			S7Addresses{
				addresses:      []string{"DB2.X0.0", "DB2.W2", "DB2.X0.0"},
				expectedErrMsg: []string{"duplicate address", "DB2.X0.0"},
			}),
		Entry("DB without block number is rejected",
			S7Addresses{
				addresses:      []string{"DB.W0"},
				expectedErrMsg: []string{"DB requires a block number"},
			}),
	)

	Describe("BuildBatches", func() {
		DescribeTable("splits 30 addresses correctly for different PDU sizes",
			func(pduSize, expectedBatches, expectedFirst, expectedLast int) {
				// Create 30 WORD addresses
				addresses := make([]string, 30)
				for i := range 30 {
					addresses[i] = fmt.Sprintf("DB1.W%d", i*2)
				}

				parsed, err := s7comm_plugin.ParseAddresses(addresses)
				Expect(err).NotTo(HaveOccurred())
				Expect(parsed).To(HaveLen(30))

				batches, err := s7comm_plugin.BuildBatches(parsed, pduSize)
				Expect(err).NotTo(HaveOccurred())

				Expect(batches).To(HaveLen(expectedBatches))
				Expect(batches[0]).To(HaveLen(expectedFirst))
				Expect(batches[len(batches)-1]).To(HaveLen(expectedLast))
			},
			Entry("PDU 240 - limited by request size", 240, 2, 18, 12),
			Entry("PDU 480 - limited by 20-item protocol limit", 480, 2, 20, 10),
			Entry("PDU 960 - limited by 20-item protocol limit", 960, 2, 20, 10),
		)

		It("returns error when single item exceeds PDU size", func() {
			parsed, err := s7comm_plugin.ParseAddresses([]string{"DB1.S0.250"})
			Expect(err).NotTo(HaveOccurred())

			_, err = s7comm_plugin.BuildBatches(parsed, 240)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exceeds PDU size"))
		})
	})
})

var _ = Describe("S7Comm Test Against Local PLC", func() {
	Describe("Communication with a Remote S7 Instance", func() {
		var (
			endpoint string
			rack     int
			slot     int
			ctx      context.Context
			input    *s7comm_plugin.S7CommInput
			cancel   context.CancelFunc
		)

		BeforeEach(func() {
			endpoint = os.Getenv("TEST_S7_TCPDEVICE")
			rackStr := os.Getenv("TEST_S7_RACK")
			slotStr := os.Getenv("TEST_S7_SLOT")

			if endpoint == "" || rackStr == "" || slotStr == "" {
				Skip("Skipping test: environment variables not set")
			}

			var err error
			rack, err = strconv.Atoi(rackStr)
			Expect(err).NotTo(HaveOccurred())

			slot, err = strconv.Atoi(slotStr)
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

			addresses := []string{"DB2.W0"}

			parsedAddresses, err := s7comm_plugin.ParseAddresses(addresses)
			Expect(err).NotTo(HaveOccurred())

			input = &s7comm_plugin.S7CommInput{
				TcpDevice:       endpoint,
				Rack:            rack,
				Slot:            slot,
				ParsedAddresses: parsedAddresses,
			}
		})

		AfterEach(func() {
			if input != nil && ctx != nil {
				input.Close(ctx)
			}

			if ctx != nil {
				cancel()
			}
		})

		It("connects and reads data successfully", func() {
			By("Connecting to the remote instance", func() {
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())
			})

			By("Reading data", func() {
				messageBatch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(messageBatch).To(HaveLen(1))

				for _, message := range messageBatch {
					messageStructuredMut, err := message.AsStructuredMut()
					Expect(err).NotTo(HaveOccurred())
					Expect(messageStructuredMut).To(BeAssignableToTypeOf(json.Number("22.565684")))

					s7Address, wasFound := message.MetaGet("s7_address")
					Expect(wasFound).To(BeTrue())
					Expect(s7Address).To(Equal("DB2.W0"))
				}
			})
		})
	})
})
