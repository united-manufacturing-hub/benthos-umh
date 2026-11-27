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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/robinson/gos7"

	"github.com/united-manufacturing-hub/benthos-umh/s7comm_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin/tagprocessortest"
)

// fakeS7Client answers AGReadMulti with fixed PLC bytes. ReadBatch passes
// copies of the items whose Data slices share memory with the parsed
// addresses, so copy() makes the bytes visible to the converter.
type fakeS7Client struct {
	gos7.Client
	plcBytes []byte
}

func (c *fakeS7Client) AGReadMulti(dataItems []gos7.S7DataItem, _ int) error {
	copy(dataItems[0].Data, c.plcBytes)
	return nil
}

// s7String prepends the S7 STRING header: maximum length, then actual length.
func s7String(value string) []byte {
	return append([]byte{byte(len(value)), byte(len(value))}, value...)
}

var _ = Describe("S7 value type through tag_processor", func() {
	DescribeTable("keeps the type and value of the PLC value",
		func(address string, plcBytes []byte, plcValue any) {
			parsedAddresses, err := s7comm_plugin.ParseAddresses([]string{address})
			Expect(err).NotTo(HaveOccurred())
			input := &s7comm_plugin.S7CommInput{
				Client:  &fakeS7Client{plcBytes: plcBytes},
				Batches: [][]s7comm_plugin.S7DataItemWithAddressAndConverter{parsedAddresses},
			}

			batch, _, err := input.ReadBatch(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(batch).To(HaveLen(1))

			tagprocessortest.ExpectTagProcessorKeepsValue(plcValue, batch[0])
		},
		Entry("numeric STRING", "DB1.S0.4", s7String(tagprocessortest.NumericString), tagprocessortest.NumericString),
		Entry("non-numeric STRING", "DB1.S0.3", s7String(tagprocessortest.NonNumericString), tagprocessortest.NonNumericString),
		Entry("17-digit numeric STRING", "DB1.S0.17", s7String(tagprocessortest.SeventeenDigitString), tagprocessortest.SeventeenDigitString),
		Entry("digit CHAR", "DB1.C0", []byte{'5'}, "5"),
		Entry("INT", "DB1.I0", []byte{0xFF, 0xFE}, tagprocessortest.NegativeInteger),
		Entry("DINT", "DB1.DI0", []byte{0x00, 0x00, 0x00, 0x2A}, tagprocessortest.PositiveInteger),
		Entry("REAL", "DB1.R0", []byte{0x41, 0xBC, 0x00, 0x00}, tagprocessortest.FractionalNumber),
		Entry("BOOL", "DB1.X0.0", []byte{0x01}, true),
	)
})
