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

package opcua_plugin

import (
	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin/tagprocessortest"
)

var _ = Describe("OPC UA value type through tag_processor", func() {
	DescribeTable("keeps the type and value of the node value",
		func(nodeValue any) {
			input := &OPCUAInput{OPCUAConnection: &OPCUAConnection{}}
			dataValue := &ua.DataValue{Value: ua.MustVariant(nodeValue), Status: ua.StatusOK}
			nodeDef := NodeDef{NodeID: ua.NewNumericNodeID(0, 1234), BrowseName: "tag"}

			message := input.createMessageFromValue(dataValue, nodeDef)
			Expect(message).NotTo(BeNil())

			tagprocessortest.ExpectTagProcessorKeepsValue(nodeValue, message)
		},
		Entry("numeric String", tagprocessortest.NumericString),
		Entry("non-numeric String", tagprocessortest.NonNumericString),
		// duplicates "preserves a numeric-looking string through tag_processor" in
		// core_read_test.go, which also sets msg.meta.datatype from opcua_tag_type
		Entry("17-digit numeric String", tagprocessortest.SeventeenDigitString),
		Entry("Int16", int16(tagprocessortest.NegativeInteger)),
		Entry("Int32", int32(tagprocessortest.PositiveInteger)),
		Entry("Int64", int64(tagprocessortest.NegativeInteger)),
		Entry("UInt32", uint32(tagprocessortest.PositiveInteger)),
		Entry("Float", float32(tagprocessortest.FractionalNumber)),
		Entry("Double", float64(tagprocessortest.FractionalNumber)),
		Entry("Boolean", true),
		Entry("Int32 array", []int32{tagprocessortest.PositiveInteger, tagprocessortest.NegativeInteger}),
		Entry("Double array", []float64{tagprocessortest.FractionalNumber, tagprocessortest.NegativeInteger}),
		Entry("Boolean array", []bool{true, false}),
		Entry("String array", []string{tagprocessortest.NumericString, tagprocessortest.NonNumericString}),
	)
})
