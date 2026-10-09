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

package eip_plugin_test

import (
	"context"
	"reflect"

	"github.com/danomagnum/gologix"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/eip_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin/tagprocessortest"
)

const survivalTagName = "tag"

var _ = Describe("Ethernet/IP value type through tag_processor", func() {
	DescribeTable("keeps the type and value of the tag",
		func(cipType gologix.CIPType, plcValue any) {
			item := &CIPReadItem{TagName: survivalTagName, CIPDatatype: cipType}
			if reflect.ValueOf(plcValue).Kind() == reflect.Slice {
				item.IsArray = true
				item.ArrayLength = reflect.ValueOf(plcValue).Len()
			}
			input := &EIPInput{
				Items: []*CIPReadItem{item},
				CIP:   &MockCIPReader{Tags: map[string]any{survivalTagName: plcValue}},
			}

			batch, _, err := input.ReadBatch(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(batch).To(HaveLen(1))

			tagprocessortest.ExpectTagProcessorKeepsValue(plcValue, batch[0])
		},
		Entry("numeric STRING", gologix.CIPTypeSTRING, tagprocessortest.NumericString),
		Entry("non-numeric STRING", gologix.CIPTypeSTRING, tagprocessortest.NonNumericString),
		Entry("17-digit numeric STRING", gologix.CIPTypeSTRING, tagprocessortest.SeventeenDigitString),
		Entry("INT", gologix.CIPTypeINT, int16(tagprocessortest.NegativeInteger)),
		Entry("DINT", gologix.CIPTypeDINT, int32(tagprocessortest.PositiveInteger)),
		Entry("REAL", gologix.CIPTypeREAL, float32(tagprocessortest.FractionalNumber)),
		Entry("LREAL", gologix.CIPTypeLREAL, float64(tagprocessortest.FractionalNumber)),
		Entry("BOOL", gologix.CIPTypeBOOL, true),
		Entry("INT array", gologix.CIPTypeINT, []int16{tagprocessortest.NegativeInteger, tagprocessortest.PositiveInteger}),
		Entry("DINT array", gologix.CIPTypeDINT, []int32{tagprocessortest.NegativeInteger, tagprocessortest.PositiveInteger}),
		Entry("REAL array", gologix.CIPTypeREAL, []float32{tagprocessortest.FractionalNumber, tagprocessortest.NegativeInteger}),
	)
})
