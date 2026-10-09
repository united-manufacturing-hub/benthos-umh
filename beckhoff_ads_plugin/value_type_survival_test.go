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

package beckhoff_ads_plugin

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin/tagprocessortest"
)

var _ = Describe("Beckhoff ADS value type through tag_processor", func() {
	// go-ads decodes every symbol into a Go string; the plugin derives the
	// type from the symbol's base type, so each entry pairs the base type
	// and the go-ads string with the PLC value they represent.
	DescribeTable("keeps the type and value of the symbol value",
		func(baseType string, goAdsValue string, plcValue any) {
			input := &AdsCommInput{}
			symbol := &PlcSymbol{Name: "MAIN.tag", DataType: baseType, BaseType: baseType}

			message := input.newSymbolMessage(symbol, goAdsValue, time.Time{})

			tagprocessortest.ExpectTagProcessorKeepsValue(plcValue, message)
		},
		Entry("numeric STRING", "STRING", tagprocessortest.NumericString, tagprocessortest.NumericString),
		Entry("non-numeric STRING", "STRING", tagprocessortest.NonNumericString, tagprocessortest.NonNumericString),
		Entry("17-digit numeric STRING", "STRING", tagprocessortest.SeventeenDigitString, tagprocessortest.SeventeenDigitString),
		Entry("INT", "INT", "-2", tagprocessortest.NegativeInteger),
		Entry("DINT", "DINT", "42", tagprocessortest.PositiveInteger),
		Entry("REAL", "REAL", "23.5", tagprocessortest.FractionalNumber),
		Entry("LREAL", "LREAL", "23.5", tagprocessortest.FractionalNumber),
		Entry("BOOL", "BOOL", "true", true),
	)
})
