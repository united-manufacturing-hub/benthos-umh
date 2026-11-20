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

package tag_processor_plugin

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeTable("convertValue datatype handling",
	func(input any, datatype string, want any, wantErr bool) {
		p := &TagProcessor{}
		got, err := p.convertValue(input, datatype)
		if wantErr {
			Expect(err).To(HaveOccurred())
			return
		}
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(want))
	},

	// auto-detect (no msg.meta.datatype set)
	Entry("bool passes through", true, "", true, false),
	Entry("float to json.Number", 23.5, "", json.Number("23.5"), false),
	Entry("int to json.Number", 42, "", json.Number("42"), false),
	Entry("non-numeric string stays string", "hello", "", "hello", false),
	Entry("numeric string stays string", "2340925", "", "2340925", false),
	Entry("float-string stays string", "23.5", "", "23.5", false),
	Entry("large-int string stays exact", "12345678901234567890", "", "12345678901234567890", false),
	Entry("array to JSON string", []any{1, 2}, "", "[1,2]", false),
	Entry("object to JSON string", map[string]any{"a": 1}, "", `{"a":1}`, false),

	// explicit msg.meta.datatype override
	Entry("datatype number coerces numeric string", "42", "number", json.Number("42"), false),
	Entry("datatype number keeps digits exact", "2340925", "number", json.Number("2340925"), false),
	Entry("datatype number rejects non-numeric string", "abc", "number", nil, true),
	Entry("datatype string forces number to string", 42, "string", "42", false),
	Entry("datatype string keeps numeric string", "2340925", "string", "2340925", false),
	Entry("datatype bool coerces string", "true", "bool", true, false),
	Entry("datatype bool rejects non-bool string", "yes", "bool", nil, true),
)
