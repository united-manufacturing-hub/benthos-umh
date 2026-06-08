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

package opcua_plugin

import (
	"encoding/json"

	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// ENG-5011: an OPC UA String value must be emitted as a valid JSON string so it
// round-trips as a string downstream. A numeric-looking string ("83280661010132726")
// must not be re-parsed as a (lossy) number by the downstream json.Unmarshal.
var _ = Describe("getBytesFromValue string encoding", func() {
	DescribeTable("emits a valid JSON string that round-trips losslessly",
		func(value string, expectedBytes string) {
			g := &OPCUAConnection{}
			dv := &ua.DataValue{Value: ua.MustVariant(value), Status: ua.StatusOK}

			b, tagType := g.getBytesFromValue(dv, NodeDef{})

			Expect(string(b)).To(Equal(expectedBytes))
			Expect(tagType).To(Equal("string"))

			// Downstream contract: unmarshalling the body yields the original string.
			var decoded any
			Expect(json.Unmarshal(b, &decoded)).To(Succeed())
			Expect(decoded).To(Equal(value))
		},
		Entry("numeric-looking string from the bug report", "83280661010132726", `"83280661010132726"`),
		Entry("plain string", "hello", `"hello"`),
		Entry("string with a quote", `he"llo`, `"he\"llo"`),
		Entry("string with a newline", "line1\nline2", `"line1\nline2"`),
	)
})
