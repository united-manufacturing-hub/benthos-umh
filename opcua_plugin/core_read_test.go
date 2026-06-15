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
	"context"
	"encoding/json"
	"time"

	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin"
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

	// Full-chain regression for ENG-5011: the message built by createMessageFromValue
	// must survive tag_processor unchanged when datatype is wired from opcua_tag_type,
	// mirroring a real bridge pipeline.
	It("preserves a numeric-looking string through tag_processor", func() {
		input := &OPCUAInput{OPCUAConnection: &OPCUAConnection{}}
		value := "83280661010132726"
		dv := &ua.DataValue{Value: ua.MustVariant(value), Status: ua.StatusOK}
		nodeDef := NodeDef{NodeID: ua.NewNumericNodeID(0, 1234)}

		inputMsg := input.createMessageFromValue(dv, nodeDef)
		Expect(inputMsg).NotTo(BeNil())

		builder := service.NewStreamBuilder()

		msgHandler, err := builder.AddProducerFunc()
		Expect(err).NotTo(HaveOccurred())

		err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "lastMarkingCode";
    msg.meta.datatype = msg.meta.opcua_tag_type;
    return msg;
`)
		Expect(err).NotTo(HaveOccurred())

		var messages []*service.Message
		err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
			messages = append(messages, msg)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())

		stream, err := builder.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go func() {
			_ = stream.Run(ctx)
		}()

		err = msgHandler(ctx, inputMsg)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int {
			return len(messages)
		}).Should(Equal(1))

		structured, err := messages[0].AsStructured()
		Expect(err).NotTo(HaveOccurred())

		payload, ok := structured.(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(payload["value"]).To(Equal(value))
	})
})
