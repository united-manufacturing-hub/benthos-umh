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

var _ = Describe("createMessageFromValue tag metadata", func() {
	metaOf := func(correctTagPath bool, path, browseName string, nodeID *ua.NodeID) (string, string, string) {
		input := &OPCUAInput{OPCUAConnection: &OPCUAConnection{}, CorrectTagPath: correctTagPath}
		dv := &ua.DataValue{Value: ua.MustVariant(int16(1)), Status: ua.StatusOK}
		msg := input.createMessageFromValue(dv, NodeDef{NodeID: nodeID, BrowseName: browseName, Path: path})
		Expect(msg).NotTo(BeNil())
		tagPath, _ := msg.MetaGet("opcua_tag_path")
		tagGroup, _ := msg.MetaGet("opcua_tag_group")
		tagName, _ := msg.MetaGet("opcua_tag_name")
		return tagPath, tagGroup, tagName
	}

	// Legacy mode (compatibility.correct_tag_path unset or false) is frozen
	// behaviour: every expectation in this table is today's output, malformed
	// paths and all, and must never change (ENG-7953).
	DescribeTable("legacy mode: sets opcua_tag_path, opcua_tag_group and opcua_tag_name",
		func(path, browseName, wantPath, wantGroup, wantName string) {
			gotPath, gotGroup, gotName := metaOf(false, path, browseName, ua.NewNumericNodeID(3, 1))
			Expect(gotPath).To(Equal(wantPath), "opcua_tag_path")
			Expect(gotGroup).To(Equal(wantGroup), "opcua_tag_group")
			Expect(gotName).To(Equal(wantName), "opcua_tag_name")
		},
		Entry("array element",
			"DB_00Line.Station.aSensor.1", "1",
			"DB_00Line.Station.aSensor",
			"DB_00Line.Station.aSensor", "1"),
		Entry("field of an array element",
			"DB_00Line.Station.aSensor.0.iValue", "iValue",
			"DB_00Line.Station.aSensor.0",
			"DB_00Line.Station.aSensor.0", "iValue"),
		Entry("node at the root of the browse", "xLifeBit", "xLifeBit", "", "xLifeBit", "xLifeBit"),
		Entry("node without a browse path", "", "ProductName", "", "ProductName", "ProductName"),
		Entry("browse name that sanitize changes", "Line.Pump_1", "Pump 1", "Line.Pump_1", "Line.Pump_1", "Pump_1"),
		Entry("root node whose browse name sanitize changes", "Pump_1", "Pump 1", "", "Pump_1", "Pump_1"),
		// The bug cases, asserted in legacy mode to pin the frozen malformed
		// output the flag opts out of.
		Entry("array element 0 whose name repeats in the path",
			"DB_00Line.Station.aSensor.0", "0",
			"DB_0Line.Station.aSensor.0",
			"DB_0Line.Station.aSensor.0", "0"),
		Entry("name that ends a parent segment",
			"Line.pump.pump", "pump",
			"Line..pump",
			"Line..pump", "pump"),
		Entry("name that starts a parent segment",
			"TempSensor.Temp", "Temp",
			"Sensor.Temp",
			"Sensor.Temp", "Temp"),
		Entry("parent with the same name as the node",
			"Temp.Temp", "Temp",
			".Temp",
			".Temp", "Temp"),
		Entry("grandparent with the same name",
			"Temp.Sub.Temp", "Temp",
			".Sub.Temp",
			".Sub.Temp", "Temp"),
	)

	// Corrected mode (compatibility.correct_tag_path: true), ENG-7953.
	DescribeTable("corrected mode: sets opcua_tag_path, opcua_tag_group and opcua_tag_name",
		func(path, browseName, wantPath, wantGroup, wantName string) {
			gotPath, gotGroup, gotName := metaOf(true, path, browseName, ua.NewNumericNodeID(3, 1))
			Expect(gotPath).To(Equal(wantPath), "opcua_tag_path")
			Expect(gotGroup).To(Equal(wantGroup), "opcua_tag_group")
			Expect(gotName).To(Equal(wantName), "opcua_tag_name")
		},
		Entry("array element 0 whose name repeats in the path",
			"DB_00Line.Station.aSensor.0", "0",
			"DB_00Line.Station.aSensor",
			"DB_00Line.Station.aSensor", "0"),
		Entry("name that ends a parent segment",
			"Line.pump.pump", "pump",
			"Line.pump",
			"Line.pump", "pump"),
		Entry("name that starts a parent segment",
			"TempSensor.Temp", "Temp",
			"TempSensor",
			"TempSensor", "Temp"),
		Entry("parent with the same name as the node",
			"Temp.Temp", "Temp",
			"Temp",
			"Temp", "Temp"),
		Entry("grandparent with the same name",
			"Temp.Sub.Temp", "Temp",
			"Temp.Sub",
			"Temp.Sub", "Temp"),
		Entry("node at the root of the browse", "xLifeBit", "xLifeBit", "", "xLifeBit", "xLifeBit"),
		Entry("node with no browse path", "", "ProductName", "", "ProductName", "ProductName"),
		Entry("browse name that sanitize changes", "Line.Pump_1", "Pump 1", "Line.Pump_1", "Line.Pump_1", "Pump_1"),
		Entry("root node whose browse name sanitize changes", "Pump_1", "Pump 1", "", "Pump_1", "Pump_1"),
	)

	// UpdateNodePaths deduplicates two nodes with the same browse path by
	// putting a sanitized NodeID in the last path segment. Cutting that
	// segment (as the legacy calculation can, when the browse name also appears
	// earlier in the path) would give both nodes the same topic again.
	It("keeps two deduplicated nodes on two different topics in both modes", func() {
		base := func() []NodeDef {
			return []NodeDef{
				{NodeID: ua.NewNumericNodeID(2, 1001), BrowseName: "Temp", Path: "Temp.Temp"},
				{NodeID: ua.NewNumericNodeID(2, 1002), BrowseName: "Temp", Path: "Temp.Temp"},
			}
		}

		legacyNodes := base()
		UpdateNodePaths(legacyNodes)
		correctedNodes := base()
		UpdateNodePaths(correctedNodes)

		// Deduplication itself is byte-identical between the modes: it runs
		// before the topic calculation and knows nothing about the flag.
		Expect(legacyNodes).To(Equal(correctedNodes))
		Expect(legacyNodes[0].Path).To(Equal("Temp.ns_2_i_1001"))
		Expect(legacyNodes[1].Path).To(Equal("Temp.ns_2_i_1002"))

		for _, correct := range []bool{false, true} {
			input := &OPCUAInput{OPCUAConnection: &OPCUAConnection{}, CorrectTagPath: correct}
			first := input.tagTopicPartsOf(legacyNodes[0])
			second := input.tagTopicPartsOf(legacyNodes[1])
			Expect(first.topic()).NotTo(Equal(second.topic()), "mode correct_tag_path=%v", correct)
		}
	})
})
