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
	"os"

	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("getBytesFromValue", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	var (
		conn    *OPCUAConnection
		nodeDef NodeDef
	)

	BeforeEach(func() {
		logger := service.MockResources().Logger()
		conn = &OPCUAConnection{
			Log: logger,
		}
		nodeDef = NodeDef{
			NodeID:     ua.NewNumericNodeID(0, 1001),
			BrowseName: "TestNode",
		}
	})

	It("should return nil for non-OK status (StatusBadDataTypeIDUnknown)", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusBadDataTypeIDUnknown,
			Value:  ua.MustVariant(&ua.ExtensionObject{Value: nil}),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(BeNil())
		Expect(tagType).To(BeEmpty())
	})

	It("should return nil for BAD status (StatusBadNodeIDUnknown)", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusBadNodeIDUnknown,
			Value:  ua.MustVariant(int32(42)),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(BeNil())
		Expect(tagType).To(BeEmpty())
	})

	It("should still emit bytes for UNCERTAIN status with a valid value", func() {
		// UNCERTAIN codes (top bits 0b01) carry usable data per OPC UA Part 8
		// and must not be silently dropped alongside BAD codes.
		dataValue := &ua.DataValue{
			Status: ua.StatusUncertain,
			Value:  ua.MustVariant(int32(42)),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(Equal([]byte("42")))
		Expect(tagType).To(Equal("number"))
	})

	It("should return bytes for OK status with int32 value", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant(int32(42)),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(Equal([]byte("42")))
		Expect(tagType).To(Equal("number"))
	})

	It("should return bytes for OK status with float64 value", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant(float64(3.14)),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(Equal([]byte("3.14")))
		Expect(tagType).To(Equal("number"))
	})

	It("should return bytes for OK status with string value", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant("hello"),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(Equal([]byte("hello")))
		Expect(tagType).To(Equal("string"))
	})

	It("should return bytes for OK status with bool value", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant(true),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(Equal([]byte("true")))
		Expect(tagType).To(Equal("bool"))
	})

	It("should return nil when variant is nil", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  nil,
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(BeNil())
		Expect(tagType).To(BeEmpty())
	})

	Context("ExtensionObject handling", func() {
		It("should return nil for ExtensionObject with nil Value (undecodable UDT)", func() {
			extObj := &ua.ExtensionObject{
				TypeID: &ua.ExpandedNodeID{
					NodeID: ua.NewNumericNodeID(4, 202),
				},
				EncodingMask: ua.ExtensionObjectBinary,
				Value:        nil,
			}
			dataValue := &ua.DataValue{
				Status: ua.StatusOK,
				Value:  ua.MustVariant(extObj),
			}

			b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
			Expect(b).To(BeNil())
			Expect(tagType).To(BeEmpty())
		})

		It("should return JSON bytes for ExtensionObject with decoded Value", func() {
			decodedValue := &ua.DataChangeFilter{
				Trigger:       ua.DataChangeTriggerStatusValue,
				DeadbandType:  uint32(ua.DeadbandTypeAbsolute),
				DeadbandValue: 0.5,
			}
			extObj := &ua.ExtensionObject{
				TypeID: &ua.ExpandedNodeID{
					NodeID: ua.NewNumericNodeID(0, 724),
				},
				EncodingMask: ua.ExtensionObjectBinary,
				Value:        decodedValue,
			}
			dataValue := &ua.DataValue{
				Status: ua.StatusOK,
				Value:  ua.MustVariant(extObj),
			}

			b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
			Expect(b).NotTo(BeNil())
			Expect(tagType).To(Equal("string"))
			Expect(string(b)).To(ContainSubstring("DeadbandValue"))
		})

		It("should return nil for array of ExtensionObjects all with nil Values", func() {
			extObjs := []*ua.ExtensionObject{
				{
					TypeID:       &ua.ExpandedNodeID{NodeID: ua.NewNumericNodeID(4, 202)},
					EncodingMask: ua.ExtensionObjectBinary,
					Value:        nil,
				},
				{
					TypeID:       &ua.ExpandedNodeID{NodeID: ua.NewNumericNodeID(4, 203)},
					EncodingMask: ua.ExtensionObjectBinary,
					Value:        nil,
				},
			}
			dataValue := &ua.DataValue{
				Status: ua.StatusOK,
				Value:  ua.MustVariant(extObjs),
			}

			b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
			Expect(b).To(BeNil())
			Expect(tagType).To(BeEmpty())
		})

		It("should return JSON array with only decoded values from mixed ExtensionObject array", func() {
			decodedValue := &ua.DataChangeFilter{
				Trigger:       ua.DataChangeTriggerStatusValue,
				DeadbandType:  uint32(ua.DeadbandTypeAbsolute),
				DeadbandValue: 1.5,
			}
			extObjs := []*ua.ExtensionObject{
				{
					TypeID:       &ua.ExpandedNodeID{NodeID: ua.NewNumericNodeID(4, 202)},
					EncodingMask: ua.ExtensionObjectBinary,
					Value:        nil, // undecodable
				},
				{
					TypeID:       &ua.ExpandedNodeID{NodeID: ua.NewNumericNodeID(0, 724)},
					EncodingMask: ua.ExtensionObjectBinary,
					Value:        decodedValue, // decoded
				},
			}
			dataValue := &ua.DataValue{
				Status: ua.StatusOK,
				Value:  ua.MustVariant(extObjs),
			}

			b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
			Expect(b).NotTo(BeNil())
			Expect(tagType).To(Equal("string"))
			// Should be a JSON array with exactly one element (the decoded one)
			Expect(string(b)).To(HavePrefix("["))
			Expect(string(b)).To(ContainSubstring("DeadbandValue"))
		})
	})
})

var _ = Describe("createMessageFromValue tag group derivation", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	var input *OPCUAInput

	BeforeEach(func() {
		logger := service.MockResources().Logger()
		input = &OPCUAInput{
			OPCUAConnection: &OPCUAConnection{Log: logger},
		}
	})

	// makeMsg builds a minimal valid DataValue + NodeDef and runs the message
	// through createMessageFromValue, returning the (sanitized) tag_name,
	// tag_group, and tag_path metadata so the tests can focus on the tag-group
	// derivation.
	makeMsg := func(path, browseName string) (tagName, tagGroup, tagPath string) {
		dv := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant(int32(42)),
		}
		nd := NodeDef{
			NodeID:     ua.NewNumericNodeID(2, 1),
			NodeClass:  ua.NodeClassVariable,
			BrowseName: browseName,
			Path:       path,
		}
		msg := input.createMessageFromValue(dv, nd)
		Expect(msg).NotTo(BeNil())

		tagName, _ = msg.MetaGet("opcua_tag_name")
		tagGroup, _ = msg.MetaGet("opcua_tag_group")
		tagPath, _ = msg.MetaGet("opcua_tag_path")
		return
	}

	It("strips the sanitized tag name from a normal nested path", func() {
		tagName, tagGroup, tagPath := makeMsg("Plant.Line.Sensor", "Sensor")
		Expect(tagName).To(Equal("Sensor"))
		Expect(tagGroup).To(Equal("Plant.Line"))
		Expect(tagPath).To(Equal("Plant.Line"))
	})

	It("only strips the trailing occurrence when the tag name repeats in the path", func() {
		// Regression: the previous implementation used strings.Replace with n=1,
		// which removed the FIRST match anywhere in the path. The trailing-suffix
		// approach must keep the leading "Temperature" segment intact so it
		// remains part of the tag group.
		tagName, tagGroup, tagPath := makeMsg("Plant.Temperature.Sensor.Temperature", "Temperature")
		Expect(tagName).To(Equal("Temperature"))
		Expect(tagGroup).To(Equal("Plant.Temperature.Sensor"))
		Expect(tagPath).To(Equal("Plant.Temperature.Sensor"))
	})

	It("treats a root-level node (path == tag name) as its own group with empty tag path", func() {
		// When the path equals the sanitized tag name, the trimmed group is
		// empty, so the code falls back to tagGroup = tagName and signals
		// "root" by setting opcua_tag_path to an empty string.
		tagName, tagGroup, tagPath := makeMsg("Sensor", "Sensor")
		Expect(tagName).To(Equal("Sensor"))
		Expect(tagGroup).To(Equal("Sensor"))
		Expect(tagPath).To(BeEmpty())
	})

	It("matches the sanitized tag name against the sanitized path", func() {
		// Regression: the path is built from sanitized BrowseNames in the browse
		// phase, so the raw BrowseName ("My Sensor") will never appear verbatim
		// in the path. The fix sanitizes the BrowseName before stripping it.
		tagName, tagGroup, tagPath := makeMsg("My_Folder.My_Sensor", "My Sensor")
		Expect(tagName).To(Equal("My_Sensor"))
		Expect(tagGroup).To(Equal("My_Folder"))
		Expect(tagPath).To(Equal("My_Folder"))
	})
})
