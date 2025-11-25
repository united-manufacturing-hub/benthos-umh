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
)

var _ = Describe("DataType Handling", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	Describe("getDataTypeString", func() {
		Context("when processing builtin OPC UA data types", func() {
			It("should return 'bool' for TypeIDBoolean (i=1)", func() {
				result := getDataTypeString(uint32(ua.TypeIDBoolean))
				Expect(result).To(Equal("bool"))
			})

			It("should return 'int32' for TypeIDInt32 (i=6)", func() {
				result := getDataTypeString(uint32(ua.TypeIDInt32))
				Expect(result).To(Equal("int32"))
			})

			It("should return 'string' for TypeIDString (i=12)", func() {
				result := getDataTypeString(uint32(ua.TypeIDString))
				Expect(result).To(Equal("string"))
			})
		})

		Context("when processing unknown/custom type IDs", func() {
			It("should return formatted string for unknown type ID 0", func() {
				result := getDataTypeString(0)
				Expect(result).To(Equal("ns=0;i=0"))
			})

			It("should return formatted string for unknown type ID 999", func() {
				result := getDataTypeString(999)
				Expect(result).To(Equal("ns=0;i=999"))
			})
		})
	})

	Describe("processNodeAttributes with custom data types", func() {
		var logger Logger

		BeforeEach(func() {
			logger = &mockLogger{}
		})

		Context("when processing builtin OPC UA data types", func() {
			It("should correctly identify Boolean type (ns=0;i=1)", func() {
				nodeID := ua.NewNumericNodeID(0, 1234)
				dataTypeValue := ua.MustVariant(ua.NewNumericNodeID(0, uint32(ua.TypeIDBoolean)))

				attributes := []*ua.DataValue{
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(int64(ua.NodeClassVariable)), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(&ua.QualifiedName{NamespaceIndex: 0, Name: "TestNode"}), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant("Test Node"), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(uint32(ua.AccessLevelTypeCurrentRead)), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: dataTypeValue, Status: ua.StatusOK},
				}

				def := &NodeDef{NodeID: nodeID}
				err := processNodeAttributes(attributes, def, "TestPath", logger)

				Expect(err).NotTo(HaveOccurred())
				Expect(def.DataType).To(Equal("bool"))
				Expect(def.DataTypeID).To(Equal(ua.TypeIDBoolean))
			})
		})

		Context("when processing custom vendor-specific data types", func() {
			It("should handle custom numeric type (ns=2;i=100) - currently returns ns=0;i=0", func() {
				nodeID := ua.NewNumericNodeID(0, 1234)
				// Custom type in namespace 2 - IntID() will return 0
				customDataType := ua.NewNumericNodeID(2, 100)
				dataTypeValue := ua.MustVariant(customDataType)

				attributes := []*ua.DataValue{
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(int64(ua.NodeClassVariable)), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(&ua.QualifiedName{NamespaceIndex: 0, Name: "TestNode"}), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant("Test Node"), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(uint32(ua.AccessLevelTypeCurrentRead)), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: dataTypeValue, Status: ua.StatusOK},
				}

				def := &NodeDef{NodeID: nodeID}
				err := processNodeAttributes(attributes, def, "TestPath", logger)

				Expect(err).NotTo(HaveOccurred())
				// After fix: DataType should preserve full NodeID string "ns=2;i=100"
				Expect(def.DataType).To(Equal("ns=2;i=100"))
				// DataTypeID gets the numeric part (100) even though namespace is 2
				Expect(def.DataTypeID).To(Equal(ua.TypeID(100)))
			})

			It("should handle custom string type (ns=2;s=MyCustomType) - currently returns ns=0;i=0", func() {
				nodeID := ua.NewNumericNodeID(0, 1234)
				// Custom type with string identifier - IntID() will return 0
				customDataType := ua.NewStringNodeID(2, "MyCustomType")
				dataTypeValue := ua.MustVariant(customDataType)

				attributes := []*ua.DataValue{
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(int64(ua.NodeClassVariable)), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(&ua.QualifiedName{NamespaceIndex: 0, Name: "TestNode"}), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant("Test Node"), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(uint32(ua.AccessLevelTypeCurrentRead)), Status: ua.StatusOK},
					{EncodingMask: ua.DataValueValue, Value: dataTypeValue, Status: ua.StatusOK},
				}

				def := &NodeDef{NodeID: nodeID}
				err := processNodeAttributes(attributes, def, "TestPath", logger)

				Expect(err).NotTo(HaveOccurred())
				// CURRENT BEHAVIOR: IntID() returns 0 for string types, getDataTypeString(0) returns "ns=0;i=0"
				// After fix: Should return "ns=2;s=MyCustomType"
				Expect(def.DataType).To(Equal("ns=2;s=MyCustomType"))
				Expect(def.DataTypeID).To(Equal(ua.TypeID(0)))
			})
		})
	})
})

// mockLogger is a simple mock implementation of Logger for testing
type mockLogger struct{}

func (m *mockLogger) Debugf(format string, args ...interface{}) {}
func (m *mockLogger) Infof(format string, args ...interface{})  {}
func (m *mockLogger) Warnf(format string, args ...interface{})  {}
func (m *mockLogger) Errorf(format string, args ...interface{}) {}
