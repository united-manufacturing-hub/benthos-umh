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
	"fmt"
	"math/rand"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("OPC UA Output", func() {
	var (
		opcsimIp   = "localhost"
		opcsimPort = "50000"

		builder *service.StreamBuilder
	)

	BeforeEach(func() {
		testActivated := os.Getenv("TEST_OPCUA_WRITE_SIMULATOR")
		if testActivated == "" {
			Skip("Skipping write unit tests against simulator: TEST_OPCUA_WRITE_SIMULATOR not set")
		}
		if os.Getenv("TEST_OPCSIM_IP") != "" {
			opcsimIp = os.Getenv("TEST_OPCSIM_IP")
		}
		if os.Getenv("TEST_OPCSIM_PORT") != "" {
			opcsimPort = os.Getenv("TEST_OPCSIM_PORT")
		}

		builder = service.NewStreamBuilder()
		err := builder.SetLoggerYAML(`level: off`)
		Expect(err).NotTo(HaveOccurred())
	})

	Context("configuration parsing", func() {
		When("creating a configuration with all fields", func() {
			It("should successfully parse the configuration", func() {
				err := builder.AddOutputYAML(fmt.Sprintf(`
opcua:
  endpoint: "opc.tcp://%s:%s"
  nodeMappings:
    - nodeId: "ns=4;i=6210"
      valueFrom: "setpoint"
      dataType: "Float"
    - nodeId: "ns=4;i=6211"
      valueFrom: "status"
      dataType: "Boolean"
  handshake:
    enabled: true
    readbackTimeoutMs: 2000
    maxWriteAttempts: 3
    timeBetweenRetriesMs: 1000
`, opcsimIp, opcsimPort))
				Expect(err).NotTo(HaveOccurred())

				// Verify the configuration by building the stream
				stream, err := builder.Build()
				Expect(err).NotTo(HaveOccurred())
				Expect(stream).NotTo(BeNil())
			})
		})

		When("creating a configuration with minimal settings", func() {
			It("should use default values for omitted fields", func() {
				err := builder.AddOutputYAML(fmt.Sprintf(`
opcua:
  endpoint: "opc.tcp://%s:%s"
  nodeMappings:
    - nodeId: "ns=4;i=6210"
      valueFrom: "setpoint"
      dataType: "Float"
`, opcsimIp, opcsimPort))
				Expect(err).NotTo(HaveOccurred())

				// Verify the configuration by building the stream
				stream, err := builder.Build()
				Expect(err).NotTo(HaveOccurred())
				Expect(stream).NotTo(BeNil())
			})
		})

		It("should fail with invalid node mapping", func() {
			err := builder.AddOutputYAML(fmt.Sprintf(`
opcua:
  endpoint: "opc.tcp://%s:%s"
  nodeMappings:
    - nodeId: "ns=4;i=6210"
      # missing valueFrom field
`, opcsimIp, opcsimPort))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("valueFrom"))
		})
	})

	Context("opc-plc direct", func() {
		When("writing to a simulator", func() {
			It("should connect and write successfully", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeID, _ := service.NewInterpolatedString("ns=4;i=6210")
				dataType, _ := service.NewInterpolatedString("Float")
				
				output := &OPCUAOutput{
					OPCUAConnection: &OPCUAConnection{
						Endpoint: fmt.Sprintf("opc.tcp://%s:%s", opcsimIp, opcsimPort),
					},
					NodeMappings: []NodeMapping{
						{
							NodeID:    nodeID,
							ValueFrom: "setpoint",
							DataType:  dataType,
						},
					},
				}

				// Attempt to connect
				err := output.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Write a message with random float value between -100 and 100
				randomFloat := float32(-100 + rand.Float32()*200) // generates a random float32 between -100 and 100
				jsonData := []byte(fmt.Sprintf(`{"setpoint": %f}`, randomFloat))
				msg := service.NewMessage(jsonData)
				err = output.Write(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				// Close connection
				if output.Client != nil {
					err := output.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})
	})

	Context("opc-plc with YAML configuration", func() {
		When("writing to a simulator", func() {
			It("should connect and write successfully", func() {
				err := builder.AddOutputYAML(fmt.Sprintf(`
opcua:
  endpoint: "opc.tcp://%s:%s"
  nodeMappings:
    - nodeId: "ns=4;i=6210"
      valueFrom: "setpoint"
      dataType: "Float"
`, opcsimIp, opcsimPort))
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddInputYAML(`
generate:
  mapping: |
    root = {
      "setpoint": 12.1
    }
  interval: "1s"
  count: 1
`)
				Expect(err).NotTo(HaveOccurred())

				stream, err := builder.Build()
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				err = stream.Run(ctx)
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	Context("convertToVariant", func() {
		var output *OPCUAOutput

		BeforeEach(func() {
			output = &OPCUAOutput{}
		})

		DescribeTable("data type conversions",
			func(dataType string, input interface{}, expectedType interface{}, shouldSucceed bool) {
				variant, err := output.convertToVariant(input, dataType)
				if shouldSucceed {
					Expect(err).NotTo(HaveOccurred())
					Expect(variant).NotTo(BeNil())
					Expect(variant.Value()).To(BeAssignableToTypeOf(expectedType))
				} else {
					Expect(err).To(HaveOccurred())
				}
			},
			// JSON Number tests for all numeric types
			Entry("json.Number integer to Boolean", "Boolean", json.Number("1"), true, true),
			Entry("json.Number zero to Boolean", "Boolean", json.Number("0"), false, true),
			Entry("json.Number float to Boolean", "Boolean", json.Number("1.0"), true, true),
			Entry("json.Number invalid to Boolean", "Boolean", json.Number("invalid"), true, false),

			Entry("json.Number to SByte in range", "SByte", json.Number("42"), int8(0), true),
			Entry("json.Number to SByte out of range", "SByte", json.Number("128"), int8(0), false),
			Entry("json.Number float to SByte", "SByte", json.Number("42.0"), int8(0), true),
			Entry("json.Number invalid to SByte", "SByte", json.Number("invalid"), int8(0), false),

			Entry("json.Number to Byte in range", "Byte", json.Number("42"), uint8(0), true),
			Entry("json.Number to Byte out of range", "Byte", json.Number("256"), uint8(0), false),
			Entry("json.Number negative to Byte", "Byte", json.Number("-1"), uint8(0), false),
			Entry("json.Number float to Byte", "Byte", json.Number("42.0"), uint8(0), true),

			Entry("json.Number to Int16 in range", "Int16", json.Number("32767"), int16(0), true),
			Entry("json.Number to Int16 out of range", "Int16", json.Number("32768"), int16(0), false),
			Entry("json.Number float to Int16", "Int16", json.Number("42.0"), int16(0), true),

			Entry("json.Number to UInt16 in range", "UInt16", json.Number("65535"), uint16(0), true),
			Entry("json.Number to UInt16 out of range", "UInt16", json.Number("65536"), uint16(0), false),
			Entry("json.Number negative to UInt16", "UInt16", json.Number("-1"), uint16(0), false),

			Entry("json.Number to Int32", "Int32", json.Number("42"), int32(0), true),
			Entry("json.Number float to Int32", "Int32", json.Number("42.5"), int32(0), true),
			Entry("json.Number negative to Int32", "Int32", json.Number("-42"), int32(0), true),
			Entry("json.Number invalid to Int32", "Int32", json.Number("invalid"), int32(0), false),

			Entry("json.Number to UInt32 in range", "UInt32", json.Number("4294967295"), uint32(0), true),
			Entry("json.Number negative to UInt32", "UInt32", json.Number("-1"), uint32(0), false),
			Entry("json.Number float to UInt32", "UInt32", json.Number("42.0"), uint32(0), true),

			Entry("json.Number to Int64", "Int64", json.Number("9223372036854775807"), int64(0), true),
			Entry("json.Number float to Int64", "Int64", json.Number("42.0"), int64(0), true),
			Entry("json.Number negative to Int64", "Int64", json.Number("-42"), int64(0), true),

			Entry("json.Number to UInt64 in range", "UInt64", json.Number("18446744073709551615"), uint64(0), true),
			Entry("json.Number negative to UInt64", "UInt64", json.Number("-1"), uint64(0), false),
			Entry("json.Number float to UInt64", "UInt64", json.Number("42.0"), uint64(0), true),

			Entry("json.Number to Float", "Float", json.Number("42.5"), float32(0), true),
			Entry("json.Number integer to Float", "Float", json.Number("42"), float32(0), true),
			Entry("json.Number negative to Float", "Float", json.Number("-42.5"), float32(0), true),
			Entry("json.Number invalid to Float", "Float", json.Number("invalid"), float32(0), false),

			Entry("json.Number to Double", "Double", json.Number("42.5"), float64(0), true),
			Entry("json.Number integer to Double", "Double", json.Number("42"), float64(0), true),
			Entry("json.Number negative to Double", "Double", json.Number("-42.5"), float64(0), true),
			Entry("json.Number invalid to Double", "Double", json.Number("invalid"), float64(0), false),

			// JSON String tests
			Entry("JSON string to String", "String", "test", "", true),
			Entry("JSON string to Boolean true", "Boolean", "true", true, true),
			Entry("JSON string to Boolean false", "Boolean", "false", true, true),
			Entry("JSON string to Int32", "Int32", "42", int32(0), true),
			Entry("JSON string to Float", "Float", "42.5", float32(0), true),
			Entry("JSON string to DateTime RFC3339", "DateTime", "2024-03-15T12:00:00Z", time.Time{}, true),
			Entry("JSON string to DateTime simple", "DateTime", "2024-03-15", time.Time{}, true),

			// JSON Boolean tests
			Entry("JSON boolean true to Boolean", "Boolean", true, true, true),
			Entry("JSON boolean false to Boolean", "Boolean", false, true, true),
			Entry("JSON boolean to String", "String", true, "", true),

			// JSON Object tests
			Entry("JSON object to String", "String", map[string]interface{}{"key": "value"}, "", true),
			Entry("JSON object to Boolean", "Boolean", map[string]interface{}{"key": "value"}, true, false),
			Entry("JSON object to Int32", "Int32", map[string]interface{}{"key": "value"}, int32(0), false),

			// JSON Array tests
			Entry("JSON array to String", "String", []interface{}{"value1", "value2"}, "", true),
			Entry("JSON array to Boolean", "Boolean", []interface{}{"value1", "value2"}, true, false),
			Entry("JSON array to Int32", "Int32", []interface{}{"value1", "value2"}, int32(0), false),

			// JSON Null tests
			Entry("JSON null to String", "String", nil, "", false),
			Entry("JSON null to Boolean", "Boolean", nil, true, false),
			Entry("JSON null to Int32", "Int32", nil, int32(0), false),

			// Original basic type tests remain unchanged...
			Entry("Boolean from bool", "Boolean", true, true, true),
			Entry("Boolean from int", "Boolean", 1, true, true),
			Entry("Boolean from string", "Boolean", "true", true, true),
			Entry("Boolean invalid string", "Boolean", "invalid", true, false),

			Entry("SByte from int8", "SByte", int8(42), int8(0), true),
			Entry("SByte from int in range", "SByte", 42, int8(0), true),
			Entry("SByte out of range high", "SByte", 128, int8(0), false),
			Entry("SByte out of range low", "SByte", -129, int8(0), false),

			Entry("Byte from uint8", "Byte", uint8(42), uint8(0), true),
			Entry("Byte from int in range", "Byte", 42, uint8(0), true),
			Entry("Byte out of range high", "Byte", 256, uint8(0), false),
			Entry("Byte out of range low", "Byte", -1, uint8(0), false),

			Entry("Int16 from int16", "Int16", int16(42), int16(0), true),
			Entry("Int16 from int in range", "Int16", 32767, int16(0), true),
			Entry("Int16 out of range high", "Int16", 32768, int16(0), false),

			Entry("UInt16 from uint16", "UInt16", uint16(42), uint16(0), true),
			Entry("UInt16 from int in range", "UInt16", 65535, uint16(0), true),
			Entry("UInt16 out of range high", "UInt16", 65536, uint16(0), false),

			Entry("Int32 from int32", "Int32", int32(42), int32(0), true),
			Entry("Int32 from int", "Int32", 42, int32(0), true),
			Entry("Int32 from string", "Int32", "42", int32(0), true),
			Entry("Int32 invalid string", "Int32", "invalid", int32(0), false),

			Entry("UInt32 from uint32", "UInt32", uint32(42), uint32(0), true),
			Entry("UInt32 from int in range", "UInt32", 42, uint32(0), true),
			Entry("UInt32 negative", "UInt32", -1, uint32(0), false),

			Entry("Int64 from int64", "Int64", int64(42), int64(0), true),
			Entry("Int64 from int", "Int64", 42, int64(0), true),
			Entry("Int64 from string", "Int64", "42", int64(0), true),

			Entry("UInt64 from uint64", "UInt64", uint64(42), uint64(0), true),
			Entry("UInt64 from int in range", "UInt64", 42, uint64(0), true),
			Entry("UInt64 negative", "UInt64", -1, uint64(0), false),

			Entry("Float from float32", "Float", float32(42.5), float32(0), true),
			Entry("Float from int", "Float", 42, float32(0), true),
			Entry("Float from string", "Float", "42.5", float32(0), true),
			Entry("Float invalid string", "Float", "invalid", float32(0), false),

			Entry("Double from float64", "Double", 42.5, float64(0), true),
			Entry("Double from int", "Double", 42, float64(0), true),
			Entry("Double from string", "Double", "42.5", float64(0), true),
			Entry("Double invalid string", "Double", "invalid", float64(0), false),

			Entry("String from string", "String", "test", "", true),
			Entry("String from int", "String", 42, "", true),
			Entry("String from float", "String", 42.5, "", true),
			Entry("String from bool", "String", true, "", true),

			Entry("DateTime from time.Time", "DateTime", time.Now(), time.Time{}, true),
			Entry("DateTime from RFC3339 string", "DateTime", "2024-03-15T12:00:00Z", time.Time{}, true),
			Entry("DateTime from simple date string", "DateTime", "2024-03-15", time.Time{}, true),
			Entry("DateTime invalid string", "DateTime", "invalid", time.Time{}, false),

			// Test json.Number
			Entry("json.Number to Boolean", "Boolean", json.Number("1"), true, true),
			Entry("json.Number to Boolean", "Boolean", json.Number("0"), false, true),
			Entry("json.Number to Boolean", "Boolean", json.Number("1.0"), true, true),
			Entry("json.Number to Boolean", "Boolean", json.Number("0.0"), false, true),

			Entry("json.Number to Int32", "Int32", json.Number("42"), int32(0), true),
			Entry("json.Number to Int32", "Int32", json.Number("42.5"), int32(0), true),
			Entry("json.Number to Int32", "Int32", json.Number("-42"), int32(0), true),
			Entry("json.Number to Int32", "Int32", json.Number("invalid"), int32(0), false),

			Entry("Unsupported type", "InvalidType", "test", nil, false),
		)
	})
})
