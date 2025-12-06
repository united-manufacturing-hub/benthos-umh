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

package topic_browser_plugin

import (
	"math"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

var _ = Describe("Event Processing", func() {
	Describe("messageToEvent", func() {
		Context("with time series data", func() {
			It("processes valid time series data correctly", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1234567890),
					"value":        float64(25.5),
				})

				event, err := messageToEvent(msg)
				Expect(err).To(BeNil())
				Expect(event.GetTs()).NotTo(BeNil())
				Expect(event.GetTs().GetTimestampMs()).To(Equal(int64(1234567890)))
				Expect(event.GetTs().GetScalarType()).To(Equal(proto.ScalarType_NUMERIC))
				Expect(event.GetTs().GetNumericValue()).NotTo(BeNil())
				Expect(event.GetTs().GetNumericValue().GetValue()).To(Equal(float64(25.5)))
			})

			It("rejects time series data with missing timestamp", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"value": float64(25.5),
				})

				event, err := messageToEvent(msg)
				Expect(err).To(BeNil())
				Expect(event.GetRel()).NotTo(BeNil())
				Expect(event.GetTs()).To(BeNil())
			})

			It("processes UMH Classic format as relational data", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1234567890),
					"temperature":  float64(25.5), // UMH Classic format
				})

				event, err := messageToEvent(msg)
				Expect(err).To(BeNil())               // Should not error, processed as relational
				Expect(event.GetRel()).NotTo(BeNil()) // Should be processed as relational
				Expect(event.GetTs()).To(BeNil())     // Should not be timeseries
			})

			It("rejects time series data with too many fields", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1234567890),
					"value":        float64(25.5),
					"humidity":     float64(60.0),
				})

				event, err := messageToEvent(msg)
				Expect(err).To(BeNil())
				Expect(event.GetRel()).NotTo(BeNil())
				Expect(event.GetTs()).To(BeNil())
			})

			It("should handle different value types", func() {
				testCases := []struct {
					value      interface{}
					scalarType proto.ScalarType
				}{
					{float64(123.45), proto.ScalarType_NUMERIC},
					{int64(123), proto.ScalarType_NUMERIC},
					{uint64(123), proto.ScalarType_NUMERIC},
					{true, proto.ScalarType_BOOLEAN},
					{"test", proto.ScalarType_STRING},
				}

				for _, tc := range testCases {
					msg := service.NewMessage(nil)
					msg.SetStructured(map[string]interface{}{
						"timestamp_ms": 1234567890,
						"value":        tc.value,
					})

					event, err := messageToEvent(msg)
					Expect(err).To(BeNil())
					Expect(event.GetTs()).NotTo(BeNil())
					Expect(event.GetTs().GetTimestampMs()).To(Equal(int64(1234567890)))
					Expect(event.GetTs().GetScalarType()).To(Equal(tc.scalarType))

					// Check the specific value based on scalar type
					switch tc.scalarType {
					case proto.ScalarType_NUMERIC:
						Expect(event.GetTs().GetNumericValue()).NotTo(BeNil())
					case proto.ScalarType_STRING:
						Expect(event.GetTs().GetStringValue()).NotTo(BeNil())
						Expect(event.GetTs().GetStringValue().GetValue()).To(Equal("test"))
					case proto.ScalarType_BOOLEAN:
						Expect(event.GetTs().GetBooleanValue()).NotTo(BeNil())
						Expect(event.GetTs().GetBooleanValue().GetValue()).To(Equal(true))
					default:
					}
				}
			})

			It("rejects invalid JSON format", func() {
				msg := service.NewMessage([]byte("invalid json {"))

				event, err := messageToEvent(msg)
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(ContainSubstring("invalid JSON format"))
				Expect(event).To(BeNil())
			})

			It("rejects JSON arrays as invalid", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured([]interface{}{"not", "a", "map"})

				event, err := messageToEvent(msg)
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(ContainSubstring("payload is not a JSON object"))
				Expect(event).To(BeNil())
			})

			It("accepts float timestamps without fractional parts", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": 1234567890.0, // No fractional part
					"value":        25.5,
				})

				event, err := messageToEvent(msg)
				Expect(err).To(BeNil())
				Expect(event.GetTs()).NotTo(BeNil())
				Expect(event.GetTs().GetTimestampMs()).To(Equal(int64(1234567890)))
			})

			It("rejects nil timestamp values", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": nil,
					"value":        25.5,
				})

				event, err := messageToEvent(msg)
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(ContainSubstring("cannot be nil"))
				Expect(event).To(BeNil())
			})

			It("rejects nil value fields", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1234567890),
					"value":        nil,
				})

				event, err := messageToEvent(msg)
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(ContainSubstring("cannot be nil"))
				Expect(event).To(BeNil())
			})
		})

		Context("with relational data", func() {
			It("rejects raw bytes data as invalid", func() {
				rawData := []byte("raw data")
				msg := service.NewMessage(rawData)

				event, err := messageToEvent(msg)
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(ContainSubstring("invalid JSON format"))
				Expect(event).To(BeNil())
			})

			It("processes valid JSON objects as relational", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"order_id": 123,
					"customer": "ACME Corp",
				})

				event, err := messageToEvent(msg)
				Expect(err).To(BeNil())
				Expect(event.GetRel()).NotTo(BeNil())
				Expect(event.GetTs()).To(BeNil())
			})

			It("processes large relational data without size limits", func() {
				// Create relational data larger than 1KB (time-series limit)
				largeDescription := make([]byte, 2048) // 2KB > 1KB time-series limit
				for i := range largeDescription {
					largeDescription[i] = 'X'
				}

				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"order_id":    123,
					"customer":    "ACME Corp",
					"description": string(largeDescription), // Large field that would exceed time-series limit
					"metadata":    map[string]interface{}{"priority": "high"},
				})

				event, err := messageToEvent(msg)
				Expect(err).To(BeNil())               // Should NOT fail due to size
				Expect(event.GetRel()).NotTo(BeNil()) // Should be processed as relational
				Expect(event.GetTs()).To(BeNil())     // Should not be time-series
			})
		})
	})

	Describe("processTimeSeriesData", func() {
		It("extracts timestamp and value correctly", func() {
			data := map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"value":        float64(1013.25),
			}

			event, err := processTimeSeriesData(data)
			Expect(err).To(BeNil())
			Expect(event.GetTs()).NotTo(BeNil())
			Expect(event.GetTs().GetTimestampMs()).To(Equal(int64(1234567890)))
			Expect(event.GetTs().GetScalarType()).To(Equal(proto.ScalarType_NUMERIC))
			Expect(event.GetTs().GetNumericValue()).NotTo(BeNil())
			Expect(event.GetTs().GetNumericValue().GetValue()).To(Equal(float64(1013.25)))
		})

		It("rejects UMH Classic format with arbitrary key names", func() {
			data := map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"pressure":     float64(1013.25), // Should be "value" in UMH-Core
			}

			event, err := processTimeSeriesData(data)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("time-series payload must have exactly"))
			Expect(event).To(BeNil())
		})

		It("rejects data missing the value key", func() {
			data := map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"temperature":  float64(25.0), // Missing "value" key
			}

			event, err := processTimeSeriesData(data)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("time-series payload must have exactly"))
			Expect(event).To(BeNil())
		})

		It("handles conversion errors gracefully", func() {
			// Create a value that can't be converted to bytes
			type Unconvertible struct {
				Channel chan int
			}
			data := map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"value":        Unconvertible{make(chan int)},
			}

			event, err := processTimeSeriesData(data)
			Expect(err).NotTo(BeNil())
			Expect(event).To(BeNil())
		})

		It("should not panic when there are multiple non-timestamp fields", func() {
			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"value":        float64(1013.25),
				"temperature":  float64(25.0),
			})

			// This should be processed as relational data, not timeseries
			event, err := messageToEvent(msg)
			Expect(err).To(BeNil())
			Expect(event.GetRel()).NotTo(BeNil()) // Should be relational data
			Expect(event.GetTs()).To(BeNil())     // Should not be timeseries
		})

		It("rejects NaN values in time series data", func() {
			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"value":        math.NaN(),
			})

			event, err := messageToEvent(msg)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("NaN"))
			Expect(event).To(BeNil())
		})

		It("rejects positive infinity values in time series data", func() {
			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"value":        math.Inf(1),
			})

			event, err := messageToEvent(msg)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("Inf"))
			Expect(event).To(BeNil())
		})

		It("rejects negative infinity values in time series data", func() {
			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"value":        math.Inf(-1),
			})

			event, err := messageToEvent(msg)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("Inf"))
			Expect(event).To(BeNil())
		})

		It("rejects oversized payloads in time series data", func() {
			// Create a large string value (>1 MiB)
			largeValue := make([]byte, 1048577) // 1 MiB + 1 byte
			for i := range largeValue {
				largeValue[i] = 'A'
			}

			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"value":        string(largeValue),
			})

			event, err := messageToEvent(msg)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("time-series payload"))
			Expect(event).To(BeNil())
		})

		It("rejects float timestamps with fractional parts", func() {
			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]interface{}{
				"timestamp_ms": 1234567890.5, // Has fractional part
				"value":        25.5,
			})

			event, err := messageToEvent(msg)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("precision loss"))
			Expect(event).To(BeNil())
		})
	})

	Describe("processRelationalStructured", func() {
		It("processes structured data correctly", func() {
			structuredData := map[string]interface{}{
				"order_id": 123,
				"customer": "ACME Corp",
			}

			event, err := processRelationalStructured(structuredData)
			Expect(err).To(BeNil())
			Expect(event.GetRel()).NotTo(BeNil())
			Expect(event.GetTs()).To(BeNil())
		})
	})
})
