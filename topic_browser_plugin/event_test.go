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
	"errors"
	"math"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// mockMessage implements service.Message for testing
type mockMessage struct {
	structured interface{}
	bytes      []byte
}

func (m *mockMessage) AsStructured() (interface{}, error) {
	if m.structured != nil {
		return m.structured, nil
	}
	return nil, errors.New("not structured")
}

func (m *mockMessage) AsBytes() ([]byte, error) {
	if m.bytes != nil {
		return m.bytes, nil
	}
	return nil, errors.New("not bytes")
}

// Implement remaining service.Message interface methods
func (m *mockMessage) GetMeta(key string) (string, bool)        { return "", false }
func (m *mockMessage) SetMeta(key, value string)                {}
func (m *mockMessage) DeleteMeta(key string)                    {}
func (m *mockMessage) MetaIter(f func(k, v string) error) error { return nil }
func (m *mockMessage) Copy() service.Message                    { panic("not implemented") }
func (m *mockMessage) DeepCopy() service.Message                { panic("not implemented") }

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
				Expect(event.GetTs().GetValue().TypeUrl).To(HavePrefix("golang/float64"))
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
					typePrefix string
				}{
					{float64(123.45), "golang/float64"},
					{int64(123), "golang/int64"},
					{uint64(123), "golang/uint64"},
					{true, "golang/bool"},
					{"test", "golang/string"},
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
					Expect(event.GetTs().GetValue().TypeUrl).To(HavePrefix(tc.typePrefix))
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
				Expect(err.Error()).To(ContainSubstring("relational data must be a JSON object"))
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
			Expect(event.GetTs().GetValue().TypeUrl).To(HavePrefix("golang/float64"))
		})

		It("rejects UMH Classic format with arbitrary key names", func() {
			data := map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"pressure":     float64(1013.25), // Should be "value" in UMH-Core
			}

			event, err := processTimeSeriesData(data)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("time-series data must have exactly 'timestamp_ms' and 'value' keys"))
			Expect(event).To(BeNil())
		})

		It("rejects data missing the value key", func() {
			data := map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"temperature":  float64(25.0), // Missing "value" key
			}

			event, err := processTimeSeriesData(data)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("time-series data must have exactly 'timestamp_ms' and 'value' keys"))
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
			// Create a large string value (>1KB)
			largeValue := make([]byte, 1025) // 1025 bytes > 1KB limit
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
			Expect(err.Error()).To(ContainSubstring("payload size"))
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
	})

	Describe("processRelationalData", func() {
		It("processes raw bytes correctly", func() {
			rawData := []byte("raw data")
			msg := service.NewMessage(rawData)

			event, err := processRelationalData(msg)
			Expect(err).To(BeNil())
			Expect(event.GetRel()).NotTo(BeNil())
			Expect(event.GetTs()).To(BeNil())
		})
	})
})
