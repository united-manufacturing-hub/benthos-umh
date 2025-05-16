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

package tag_browser_plugin

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
				name := "temperature"
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1234567890),
					name:           float64(25.5),
				})

				event, err := messageToEvent(msg, wrapperspb.String(name))
				Expect(err).To(BeNil())
				Expect(event.IsTimeseries).To(BeTrue())
				Expect(event.TimestampMs).To(Equal(wrapperspb.Int64(1234567890)))
				Expect(event.Value.TypeUrl).To(HavePrefix("golang/float64"))
			})

			It("rejects time series data with missing timestamp", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"temperature": float64(25.5),
				})

				event, err := messageToEvent(msg, nil)
				Expect(err).To(BeNil())
				Expect(event.IsTimeseries).To(BeFalse())
				Expect(event.TimestampMs).To(BeNil())
			})

			It("rejects time series data with too many fields", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1234567890),
					"temperature":  float64(25.5),
					"humidity":     float64(60.0),
				})

				event, err := messageToEvent(msg, nil)
				Expect(err).To(BeNil())
				Expect(event.IsTimeseries).To(BeFalse())
				Expect(event.TimestampMs).To(BeNil())
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

					event, err := messageToEvent(msg, wrapperspb.String("value"))
					Expect(err).To(BeNil())
					Expect(event.IsTimeseries).To(BeTrue())
					Expect(event.TimestampMs).To(Equal(wrapperspb.Int64(1234567890)))
					Expect(event.Value.TypeUrl).To(HavePrefix(tc.typePrefix))
				}
			})
		})

		Context("with relational data", func() {
			It("processes raw bytes data correctly", func() {
				rawData := []byte("raw data")
				msg := service.NewMessage(rawData)

				event, err := messageToEvent(msg, nil)
				Expect(err).To(BeNil())
				Expect(event.IsTimeseries).To(BeFalse())
				Expect(event.TimestampMs).To(BeNil())
				Expect(event.Value.TypeUrl).To(Equal("golang/[]byte"))
				Expect(event.Value.Value).To(Equal(rawData))
			})

			It("handles non-map structured data", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured([]interface{}{"not", "a", "map"})

				event, err := messageToEvent(msg, nil)
				Expect(err).To(BeNil())
				Expect(event.IsTimeseries).To(BeFalse())
				Expect(event.TimestampMs).To(BeNil())
			})
		})
	})

	Describe("processTimeSeriesData", func() {
		It("extracts timestamp and value correctly", func() {
			data := map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"pressure":     float64(1013.25),
			}

			event, err := processTimeSeriesData(data, wrapperspb.String("pressure"))
			Expect(err).To(BeNil())
			Expect(event.IsTimeseries).To(BeTrue())
			Expect(event.TimestampMs).To(Equal(wrapperspb.Int64(1234567890)))
			Expect(event.Value.TypeUrl).To(HavePrefix("golang/float64"))
		})

		It("handles conversion errors gracefully", func() {
			// Create a value that can't be converted to bytes
			type Unconvertible struct {
				Channel chan int
			}
			data := map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"invalid":      Unconvertible{make(chan int)},
			}

			event, err := processTimeSeriesData(data, nil)
			Expect(err).NotTo(BeNil())
			Expect(event).To(BeNil())
		})

		It("should not panic when expectedTagNameForTimeseries is nil", func() {
			data := map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"pressure":     float64(1013.25),
			}

			_, err := processTimeSeriesData(data, nil)
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("expected tag name for timeseries, but was empty"))
		})
	})

	Describe("processRelationalData", func() {
		It("processes raw bytes correctly", func() {
			rawData := []byte("raw data")
			msg := service.NewMessage(rawData)

			event, err := processRelationalData(msg)
			Expect(err).To(BeNil())
			Expect(event.IsTimeseries).To(BeFalse())
			Expect(event.TimestampMs).To(BeNil())
			Expect(event.Value.TypeUrl).To(Equal("golang/[]byte"))
			Expect(event.Value.Value).To(Equal(rawData))
		})
	})
})
