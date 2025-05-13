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
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1234567890),
					"temperature":  float64(25.5),
				})

				event, valueName, err := messageToEvent(msg)
				Expect(err).To(BeNil())
				Expect(event.IsTimeseries).To(BeTrue())
				Expect(event.TimestampMs).To(Equal(wrapperspb.Int64(1234567890)))
				Expect(*valueName).To(Equal("temperature"))
				Expect(event.Value.TypeUrl).To(HavePrefix("golang/float64"))
			})

			It("rejects time series data with missing timestamp", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"temperature": float64(25.5),
				})

				event, valueName, err := messageToEvent(msg)
				Expect(err).To(BeNil())
				Expect(event.IsTimeseries).To(BeFalse())
				Expect(event.TimestampMs).To(BeNil())
				Expect(valueName).To(BeNil())
			})

			It("rejects time series data with too many fields", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1234567890),
					"temperature":  float64(25.5),
					"humidity":     float64(60.0),
				})

				event, valueName, err := messageToEvent(msg)
				Expect(err).To(BeNil())
				Expect(event.IsTimeseries).To(BeFalse())
				Expect(event.TimestampMs).To(BeNil())
				Expect(valueName).To(BeNil())
			})

			It("handles different value types in time series data", func() {
				testCases := []struct {
					value      interface{}
					typePrefix string
				}{
					{int64(42), "golang/int64"},
					{float64(3.14), "golang/float64"},
					{true, "golang/bool"},
					{"test", "golang/string"},
				}

				for _, tc := range testCases {
					msg := service.NewMessage(nil)
					msg.SetStructured(map[string]interface{}{
						"timestamp_ms": int64(1234567890),
						"value":        tc.value,
					})

					event, valueName, err := messageToEvent(msg)
					Expect(err).To(BeNil())
					Expect(event.IsTimeseries).To(BeTrue())
					Expect(event.TimestampMs).To(Equal(wrapperspb.Int64(1234567890)))
					Expect(*valueName).To(Equal("value"))
					Expect(event.Value.TypeUrl).To(HavePrefix(tc.typePrefix))
				}
			})
		})

		Context("with relational data", func() {
			It("processes raw bytes data correctly", func() {
				rawData := []byte("raw data")
				msg := service.NewMessage(rawData)

				event, valueName, err := messageToEvent(msg)
				Expect(err).To(BeNil())
				Expect(event.IsTimeseries).To(BeFalse())
				Expect(event.TimestampMs).To(BeNil())
				Expect(valueName).To(BeNil())
				Expect(event.Value.TypeUrl).To(Equal("golang/bytes"))
				Expect(event.Value.Value).To(Equal(rawData))
			})

			It("handles non-map structured data", func() {
				msg := service.NewMessage(nil)
				msg.SetStructured([]interface{}{"not", "a", "map"})

				event, valueName, err := messageToEvent(msg)
				Expect(err).To(BeNil())
				Expect(event.IsTimeseries).To(BeFalse())
				Expect(event.TimestampMs).To(BeNil())
				Expect(valueName).To(BeNil())
			})
		})
	})

	Describe("processTimeSeriesData", func() {
		It("extracts timestamp and value correctly", func() {
			data := map[string]interface{}{
				"timestamp_ms": int64(1234567890),
				"pressure":     float64(1013.25),
			}

			event, valueName, err := processTimeSeriesData(data)
			Expect(err).To(BeNil())
			Expect(event.IsTimeseries).To(BeTrue())
			Expect(event.TimestampMs).To(Equal(wrapperspb.Int64(1234567890)))
			Expect(*valueName).To(Equal("pressure"))
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

			event, valueName, err := processTimeSeriesData(data)
			Expect(err).NotTo(BeNil())
			Expect(event).To(BeNil())
			Expect(valueName).To(BeNil())
		})
	})

	Describe("processRelationalData", func() {
		It("processes raw bytes correctly", func() {
			rawData := []byte("raw data")
			msg := service.NewMessage(rawData)

			event, valueName, err := processRelationalData(msg)
			Expect(err).To(BeNil())
			Expect(event.IsTimeseries).To(BeFalse())
			Expect(event.TimestampMs).To(BeNil())
			Expect(valueName).To(BeNil())
			Expect(event.Value.TypeUrl).To(Equal("golang/bytes"))
			Expect(event.Value.Value).To(Equal(rawData))
		})
	})
})
