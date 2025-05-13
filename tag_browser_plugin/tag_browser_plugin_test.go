package tag_browser_plugin

import (
	"context"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("TagBrowserProcessor", func() {
	var processor *TagBrowserProcessor

	BeforeEach(func() {
		processor = NewTagBrowserProcessor()
	})

	Describe("ProcessBatch", func() {
		It("processes a single message successfully", func() {
			// Create a message with basic metadata
			msg := service.NewMessage(nil)
			msg.MetaSet("topic", "umh.v1.test-topic._historian")
			msg.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600000),
				"some_value":   13,
			})

			// Process the message
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(HaveLen(1))

			// Verify the output message
			outputMsg := result[0][0]
			Expect(outputMsg).NotTo(BeNil())

			// Dump to disk for testing
			/*
				bytes, err := outputMsg.AsBytes()
				Expect(err).To(BeNil())
				Expect(bytes).NotTo(BeNil())
				err = os.WriteFile("single_message.proto", bytes, 0644)
				Expect(err).To(BeNil())
			*/
		})

		It("handles empty batch", func() {
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(0))
		})

		It("handles message with missing required metadata", func() {
			msg := service.NewMessage(nil)
			// Missing required metadata fields

			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
			Expect(err).NotTo(BeNil())
			Expect(result).To(BeNil())
		})

		It("caches UNS map entries", func() {
			// Create two messages with the same UNS tree ID
			msg1 := service.NewMessage(nil)
			msg1.MetaSet("topic", "umh.v1.test-topic._historian")
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600000),
				"some_value":   3,
			})

			msg2 := service.NewMessage(nil)
			msg2.MetaSet("topic", "umh.v1.test-topic._historian")
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600001),
				"some_value":   5,
			})

			// Process both messages
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1, msg2})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(HaveLen(1))

			// Verify the output messages
			outputMsg := result[0][0]
			Expect(outputMsg).NotTo(BeNil())

			// Dump to disk for testing
			/*
				bytes, err := outputMsg.AsBytes()
				Expect(err).To(BeNil())
				Expect(bytes).NotTo(BeNil())
				err = os.WriteFile("multiple_messages.proto", bytes, 0644)
				Expect(err).To(BeNil())
			*/
		})
	})
})
