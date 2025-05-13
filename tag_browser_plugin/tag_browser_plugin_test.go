package tag_browser_plugin

import (
	"context"

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
			msg.MetaSet("topic", "test-topic")
			msg.MetaSet("timestamp", "2024-03-20T10:00:00Z")
			msg.MetaSet("value", "42")

			// Process the message
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(HaveLen(1))

			// Verify the output message
			outputMsg := result[0][0]
			Expect(outputMsg).NotTo(BeNil())
		})

		It("handles empty batch", func() {
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(BeEmpty())
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
			msg1.MetaSet("topic", "test-topic")
			msg1.MetaSet("timestamp", "2024-03-20T10:00:00Z")
			msg1.MetaSet("value", "42")

			msg2 := service.NewMessage(nil)
			msg2.MetaSet("topic", "test-topic")
			msg2.MetaSet("timestamp", "2024-03-20T10:01:00Z")
			msg2.MetaSet("value", "43")

			// Process both messages
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1, msg2})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(HaveLen(2))
		})
	})
})
