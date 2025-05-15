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
	"context"
	lru "github.com/hashicorp/golang-lru"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("TagBrowserProcessor", func() {
	var processor *TagBrowserProcessor

	BeforeEach(func() {
		processor = NewTagBrowserProcessor(nil, nil, 0)
	})

	Describe("ProcessBatch", func() {
		It("processes a single message successfully", func() {
			// Create a message with basic metadata
			msg := service.NewMessage(nil)
			msg.MetaSet("umh_topic", "umh.v1.test-topic._historian.some_value")
			msg.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600000),
				"some_value":   13,
			})

			// Process the message
			var err error
			processor.topicMetadataCache, err = lru.New(1)
			Expect(err).To(BeNil())
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
			msg := service.NewMessage([]byte("test"))
			// No metadata set

			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
			Expect(err).To(BeNil())
			Expect(result).To(BeNil())
		})

		It("caches UNS map entries", func() {
			// Create two messages with the same UNS tree ID
			msg1 := service.NewMessage(nil)
			msg1.MetaSet("umh_topic", "umh.v1.test-topic._historian.some_value")
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600000),
				"some_value":   3,
			})

			msg2 := service.NewMessage(nil)
			msg2.MetaSet("umh_topic", "umh.v1.test-topic._historian.some_value")
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600001),
				"some_value":   5,
			})

			// Process both messages
			var err error
			processor.topicMetadataCache, err = lru.New(1)
			Expect(err).To(BeNil())
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

		It("caches UNS map entries accross multiple invocations", func() {
			// Create two messages with the same UNS tree ID
			msg1 := service.NewMessage(nil)
			msg1.MetaSet("umh_topic", "umh.v1.test-topic._historian.some_value")
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600000),
				"some_value":   3,
			})

			msg2 := service.NewMessage(nil)
			msg2.MetaSet("umh_topic", "umh.v1.test-topic._historian.some_value")
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600001),
				"some_value":   5,
			})

			// Process first messages
			var err error
			processor.topicMetadataCache, err = lru.New(1)
			Expect(err).To(BeNil())
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(HaveLen(1))

			// Verify the output messages
			outputMsg := result[0][0]
			Expect(outputMsg).NotTo(BeNil())

			// Process 2nd messages
			processor.topicMetadataCache, err = lru.New(1)
			Expect(err).To(BeNil())
			result2, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
			Expect(err).To(BeNil())
			Expect(result2).To(HaveLen(1))
			Expect(result2[0]).To(HaveLen(1))

			// Verify the output messages
			outputMsg2 := result2[0][0]
			Expect(outputMsg2).NotTo(BeNil())

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
