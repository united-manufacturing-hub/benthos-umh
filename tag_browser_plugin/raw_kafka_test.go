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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Raw Kafka Message Processing", func() {
	Describe("messageToRawKafkaMsg", func() {
		It("extracts headers and payload correctly", func() {
			// Create a message with metadata and payload
			msg := service.NewMessage([]byte("test payload"))
			msg.MetaSet("header1", "value1")
			msg.MetaSet("header2", "value2")

			// Process the message
			kafkaMsg, err := messageToRawKafkaMsg(msg)
			Expect(err).To(BeNil())
			Expect(kafkaMsg).NotTo(BeNil())

			// Verify headers
			Expect(kafkaMsg.Headers).To(HaveLen(2))
			Expect(kafkaMsg.Headers).To(HaveKeyWithValue("header1", "value1"))
			Expect(kafkaMsg.Headers).To(HaveKeyWithValue("header2", "value2"))

			// Verify payload
			Expect(kafkaMsg.Payload).To(Equal("test payload"))
		})

		It("handles message with no headers", func() {
			// Create a message with only payload
			msg := service.NewMessage([]byte("test payload"))

			// Process the message
			kafkaMsg, err := messageToRawKafkaMsg(msg)
			Expect(err).To(BeNil())
			Expect(kafkaMsg).NotTo(BeNil())

			// Verify headers are empty
			Expect(kafkaMsg.Headers).To(BeEmpty())

			// Verify payload
			Expect(kafkaMsg.Payload).To(Equal("test payload"))
		})

		It("handles message with empty payload", func() {
			// Create a message with headers but empty payload
			msg := service.NewMessage(nil)
			msg.MetaSet("header1", "value1")

			// Process the message
			kafkaMsg, err := messageToRawKafkaMsg(msg)
			Expect(err).To(BeNil())
			Expect(kafkaMsg).NotTo(BeNil())

			// Verify headers
			Expect(kafkaMsg.Headers).To(HaveLen(1))
			Expect(kafkaMsg.Headers).To(HaveKeyWithValue("header1", "value1"))

			// Verify payload is empty
			Expect(kafkaMsg.Payload).To(BeEmpty())
		})

		It("handles message with special characters in headers", func() {
			// Create a message with special characters in headers
			msg := service.NewMessage([]byte("test payload"))
			msg.MetaSet("header-1", "value-1")
			msg.MetaSet("header_2", "value_2")
			msg.MetaSet("header.3", "value.3")

			// Process the message
			kafkaMsg, err := messageToRawKafkaMsg(msg)
			Expect(err).To(BeNil())
			Expect(kafkaMsg).NotTo(BeNil())

			// Verify headers
			Expect(kafkaMsg.Headers).To(HaveLen(3))
			Expect(kafkaMsg.Headers).To(HaveKeyWithValue("header-1", "value-1"))
			Expect(kafkaMsg.Headers).To(HaveKeyWithValue("header_2", "value_2"))
			Expect(kafkaMsg.Headers).To(HaveKeyWithValue("header.3", "value.3"))

			// Verify payload
			Expect(kafkaMsg.Payload).To(Equal("test payload"))
		})

		It("handles message with binary payload", func() {
			// Create a message with binary payload
			binaryData := []byte{0x00, 0x01, 0x02, 0x03, 0xFF}
			msg := service.NewMessage(binaryData)
			msg.MetaSet("content-type", "application/octet-stream")

			// Process the message
			kafkaMsg, err := messageToRawKafkaMsg(msg)
			Expect(err).To(BeNil())
			Expect(kafkaMsg).NotTo(BeNil())

			// Verify headers
			Expect(kafkaMsg.Headers).To(HaveLen(1))
			Expect(kafkaMsg.Headers).To(HaveKeyWithValue("content-type", "application/octet-stream"))

			// Verify payload is converted to string correctly
			Expect(kafkaMsg.Payload).To(Equal(string(binaryData)))
			Expect([]byte(kafkaMsg.Payload)).To(Equal(binaryData))
		})
	})
})
