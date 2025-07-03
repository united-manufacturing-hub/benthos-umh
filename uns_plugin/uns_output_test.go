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

package uns_plugin

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

type TestMessagePublisher interface {
	// Helper methods for Mock implementation
	IsProduceSyncCalled() bool
	IsCreateTopicCalled() bool
	GetRequestedProduceMessages() []Record
	WithConnectFunc(func(...kgo.Opt) error)
	WithCloseFunc(func() error)
	WithProduceFunc(func(context.Context, []Record) error)
	WithTopicExistsFunc(func(context.Context, string) (bool, int, error))
	WithCreateTopicFunc(func(context.Context, string, int32) error)
	MessagePublisher
}

type ConnectFunc func(...kgo.Opt) error
type CloseFunc func() error
type ProduceFunc func(context.Context, []Record) error
type TopicExistsFunc func(context.Context, string) (bool, int, error)
type CreateTopicFunc func(context.Context, string, int32) error

type MockKafkaClient struct {
	// Mock behaviours
	connectFunc     ConnectFunc
	closeFunc       CloseFunc
	produceFunc     ProduceFunc
	topicExistsFunc TopicExistsFunc
	createTopicFunc CreateTopicFunc

	// Fields for test observation
	produceSyncCalled        bool
	createTopicCalled        bool
	requestedProduceMessages []Record
}

func (m *MockKafkaClient) Connect(...kgo.Opt) error {
	return m.connectFunc()
}

func (m *MockKafkaClient) Close() error {
	return m.closeFunc()
}

func (m *MockKafkaClient) ProduceSync(ctx context.Context, records []Record) error {
	m.produceSyncCalled = true
	m.requestedProduceMessages = records
	return m.produceFunc(ctx, records)
}

func (m *MockKafkaClient) CreateTopic(ctx context.Context, topic string, partition int32) error {
	m.createTopicCalled = true
	return m.createTopicFunc(ctx, topic, partition)
}

func (m *MockKafkaClient) IsTopicExists(ctx context.Context, topic string) (bool, int, error) {
	return m.topicExistsFunc(ctx, topic)
}

func (m *MockKafkaClient) IsProduceSyncCalled() bool {
	return m.produceSyncCalled
}

func (m *MockKafkaClient) IsCreateTopicCalled() bool {
	return m.createTopicCalled
}

func (m *MockKafkaClient) GetRequestedProduceMessages() []Record {
	return m.requestedProduceMessages
}

func (m *MockKafkaClient) WithConnectFunc(f func(...kgo.Opt) error) {
	m.connectFunc = f
}

func (m *MockKafkaClient) WithCloseFunc(f func() error) {
	m.closeFunc = f
}

func (m *MockKafkaClient) WithProduceFunc(f func(context.Context, []Record) error) {
	m.produceFunc = f
}

func (m *MockKafkaClient) WithTopicExistsFunc(f func(context.Context, string) (bool, int, error)) {
	m.topicExistsFunc = f
}

func (m *MockKafkaClient) WithCreateTopicFunc(f func(context.Context, string, int32) error) {
	m.createTopicFunc = f
}

var _ = Describe("Initializing uns output plugin", func() {
	var (
		outputPlugin service.BatchOutput
		unsClient    *unsOutput
		ctx          context.Context
		cancel       context.CancelFunc
		mockClient   TestMessagePublisher
	)

	BeforeEach(func() {
		// Default mock behaviours for the happy path
		// These functions can be overridden in the following tests
		mockClient = &MockKafkaClient{
			connectFunc: func(...kgo.Opt) error {
				return nil
			},
			closeFunc: func() error {
				return nil
			},
			produceFunc: func(ctx context.Context, r []Record) error {
				if len(r) == 0 {
					return errors.New("produceSync is called with empty messages list")
				}
				return nil
			},
			topicExistsFunc: func(ctx context.Context, s string) (bool, int, error) {
				return true, 1, nil
			},
			createTopicFunc: func(ctx context.Context, s string, i int32) error {
				return nil
			},
		}

		unsConf := unsOutputConfig{
			bridgedBy: "default-test-bridge",
		}
		umh_topic, _ := service.NewInterpolatedString("${! meta(\"umh_topic\") }")
		unsConf.umh_topic = umh_topic
		outputPlugin = newUnsOutputWithClient(mockClient, unsConf, nil, nil)
		unsClient = outputPlugin.(*unsOutput)
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	})

	AfterEach(func() {
		if cancel != nil {
			cancel()
		}
	})

	Context("calling outputConfig method", func() {
		It("should return a valid configspec", func() {
			spec := outputConfig()
			Expect(spec).NotTo(BeNil())
		})
	})

	Context("calling Connect function", func() {
		It("should initialize the kafka client", func() {
			err := outputPlugin.Connect(ctx)
			Expect(err).To(BeNil())
			Expect(unsClient.client).NotTo(BeNil())
		})

		When("the internal kafka client throws error", func() {
			JustBeforeEach(func() {
				mockClient.WithConnectFunc(func(o ...kgo.Opt) error {
					return errors.New("mock kafka client error: no valid seedbrokers")
				})
			})
			It("should throw the error", func() {
				err := outputPlugin.Connect(ctx)
				Expect(err.Error()).To(BeEquivalentTo("error while creating a kafka client with broker : mock kafka client error: no valid seedbrokers"))
			})
		})

		When("the default output topic does not exists", func() {
			JustBeforeEach(func() {
				mockClient.WithTopicExistsFunc(func(ctx context.Context, s string) (bool, int, error) {
					return false, 0, nil
				})
				mockClient.WithCreateTopicFunc(func(ctx context.Context, s string, i int32) error {
					return nil
				})
			})

			It("should create the default output topic", func() {
				err := outputPlugin.Connect(ctx)
				Expect(err).To(BeNil())

				client, ok := unsClient.client.(TestMessagePublisher)
				Expect(ok).To(BeTrue())
				Expect(client.IsCreateTopicCalled()).To(BeTrue())

			})
		})

		When("the default topic exists but with an unexpected partition count", func() {
			JustBeforeEach(func() {
				partitionCount := 15 // a number different from the expected defaultOutputTopicPartitionCount
				mockClient.WithTopicExistsFunc(func(ctx context.Context, s string) (bool, int, error) {
					return true, partitionCount, nil
				})
			})

			It("should return an error regarding the partitionCount", func() {
				err := outputPlugin.Connect(ctx)
				Expect(err.Error()).To(BeEquivalentTo("default output topic 'umh.messages' has a mismatched partition count: required 1, actual 15"))
			})
		})
	})

	Context("calling Close function", func() {
		It("should close the underlying kafka client", func() {
			err := outputPlugin.Connect(ctx)
			Expect(err).To(BeNil())

			err = outputPlugin.Close(ctx)
			Expect(err).To(BeNil())
			Expect(unsClient.client).To(BeNil())
		})

	})

	Context("calling WriteBatch function", func() {
		JustBeforeEach(func() {
			// Lets group all the Connect calls
			_ = outputPlugin.Connect(ctx)
		})
		When("with empty list of message", func() {
			It("should throw error about empty message list", func() {
				err := outputPlugin.WriteBatch(ctx, nil)
				Expect(err).To(BeNil())
			})
		})
		When("with list of messages", func() {
			It("should call produceSync internally with the given list of messages", func() {
				var msgs service.MessageBatch
				for range 10 {
					msg := service.NewMessage([]byte(`{"data": "test"}`))
					msg.MetaSet("umh_topic", "umh.v1.enterprise._historian.test")
					msgs = append(msgs, msg)
				}
				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err).To(BeNil())

				client, ok := unsClient.client.(TestMessagePublisher)
				Expect(ok).To(BeTrue())
				produceFuncCalled := client.IsProduceSyncCalled()
				Expect(produceFuncCalled).To(BeTrue())
				messages := client.GetRequestedProduceMessages()
				Expect(len(messages)).To(BeNumerically("==", 10))
				for _, m := range messages {
					Expect(m.Topic).To(BeEquivalentTo(defaultOutputTopic))
					Expect(m.Value).To(BeEquivalentTo([]byte(`{"data": "test"}`)))
					Expect(m.Key).To(BeEquivalentTo([]byte("umh.v1.enterprise._historian.test")))
				}
			})

			It("should throw an error if the message does not have a topic key", Label("test"), func() {
				// The meta key topic is not set
				msg := service.NewMessage([]byte(`{"data": "test"}`))
				msgs := service.MessageBatch{msg}
				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("umh_topic is not set or is empty in message 0, umh_topic is mandatory"))
			})
		})

		When("the internal kafka client throws a produce error", func() {
			// Mock the Producefunc with an error
			JustBeforeEach(func() {
				mockClient.WithProduceFunc(func(ctx context.Context, r []Record) error {
					return errors.New("leader partition not found")
				})
			})

			It("should throw an error", func() {
				var msgs service.MessageBatch
				for range 10 {
					msg := service.NewMessage(nil)
					msg.MetaSet("umh_topic", "umh.v1.enterprise._historian.messages")
					msg.SetStructured(map[string]any{
						"value": "mock message",
					})
					msgs = append(msgs, msg)
				}
				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err.Error()).To(BeEquivalentTo("error writing batch output to kafka: leader partition not found"))
			})
		})

		When("with umh_topic containing consecutive dots", func() {
			It("should reject consecutive dots in the message key", func() {
				msg := service.NewMessage([]byte(`{"value": false, "timestamp_ms": 1750930223716}`))
				// Set umh_topic with consecutive dots (similar to the original problem)
				msg.MetaSet("umh_topic", "umh.v1.UMH-Systems-GmbH---Dev-Team......._historian.Root.Objects.SiemensPLC_fallback._System._EnableDiagnostics")
				msgs := service.MessageBatch{msg}

				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid UMH topic"))
				Expect(err.Error()).To(ContainSubstring("cannot be empty"))
			})

			It("should reject multiple consecutive dots in different parts of the topic", func() {
				msg := service.NewMessage([]byte(`{"value": 42.5, "timestamp_ms": 1750930223716}`))
				// Test with multiple consecutive dots in different locations
				msg.MetaSet("umh_topic", "umh.v1.enterprise...site.area.._historian..virtual.path.tag_name")
				msgs := service.MessageBatch{msg}

				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid UMH topic"))
				Expect(err.Error()).To(ContainSubstring("cannot be empty"))
			})

			It("should reject message keys with invalid characters", func() {
				msg := service.NewMessage([]byte(`{"value": 42.5, "timestamp_ms": 1750930223716}`))
				// Test with invalid characters
				msg.MetaSet("umh_topic", "umh.v1.enterprise@#$.site.area._historian.tag")
				msgs := service.MessageBatch{msg}

				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid UMH topic"))
				Expect(err.Error()).To(ContainSubstring("invalid characters"))
			})

			It("should reject message keys with leading dots", func() {
				msg := service.NewMessage([]byte(`{"value": 42.5, "timestamp_ms": 1750930223716}`))
				msg.MetaSet("umh_topic", ".umh.v1.enterprise.site.area._historian.tag")
				msgs := service.MessageBatch{msg}

				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid UMH topic"))
				Expect(err.Error()).To(ContainSubstring("must start with umh.v1"))
			})

			It("should reject message keys with trailing dots", func() {
				msg := service.NewMessage([]byte(`{"value": 42.5, "timestamp_ms": 1750930223716}`))
				msg.MetaSet("umh_topic", "umh.v1.enterprise.site.area._historian.tag.")
				msgs := service.MessageBatch{msg}

				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid UMH topic"))
				Expect(err.Error()).To(ContainSubstring("cannot be empty"))
			})

			It("should accept valid message keys", func() {
				msg := service.NewMessage([]byte(`{"value": 42.5, "timestamp_ms": 1750930223716}`))
				msg.MetaSet("umh_topic", "umh.v1.enterprise.site.area._historian.tag")
				msgs := service.MessageBatch{msg}

				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err).To(BeNil())

				client, ok := unsClient.client.(TestMessagePublisher)
				Expect(ok).To(BeTrue())
				Expect(client.IsProduceSyncCalled()).To(BeTrue())

				messages := client.GetRequestedProduceMessages()
				Expect(len(messages)).To(Equal(1))
				Expect(string(messages[0].Key)).To(Equal("umh.v1.enterprise.site.area._historian.tag"))
			})
		})
	})
})

var _ = Describe("topic validation", func() {
	Context("when validating message keys", func() {
		It("should reject six consecutive dots", func() {
			input := "umh.v1.UMH-Systems-GmbH---Dev-Team......._historian.Root.Objects.tag"
			_, err := topic.NewUnsTopic(input)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot be empty"))
		})

		It("should reject multiple groups of consecutive dots", func() {
			input := "umh.v1.enterprise...site..area.._historian..tag"
			_, err := topic.NewUnsTopic(input)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot be empty"))
		})

		It("should reject a very long sequence of dots", func() {
			input := "umh.v1.enterprise............site.tag"
			_, err := topic.NewUnsTopic(input)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must contain a data contract"))
		})

		It("should accept valid message keys with single dots", func() {
			input := "umh.v1.enterprise.site.area._historian.tag"
			_, err := topic.NewUnsTopic(input)
			Expect(err).To(BeNil())
		})

		It("should reject invalid characters", func() {
			input := "umh.v1.enterprise@#$.site%%..area._historian.tag"
			_, err := topic.NewUnsTopic(input)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot be empty"))
		})

		It("should reject leading dots", func() {
			input := ".umh.v1.enterprise.site.tag"
			_, err := topic.NewUnsTopic(input)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must start with umh.v1"))
		})

		It("should reject trailing dots", func() {
			input := "umh.v1.enterprise.site.tag."
			_, err := topic.NewUnsTopic(input)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must contain a data contract"))
		})

		It("should reject both leading and trailing dots", func() {
			input := ".umh.v1.enterprise.site.tag."
			_, err := topic.NewUnsTopic(input)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must start with umh.v1"))
		})

		It("should accept valid UMH topics", func() {
			validTopics := []string{
				"umh.v1.enterprise.site.area._historian.tag",
				"umh.v1.acme.berlin.assembly._analytics.temperature",
				"umh.v1.factory.line1.station2._raw.pressure_sensor",
			}

			for _, topicStr := range validTopics {
				_, err := topic.NewUnsTopic(topicStr)
				Expect(err).To(BeNil(), "Expected topic '%s' to be valid", topicStr)
			}
		})
	})
})
