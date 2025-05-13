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
)

type TestMessageConsumer interface {
	// Helper methods for Mock implementation
	IsConnectCalled() bool
	IsCloseCalled() bool
	IsCommitRecordsCalled() bool
	WithConnectFunc(func(...kgo.Opt) error)
	WithCloseFunc(func() error)
	WithPollFetchesFunc(func(context.Context) Fetches)
	WithCommitRecordsFunc(func(context.Context) error)
	MessageConsumer
}

type PollFetchesFunc func(context.Context) Fetches
type CommitRecordsFunc func(context.Context) error

type MockKafkaConsumerClient struct {
	// Mock behaviours
	connectFunc       ConnectFunc
	closeFunc         CloseFunc
	pollFetchesFunc   PollFetchesFunc
	commitRecordsFunc CommitRecordsFunc

	// Fields for test observation
	connectCalled       bool
	closeCalled         bool
	commitRecordsCalled bool
	connectOptions      []kgo.Opt
}

func (m *MockKafkaConsumerClient) Connect(opts ...kgo.Opt) error {
	m.connectCalled = true
	m.connectOptions = opts
	return m.connectFunc(opts...)
}

func (m *MockKafkaConsumerClient) Close() error {
	m.closeCalled = true
	return m.closeFunc()
}

func (m *MockKafkaConsumerClient) PollFetches(ctx context.Context) Fetches {
	return m.pollFetchesFunc(ctx)
}

func (m *MockKafkaConsumerClient) CommitRecords(ctx context.Context) error {
	m.commitRecordsCalled = true
	return m.commitRecordsFunc(ctx)
}

func (m *MockKafkaConsumerClient) IsConnectCalled() bool {
	return m.connectCalled
}

func (m *MockKafkaConsumerClient) IsCloseCalled() bool {
	return m.closeCalled
}

func (m *MockKafkaConsumerClient) IsCommitRecordsCalled() bool {
	return m.commitRecordsCalled
}

func (m *MockKafkaConsumerClient) WithConnectFunc(f func(...kgo.Opt) error) {
	m.connectFunc = f
}

func (m *MockKafkaConsumerClient) WithCloseFunc(f func() error) {
	m.closeFunc = f
}

func (m *MockKafkaConsumerClient) WithPollFetchesFunc(f func(context.Context) Fetches) {
	m.pollFetchesFunc = f
}

func (m *MockKafkaConsumerClient) WithCommitRecordsFunc(f func(context.Context) error) {
	m.commitRecordsFunc = f
}

var _ Fetches = (*MockFetches)(nil)

// MockFetches implements our Fetches interface for testing
type MockFetches struct {
	empty   bool
	err     error
	records []*kgo.Record
}

func (m *MockFetches) Empty() bool {
	return m.empty
}

func (m *MockFetches) Err() error {
	return m.err
}

func (m *MockFetches) EachRecord(fn func(*kgo.Record)) {
	for _, record := range m.records {
		fn(record)
	}
}

func (m *MockFetches) EachError(fn func(string, int32, error)) {
	if m.err != nil {
		fn("test-topic", 0, m.err)
	}
}

func (m *MockFetches) Err0() error {
	return m.err
}

var _ = Describe("Initializing uns input plugin", Label("uns_input"), func() {
	var (
		inputPlugin service.BatchInput
		unsClient   *UnsInput
		ctx         context.Context
		cancel      context.CancelFunc
		mockClient  TestMessageConsumer
		resoruces   *service.Resources
	)

	BeforeEach(func() {
		// Default mock behaviours for the happy path
		mockClient = &MockKafkaConsumerClient{
			connectFunc: func(...kgo.Opt) error {
				return nil
			},
			closeFunc: func() error {
				return nil
			},
			pollFetchesFunc: func(ctx context.Context) Fetches {
				// Return an empty fetches object by default
				return &MockFetches{
					empty: true,
				}
			},
			commitRecordsFunc: func(ctx context.Context) error {
				return nil
			},
		}

		inputConfig := UnsInputConfig{
			topic:           defaultTopicKey,
			inputKafkaTopic: defaultInputKafkaTopic,
			brokerAddress:   defaultBrokerAddress,
			consumerGroup:   defaultConsumerGroup,
		}
		resoruces = service.MockResources()
		inputPlugin, _ = NewUnsInput(mockClient, inputConfig, resoruces.Logger(), resoruces.Metrics())
		unsClient = inputPlugin.(*UnsInput)
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	})

	AfterEach(func() {
		if cancel != nil {
			cancel()
		}
	})

	Context("calling Connect function", func() {
		It("should initialize the kafka client", func() {
			err := inputPlugin.Connect(ctx)
			Expect(err).To(BeNil())

			client, ok := unsClient.client.(TestMessageConsumer)
			Expect(ok).To(BeTrue())
			Expect(client.IsConnectCalled()).To(BeTrue())
		})

		When("the internal kafka client throws error", func() {
			JustBeforeEach(func() {
				mockClient.WithConnectFunc(func(o ...kgo.Opt) error {
					return errors.New("mock kafka client error: no valid seedbrokers")
				})
			})
			It("should throw the error", func() {
				err := inputPlugin.Connect(ctx)
				Expect(err.Error()).To(ContainSubstring("error while creating a kafka client with broker"))
				Expect(err.Error()).To(ContainSubstring("mock kafka client error: no valid seedbrokers"))
			})
		})
	})

	Context("calling Close function", func() {
		BeforeEach(func() {
			// Ensure client is connected before testing close
			_ = inputPlugin.Connect(ctx)
		})

		It("should close the underlying kafka client", func() {
			err := inputPlugin.Close(ctx)
			Expect(err).To(BeNil())

			client, ok := unsClient.client.(TestMessageConsumer)
			Expect(ok).To(BeTrue())
			Expect(client.IsCloseCalled()).To(BeTrue())
		})
	})

	Context("calling ReadBatch function", func() {
		BeforeEach(func() {
			// Ensure client is connected before testing readBatch
			_ = inputPlugin.Connect(ctx)
		})

		When("poll fetches returns empty", func() {
			BeforeEach(func() {
				mockClient.WithPollFetchesFunc(func(ctx context.Context) Fetches {
					return &MockFetches{
						empty: true,
					}
				})
			})

			It("should return nil batch", func() {
				batch, ackFn, err := inputPlugin.ReadBatch(ctx)
				Expect(err).To(BeNil())
				Expect(batch).To(BeNil())
				Expect(ackFn).To(BeNil())
			})
		})

		When("poll fetches returns error", func() {
			BeforeEach(func() {
				mockClient.WithPollFetchesFunc(func(ctx context.Context) Fetches {
					return &MockFetches{
						empty: false,
						err:   errors.New("mock fetch error"),
					}
				})
			})

			It("should return the error", func() {
				batch, ackFn, err := inputPlugin.ReadBatch(ctx)
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(Equal("mock fetch error"))
				Expect(batch).To(BeNil())
				Expect(ackFn).To(BeNil())
			})
		})

		When("poll fetches returns records", func() {
			BeforeEach(func() {
				mockClient.WithPollFetchesFunc(func(ctx context.Context) Fetches {
					records := []*kgo.Record{
						{
							Key:   []byte("umh.v1.acme.berlin.assembly.temperature"),
							Value: []byte(`{"value": 23.5}`),
							Topic: "umh.messages",
							Headers: []kgo.RecordHeader{
								{Key: "content-type", Value: []byte("application/json")},
							},
						},
						{
							Key:   []byte("umh.v1.acme.berlin.assembly.pressure"),
							Value: []byte(`{"value": 1013.25}`),
							Topic: "umh.messages",
						},
					}
					return &MockFetches{
						empty:   false,
						records: records,
					}
				})
			})

			It("should filter records based on topic regex and return a batch", func() {
				batch, ackFn, err := inputPlugin.ReadBatch(ctx)
				Expect(err).To(BeNil())
				Expect(batch).NotTo(BeNil())
				Expect(len(batch)).To(Equal(2))

				// Check first message
				b, err := batch[0].AsBytes()
				Expect(err).To(BeNil())
				Expect(string(b)).To(Equal(`{"value": 23.5}`))
				kafka_key, ok := batch[0].MetaGet("kafka_msg_key")
				Expect(kafka_key).To(Equal("umh.v1.acme.berlin.assembly.temperature"))
				Expect(ok).To(BeTrue())
				kafka_topic, ok := batch[0].MetaGet("kafka_topic")
				Expect(kafka_topic).To(Equal("umh.messages"))
				Expect(ok).To(BeTrue())

				// Check second message
				b, err = batch[1].AsBytes()
				Expect(err).To(BeNil())
				Expect(string(b)).To(Equal(`{"value": 1013.25}`))
				kafka_key, ok = batch[1].MetaGet("kafka_msg_key")
				Expect(kafka_key).To(Equal("umh.v1.acme.berlin.assembly.pressure"))
				Expect(ok).To(BeTrue())
				kafka_topic, ok = batch[1].MetaGet("kafka_topic")
				Expect(kafka_topic).To(Equal("umh.messages"))
				Expect(ok).To(BeTrue())

				// Test ack function
				Expect(ackFn).NotTo(BeNil())
				err = ackFn(ctx, nil)
				Expect(err).To(BeNil())

				client, ok := unsClient.client.(TestMessageConsumer)
				Expect(ok).To(BeTrue())
				Expect(client.IsCommitRecordsCalled()).To(BeTrue())
			})

			When("specific topic filter is applied", func() {
				BeforeEach(func() {
					inputConfig := UnsInputConfig{
						topic:           "umh\\.v1\\.acme\\.berlin\\.assembly\\.temperature",
						inputKafkaTopic: defaultInputKafkaTopic,
						brokerAddress:   defaultBrokerAddress,
						consumerGroup:   defaultConsumerGroup,
					}
					resoruces = service.MockResources()
					inputPlugin, _ = NewUnsInput(mockClient, inputConfig, resoruces.Logger(), resoruces.Metrics())
					unsClient = inputPlugin.(*UnsInput)
				})

				It("should only return records matching the filter", func() {
					batch, _, err := inputPlugin.ReadBatch(ctx)
					Expect(err).To(BeNil())
					Expect(batch).NotTo(BeNil())
					Expect(len(batch)).To(Equal(1))
					kafka_key, ok := batch[0].MetaGet("kafka_msg_key")
					Expect(kafka_key).To(Equal("umh.v1.acme.berlin.assembly.temperature"))
					Expect(ok).To(BeTrue())
				})
			})
		})

		When("ack function is called with error", func() {
			BeforeEach(func() {
				// Return records so ack function is created
				mockClient.WithPollFetchesFunc(func(ctx context.Context) Fetches {
					records := []*kgo.Record{
						{
							Key:   []byte("umh.v1.test"),
							Value: []byte(`{}`),
							Topic: "umh.messages",
						},
					}
					return &MockFetches{
						empty:   false,
						records: records,
					}
				})
			})

			It("should return the error without committing", func() {
				_, ackFn, _ := inputPlugin.ReadBatch(ctx)
				testErr := errors.New("processing error")
				err := ackFn(ctx, testErr)
				Expect(err).To(Equal(testErr))

				// CommitRecords should not be called when error is passed to ack function
				client, ok := unsClient.client.(TestMessageConsumer)
				Expect(ok).To(BeTrue())
				Expect(client.IsCommitRecordsCalled()).To(BeFalse())
			})
		})

		When("commit records fails", func() {
			BeforeEach(func() {
				// Return records so ack function is created
				mockClient.WithPollFetchesFunc(func(ctx context.Context) Fetches {
					records := []*kgo.Record{
						{
							Key:   []byte("umh.v1.test"),
							Value: []byte(`{}`),
							Topic: "umh.messages",
						},
					}
					return &MockFetches{
						empty:   false,
						records: records,
					}
				})

				mockClient.WithCommitRecordsFunc(func(ctx context.Context) error {
					return errors.New("commit error")
				})
			})

			It("should return the commit error", func() {
				_, ackFn, _ := inputPlugin.ReadBatch(ctx)
				err := ackFn(ctx, nil)
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(Equal("commit error"))
			})
		})

		When("the topic regex is invalid", func() {
			BeforeEach(func() {
				unsClient.config.topic = "[" // Invalid regex
				// Return records so that it doesn't return nil error due to empty fetch result
				mockClient.WithPollFetchesFunc(func(ctx context.Context) Fetches {
					records := []*kgo.Record{
						{
							Key:   []byte("umh.v1.test"),
							Value: []byte(`{}`),
							Topic: "umh.messages",
						},
					}
					return &MockFetches{
						empty:   false,
						records: records,
					}
				})

			})

			// It("should return a compile error", func() {
			// 	batch, ackFn, err := inputPlugin.ReadBatch(ctx)
			// 	Expect(err).NotTo(BeNil())
			// 	Expect(err.Error()).To(ContainSubstring("error compiling topic regex"))
			// 	Expect(batch).To(BeNil())
			// 	Expect(ackFn).To(BeNil())
			// })
		})
	})
})
