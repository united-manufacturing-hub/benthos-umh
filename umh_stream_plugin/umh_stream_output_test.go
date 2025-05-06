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

package umhstreamplugin

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TestStreamer interface {
	// Helper methods for Mock implementation
	IsProduceSyncCalled() bool
	GetRequestedProduceMessages() []Record
	WithConnectFunc(func(...kgo.Opt) error)
	WithCloseFunc(func() error)
	WithProduceFunc(func(context.Context, []Record) error)
	Streamer
}

type ConnectFunc func(...kgo.Opt) error
type CloseFunc func() error
type ProduceFunc func(context.Context, []Record) error

type MockKafkaClient struct {
	// Mock behaviours
	connectFunc ConnectFunc
	closeFunc   CloseFunc
	produceFunc ProduceFunc

	// Fields for test observation
	produceSyncCalled        bool
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

func (m *MockKafkaClient) IsProduceSyncCalled() bool {
	return m.produceSyncCalled
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

var _ = Describe("Initializing UMH stream output plugin", func() {
	var (
		outputPlugin    service.BatchOutput
		umhStreamClient *umhStreamOutput
		ctx             context.Context
		cancel          context.CancelFunc
		mockClient      TestStreamer
		topicKey        *service.InterpolatedString
	)

	BeforeEach(func() {
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
		}

		topicKey, _ = service.NewInterpolatedString("topic")
		outputPlugin = newUMHStreamOutputWithClient(mockClient, topicKey, nil)
		umhStreamClient = outputPlugin.(*umhStreamOutput)
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
			Expect(umhStreamClient.client).NotTo(BeNil())
		})

		When("the internal kafka client throws error", func() {
			JustBeforeEach(func() {
				mockClient.WithConnectFunc(func(o ...kgo.Opt) error {
					return errors.New("mock kafka client error: no valid seedbrokers")
				})
			})
			It("should throw the error", func() {
				err := outputPlugin.Connect(ctx)
				Expect(err.Error()).To(BeEquivalentTo("error while creating a kafka client: mock kafka client error: no valid seedbrokers"))
			})
		})
	})

	Context("calling Close function", func() {
		It("should close the underlying kafka client", func() {
			err := outputPlugin.Connect(ctx)
			Expect(err).To(BeNil())

			err = outputPlugin.Close(ctx)
			Expect(err).To(BeNil())
			Expect(umhStreamClient.client).To(BeNil())
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
				Expect(err.Error()).To(BeEquivalentTo("error while writing batch output to kafka: produceSync is called with empty messages list"))
			})
		})
		When("with list of messages", func() {
			It("should call produceSync internally with the given list of messages", func() {
				var msgs service.MessageBatch
				for range 10 {
					msg := service.NewMessage(nil)
					msg.MetaSet("topic", "umh.v1.messages")
					msg.SetStructured(map[string]any{
						"value": "mock message",
					})
					msgs = append(msgs, msg)
				}
				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err).To(BeNil())

				client, ok := umhStreamClient.client.(TestStreamer)
				Expect(ok).To(BeTrue())
				produceFuncCalled := client.IsProduceSyncCalled()
				Expect(produceFuncCalled).To(BeTrue())
				messages := client.GetRequestedProduceMessages()
				Expect(len(messages)).To(BeNumerically("==", 10))
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
					msg.MetaSet("topic", "umh.v1.messages")
					msg.SetStructured(map[string]any{
						"value": "mock message",
					})
					msgs = append(msgs, msg)
				}
				err := outputPlugin.WriteBatch(ctx, msgs)
				Expect(err.Error()).To(BeEquivalentTo("error while writing batch output to kafka: leader partition not found"))
			})
		})
	})
})
