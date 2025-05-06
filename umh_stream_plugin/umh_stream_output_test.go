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
	Streamer
}

type MockKafkaClient struct {
	produceSyncCalled        bool
	requestedProduceMessages []Record
}

func (m *MockKafkaClient) Connect(...kgo.Opt) error {
	return nil
}

func (m *MockKafkaClient) Close() error {
	return nil
	// return errors.New("attempting to close an nil kafka client")
}

func (m *MockKafkaClient) ProduceSync(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return errors.New("produceSync is called with empty messages list")
	}
	m.produceSyncCalled = true
	m.requestedProduceMessages = records
	return nil
}

func (m *MockKafkaClient) IsProduceSyncCalled() bool {
	return m.produceSyncCalled
}

func (m *MockKafkaClient) GetRequestedProduceMessages() []Record {
	return m.requestedProduceMessages
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
		mockClient = &MockKafkaClient{}
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
			// client should be nil before connecting
			err := outputPlugin.Connect(ctx)
			Expect(err).To(BeNil())
			Expect(umhStreamClient.client).NotTo(BeNil())
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
		When("with empty list of message", func() {
			It("should throw error about empty message list", func() {
				_ = outputPlugin.Connect(ctx)
				err := outputPlugin.WriteBatch(ctx, nil)
				Expect(err.Error()).To(BeEquivalentTo("error while writing batch output to kafka: produceSync is called with empty messages list"))
			})
		})
		When("with list of messages", func() {
			It("should call produceSync internally with the given list of messages", func() {
				_ = outputPlugin.Connect(ctx)
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
	})
})
