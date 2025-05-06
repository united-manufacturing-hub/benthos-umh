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
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultOutputTopic   = "umh.v1.messages"
	defaultBrokerAddress = "localhost:9092"
	defaultClientID      = "umh_core"
)

func outputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Writes to the local kafka topic, setting each message's key based on the given topic variable").
		Description("Sends messages to the umh core kafka topic with specified key").
		Field(service.NewInterpolatedStringField("topic").
			Description("key to use when sending messages to the output topic. Supports interpolation.").
			Example("${! meta(\"topic\") }").
			Example("enterprise.site.area_historian"))
}

type umhStreamOutput struct {
	topic  *service.InterpolatedString
	client Streamer
	log    *service.Logger
}

func newUMHStreamOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	// maximum number of messages that can be in processing simultaneously before requiring acknowledgements
	maxInFlight := 100

	batchPolicy := service.BatchPolicy{
		Count:  100,     //max number of messages per batch
		Period: "100ms", // timeout to ensure timely delivery even if the count aren't met
	}

	topic, err := conf.FieldInterpolatedString("topic")
	if err != nil {
		return nil, batchPolicy, 0, fmt.Errorf("error while parsing topic string from the config: %v", err)
	}

	return newUMHStreamOutputWithClient(NewClient(), topic, mgr.Logger()), batchPolicy, maxInFlight, nil
}

// Testable constructor that accepts client
func newUMHStreamOutputWithClient(client Streamer, topic *service.InterpolatedString, logger *service.Logger) service.BatchOutput {
	return &umhStreamOutput{
		client: client,
		topic:  topic,
		log:    logger,
	}
}

// Close closes the underlying kafka client
func (o *umhStreamOutput) Close(ctx context.Context) error {
	o.log.Infof("Attempting to close the umh_stream kafka client")
	if o.client != nil {
		o.client.Close()
		o.client = nil
	}
	o.log.Infof("umh_stream kafka client closed successfully")
	return nil
}

// Connect initializes the kafka client
func (o *umhStreamOutput) Connect(ctx context.Context) error {
	o.log.Infof("Connecting to umh core stream kafka broker: %v", defaultBrokerAddress)

	if o.client == nil {
		o.client = NewClient()
	}
	// Create the kafka client
	err := o.client.Connect(
		kgo.SeedBrokers(defaultBrokerAddress),       // bootstrap broker addresses
		kgo.AllowAutoTopicCreation(),                // Allow creating the defaultOutputTopic if it doesn't exists
		kgo.ClientID(defaultClientID),               // client id for all requests sent to the broker
		kgo.ConnIdleTimeout(15*time.Minute),         // Rough amount of time to allow connections to be idle. Default value at franz-go is 20
		kgo.DialTimeout(5*time.Second),              // Timeout while connecting to the broker. 5 second is more than enough to connect to a local broker
		kgo.DefaultProduceTopic(defaultOutputTopic), // topic to produce messages to. The plugin writes messages only to a single default topic
		kgo.RequiredAcks(kgo.LeaderAck()),           // Partition leader has to send acknowledment on successful write
		kgo.MaxBufferedRecords(1000),                // Max amount of records hte client will buffer
		kgo.ProduceRequestTimeout(5*time.Second),    // Produce Request Timeout
		kgo.ProducerLinger(1*time.Second),           // Duration on how long individual partitons will linger waiting for more records before triggering a Produce request
		kgo.DisableIdempotentWrite(),                // Idempotent write is disabled since the  plugin is going to communicate with a local kafka broker where one could assume less network failures. With Idempotency enabled, RequiredAcks should be from all insync replicas which will increase the latency
	)
	if err != nil {
		return fmt.Errorf("error while creating a kafka client: %v", err)
	}

	// Now the client is created, let's check for the defaultOutputTopic. Create it if it doesn't exists
	o.log.Infof("Connection to the kafka broker %s is successful", defaultBrokerAddress)

	return nil
}

// WriteBatch implements service.BatchOutput.
func (o *umhStreamOutput) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {

	records := make([]Record, 0, len(msgs))
	for _, msg := range msgs {
		key, err := o.topic.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to resolve topic field: %v", err)
		}

		o.log.Tracef("sending message with key: %s", key)

		msgAsBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		record := Record{
			Topic: defaultOutputTopic,
			Key:   []byte(key),
			Value: msgAsBytes,
		}

		records = append(records, record)

	}
	if err := o.client.ProduceSync(ctx, records); err != nil {
		return fmt.Errorf("error while writing batch output to kafka: %v", err)
	}
	return nil
}
