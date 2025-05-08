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
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

type Producer interface {
	ProduceSync(context.Context, []Record) error
}

type Admin interface {
	// IsTopicExists checks if a topic exists in the broker and returns a boolean value. If the topic exists, then its partition count is returned as the second value. If the topic is missing, the partition count will be 0
	IsTopicExists(context.Context, string) (bool, int, error)
	// CreateTopic create a topic in the broker
	// Second argument is the topic name
	// Third argument int32 is the number of partitions
	CreateTopic(context.Context, string, int32) error
}

type MessagePublisher interface {
	Connect(...kgo.Opt) error
	Close() error
	Producer
	Admin
}

// Client is a wrapper for franz-go kafka client
type Client struct {
	client      *kgo.Client
	adminClient *kadm.Client
}

// NewClient initializes the franz-go client
func NewClient() MessagePublisher {
	return &Client{}
}

// Connect connects to the seedbroker with the given kafka client options
func (k *Client) Connect(opts ...kgo.Opt) error {
	var err error
	k.client, err = kgo.NewClient(opts...)
	if err != nil {
		return err
	}

	k.adminClient = kadm.NewClient(k.client)
	return nil
}

// Close closes the underlying franz-go kafka client
func (k *Client) Close() error {
	if k.client != nil {
		// franz-go client.Close() never returns an error
		k.client.Close()
	}
	return nil
}

// ProduceSync produces a message batch to kafka
func (k *Client) ProduceSync(ctx context.Context, records []Record) error {
	if k.client == nil {
		return errors.New("attempt to produce using a nil kafka client")
	}

	kgoRecords := make([]*kgo.Record, 0, len(records))
	for _, r := range records {
		kgoRecord := &kgo.Record{
			Topic: r.Topic,
			Key:   r.Key,
			Value: r.Value,
		}

		if len(r.Headers) > 0 {
			recordHeaders := make([]kgo.RecordHeader, 0, len(r.Headers))
			for k, v := range r.Headers {
				header := kgo.RecordHeader{
					Key:   k,
					Value: v,
				}
				recordHeaders = append(recordHeaders, header)
			}
			kgoRecord.Headers = recordHeaders
		}

		kgoRecords = append(kgoRecords, kgoRecord)
	}

	return k.client.ProduceSync(ctx, kgoRecords...).FirstErr()
}

func (k *Client) IsTopicExists(ctx context.Context, topic string) (bool, int, error) {
	topicDetails, err := k.adminClient.ListTopics(ctx, topic)
	if err != nil {
		return false, 0, err
	}

	for _, td := range topicDetails {
		if td.Topic == topic {
			return true, len(td.Partitions.Numbers()), nil
		}
	}

	return false, 0, nil
}

func (k *Client) CreateTopic(ctx context.Context, topic string, partition int32) error {
	if partition < 1 {
		return fmt.Errorf("Invalid partition %d specified to create a topic", partition)
	}

	if topic == "" {
		return errors.New("empty topic name specified for topic creation")
	}

	//Since the plugin is going to communicate with the local broker, replication factor of 1 is a good default value
	replicationFactor := 1
	cleanupPolicy := "compact,delete"
	configs := map[string]*string{
		"cleanup.policy": &cleanupPolicy,
	}
	resp, err := k.adminClient.CreateTopic(ctx, partition, int16(replicationFactor), configs, topic)
	if err != nil {
		return err
	}

	if resp.Err != nil {
		return resp.Err
	}

	return nil
}
