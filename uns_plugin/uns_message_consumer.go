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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type MessageConsumer interface {
	Connect(...kgo.Opt) error
	Close() error
}

// ConsumerClient is a wrapper for franz-go kafka client optimized to consume Kafka topics
type ConsumerClient struct {
	client      *kgo.Client
	adminClient *kadm.Client
}

// NewConsumerClient initializes the franz-go client
func NewConsumerClient() MessageConsumer {
	return &ConsumerClient{}
}

// Connect connects to the seedbroker with the given kafka client options
func (k *ConsumerClient) Connect(opts ...kgo.Opt) error {
	var err error
	k.client, err = kgo.NewClient(opts...)
	if err != nil {
		return err
	}

	k.adminClient = kadm.NewClient(k.client)
	return nil
}

// Close closes the underlying franz-go kafka client
func (k *ConsumerClient) Close() error {
	if k.client != nil {
		// franz-go client.Close() never returns an error
		k.client.Close()
	}
	return nil
}
