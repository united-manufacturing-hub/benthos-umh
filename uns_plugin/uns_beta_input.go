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

// uns_beta input (ENG-5094): a thin adapter that delegates to the official
// redpanda Connect input via OwnedInput so that NACKed batches are never
// committed away.

package uns_plugin

import (
	"context"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Registers the official "redpanda" input that uns_beta delegates to.
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
)

func unsBetaConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("broker_address").
			Description("Kafka broker address to connect to. This can be a single address or multiple addresses separated by commas. For example: \"localhost:9092\" or \"broker1:9092,broker2:9092\".")).
		Field(service.NewStringField("consumer_group").
			Description("Consumer group used when consuming umh.messages."))
}

func init() {
	if err := service.RegisterBatchInput("uns_beta", unsBetaConfigSpec(), newUnsBetaInput); err != nil {
		panic(err)
	}
}

func newUnsBetaInput(conf *service.ParsedConfig, _ *service.Resources) (service.BatchInput, error) {
	brokerAddress, err := conf.FieldString("broker_address")
	if err != nil {
		return nil, err
	}
	if brokerAddress == "" {
		return nil, errors.New("broker_address must not be empty")
	}
	consumerGroup, err := conf.FieldString("consumer_group")
	if err != nil {
		return nil, err
	}
	if consumerGroup == "" {
		// An empty group parses fine downstream but silently disables offset
		// commits (full-topic replay on every restart), so fail here.
		return nil, errors.New("consumer_group must not be empty")
	}

	innerYAML := fmt.Sprintf(`
input:
  redpanda:
    seed_brokers: [%q]
    topics: [%q]
    consumer_group: %q
    start_offset: earliest
`, brokerAddress, defaultInputKafkaTopic, consumerGroup)
	// start_offset matches redpanda's current default; pinned deliberately so an
	// upstream default change cannot alter uns_beta's first-connect behavior.

	// ParseYAML(innerYAML, nil) builds the inner redpanda input on a fresh
	// internal manager whose logger and metrics are noops; the public API has
	// no hook to hand it the outer Resources, so the inner input's own logs
	// and metrics are dropped.
	innerSpec := service.NewConfigSpec().Field(service.NewInputField("input"))
	parsed, err := innerSpec.ParseYAML(innerYAML, nil)
	if err != nil {
		return nil, err
	}
	inner, err := parsed.FieldInput("input")
	if err != nil {
		return nil, err
	}
	return &unsBetaInput{inner: inner}, nil
}

// unsBetaInput adapts *service.OwnedInput (which manages its own connectivity)
// to the service.BatchInput interface.
type unsBetaInput struct {
	inner *service.OwnedInput
}

// Connect is a no-op: the delegated OwnedInput connects lazily on first
// ReadBatch.
func (i *unsBetaInput) Connect(context.Context) error {
	return nil
}

func (i *unsBetaInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return i.inner.ReadBatch(ctx)
}

func (i *unsBetaInput) Close(ctx context.Context) error {
	return i.inner.Close(ctx)
}
