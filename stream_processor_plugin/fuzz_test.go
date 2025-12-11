//go:build fuzz
// +build fuzz

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

package stream_processor_plugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/constants"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/processor"
)

// FuzzTestCase represents a single test case for fuzzing
type FuzzTestCase struct {
	Topic   string
	Payload string
	Mapping string
}

// FuzzStreamProcessor is the main fuzz entry point that tests the stream processor
// at the highest level by fuzzing the Process method with various inputs.
// This comprehensively tests all underlying functionality including JavaScript
// evaluation, variable handling, message processing, and security constraints.
func FuzzStreamProcessor(f *testing.F) {
	// Add seed inputs to fuzzer
	for _, seed := range getFuzzSeedInputs() {
		f.Add(seed.Topic, seed.Payload, seed.Mapping)
	}

	f.Fuzz(func(t *testing.T, topic, payload, mappingStr string) {
		runSingleFuzzTest(t, topic, payload, mappingStr)
	})
}

// getFuzzSeedInputs returns comprehensive seed inputs for fuzzing
func getFuzzSeedInputs() []FuzzTestCase {
	return []FuzzTestCase{
		// Valid UMH topics with various data types
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 25.5, "timestamp_ms": 1234567890}`,
			Mapping: `{"result": "press + 1", "status": "press > 20 ? 'high' : 'low'"}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.temp",
			Payload: `{"value": 80.0, "timestamp_ms": 1234567890}`,
			Mapping: `{"celsius": "temp", "fahrenheit": "temp * 9/5 + 32"}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.flow",
			Payload: `{"value": 100.0, "timestamp_ms": 1234567890}`,
			Mapping: `{"flow_rate": "flow", "flow_status": "flow > 50 ? 'normal' : 'low'"}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.run",
			Payload: `{"value": true, "timestamp_ms": 1234567890}`,
			Mapping: `{"running": "run", "status_code": "run ? 1 : 0"}`,
		},

		// Edge cases with special values
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 0, "timestamp_ms": 1234567890}`,
			Mapping: `{"result": "press / 0", "check": "isFinite(press) ? press : 0"}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.temp",
			Payload: `{"value": null, "timestamp_ms": 1234567890}`,
			Mapping: `{"temp_check": "temp == null ? 'no_data' : temp"}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": -999.99, "timestamp_ms": 1234567890}`,
			Mapping: `{"abs_press": "Math.abs(press)", "valid": "press > -1000"}`,
		},

		// Complex expressions
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 25.5, "timestamp_ms": 1234567890}`,
			Mapping: `{"complex": "Math.sqrt(Math.pow(press, 2) + Math.pow(temp || 0, 2))", "rounded": "Math.round(press * 100) / 100"}`,
		},

		// String processing
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.status",
			Payload: `{"value": "running", "timestamp_ms": 1234567890}`,
			Mapping: `{"upper": "status.toUpperCase()", "length": "status.length"}`,
		},

		// Invalid/malformed inputs
		{
			Topic:   "invalid.topic.structure",
			Payload: `{"value": 25.5, "timestamp_ms": 1234567890}`,
			Mapping: `{"result": "42"}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `invalid json payload`,
			Mapping: `{"result": "press"}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 25.5, "timestamp_ms": 1234567890}`,
			Mapping: `invalid mapping json`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 25.5, "timestamp_ms": 1234567890}`,
			Mapping: `{"result": ""}`,
		},

		// Security test cases (should be blocked)
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 25.5, "timestamp_ms": 1234567890}`,
			Mapping: `{"danger": "eval('1+1')", "safe": "press"}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 25.5, "timestamp_ms": 1234567890}`,
			Mapping: `{"danger": "Function('return 1+1')()", "safe": "press"}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 25.5, "timestamp_ms": 1234567890}`,
			Mapping: `{"danger": "require('fs')", "safe": "press"}`,
		},

		// Performance stress cases
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 2.0, "timestamp_ms": 1234567890}`,
			Mapping: `{"power": "Math.pow(press, 100)", "factorial": "press > 10 ? 'too_big' : press"}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 25.5, "timestamp_ms": 1234567890}`,
			Mapping: `{"loop_test": "press > 0 ? 'positive' : 'negative'"}`,
		},

		// Empty/minimal cases
		{
			Topic:   "",
			Payload: `{}`,
			Mapping: `{}`,
		},
		{
			Topic:   "umh.v1.corpA.plant-A.aawd._raw.press",
			Payload: `{"value": 25.5, "timestamp_ms": 1234567890}`,
			Mapping: `{}`,
		},
	}
}

// runSingleFuzzTest executes a single fuzz test case
func runSingleFuzzTest(t *testing.T, topic, payload, mappingStr string) {
	// Skip extremely long inputs to prevent timeout
	if len(topic) > constants.MaxTopicLength || len(payload) > constants.MaxInputLength || len(mappingStr) > constants.MaxInputLength {
		return
	}

	// Test with timeout to prevent infinite loops
	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultJSTimeout)
	defer cancel()

	// Create and test processor
	processor := createTestProcessor(t, mappingStr)
	if processor == nil {
		return // Configuration error, skip
	}
	defer processor.Close(ctx)

	// Test main processing flow
	testMessageProcessing(t, processor, ctx, topic, payload, mappingStr)

	// Test additional edge cases
	testEdgeCases(t, processor, topic, payload, mappingStr)
}

// createTestProcessor creates a processor instance for testing
func createTestProcessor(t *testing.T, mappingStr string) *processor.StreamProcessor {
	config := createFuzzConfig(mappingStr)
	processor, err := createFuzzProcessor(config)
	if err != nil {
		// Configuration errors are expected for invalid inputs
		return nil
	}
	return processor
}

// testMessageProcessing tests the core message processing functionality
func testMessageProcessing(t *testing.T, processor *processor.StreamProcessor, ctx context.Context, topic, payload, mappingStr string) {
	// Process message with panic recovery
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Processor panicked for topic=%q, payload=%q, mapping=%q: %v",
					topic, payload, mappingStr, r)
			}
		}()

		// Create and process message
		msg := createTestMessage(topic, payload)
		batch := service.MessageBatch{msg}
		results, err := processor.ProcessBatch(ctx, batch)
		// Verify results
		if err != nil {
			// Processing errors are expected for invalid inputs
			return
		}

		validateProcessingResults(t, results)
	}()
}

// createTestMessage creates a test message with the given topic and payload
func createTestMessage(topic, payload string) *service.Message {
	msg := service.NewMessage([]byte(payload))
	if topic != "" {
		msg.MetaSet("umh_topic", topic)
	}
	return msg
}

// validateProcessingResults validates that processing results are structurally correct
func validateProcessingResults(t *testing.T, results []service.MessageBatch) {
	for _, resultBatch := range results {
		for _, resultMsg := range resultBatch {
			if resultMsg == nil {
				t.Error("Got nil message in result")
				continue
			}

			// Verify message has valid content
			content, err := resultMsg.AsBytes()
			if err != nil {
				t.Errorf("Failed to get message content: %v", err)
				continue
			}

			// Verify content is valid JSON if non-empty
			if len(content) > 0 {
				var parsed interface{}
				if err := json.Unmarshal(content, &parsed); err != nil {
					t.Errorf("Result message is not valid JSON: %v", err)
				}
			}
		}
	}
}

// createFuzzConfig creates a processor configuration from fuzzed mapping string
func createFuzzConfig(mappingStr string) *service.ParsedConfig {
	// Parse mapping JSON
	var mapping map[string]interface{}
	if err := json.Unmarshal([]byte(mappingStr), &mapping); err != nil {
		// Use empty mapping for invalid JSON
		mapping = map[string]interface{}{}
	}

	// Create configuration
	configSpec := service.NewConfigSpec().
		Field(service.NewStringMapField("sources")).
		Field(service.NewObjectField("mapping"))

	yamlConfig := fmt.Sprintf(`
sources:
  press: "press"
  temp: "temp"
  flow: "flow"
  run: "run"
  status: "status"
mapping: %s
`, mappingStr)

	parsedConfig, err := configSpec.ParseYAML(yamlConfig, nil)
	if err != nil {
		// Return minimal valid config for parsing errors
		yamlConfig = `
sources:
  press: "press"
  temp: "temp"
  flow: "flow"
  run: "run"
  status: "status"
mapping: {}
`
		parsedConfig, _ = configSpec.ParseYAML(yamlConfig, nil)
	}

	return parsedConfig
}

// createFuzzProcessor creates a processor instance for fuzzing
func createFuzzProcessor(config *service.ParsedConfig) (*processor.StreamProcessor, error) {
	// Parse configuration into StreamProcessorConfig
	sources, err := config.FieldStringMap("sources")
	if err != nil {
		return nil, err
	}

	var mapping map[string]interface{}
	if config.Contains("mapping") {
		mappingAny, err := config.FieldAny("mapping")
		if err != nil {
			return nil, err
		}
		if m, ok := mappingAny.(map[string]interface{}); ok {
			mapping = m
		}
	}

	streamConfig := config.StreamProcessorConfig{
		Mode:        "timeseries",
		OutputTopic: "umh.v1.corpA.plant-A.aawd",
		Model: config.ModelConfig{
			Name:    "fuzz",
			Version: "v1",
		},
		Sources: sources,
		Mapping: mapping,
	}

	resources := service.MockResources()
	return processor.newStreamProcessor(streamConfig, resources.Logger(), resources.Metrics())
}

// testEdgeCases tests additional edge cases with the same inputs
func testEdgeCases(t *testing.T, processor *processor.StreamProcessor, topic, payload, mappingStr string) {
	ctx := context.Background()

	// Test empty message
	testEmptyMessage(t, processor, ctx, topic)

	// Test message with invalid metadata
	testInvalidMetadata(t, processor, ctx, payload)
}

// testEmptyMessage tests processing with empty message content
func testEmptyMessage(t *testing.T, processor *processor.StreamProcessor, ctx context.Context, topic string) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Processor panicked with empty message: %v", r)
			}
		}()

		emptyMsg := service.NewMessage([]byte{})
		if topic != "" {
			emptyMsg.MetaSet("umh_topic", topic)
		}

		batch := service.MessageBatch{emptyMsg}
		_, _ = processor.ProcessBatch(ctx, batch)
	}()
}

// testInvalidMetadata tests processing with invalid metadata
func testInvalidMetadata(t *testing.T, processor *processor.StreamProcessor, ctx context.Context, payload string) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Processor panicked with invalid metadata: %v", r)
			}
		}()

		invalidMsg := service.NewMessage([]byte(payload))
		invalidMsg.MetaSet("umh_topic", string([]byte{0x00, 0x01, 0x02})) // Invalid UTF-8

		batch := service.MessageBatch{invalidMsg}
		_, _ = processor.ProcessBatch(ctx, batch)
	}()
}

// sanitizeVariables ensures variables don't cause runtime panics
func sanitizeVariables(vars map[string]interface{}) map[string]interface{} {
	sanitized := make(map[string]interface{})
	for k, v := range vars {
		switch val := v.(type) {
		case float64:
			if math.IsNaN(val) || math.IsInf(val, 0) {
				sanitized[k] = 0.0
			} else {
				sanitized[k] = val
			}
		case string:
			// Limit string length to prevent memory exhaustion
			if len(val) > constants.MaxStringLength {
				sanitized[k] = val[:constants.MaxStringLength]
			} else {
				sanitized[k] = val
			}
		case bool, int, int32, int64, uint, uint32, uint64:
			sanitized[k] = val
		case nil:
			sanitized[k] = nil
		default:
			// Convert complex types to string representation
			sanitized[k] = fmt.Sprintf("%v", val)
		}
	}
	return sanitized
}
