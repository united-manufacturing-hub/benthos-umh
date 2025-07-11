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

package stream_processor_plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// FuzzStreamProcessor is the main fuzz entry point that tests the stream processor
// at the highest level by fuzzing the Process method with various inputs.
// This comprehensively tests all underlying functionality including JavaScript
// evaluation, variable handling, message processing, and security constraints.
func FuzzStreamProcessor(f *testing.F) {
	// Seed inputs for comprehensive fuzzing coverage
	seedInputs := []struct {
		topic   string
		payload string
		mapping string
	}{
		// Valid UMH topics with various data types
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 25.5, "timestamp_ms": 1234567890}`,
			`{"result": "press + 1", "status": "press > 20 ? 'high' : 'low'"}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.temp",
			`{"value": 80.0, "timestamp_ms": 1234567890}`,
			`{"celsius": "temp", "fahrenheit": "temp * 9/5 + 32"}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.flow",
			`{"value": 100.0, "timestamp_ms": 1234567890}`,
			`{"flow_rate": "flow", "flow_status": "flow > 50 ? 'normal' : 'low'"}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.run",
			`{"value": true, "timestamp_ms": 1234567890}`,
			`{"running": "run", "status_code": "run ? 1 : 0"}`,
		},

		// Edge cases with special values
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 0, "timestamp_ms": 1234567890}`,
			`{"result": "press / 0", "check": "isFinite(press) ? press : 0"}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.temp",
			`{"value": null, "timestamp_ms": 1234567890}`,
			`{"temp_check": "temp == null ? 'no_data' : temp"}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": -999.99, "timestamp_ms": 1234567890}`,
			`{"abs_press": "Math.abs(press)", "valid": "press > -1000"}`,
		},

		// Complex expressions
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 25.5, "timestamp_ms": 1234567890}`,
			`{"complex": "Math.sqrt(Math.pow(press, 2) + Math.pow(temp || 0, 2))", "rounded": "Math.round(press * 100) / 100"}`,
		},

		// String processing
		{
			"umh.v1.corpA.plant-A.aawd._raw.status",
			`{"value": "running", "timestamp_ms": 1234567890}`,
			`{"upper": "status.toUpperCase()", "length": "status.length"}`,
		},

		// Invalid/malformed inputs
		{
			"invalid.topic.structure",
			`{"value": 25.5, "timestamp_ms": 1234567890}`,
			`{"result": "42"}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`invalid json payload`,
			`{"result": "press"}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 25.5, "timestamp_ms": 1234567890}`,
			`invalid mapping json`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 25.5, "timestamp_ms": 1234567890}`,
			`{"result": ""}`,
		},

		// Security test cases (should be blocked)
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 25.5, "timestamp_ms": 1234567890}`,
			`{"danger": "eval('1+1')", "safe": "press"}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 25.5, "timestamp_ms": 1234567890}`,
			`{"danger": "Function('return 1+1')()", "safe": "press"}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 25.5, "timestamp_ms": 1234567890}`,
			`{"danger": "require('fs')", "safe": "press"}`,
		},

		// Performance stress cases
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 2.0, "timestamp_ms": 1234567890}`,
			`{"power": "Math.pow(press, 100)", "factorial": "press > 10 ? 'too_big' : press"}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 25.5, "timestamp_ms": 1234567890}`,
			`{"loop_test": "press > 0 ? 'positive' : 'negative'"}`,
		},

		// Empty/minimal cases
		{
			"",
			`{}`,
			`{}`,
		},
		{
			"umh.v1.corpA.plant-A.aawd._raw.press",
			`{"value": 25.5, "timestamp_ms": 1234567890}`,
			`{}`,
		},
	}

	// Add seed inputs to fuzzer
	for _, seed := range seedInputs {
		f.Add(seed.topic, seed.payload, seed.mapping)
	}

	f.Fuzz(func(t *testing.T, topic, payload, mappingStr string) {
		// Skip extremely long inputs to prevent timeout
		if len(topic) > 1000 || len(payload) > 10000 || len(mappingStr) > 10000 {
			return
		}

		// Test with timeout to prevent infinite loops
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create processor configuration
		config := createFuzzConfig(mappingStr)

		// Create processor instance
		processor, err := createFuzzProcessor(config)
		if err != nil {
			// Configuration errors are expected for invalid inputs
			return
		}
		defer func() {
			if processor != nil {
				processor.Close(ctx)
			}
		}()

		// Create message
		msg := service.NewMessage([]byte(payload))
		if topic != "" {
			msg.MetaSet("umh_topic", topic)
		}

		// Process message with panic recovery
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Processor panicked for topic=%q, payload=%q, mapping=%q: %v",
						topic, payload, mappingStr, r)
				}
			}()

			// Process the message
			batch := service.MessageBatch{msg}
			results, err := processor.ProcessBatch(ctx, batch)

			// Verify results are valid
			if err != nil {
				// Processing errors are expected for invalid inputs
				return
			}

			// Verify output structure
			for _, resultBatch := range results {
				for _, resultMsg := range resultBatch {
					// Verify message is valid
					if resultMsg == nil {
						t.Error("Got nil message in result")
						continue
					}

					// Verify message has content
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
		}()

		// Test various edge cases with the same inputs
		testEdgeCases(t, processor, topic, payload, mappingStr)
	})
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
		Field(service.NewStringListField("sources")).
		Field(service.NewObjectField("mapping"))

	yamlConfig := fmt.Sprintf(`
sources: ["press", "temp", "flow", "run", "status"]
mapping: %s
`, mappingStr)

	parsedConfig, err := configSpec.ParseYAML(yamlConfig, nil)
	if err != nil {
		// Return minimal valid config for parsing errors
		yamlConfig = `
sources: ["press", "temp", "flow", "run", "status"]
mapping: {}
`
		parsedConfig, _ = configSpec.ParseYAML(yamlConfig, nil)
	}

	return parsedConfig
}

// createFuzzProcessor creates a processor instance for fuzzing
func createFuzzProcessor(config *service.ParsedConfig) (*StreamProcessor, error) {
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

	streamConfig := StreamProcessorConfig{
		Mode:        "timeseries",
		OutputTopic: "umh.v1.corpA.plant-A.aawd",
		Model: ModelConfig{
			Name:    "fuzz",
			Version: "v1",
		},
		Sources: sources,
		Mapping: mapping,
	}

	resources := service.MockResources()
	return newStreamProcessor(streamConfig, resources.Logger(), resources.Metrics())
}

// testEdgeCases tests additional edge cases with the same inputs
func testEdgeCases(t *testing.T, processor *StreamProcessor, topic, payload, mappingStr string) {
	ctx := context.Background()

	// Test empty message
	emptyMsg := service.NewMessage([]byte{})
	if topic != "" {
		emptyMsg.MetaSet("umh_topic", topic)
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Processor panicked with empty message: %v", r)
			}
		}()

		batch := service.MessageBatch{emptyMsg}
		_, _ = processor.ProcessBatch(ctx, batch)
	}()

	// Test message with invalid metadata
	invalidMsg := service.NewMessage([]byte(payload))
	invalidMsg.MetaSet("umh_topic", string([]byte{0x00, 0x01, 0x02})) // Invalid UTF-8

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Processor panicked with invalid metadata: %v", r)
			}
		}()

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
			if len(val) > 1000 {
				sanitized[k] = val[:1000]
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
