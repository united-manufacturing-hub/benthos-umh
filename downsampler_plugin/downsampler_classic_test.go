package downsampler_plugin

import (
	"context"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownsamplerProcessor_UMHClassicFormat(t *testing.T) {
	testCases := []struct {
		name           string
		messages       []map[string]interface{}
		expectedOutput []map[string]interface{}
		description    string
	}{
		{
			name: "first_message_all_fields_kept",
			messages: []map[string]interface{}{
				{
					"temperature":  25.0,
					"humidity":     60.0,
					"pressure":     1000.0,
					"status":       "RUNNING",
					"timestamp_ms": int64(1000),
				},
			},
			expectedOutput: []map[string]interface{}{
				{
					"temperature":  25.0,
					"humidity":     60.0,
					"pressure":     1000.0,
					"status":       "RUNNING",
					"timestamp_ms": int64(1000),
				},
			},
			description: "First message should keep all fields",
		},
		{
			name: "partial_field_filtering",
			messages: []map[string]interface{}{
				{
					"temperature":  25.0,
					"humidity":     60.0,
					"pressure":     1000.0,
					"status":       "RUNNING",
					"timestamp_ms": int64(1000),
				},
				{
					"temperature":  25.2,      // 0.2°C change < 0.5°C threshold - DROP
					"humidity":     61.5,      // 1.5% change > 1.0% threshold - KEEP
					"pressure":     1002.0,    // 2.0 Pa change < 5.0 Pa threshold - DROP
					"status":       "RUNNING", // String unchanged - DROP
					"timestamp_ms": int64(2000),
				},
			},
			expectedOutput: []map[string]interface{}{
				{
					"temperature":  25.0,
					"humidity":     60.0,
					"pressure":     1000.0,
					"status":       "RUNNING",
					"timestamp_ms": int64(1000),
				},
				{
					"humidity":     61.5,
					"timestamp_ms": int64(2000),
				},
			},
			description: "Second message should only keep humidity field",
		},
		{
			name: "all_fields_filtered_message_dropped",
			messages: []map[string]interface{}{
				{
					"temperature":  25.0,
					"humidity":     60.0,
					"pressure":     1000.0,
					"timestamp_ms": int64(1000),
				},
				{
					"temperature":  25.2,   // 0.2°C change < 0.5°C threshold - DROP
					"humidity":     60.3,   // 0.3% change < 1.0% threshold - DROP
					"pressure":     1003.0, // 3.0 Pa change < 5.0 Pa threshold - DROP
					"timestamp_ms": int64(2000),
				},
			},
			expectedOutput: []map[string]interface{}{
				{
					"temperature":  25.0,
					"humidity":     60.0,
					"pressure":     1000.0,
					"timestamp_ms": int64(1000),
				},
				// Second message completely dropped
			},
			description: "Message should be dropped if no fields meet thresholds",
		},
		{
			name: "boolean_and_string_handling",
			messages: []map[string]interface{}{
				{
					"machine_enabled": true,
					"status":          "RUNNING",
					"timestamp_ms":    int64(1000),
				},
				{
					"machine_enabled": false,     // Boolean change - KEEP
					"status":          "RUNNING", // String unchanged - DROP
					"timestamp_ms":    int64(2000),
				},
				{
					"machine_enabled": false,  // Boolean unchanged - DROP
					"status":          "IDLE", // String changed - KEEP
					"timestamp_ms":    int64(3000),
				},
			},
			expectedOutput: []map[string]interface{}{
				{
					"machine_enabled": true,
					"status":          "RUNNING",
					"timestamp_ms":    int64(1000),
				},
				{
					"machine_enabled": false,
					"timestamp_ms":    int64(2000),
				},
				{
					"status":       "IDLE",
					"timestamp_ms": int64(3000),
				},
			},
			description: "Boolean and string changes should be handled correctly",
		},
		{
			name: "mixed_types_with_thresholds",
			messages: []map[string]interface{}{
				{
					"temperature":  25.0,
					"count":        100,
					"active":       true,
					"mode":         "AUTO",
					"timestamp_ms": int64(1000),
				},
				{
					"temperature":  25.8,     // 0.8°C change > 0.5°C threshold - KEEP
					"count":        102,      // 2 change = default 2.0 threshold - KEEP
					"active":       true,     // Boolean unchanged - DROP
					"mode":         "MANUAL", // String changed - KEEP
					"timestamp_ms": int64(2000),
				},
			},
			expectedOutput: []map[string]interface{}{
				{
					"temperature":  25.0,
					"count":        100,
					"active":       true,
					"mode":         "AUTO",
					"timestamp_ms": int64(1000),
				},
				{
					"temperature":  25.8,
					"count":        102,
					"mode":         "MANUAL",
					"timestamp_ms": int64(2000),
				},
			},
			description: "Mixed data types should be handled with appropriate thresholds",
		},
		{
			name: "only_timestamp_preserved_on_filtering",
			messages: []map[string]interface{}{
				{
					"value1":       10.0,
					"value2":       20.0,
					"timestamp_ms": int64(1000),
				},
				{
					"value1":       10.5, // 0.5 change < 2.0 default threshold - DROP
					"value2":       20.8, // 0.8 change < 2.0 default threshold - DROP
					"timestamp_ms": int64(2000),
				},
			},
			expectedOutput: []map[string]interface{}{
				{
					"value1":       10.0,
					"value2":       20.0,
					"timestamp_ms": int64(1000),
				},
				// Second message completely dropped (no measurement fields remain)
			},
			description: "Message dropped when only timestamp would remain",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create fresh processor for each test case to avoid state contamination
			config := DownsamplerConfig{
				Algorithm: "deadband",
				Threshold: 2.0, // Default threshold
				TopicThresholds: []TopicThreshold{
					{Pattern: "*.temperature", Threshold: 0.5},
					{Pattern: "*.humidity", Threshold: 1.0},
					{Pattern: "*.pressure", Threshold: 5.0},
				},
			}

			resources := service.MockResources()
			processor, err := newDownsamplerProcessor(config, resources.Logger(), resources.Metrics())
			require.NoError(t, err)

			var results []map[string]interface{}

			for _, msgData := range tc.messages {
				// Create message with UMH classic _historian metadata
				msg := service.NewMessage(nil)
				msg.MetaSet("data_contract", "_historian")
				msg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
				msg.SetStructured(msgData)

				// Process the message
				batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
				require.NoError(t, err)

				// Extract results
				for _, batch := range batches {
					for _, processedMsg := range batch {
						data, err := processedMsg.AsStructured()
						require.NoError(t, err)
						results = append(results, data.(map[string]interface{}))
					}
				}
			}

			// Verify results
			assert.Equal(t, len(tc.expectedOutput), len(results),
				"Expected %d messages, got %d for case: %s",
				len(tc.expectedOutput), len(results), tc.description)

			for i, expected := range tc.expectedOutput {
				if i < len(results) {
					assert.Equal(t, expected, results[i],
						"Message %d mismatch for case: %s", i, tc.description)
				}
			}
		})
	}
}

func TestDownsamplerProcessor_UMHCoreVsClassicFormat(t *testing.T) {
	config := DownsamplerConfig{
		Algorithm: "deadband",
		Threshold: 1.0,
	}

	resources := service.MockResources()
	processor, err := newDownsamplerProcessor(config, resources.Logger(), resources.Metrics())
	require.NoError(t, err)

	t.Run("umh_core_format_single_value", func(t *testing.T) {
		// UMH-core format with single "value" field
		msg := service.NewMessage(nil)
		msg.MetaSet("data_contract", "_historian")
		msg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
		msg.SetStructured(map[string]interface{}{
			"value":        25.0,
			"timestamp_ms": int64(1000),
		})

		batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
		require.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Len(t, batches[0], 1)

		data, err := batches[0][0].AsStructured()
		require.NoError(t, err)
		expected := map[string]interface{}{
			"value":        25.0,
			"timestamp_ms": int64(1000),
		}
		assert.Equal(t, expected, data)
	})

	t.Run("umh_classic_format_multiple_fields", func(t *testing.T) {
		// UMH classic format without "value" field
		msg := service.NewMessage(nil)
		msg.MetaSet("data_contract", "_historian")
		msg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
		msg.SetStructured(map[string]interface{}{
			"temperature":  25.0,
			"humidity":     60.0,
			"timestamp_ms": int64(1000),
		})

		batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
		require.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Len(t, batches[0], 1)

		data, err := batches[0][0].AsStructured()
		require.NoError(t, err)
		expected := map[string]interface{}{
			"temperature":  25.0,
			"humidity":     60.0,
			"timestamp_ms": int64(1000),
		}
		assert.Equal(t, expected, data)
	})
}

func TestDownsamplerProcessor_SeriesStatePerKey(t *testing.T) {
	config := DownsamplerConfig{
		Algorithm: "deadband",
		Threshold: 1.0,
	}

	resources := service.MockResources()
	processor, err := newDownsamplerProcessor(config, resources.Logger(), resources.Metrics())
	require.NoError(t, err)

	// Send first message to establish baseline
	msg1 := service.NewMessage(nil)
	msg1.MetaSet("data_contract", "_historian")
	msg1.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
	msg1.SetStructured(map[string]interface{}{
		"temp_sensor_1": 25.0,
		"temp_sensor_2": 30.0,
		"timestamp_ms":  int64(1000),
	})

	_, err = processor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
	require.NoError(t, err)

	// Verify series states were created independently
	expectedSeriesIDs := []string{
		"umh.v1.plant1.line1._historian.temp_sensor_1",
		"umh.v1.plant1.line1._historian.temp_sensor_2",
	}

	processor.stateMutex.RLock()
	for _, seriesID := range expectedSeriesIDs {
		state, exists := processor.seriesState[seriesID]
		assert.True(t, exists, "Series state should exist for %s", seriesID)
		if exists {
			assert.NotNil(t, state.algorithm, "Algorithm should be initialized for %s", seriesID)
		}
	}
	processor.stateMutex.RUnlock()

	// Send second message with different changes per sensor
	msg2 := service.NewMessage(nil)
	msg2.MetaSet("data_contract", "_historian")
	msg2.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
	msg2.SetStructured(map[string]interface{}{
		"temp_sensor_1": 25.5, // 0.5°C change < 1.0°C threshold - should be dropped
		"temp_sensor_2": 32.0, // 2.0°C change > 1.0°C threshold - should be kept
		"timestamp_ms":  int64(2000),
	})

	batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg2})
	require.NoError(t, err)
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0], 1)

	data, err := batches[0][0].AsStructured()
	require.NoError(t, err)
	expected := map[string]interface{}{
		"temp_sensor_2": 32.0, // Only sensor 2 kept
		"timestamp_ms":  int64(2000),
	}
	assert.Equal(t, expected, data)
}

func TestDownsamplerProcessor_MetadataAnnotation(t *testing.T) {
	config := DownsamplerConfig{
		Algorithm: "deadband",
		Threshold: 1.0,
	}

	resources := service.MockResources()
	processor, err := newDownsamplerProcessor(config, resources.Logger(), resources.Metrics())
	require.NoError(t, err)

	// Test UMH-core format metadata
	msg1 := service.NewMessage(nil)
	msg1.MetaSet("data_contract", "_historian")
	msg1.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
	msg1.SetStructured(map[string]interface{}{
		"value":        25.0,
		"timestamp_ms": int64(1000),
	})

	batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
	require.NoError(t, err)
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0], 1)

	metadata, _ := batches[0][0].MetaGet("downsampled_by")
	assert.Contains(t, metadata, "deadband")
	assert.Contains(t, metadata, "threshold")

	// Test UMH classic format metadata
	msg2 := service.NewMessage(nil)
	msg2.MetaSet("data_contract", "_historian")
	msg2.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
	msg2.SetStructured(map[string]interface{}{
		"temperature":  25.0,
		"humidity":     60.0,
		"pressure":     1000.0,
		"timestamp_ms": int64(1000),
	})

	batches, err = processor.ProcessBatch(context.Background(), service.MessageBatch{msg2})
	require.NoError(t, err)
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0], 1)

	metadata, _ = batches[0][0].MetaGet("downsampled_by")
	assert.Contains(t, metadata, "deadband")
	assert.Contains(t, metadata, "filtered")
	assert.Contains(t, metadata, "_of_")
	assert.Contains(t, metadata, "_keys")
}
