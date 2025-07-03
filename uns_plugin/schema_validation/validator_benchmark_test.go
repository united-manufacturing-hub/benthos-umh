package schemavalidation

import (
	"fmt"
	"sync"
	"testing"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

// setupBenchmarkValidator creates a validator with pre-loaded schemas for benchmarking
func setupBenchmarkValidator() *Validator {
	validator := NewValidator()

	// Use the exact same schema format as validator_test.go
	sensorSchema := []byte(`{
		"type": "object",
		"properties": {
			"virtual_path": {
				"type": "string",
				"enum": ["vibration.x-axis", "temperature", "humidity", "pressure"]
			},
			"fields": {
				"type": "object",
				"properties": {
					"value": {
						"type": "object",
						"properties": {
							"timestamp_ms": {"type": "number"},
							"value": {"type": "number"}
						},
						"required": ["timestamp_ms", "value"],
						"additionalProperties": false
					}
				},
				"additionalProperties": false
			}
		},
		"required": ["virtual_path", "fields"],
		"additionalProperties": false
	}`)

	// Load the schema
	validator.LoadSchema("_sensor_data", 1, sensorSchema)

	return validator
}

// Test payloads for benchmarking
var (
	validNumberPayload = []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)
	validZeroPayload   = []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 0}}`)
	validLargePayload  = []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 999999999.999}}`)
)

// BenchmarkValidation_SingleThread measures single-threaded validation performance
func BenchmarkValidation_SingleThread(b *testing.B) {
	validator := setupBenchmarkValidator()

	topics := []string{
		"umh.v1.enterprise.site.area._sensor_data-v1.vibration.x-axis",
		"umh.v1.enterprise.site.area._sensor_data-v1.temperature",
		"umh.v1.enterprise.site.area._sensor_data-v1.humidity",
		"umh.v1.enterprise.site.area._sensor_data-v1.pressure",
	}

	payloads := [][]byte{validNumberPayload, validZeroPayload, validLargePayload}

	// Pre-create topic objects to avoid topic parsing overhead in benchmark
	topicObjects := make([]*topic.UnsTopic, len(topics))
	for i, topicStr := range topics {
		topicObj, err := topic.NewUnsTopic(topicStr)
		if err != nil {
			b.Fatalf("Failed to create topic %s: %v", topicStr, err)
		}
		topicObjects[i] = topicObj
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		topicObj := topicObjects[i%len(topicObjects)]
		payload := payloads[i%len(payloads)]

		err := validator.Validate(topicObj, payload)
		if err != nil {
			b.Fatalf("Validation failed: %v", err)
		}
	}
}

// BenchmarkValidation_Concurrent measures concurrent validation performance
func BenchmarkValidation_Concurrent(b *testing.B) {
	validator := setupBenchmarkValidator()

	topics := []string{
		"umh.v1.enterprise.site.area._sensor_data-v1.vibration.x-axis",
		"umh.v1.enterprise.site.area._sensor_data-v1.temperature",
		"umh.v1.enterprise.site.area._sensor_data-v1.humidity",
		"umh.v1.enterprise.site.area._sensor_data-v1.pressure",
	}

	payloads := [][]byte{validNumberPayload, validZeroPayload, validLargePayload}

	// Pre-create topic objects
	topicObjects := make([]*topic.UnsTopic, len(topics))
	for i, topicStr := range topics {
		topicObj, err := topic.NewUnsTopic(topicStr)
		if err != nil {
			b.Fatalf("Failed to create topic %s: %v", topicStr, err)
		}
		topicObjects[i] = topicObj
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			topicObj := topicObjects[i%len(topicObjects)]
			payload := payloads[i%len(payloads)]

			err := validator.Validate(topicObj, payload)
			if err != nil {
				b.Fatalf("Validation failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkValidation_HighThroughput simulates high-throughput scenarios
func BenchmarkValidation_HighThroughput(b *testing.B) {
	validator := setupBenchmarkValidator()

	// Create topic variety using only valid schema combinations
	topics := make([]*topic.UnsTopic, 0, 20)
	tagNames := []string{"vibration.x-axis", "temperature", "humidity", "pressure"}

	for _, tagName := range tagNames {
		topicStr := fmt.Sprintf("umh.v1.enterprise.site.area._sensor_data-v1.%s", tagName)
		topicObj, err := topic.NewUnsTopic(topicStr)
		if err == nil {
			topics = append(topics, topicObj)
		}
	}

	if len(topics) == 0 {
		b.Fatal("No valid topics created")
	}

	payloads := [][]byte{validNumberPayload, validZeroPayload, validLargePayload}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			topicObj := topics[i%len(topics)]
			payload := payloads[i%len(payloads)]

			validator.Validate(topicObj, payload) // Don't fail on validation errors in high throughput test
			i++
		}
	})
}

// BenchmarkValidation_RealWorld simulates real-world mixed scenarios
func BenchmarkValidation_RealWorld(b *testing.B) {
	validator := setupBenchmarkValidator()

	// Mix of valid and invalid scenarios (90% valid, 10% invalid)
	scenarios := []struct {
		topicStr string
		payload  []byte
		valid    bool
	}{
		// Valid scenarios (90%)
		{"umh.v1.enterprise.site.area._sensor_data-v1.temperature", validNumberPayload, true},
		{"umh.v1.enterprise.site.area._sensor_data-v1.humidity", validNumberPayload, true},
		{"umh.v1.enterprise.site.area._sensor_data-v1.pressure", validNumberPayload, true},
		{"umh.v1.enterprise.site.area._pump_data-v1.count", validNumberPayload, true},
		{"umh.v1.enterprise.site.area._pump_data-v1.vibration.x-axis", validNumberPayload, true},
		{"umh.v1.enterprise.site.area._pump_data-v1.vibration.y-axis", validNumberPayload, true},
		{"umh.v1.enterprise.site.area._pump_data-v1.serialNumber", validNumberPayload, true},
		{"umh.v1.enterprise.site.area._motor_data-v1.temperature", validNumberPayload, true},
		{"umh.v1.enterprise.site.area._sensor_data-v2.temperature", validNumberPayload, true},
		// Invalid scenarios (10%)
		{"umh.v1.enterprise.site.area._sensor_data-v1.invalid_tag", validNumberPayload, false},
	}

	// Pre-create topic objects for valid scenarios
	topicObjects := make([]*topic.UnsTopic, len(scenarios))
	for i, scenario := range scenarios {
		if scenario.valid {
			topicObj, err := topic.NewUnsTopic(scenario.topicStr)
			if err != nil {
				b.Fatalf("Failed to create topic %s: %v", scenario.topicStr, err)
			}
			topicObjects[i] = topicObj
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			scenario := scenarios[i%len(scenarios)]

			if scenario.valid {
				topicObj := topicObjects[i%len(scenarios)]
				if topicObj != nil {
					validator.Validate(topicObj, scenario.payload)
				}
			} else {
				// For invalid scenarios, create topic on-the-fly (performance penalty expected)
				topicObj, err := topic.NewUnsTopic(scenario.topicStr)
				if err == nil {
					validator.Validate(topicObj, scenario.payload)
				}
			}
			i++
		}
	})
}

// BenchmarkValidation_MemoryUsage measures memory allocations per validation
func BenchmarkValidation_MemoryUsage(b *testing.B) {
	validator := setupBenchmarkValidator()

	topicObj, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := validator.Validate(topicObj, validNumberPayload)
		if err != nil {
			b.Fatalf("Validation failed: %v", err)
		}
	}
}

// BenchmarkValidation_DifferentPayloadSizes tests with various payload sizes
func BenchmarkValidation_DifferentPayloadSizes(b *testing.B) {
	validator := setupBenchmarkValidator()

	topicObj, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	// Different payload sizes
	payloads := [][]byte{
		// Small payload
		[]byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`),
		// Medium payload with more precision
		[]byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.123456789}}`),
		// Large payload with scientific notation
		[]byte(`{"value": {"timestamp_ms": 1719859200000, "value": 1.23456789e+10}}`),
	}

	for _, payload := range payloads {
		b.Run(fmt.Sprintf("PayloadSize_%d_bytes", len(payload)), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for j := 0; j < b.N; j++ {
				err := validator.Validate(topicObj, payload)
				if err != nil {
					b.Fatalf("Validation failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkConcurrentValidation_ScalabilityTest tests scalability with different goroutine counts
func BenchmarkConcurrentValidation_ScalabilityTest(b *testing.B) {
	validator := setupBenchmarkValidator()

	topicObj, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	// Test with different numbers of goroutines
	goroutineCounts := []int{1, 2, 4, 8, 16, 32}

	for _, numGoroutines := range goroutineCounts {
		b.Run(fmt.Sprintf("Goroutines_%d", numGoroutines), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			validationsPerGoroutine := b.N / numGoroutines

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < validationsPerGoroutine; j++ {
						validator.Validate(topicObj, validNumberPayload)
					}
				}()
			}

			wg.Wait()
		})
	}
}
