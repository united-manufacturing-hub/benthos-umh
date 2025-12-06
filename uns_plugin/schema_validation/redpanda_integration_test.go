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

package schemavalidation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

var (
	redpandaContainer         testcontainers.Container
	redpandaSchemaRegistryURL string
)

// RedpandaTestSchemas contains the test schemas we'll load into Redpanda
// We'll add a timestamp suffix to avoid conflicts between test runs
var redpandaTestSchemas = map[string]map[int]string{
	"_sensor_data_v1-timeseries-number": {
		1: `{
			"type": "object",
			"properties": {
				"virtual_path": {
					"type": "string",
					"enum": ["temperature"]
				},
				"fields": {
					"type": "object",
					"properties": {
						"timestamp_ms": {"type": "number"},
						"value": {"type": "number"}
					},
					"required": ["timestamp_ms", "value"],
					"additionalProperties": false
				}
			},
			"required": ["virtual_path", "fields"],
			"additionalProperties": false
		}`,
	},
	"_sensor_data_v2-timeseries-number": {
		1: `{
			"type": "object",
			"properties": {
				"virtual_path": {
					"type": "string",
					"enum": ["temperature", "humidity", "pressure"]
				},
				"fields": {
					"type": "object",
					"properties": {
						"timestamp_ms": {"type": "number"},
						"value": {"type": "number"}
					},
					"required": ["timestamp_ms", "value"],
					"additionalProperties": false
				}
			},
			"required": ["virtual_path", "fields"],
			"additionalProperties": false
		}`,
	},
	"_pump_data_v1-timeseries-number": {
		1: `{
			"type": "object",
			"properties": {
				"virtual_path": {
					"type": "string",
					"enum": ["vibration.x-axis", "vibration.y-axis", "count"]
				},
				"fields": {
					"type": "object",
					"properties": {
						"timestamp_ms": {"type": "number"},
						"value": {"type": "number"}
					},
					"required": ["timestamp_ms", "value"],
					"additionalProperties": false
				}
			},
			"required": ["virtual_path", "fields"],
			"additionalProperties": false
		}`,
	},
	"_pump_data_v1-timeseries-string": {
		1: `{
			"type": "object",
			"properties": {
				"virtual_path": {
					"type": "string",
					"enum": ["serialNumber", "status"]
				},
				"fields": {
					"type": "object",
					"properties": {
						"timestamp_ms": {"type": "number"},
						"value": {"type": "string"}
					},
					"required": ["timestamp_ms", "value"],
					"additionalProperties": false
				}
			},
			"required": ["virtual_path", "fields"],
			"additionalProperties": false
		}`,
	},
	"_string_data_v1-timeseries-string": {
		1: `{
			"type": "object",
			"properties": {
				"virtual_path": {
					"type": "string",
					"enum": ["serialNumber", "status"]
				},
				"fields": {
					"type": "object",
					"properties": {
						"timestamp_ms": {"type": "number"},
						"value": {"type": "string"}
					},
					"required": ["timestamp_ms", "value"],
					"additionalProperties": false
				}
			},
			"required": ["virtual_path", "fields"],
			"additionalProperties": false
		}`,
	},
}

// RedpandaSchemaRegistryClient for interacting with real Redpanda schema registry
type RedpandaSchemaRegistryClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewRedpandaSchemaRegistryClient(baseURL string) *RedpandaSchemaRegistryClient {
	// Create HTTP client with proper connection limits to prevent leaks
	transport := &http.Transport{
		MaxIdleConns:        10,               // Limit total idle connections
		MaxConnsPerHost:     5,                // Limit connections per host
		MaxIdleConnsPerHost: 2,                // Limit idle connections per host
		IdleConnTimeout:     90 * time.Second, // Close idle connections after 90s
		DisableKeepAlives:   false,            // Allow connection reuse
	}

	return &RedpandaSchemaRegistryClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
	}
}

// Close cleans up the HTTP client resources
func (c *RedpandaSchemaRegistryClient) Close() {
	if c.httpClient != nil && c.httpClient.Transport != nil {
		if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
		c.httpClient = nil
	}
}

func (c *RedpandaSchemaRegistryClient) RegisterSchema(subject string, version int, schema string) error {
	// Schema must be properly escaped as a JSON string
	// Create the schema registration payload with properly escaped JSON
	payload := map[string]interface{}{
		"schema":     schema,
		"schemaType": "JSON",
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal schema payload: %w", err)
	}

	// Register the schema with a specific version
	url := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, subject)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		// Read response body for debugging
		bodyBytes := make([]byte, 1024)
		resp.Body.Read(bodyBytes)
		return fmt.Errorf("schema registration failed with status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func (c *RedpandaSchemaRegistryClient) WaitForReady() error {
	ready := false
	Eventually(func() bool {
		resp, err := c.httpClient.Get(c.baseURL + "/subjects")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			ready = true
			return true
		}
		if resp != nil {
			resp.Body.Close()
		}
		return false
	}, "60s", "2s").Should(BeTrue(), "Schema registry should be ready")

	if !ready {
		return fmt.Errorf("schema registry not ready after timeout")
	}
	return nil
}

func (c *RedpandaSchemaRegistryClient) DeleteAllSubjects() error {
	// Get all subjects
	resp, err := c.httpClient.Get(c.baseURL + "/subjects")
	if err != nil {
		return fmt.Errorf("failed to get subjects: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get subjects, status: %d", resp.StatusCode)
	}

	var subjects []string
	if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
		return fmt.Errorf("failed to decode subjects: %w", err)
	}

	// Delete each subject (hard delete)
	for _, subject := range subjects {
		// First do a soft delete
		req, err := http.NewRequest(http.MethodDelete, c.baseURL+"/subjects/"+subject, nil)
		if err != nil {
			continue
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			continue
		}
		resp.Body.Close()

		// Then do a hard delete to permanently remove
		req, err = http.NewRequest(http.MethodDelete, c.baseURL+"/subjects/"+subject+"?permanent=true", nil)
		if err != nil {
			continue
		}
		resp, err = c.httpClient.Do(req)
		if err != nil {
			continue
		}
		resp.Body.Close()
	}

	return nil
}

// cleanupRedpandaContainer cleans up the Redpanda container with proper timeout handling
func cleanupRedpandaContainer() {
	if redpandaContainer != nil {
		// Use a timeout context to prevent hanging during cleanup
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Terminate the container (ignoring errors since we're cleaning up)
		_ = redpandaContainer.Terminate(ctx)
		redpandaContainer = nil
	}
}

// startRedpandaContainer starts a Redpanda container using testcontainers
func startRedpandaContainer() error {
	// Clean up any existing container first
	cleanupRedpandaContainer()

	ctx := context.Background()

	// Create Redpanda container with testcontainers using Docker Hub image
	container, err := redpanda.Run(ctx,
		"redpandadata/redpanda:latest")
	if err != nil {
		return fmt.Errorf("failed to start Redpanda container: %w", err)
	}

	// Get the schema registry URL
	schemaRegistryURL, err := container.SchemaRegistryAddress(ctx)
	if err != nil {
		container.Terminate(ctx)
		return fmt.Errorf("failed to get schema registry URL: %w", err)
	}

	redpandaContainer = container
	redpandaSchemaRegistryURL = schemaRegistryURL

	return nil
}

func setupRedpandaSchemas() error {
	client := NewRedpandaSchemaRegistryClient(redpandaSchemaRegistryURL)
	defer client.Close() // Ensure HTTP client is cleaned up

	// Wait for schema registry to be ready
	if err := client.WaitForReady(); err != nil {
		return fmt.Errorf("schema registry not ready: %w", err)
	}

	// Clean up any existing schemas first
	if err := client.DeleteAllSubjects(); err != nil {
		return fmt.Errorf("failed to cleanup existing schemas: %w", err)
	}

	// Wait for schemas to be fully removed
	Eventually(func() bool {
		resp, err := client.httpClient.Get(client.baseURL + "/subjects")
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return false
		}

		var subjects []string
		if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
			return false
		}

		return len(subjects) == 0
	}, "10s", "500ms").Should(BeTrue(), "Schemas should be fully removed after cleanup")

	// Register all test schemas in version order
	for subject, versions := range redpandaTestSchemas {
		// Get versions in sorted order to ensure consistent registration
		var sortedVersions []int
		for version := range versions {
			sortedVersions = append(sortedVersions, version)
		}

		// Sort versions to ensure we register in ascending order
		sort.Ints(sortedVersions)

		for _, version := range sortedVersions {
			schema := versions[version]
			if err := client.RegisterSchema(subject, version, schema); err != nil {
				return fmt.Errorf("failed to register schema %s v%d: %w", subject, version, err)
			}
		}
	}

	return nil
}

var _ = Describe("Real Redpanda Integration Tests", Ordered, Label("redpanda"), func() {
	var validator *Validator

	BeforeAll(func() {
		// Add panic recovery to prevent resource leaks on test failures
		defer func() {
			if r := recover(); r != nil {
				GinkgoWriter.Printf("BeforeAll panic recovered: %v\n", r)
				if validator != nil {
					validator.Close()
				}
				cleanupRedpandaContainer()
				panic(r) // Re-panic to fail the test
			}
		}()

		By("Starting Redpanda container")
		Expect(startRedpandaContainer()).To(Succeed())

		By("Setting up test schemas in Redpanda")
		Expect(setupRedpandaSchemas()).To(Succeed())

		By("Creating validator with real schema registry")
		validator = NewValidatorWithRegistry(redpandaSchemaRegistryURL)
		Expect(validator).NotTo(BeNil())
	})

	AfterAll(func() {
		// Ensure cleanup happens even if there are failures
		defer func() {
			if r := recover(); r != nil {
				GinkgoWriter.Printf("AfterAll panic recovered: %v\n", r)
				// Continue cleanup even after panic
			}

			// Final cleanup regardless of errors
			if validator != nil {
				validator.Close()
				validator = nil
			}
			cleanupRedpandaContainer()
		}()

		By("Cleaning up validator")
		if validator != nil {
			validator.Close()
			validator = nil
		}

		By("Cleaning up Redpanda container")
		cleanupRedpandaContainer()
	})

	Context("when validating against real Redpanda schema registry", Ordered, func() {
		It("should successfully validate sensor data v1", func() {
			// Create UNS topic - note that the topic still uses the original contract format
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			// Create valid payload - raw payload that will be wrapped by validator
			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			// With synchronous fetching, schema should be loaded immediately
			result := validator.Validate(unsTopic, payload)
			GinkgoWriter.Printf("Validation result: SchemaCheckPassed=%v, SchemaCheckBypassed=%v, BypassReason=%s, Error=%v\n",
				result.SchemaCheckPassed, result.SchemaCheckBypassed, result.BypassReason, result.Error)

			// Check if schemas were loaded for the contract
			hasSchema := validator.HasSchema("_sensor_data", 1)
			GinkgoWriter.Printf("Has schemas for _sensor_data v1: %v\n", hasSchema)

			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should reject invalid virtual path for sensor data v1", func() {
			// Load schema first by validating a valid message
			validTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())
			validPayload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			// Trigger schema load if not already loaded
			Eventually(func() bool {
				result := validator.Validate(validTopic, validPayload)
				return result.SchemaCheckPassed && !result.SchemaCheckBypassed
			}, "10s", "500ms").Should(BeTrue(), "Schema should be loaded for valid topic")

			// Get the result for further testing
			result := validator.Validate(validTopic, validPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Now test invalid virtual path
			invalidTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.humidity")
			Expect(err).To(BeNil())

			// Invalid payload - raw payload for humidity topic (which is not allowed in v1)
			invalidPayload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)
			result = validator.Validate(invalidTopic, invalidPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})

		It("should validate sensor data v2 with expanded virtual paths", func() {
			// Test temperature (allowed in both v1 and v2)
			tempTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v2.temperature")
			Expect(err).To(BeNil())
			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(tempTopic, payload)
			GinkgoWriter.Printf("V2 temperature validation result: SchemaCheckPassed=%v, SchemaCheckBypassed=%v, BypassReason=%s, Error=%v\n",
				result.SchemaCheckPassed, result.SchemaCheckBypassed, result.BypassReason, result.Error)

			// With synchronous fetching, schema should be loaded immediately on first validation
			if result.SchemaCheckBypassed {
				Fail(fmt.Sprintf("Schema validation was bypassed unexpectedly: %s", result.BypassReason))
			}

			// Check if schema was loaded
			hasSchema := validator.HasSchema("_sensor_data", 2)
			GinkgoWriter.Printf("Has schemas for _sensor_data v2: %v\n", hasSchema)

			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractVersion).To(Equal(uint64(2)))

			// Test humidity (allowed in v2 but not v1)
			humidityTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v2.humidity")
			Expect(err).To(BeNil())
			humidityPayload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result = validator.Validate(humidityTopic, humidityPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractVersion).To(Equal(uint64(2)))

			// Test pressure (allowed in v2 but not v1)
			pressureTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v2.pressure")
			Expect(err).To(BeNil())
			pressurePayload := []byte(`{"timestamp_ms": 1719859200000, "value": 1013.25}`)

			result = validator.Validate(pressureTopic, pressurePayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractVersion).To(Equal(uint64(2)))
		})

		It("should validate pump data with complex virtual paths", func() {
			// Test vibration.x-axis
			xAxisTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data_v1.vibration.x-axis")
			Expect(err).To(BeNil())
			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 0.5}`)

			result := validator.Validate(xAxisTopic, payload)
			GinkgoWriter.Printf("First pump data validation result: SchemaCheckPassed=%v, SchemaCheckBypassed=%v, BypassReason=%s, Error=%v\n",
				result.SchemaCheckPassed, result.SchemaCheckBypassed, result.BypassReason, result.Error)

			// With synchronous fetching, schema should be loaded immediately on first validation
			if result.SchemaCheckBypassed {
				Fail(fmt.Sprintf("Schema validation was bypassed unexpectedly: %s", result.BypassReason))
			}

			// Check if schema was loaded
			hasSchema := validator.HasSchema("_pump_data", 1)
			GinkgoWriter.Printf("Has schemas for _pump_data v1: %v\n", hasSchema)

			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractName).To(Equal("_pump_data"))

			// Test vibration.y-axis
			yAxisTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data_v1.vibration.y-axis")
			Expect(err).To(BeNil())
			yAxisPayload := []byte(`{"timestamp_ms": 1719859200000, "value": 0.5}`)

			result = validator.Validate(yAxisTopic, yAxisPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Test count
			countTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data_v1.count")
			Expect(err).To(BeNil())
			countPayload := []byte(`{"timestamp_ms": 1719859200000, "value": 1542}`)

			result = validator.Validate(countTopic, countPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
		})

		It("should validate string data types correctly", func() {
			// Test serialNumber
			serialTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._string_data_v1.serialNumber")
			Expect(err).To(BeNil())
			payload := []byte(`{"timestamp_ms": 1719859200000, "value": "SN123456789"}`)

			Eventually(func() bool {
				result := validator.Validate(serialTopic, payload)
				return result.SchemaCheckPassed && !result.SchemaCheckBypassed
			}, "10s", "500ms").Should(BeTrue(), "String data schema should be loaded and validation should pass")

			result := validator.Validate(serialTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractName).To(Equal("_string_data"))

			// Test status
			statusTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._string_data_v1.status")
			Expect(err).To(BeNil())
			statusPayload := []byte(`{"timestamp_ms": 1719859200000, "value": "RUNNING"}`)

			result = validator.Validate(statusTopic, statusPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
		})

		It("should reject invalid payload formats", func() {
			// Load schema first
			validTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())
			validPayload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			Eventually(func() bool {
				result := validator.Validate(validTopic, validPayload)
				return result.SchemaCheckPassed && !result.SchemaCheckBypassed
			}, "10s", "500ms").Should(BeTrue(), "Schema should be loaded for invalid payload test")

			result := validator.Validate(validTopic, validPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Test missing timestamp_ms
			invalidPayload := []byte(`{"value": 25.5}`)
			result = validator.Validate(validTopic, invalidPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())

			// Test wrong value type
			invalidPayload = []byte(`{"timestamp_ms": 1719859200000, "value": "not_a_number"}`)
			result = validator.Validate(validTopic, invalidPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
		})

		It("should reject wrong data types for string fields", func() {
			// Load schema first
			validTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._string_data_v1.serialNumber")
			Expect(err).To(BeNil())
			validPayload := []byte(`{"timestamp_ms": 1719859200000, "value": "SN123456789"}`)

			Eventually(func() bool {
				result := validator.Validate(validTopic, validPayload)
				return result.SchemaCheckPassed && !result.SchemaCheckBypassed
			}, "10s", "500ms").Should(BeTrue(), "String data schema should be loaded for data type test")

			result := validator.Validate(validTopic, validPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Test providing number instead of string
			invalidPayload := []byte(`{"timestamp_ms": 1719859200000, "value": 12345}`)
			result = validator.Validate(validTopic, invalidPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
		})

		It("should handle non-existent schemas gracefully", func() {
			// Test with a contract that doesn't exist
			nonExistentTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._non_existent_contract_v1.temperature")
			Expect(err).To(BeNil())
			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(nonExistentTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("no schemas found for contract"))

			// Wait for background fetch attempt and verify it still bypasses
			Eventually(func() bool {
				result = validator.Validate(nonExistentTopic, payload)
				return result.SchemaCheckBypassed
			}, "10s", "500ms").Should(BeTrue(), "Non-existent schema should still bypass after background fetch attempt")

			Expect(result.SchemaCheckBypassed).To(BeTrue())
		})

		It("should handle unversioned contracts gracefully", func() {
			// Test with an unversioned contract
			unversionedTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._unversioned_contract.temperature")
			Expect(err).To(BeNil())
			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unversionedTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("unversioned contract '_unversioned_contract' - bypassing validation (no latest fetching)"))
			Expect(result.Error).To(BeNil())
		})

		It("should validate pump data with string values", func() {
			// Test serialNumber (string value)
			serialTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data_v1.serialNumber")
			Expect(err).To(BeNil())
			serialPayload := []byte(`{"timestamp_ms": 1719859200000, "value": "P001-SN123456789"}`)

			result := validator.Validate(serialTopic, serialPayload)
			GinkgoWriter.Printf("Pump string data validation result: SchemaCheckPassed=%v, SchemaCheckBypassed=%v, BypassReason=%s, Error=%v\n",
				result.SchemaCheckPassed, result.SchemaCheckBypassed, result.BypassReason, result.Error)

			// With synchronous fetching, schema should be loaded immediately on first validation
			if result.SchemaCheckBypassed {
				Fail(fmt.Sprintf("Schema validation was bypassed unexpectedly: %s", result.BypassReason))
			}

			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractName).To(Equal("_pump_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))

			// Test status (string value)
			statusTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data_v1.status")
			Expect(err).To(BeNil())
			statusPayload := []byte(`{"timestamp_ms": 1719859200000, "value": "RUNNING"}`)

			result = validator.Validate(statusTopic, statusPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractName).To(Equal("_pump_data"))

			// Test that number values fail on string-only virtual paths
			numberPayload := []byte(`{"timestamp_ms": 1719859200000, "value": 12345}`)
			result = validator.Validate(serialTopic, numberPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})
	})

	Context("when testing foreground fetcher against real registry", func() {
		It("should automatically fetch schemas in the foreground", func() {
			// Create a fresh validator to test foreground fetching
			freshValidator := NewValidatorWithRegistry(redpandaSchemaRegistryURL)
			defer freshValidator.Close()

			// Create topic that will trigger foreground fetch
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())
			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			// First validation should not bypass
			result := freshValidator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckBypassed).To(BeFalse())
		})
	})
})
