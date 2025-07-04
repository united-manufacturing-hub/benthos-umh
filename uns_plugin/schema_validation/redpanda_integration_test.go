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
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

const (
	redpandaContainerName      = "benthos-umh-test-redpanda"
	redpandaSchemaRegistryURL  = "http://localhost:8081"
	redpandaKafkaPort          = "9092"
	redpandaSchemaRegistryPort = "8081"
)

// RedpandaTestSchemas contains the test schemas we'll load into Redpanda
// We'll add a timestamp suffix to avoid conflicts between test runs
var redpandaTestSchemas = map[string]map[int]string{
	"_sensor_data": {
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
		}`,
		2: `{
			"type": "object",
			"properties": {
				"virtual_path": {
					"type": "string",
					"enum": ["temperature", "humidity", "pressure"]
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
		}`,
	},
	"_pump_data": {
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
		}`,
	},
	"_string_data": {
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
						"value": {
							"type": "object",
							"properties": {
								"timestamp_ms": {"type": "number"},
								"value": {"type": "string"}
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
		}`,
	},
}

// RedpandaSchemaRegistryClient for interacting with real Redpanda schema registry
type RedpandaSchemaRegistryClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewRedpandaSchemaRegistryClient(baseURL string) *RedpandaSchemaRegistryClient {
	return &RedpandaSchemaRegistryClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
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
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
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
		if err == nil && resp.StatusCode == 200 {
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

	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to get subjects, status: %d", resp.StatusCode)
	}

	var subjects []string
	if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
		return fmt.Errorf("failed to decode subjects: %w", err)
	}

	// Delete each subject (hard delete)
	for _, subject := range subjects {
		// First do a soft delete
		req, err := http.NewRequest("DELETE", c.baseURL+"/subjects/"+subject, nil)
		if err != nil {
			continue
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			continue
		}
		resp.Body.Close()

		// Then do a hard delete to permanently remove
		req, err = http.NewRequest("DELETE", c.baseURL+"/subjects/"+subject+"?permanent=true", nil)
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

func cleanupRedpandaContainer() {
	// Kill and remove any existing container with our name
	exec.Command("docker", "kill", redpandaContainerName).Run()
	exec.Command("docker", "rm", "-f", redpandaContainerName).Run()

	// Wait for container to be removed completely
	Eventually(func() bool {
		cmd := exec.Command("docker", "ps", "-a", "-q", "--filter", fmt.Sprintf("name=%s", redpandaContainerName))
		output, err := cmd.Output()
		return err != nil || len(output) == 0
	}, "15s", "1s").Should(BeTrue(), "Container should be removed completely")
}

func startRedpandaContainer() error {
	// Clean up any existing container first
	cleanupRedpandaContainer()

	// Start Redpanda container
	cmd := exec.Command("docker", "run", "-d",
		"--name", redpandaContainerName,
		"-p", fmt.Sprintf("%s:9092", redpandaKafkaPort),
		"-p", fmt.Sprintf("%s:8081", redpandaSchemaRegistryPort),
		"-p", "8082:8082",
		"-p", "9644:9644",
		"redpandadata/redpanda:latest",
		"redpanda", "start",
		"--overprovisioned",
		"--smp", "1",
		"--memory", "1G",
		"--reserve-memory", "0M",
		"--node-id", "0",
		"--check=false")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start Redpanda container: %w, output: %s", err, string(output))
	}

	// Wait for Redpanda to actually start up successfully
	redpandaStarted := false
	Eventually(func() bool {
		// Check container logs for successful startup message
		logsCmd := exec.Command("docker", "logs", redpandaContainerName)
		logsOutput, err := logsCmd.CombinedOutput()
		if err != nil {
			return false
		}

		logsStr := string(logsOutput)
		if strings.Contains(logsStr, "Successfully started Redpanda!") {
			redpandaStarted = true
			return true
		}

		return false
	}, "60s", "2s").Should(BeTrue(), "Redpanda should start successfully")

	if !redpandaStarted {
		// On failure, dump full logs for debugging
		logsCmd := exec.Command("docker", "logs", redpandaContainerName)
		logsOutput, _ := logsCmd.CombinedOutput()
		return fmt.Errorf("Redpanda failed to start within timeout. Full logs:\n%s", string(logsOutput))
	}

	return nil
}

func setupRedpandaSchemas() error {
	client := NewRedpandaSchemaRegistryClient(redpandaSchemaRegistryURL)

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

		if resp.StatusCode != 200 {
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
		// Skip if Docker is not available
		if _, err := exec.LookPath("docker"); err != nil {
			Skip("Docker not available, skipping Redpanda integration tests")
		}

		// Check if Docker daemon is running
		if err := exec.Command("docker", "info").Run(); err != nil {
			Skip("Docker daemon not running, skipping Redpanda integration tests")
		}

		By("Starting Redpanda container")
		Expect(startRedpandaContainer()).To(Succeed())

		By("Setting up test schemas in Redpanda")
		Expect(setupRedpandaSchemas()).To(Succeed())

		By("Creating validator with real schema registry")
		validator = NewValidatorWithRegistry(redpandaSchemaRegistryURL)
		Expect(validator).NotTo(BeNil())
	})

	AfterAll(func() {
		if validator != nil {
			validator.Close()
		}

		By("Cleaning up Redpanda container")
		cleanupRedpandaContainer()
	})

	Context("when validating against real Redpanda schema registry", Ordered, func() {
		It("should successfully validate sensor data v1", func() {
			// Create UNS topic with unique suffix
			contractName := "_sensor_data"
			unsTopic, err := topic.NewUnsTopic(fmt.Sprintf("umh.v1.enterprise.site.area.%s-v1.temperature", contractName))
			Expect(err).To(BeNil())

			// Create valid payload
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// With synchronous fetching, schema should be loaded immediately
			result := validator.Validate(unsTopic, payload)
			GinkgoWriter.Printf("Validation result: SchemaCheckPassed=%v, SchemaCheckBypassed=%v, BypassReason=%s, Error=%v\n",
				result.SchemaCheckPassed, result.SchemaCheckBypassed, result.BypassReason, result.Error)

			// Check if schema was loaded
			hasSchema := validator.HasSchema(contractName, 1)
			GinkgoWriter.Printf("Has schema %s v1: %v\n", contractName, hasSchema)

			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
			Expect(result.ContractName).To(Equal(contractName))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should reject invalid virtual path for sensor data v1", func() {
			// Load schema first by validating a valid message
			validTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())
			validPayload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// Trigger schema load if not already loaded
			Eventually(func() bool {
				result := validator.Validate(validTopic, validPayload)
				return result.SchemaCheckPassed && !result.SchemaCheckBypassed
			}, "10s", "500ms").Should(BeTrue(), "Schema should be loaded for valid topic")

			// Get the result for further testing
			result := validator.Validate(validTopic, validPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Now test invalid virtual path
			invalidTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.humidity")
			Expect(err).To(BeNil())

			result = validator.Validate(invalidTopic, validPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})

		It("should validate sensor data v2 with expanded virtual paths", func() {
			// Test temperature (allowed in both v1 and v2)
			tempTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v2.temperature")
			Expect(err).To(BeNil())
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			result := validator.Validate(tempTopic, payload)
			GinkgoWriter.Printf("V2 temperature validation result: SchemaCheckPassed=%v, SchemaCheckBypassed=%v, BypassReason=%s, Error=%v\n",
				result.SchemaCheckPassed, result.SchemaCheckBypassed, result.BypassReason, result.Error)

			// With synchronous fetching, schema should be loaded immediately on first validation
			if result.SchemaCheckBypassed {
				Fail(fmt.Sprintf("Schema validation was bypassed unexpectedly: %s", result.BypassReason))
			}

			// Check if schema was loaded
			hasSchema := validator.HasSchema("_sensor_data", 2)
			GinkgoWriter.Printf("Has schema _sensor_data v2: %v\n", hasSchema)

			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractVersion).To(Equal(uint64(2)))

			// Test humidity (allowed in v2 but not v1)
			humidityTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v2.humidity")
			Expect(err).To(BeNil())

			result = validator.Validate(humidityTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractVersion).To(Equal(uint64(2)))

			// Test pressure (allowed in v2 but not v1)
			pressureTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v2.pressure")
			Expect(err).To(BeNil())
			pressurePayload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 1013.25}}`)

			result = validator.Validate(pressureTopic, pressurePayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractVersion).To(Equal(uint64(2)))
		})

		It("should validate pump data with complex virtual paths", func() {
			// Test vibration.x-axis
			xAxisTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data-v1.vibration.x-axis")
			Expect(err).To(BeNil())
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 0.5}}`)

			result := validator.Validate(xAxisTopic, payload)
			GinkgoWriter.Printf("First pump data validation result: SchemaCheckPassed=%v, SchemaCheckBypassed=%v, BypassReason=%s, Error=%v\n",
				result.SchemaCheckPassed, result.SchemaCheckBypassed, result.BypassReason, result.Error)

			// With synchronous fetching, schema should be loaded immediately on first validation
			if result.SchemaCheckBypassed {
				Fail(fmt.Sprintf("Schema validation was bypassed unexpectedly: %s", result.BypassReason))
			}

			// Check if schema was loaded
			hasSchema := validator.HasSchema("_pump_data", 1)
			GinkgoWriter.Printf("Has schema _pump_data v1: %v\n", hasSchema)

			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractName).To(Equal("_pump_data"))

			// Test vibration.y-axis
			yAxisTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data-v1.vibration.y-axis")
			Expect(err).To(BeNil())

			result = validator.Validate(yAxisTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Test count
			countTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data-v1.count")
			Expect(err).To(BeNil())
			countPayload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 1542}}`)

			result = validator.Validate(countTopic, countPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
		})

		It("should validate string data types correctly", func() {
			// Test serialNumber
			serialTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._string_data-v1.serialNumber")
			Expect(err).To(BeNil())
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": "SN123456789"}}`)

			Eventually(func() bool {
				result := validator.Validate(serialTopic, payload)
				return result.SchemaCheckPassed && !result.SchemaCheckBypassed
			}, "10s", "500ms").Should(BeTrue(), "String data schema should be loaded and validation should pass")

			result := validator.Validate(serialTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractName).To(Equal("_string_data"))

			// Test status
			statusTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._string_data-v1.status")
			Expect(err).To(BeNil())
			statusPayload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": "RUNNING"}}`)

			result = validator.Validate(statusTopic, statusPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
		})

		It("should reject invalid payload formats", func() {
			// Load schema first
			validTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())
			validPayload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			Eventually(func() bool {
				result := validator.Validate(validTopic, validPayload)
				return result.SchemaCheckPassed && !result.SchemaCheckBypassed
			}, "10s", "500ms").Should(BeTrue(), "Schema should be loaded for invalid payload test")

			result := validator.Validate(validTopic, validPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Test missing timestamp_ms
			invalidPayload := []byte(`{"value": {"value": 25.5}}`)
			result = validator.Validate(validTopic, invalidPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())

			// Test wrong value type
			invalidPayload = []byte(`{"value": {"timestamp_ms": 1719859200000, "value": "not_a_number"}}`)
			result = validator.Validate(validTopic, invalidPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
		})

		It("should reject wrong data types for string fields", func() {
			// Load schema first
			validTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._string_data-v1.serialNumber")
			Expect(err).To(BeNil())
			validPayload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": "SN123456789"}}`)

			Eventually(func() bool {
				result := validator.Validate(validTopic, validPayload)
				return result.SchemaCheckPassed && !result.SchemaCheckBypassed
			}, "10s", "500ms").Should(BeTrue(), "String data schema should be loaded for data type test")

			result := validator.Validate(validTopic, validPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Test providing number instead of string
			invalidPayload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 12345}}`)
			result = validator.Validate(validTopic, invalidPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
		})

		It("should handle non-existent schemas gracefully", func() {
			// Test with a contract that doesn't exist
			nonExistentTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._non_existent_contract-v1.temperature")
			Expect(err).To(BeNil())
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			result := validator.Validate(nonExistentTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("schema for contract '_non_existent_contract' version 1 does not exist in registry"))

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
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			result := validator.Validate(unversionedTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("unversioned contract '_unversioned_contract' - bypassing validation (no latest fetching)"))
			Expect(result.Error).To(BeNil())
		})
	})

	Context("when testing foreground fetcher against real registry", func() {
		It("should automatically fetch schemas in the foreground", func() {
			// Create a fresh validator to test foreground fetching
			freshValidator := NewValidatorWithRegistry(redpandaSchemaRegistryURL)
			defer freshValidator.Close()

			// Create topic that will trigger foreground fetch
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// First validation should not bypass
			result := freshValidator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckBypassed).To(BeFalse())
		})
	})
})
