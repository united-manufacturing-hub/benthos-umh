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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
)

// MockSchemaRegistry simulates Redpanda's Schema Registry for testing
type MockSchemaRegistry struct {
	server  *httptest.Server
	schemas map[string]map[int]*MockSchemaVersion // subject -> version -> schema
}

// MockSchemaVersion represents a schema version in the mock registry
type MockSchemaVersion struct {
	ID      int    `json:"id"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
	Subject string `json:"subject"`
}

// NewMockSchemaRegistry creates a new mock schema registry server
func NewMockSchemaRegistry() *MockSchemaRegistry {
	mock := &MockSchemaRegistry{
		schemas: make(map[string]map[int]*MockSchemaVersion),
	}

	// Create HTTP server with Redpanda-compatible API
	mux := http.NewServeMux()
	mux.HandleFunc("/subjects", mock.handleSubjects)
	mux.HandleFunc("/subjects/", mock.handleSubjectVersions)

	mock.server = httptest.NewServer(mux)
	return mock
}

// URL returns the base URL of the mock server
func (m *MockSchemaRegistry) URL() string {
	return m.server.URL
}

// Close shuts down the mock server
func (m *MockSchemaRegistry) Close() {
	m.server.Close()
}

// AddSchema adds a schema to the mock registry
func (m *MockSchemaRegistry) AddSchema(subject string, version int, schema string) {
	if m.schemas[subject] == nil {
		m.schemas[subject] = make(map[int]*MockSchemaVersion)
	}

	// Generate a unique ID (simple incrementing for mock)
	id := len(m.schemas) + version*1000

	m.schemas[subject][version] = &MockSchemaVersion{
		ID:      id,
		Version: version,
		Schema:  schema,
		Subject: subject,
	}
}

// RemoveSchema removes a schema from the mock registry
func (m *MockSchemaRegistry) RemoveSchema(subject string, version int) {
	if versions, exists := m.schemas[subject]; exists {
		delete(versions, version)
		if len(versions) == 0 {
			delete(m.schemas, subject)
		}
	}
}

// GetSchema gets a schema from the mock registry
func (m *MockSchemaRegistry) GetSchema(subject string, version int) *MockSchemaVersion {
	if versions, exists := m.schemas[subject]; exists {
		return versions[version]
	}
	return nil
}

// handleSubjects handles GET /subjects - returns all available subjects
func (m *MockSchemaRegistry) handleSubjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Collect all subjects
	var subjects []string
	for subject := range m.schemas {
		subjects = append(subjects, subject)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(subjects)
}

// handleSubjectVersions handles requests to /subjects/{subject}/versions/{version}
func (m *MockSchemaRegistry) handleSubjectVersions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the URL path: /subjects/{subject}/versions/{version}
	path := strings.TrimPrefix(r.URL.Path, "/subjects/")
	parts := strings.Split(path, "/")

	if len(parts) < 3 || parts[1] != "versions" {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	subject := parts[0]
	versionStr := parts[2]

	// Handle "latest" version request
	if versionStr == "latest" {
		m.handleLatestVersion(w, subject)
		return
	}

	// Parse version number
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		http.Error(w, "Invalid version format", http.StatusBadRequest)
		return
	}

	// Check if subject exists
	versions, subjectExists := m.schemas[subject]
	if !subjectExists {
		http.Error(w, fmt.Sprintf(`{"error_code":40401,"message":"Subject '%s' not found."}`, subject), http.StatusNotFound)
		return
	}

	// Check if version exists
	schema, versionExists := versions[version]
	if !versionExists {
		http.Error(w, fmt.Sprintf(`{"error_code":40402,"message":"Version %d not found for subject '%s'."}`, version, subject), http.StatusNotFound)
		return
	}

	// Return the schema version (Redpanda format)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schema)
}

// handleLatestVersion handles requests for the latest version of a subject
func (m *MockSchemaRegistry) handleLatestVersion(w http.ResponseWriter, subject string) {
	versions, subjectExists := m.schemas[subject]
	if !subjectExists {
		http.Error(w, fmt.Sprintf(`{"error_code":40401,"message":"Subject '%s' not found."}`, subject), http.StatusNotFound)
		return
	}

	// Find the latest version
	var latestVersion int
	var latestSchema *MockSchemaVersion
	for version, schema := range versions {
		if version > latestVersion {
			latestVersion = version
			latestSchema = schema
		}
	}

	if latestSchema == nil {
		http.Error(w, fmt.Sprintf(`{"error_code":40402,"message":"No versions found for subject '%s'."}`, subject), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(latestSchema)
}

// SetupTestSchemas adds common test schemas to the mock registry
func (m *MockSchemaRegistry) SetupTestSchemas() {
	// Add sensor data schemas v1 with different data types
	sensorDataV1NumberSchema := `{
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
	}`
	m.AddSchema("_sensor_data_v1-timeseries-number", 1, sensorDataV1NumberSchema)

	// Add sensor data schemas v2 with expanded virtual paths and number type
	sensorDataV2NumberSchema := `{
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
	}`
	m.AddSchema("_sensor_data_v2-timeseries-number", 2, sensorDataV2NumberSchema)

	// Add pump data schemas v1 with different data types
	pumpDataV1NumberSchema := `{
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
	}`
	m.AddSchema("_pump_data_v1-timeseries-number", 1, pumpDataV1NumberSchema)

	// Add pump data string schema for serial numbers
	pumpDataV1StringSchema := `{
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
	}`
	m.AddSchema("_pump_data_v1-timeseries-string", 1, pumpDataV1StringSchema)

	// Add motor controller schemas v3 (skipping v1 and v2 to test version gaps)
	motorDataV3NumberSchema := `{
		"type": "object",
		"properties": {
			"virtual_path": {
				"type": "string",
				"enum": ["rpm", "temperature", "status"]
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
	}`
	m.AddSchema("_motor_controller_v3-timeseries-number", 3, motorDataV3NumberSchema)

	// Add motor controller string schema for status
	motorDataV3StringSchema := `{
		"type": "object",
		"properties": {
			"virtual_path": {
				"type": "string",
				"enum": ["status", "mode"]
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
	}`
	m.AddSchema("_motor_controller_v3-timeseries-string", 3, motorDataV3StringSchema)

	// Add string data schemas v1 for testing different data types
	stringDataV1Schema := `{
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
	}`
	m.AddSchema("_string_data_v1-timeseries-string", 1, stringDataV1Schema)
}

// SimulateNetworkError makes the mock server return 500 errors for testing
func (m *MockSchemaRegistry) SimulateNetworkError(enable bool) {
	if enable {
		// Replace handlers with error handlers
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		})
		m.server.Config.Handler = mux
	} else {
		// Restore normal handlers
		mux := http.NewServeMux()
		mux.HandleFunc("/subjects", m.handleSubjects)
		mux.HandleFunc("/subjects/", m.handleSubjectVersions)
		m.server.Config.Handler = mux
	}
}

// GetRegisteredSubjects returns all subjects currently in the mock registry
func (m *MockSchemaRegistry) GetRegisteredSubjects() []string {
	var subjects []string
	for subject := range m.schemas {
		subjects = append(subjects, subject)
	}
	return subjects
}

// GetVersionsForSubject returns all versions for a given subject
func (m *MockSchemaRegistry) GetVersionsForSubject(subject string) []int {
	var versions []int
	if subjectVersions, exists := m.schemas[subject]; exists {
		for version := range subjectVersions {
			versions = append(versions, version)
		}
	}
	return versions
}
