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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

// schemaVersionRegex matches data contract names with version suffixes.
// Expected format: "contractname-v123" where 123 is the version number.
var schemaVersionRegex = regexp.MustCompile(`^(.+)-v(\d+)$`)

// ValidationResult contains information about the validation result and the contract used.
type ValidationResult struct {
	// SchemaCheckPassed indicates whether the schema validation passed
	SchemaCheckPassed bool
	// SchemaCheckBypassed indicates whether the schema validation was bypassed
	SchemaCheckBypassed bool
	// ContractName is the name of the contract that was validated against
	ContractName string
	// ContractVersion is the version of the contract that was validated against
	ContractVersion uint64
	// BypassReason indicates why validation was bypassed (empty if not bypassed)
	BypassReason string
	// Error contains the validation error if validation failed
	Error error
}

// Validator manages schema validation for UNS topics with thread-safe operations.
type Validator struct {
	schemas           map[string]*Schema
	schemasMutex      sync.RWMutex
	schemaRegistryURL string
	backgroundFetcher *BackgroundFetcher
}

// NewValidator creates a new Validator instance with an empty schema registry.
func NewValidator() *Validator {
	return &Validator{
		schemas: make(map[string]*Schema),
	}
}

// NewValidatorWithRegistry creates a new Validator instance with the specified schema registry URL.
func NewValidatorWithRegistry(schemaRegistryURL string) *Validator {
	validator := &Validator{
		schemas:           make(map[string]*Schema),
		schemaRegistryURL: schemaRegistryURL,
	}

	// If a schema registry URL is provided, start the background fetcher
	if schemaRegistryURL != "" {
		validator.backgroundFetcher = NewBackgroundFetcher(schemaRegistryURL, validator)
		validator.backgroundFetcher.Start()
	}

	return validator
}

// Validate validates the given UNS topic and payload against the registered schema.
// It extracts the contract and version from the topic, finds the appropriate schema,
// and validates the payload structure. Returns a ValidationResult with contract information.
func (v *Validator) Validate(unsTopic *topic.UnsTopic, payload []byte) *ValidationResult {
	if unsTopic == nil {
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: false,
			Error:               fmt.Errorf("UNS topic cannot be nil"),
		}
	}

	v.schemasMutex.RLock()
	defer v.schemasMutex.RUnlock()

	topicInfo := unsTopic.Info()
	if topicInfo == nil {
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: false,
			Error:               fmt.Errorf("topic info is nil"),
		}
	}

	contract := topicInfo.DataContract
	if contract == "" {
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: false,
			Error:               fmt.Errorf("data contract is empty"),
		}
	}

	contractName, version, err := v.ExtractSchemaVersionFromDataContract(contract)
	if err != nil {
		// For unversioned contracts, always bypass (no fetching of "latest")
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: true,
			ContractName:        contract, // Use the original contract string as fallback
			ContractVersion:     0,
			BypassReason:        fmt.Sprintf("unversioned contract '%s' - bypassing validation (no latest fetching)", contract),
			Error:               nil,
		}
	}

	if !v.HasSchema(contractName, version) {
		// Queue the schema for background fetching if we have a background fetcher
		if v.backgroundFetcher != nil {
			v.backgroundFetcher.QueueSchema(contractName, version)
		}

		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: true,
			ContractName:        contractName,
			ContractVersion:     version,
			BypassReason:        fmt.Sprintf("schema for contract '%s' version %d not found, queued for background fetch", contractName, version),
			Error:               nil,
		}
	}

	schema := v.schemas[contractName].GetVersion(version)
	if schema == nil {
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: true,
			ContractName:        contractName,
			ContractVersion:     version,
			BypassReason:        fmt.Sprintf("schema for contract '%s' version %d is nil", contractName, version),
			Error:               nil,
		}
	}

	// Build the full path for validation
	var fullPath strings.Builder
	if topicInfo.VirtualPath != nil {
		fullPath.WriteString(*topicInfo.VirtualPath)
		fullPath.WriteString(".")
	}
	fullPath.WriteString(topicInfo.Name)

	// Wrap the payload with fields and virtual_path for validation
	wrappedPayload := []byte(fmt.Sprintf(`{"fields": %s, "virtual_path": "%s"}`,
		string(payload), fullPath.String()))

	validationResult := schema.ValidateJSON(wrappedPayload)
	if validationResult == nil {
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: false,
			ContractName:        contractName,
			ContractVersion:     version,
			Error:               fmt.Errorf("schema validation result is nil"),
		}
	}

	if !validationResult.Valid {
		var validationErrors []string
		for _, validationErr := range validationResult.Errors {
			validationErrors = append(validationErrors, validationErr.Error())
		}
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: false,
			ContractName:        contractName,
			ContractVersion:     version,
			Error: fmt.Errorf("schema validation failed for contract '%s' version %d: %s",
				contractName, version, strings.Join(validationErrors, "; ")),
		}
	}

	return &ValidationResult{
		SchemaCheckPassed:   true,
		SchemaCheckBypassed: false,
		ContractName:        contractName,
		ContractVersion:     version,
		Error:               nil,
	}
}

// ExtractSchemaVersionFromDataContract parses a data contract string to extract
// the base contract name and version number.
// Expected format: "contractname-v123" -> ("contractname", 123, nil)
func (v *Validator) ExtractSchemaVersionFromDataContract(contract string) (contractName string, version uint64, err error) {
	if contract == "" {
		return "", 0, fmt.Errorf("contract string is empty")
	}

	matches := schemaVersionRegex.FindStringSubmatch(contract)
	if len(matches) != 3 {
		return "", 0, fmt.Errorf("invalid data contract format '%s', expected format: 'name-v123'", contract)
	}

	contractName = matches[1]
	version, err = strconv.ParseUint(matches[2], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid version number '%s' in contract '%s': %w", matches[2], contract, err)
	}

	return contractName, version, nil
}

// LoadSchema loads and compiles a schema for the specified contract name and version.
// The contract name must start with an underscore for easier topic matching.
func (v *Validator) LoadSchema(contractName string, version uint64, schema []byte) error {
	if contractName == "" {
		return fmt.Errorf("contract name cannot be empty")
	}

	if !strings.HasPrefix(contractName, "_") {
		return fmt.Errorf("contract name must start with an underscore, got: '%s'", contractName)
	}

	if len(schema) == 0 {
		return fmt.Errorf("schema cannot be empty for contract '%s' version %d", contractName, version)
	}

	v.schemasMutex.Lock()
	defer v.schemasMutex.Unlock()

	if _, exists := v.schemas[contractName]; !exists {
		v.schemas[contractName] = NewSchema(contractName)
	}

	if err := v.schemas[contractName].AddVersion(version, schema); err != nil {
		return fmt.Errorf("failed to add schema version %d for contract '%s': %w", version, contractName, err)
	}

	return nil
}

// HasSchema checks if a schema exists for the given contract name and version.
func (v *Validator) HasSchema(contractName string, version uint64) bool {
	v.schemasMutex.RLock()
	defer v.schemasMutex.RUnlock()

	schema, exists := v.schemas[contractName]
	if !exists {
		return false
	}

	return schema.HasVersion(version)
}

// Close stops the background fetcher and cleans up resources.
func (v *Validator) Close() {
	if v.backgroundFetcher != nil {
		v.backgroundFetcher.Stop()
	}
}
