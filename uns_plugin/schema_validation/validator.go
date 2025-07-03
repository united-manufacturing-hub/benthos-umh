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
	// Valid indicates whether the validation passed
	Valid bool
	// ContractName is the name of the contract that was validated against
	ContractName string
	// ContractVersion is the version of the contract that was validated against
	ContractVersion uint64
	// Error contains the validation error if validation failed
	Error error
}

// Validator manages schema validation for UNS topics with thread-safe operations.
type Validator struct {
	schemas      map[string]*Schema
	schemasMutex sync.RWMutex
}

// NewValidator creates a new Validator instance with an empty schema registry.
func NewValidator() *Validator {
	return &Validator{
		schemas: make(map[string]*Schema),
	}
}

// Validate validates the given UNS topic and payload against the registered schema.
// It extracts the contract and version from the topic, finds the appropriate schema,
// and validates the payload structure. Returns a ValidationResult with contract information.
func (v *Validator) Validate(unsTopic *topic.UnsTopic, payload []byte) *ValidationResult {
	if unsTopic == nil {
		return &ValidationResult{
			Valid: false,
			Error: fmt.Errorf("UNS topic cannot be nil"),
		}
	}

	v.schemasMutex.RLock()
	defer v.schemasMutex.RUnlock()

	topicInfo := unsTopic.Info()
	if topicInfo == nil {
		return &ValidationResult{
			Valid: false,
			Error: fmt.Errorf("topic info is nil"),
		}
	}

	contract := topicInfo.DataContract
	if contract == "" {
		return &ValidationResult{
			Valid: false,
			Error: fmt.Errorf("data contract is empty"),
		}
	}

	contractName, version, err := v.ExtractSchemaVersionFromDataContract(contract)
	if err != nil {
		return &ValidationResult{
			Valid: false,
			Error: fmt.Errorf("failed to extract schema version from contract '%s': %w", contract, err),
		}
	}

	if !v.HasSchema(contractName, version) {
		return &ValidationResult{
			Valid:           false,
			ContractName:    contractName,
			ContractVersion: version,
			Error:           fmt.Errorf("schema for contract '%s' version %d not found", contractName, version),
		}
	}

	schema := v.schemas[contractName].GetVersion(version)
	if schema == nil {
		return &ValidationResult{
			Valid:           false,
			ContractName:    contractName,
			ContractVersion: version,
			Error:           fmt.Errorf("schema for contract '%s' version %d is nil", contractName, version),
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
			Valid:           false,
			ContractName:    contractName,
			ContractVersion: version,
			Error:           fmt.Errorf("schema validation result is nil"),
		}
	}

	if !validationResult.Valid {
		var validationErrors []string
		for _, validationErr := range validationResult.Errors {
			validationErrors = append(validationErrors, validationErr.Error())
		}
		return &ValidationResult{
			Valid:           false,
			ContractName:    contractName,
			ContractVersion: version,
			Error: fmt.Errorf("schema validation failed for contract '%s' version %d: %s",
				contractName, version, strings.Join(validationErrors, "; ")),
		}
	}

	return &ValidationResult{
		Valid:           true,
		ContractName:    contractName,
		ContractVersion: version,
		Error:           nil,
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
