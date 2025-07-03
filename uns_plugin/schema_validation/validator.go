package schemavalidation

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

type Validator struct {
	schemas      map[string]*Schema
	schemasMutex sync.RWMutex
}

func NewValidator() *Validator {
	return &Validator{
		schemas: make(map[string]*Schema),
	}
}

func (v *Validator) Validate(topic *topic.UnsTopic, payload []byte) error {
	v.schemasMutex.RLock()
	defer v.schemasMutex.RUnlock()

	topicInfo := topic.Info()
	if topicInfo == nil {
		return fmt.Errorf("topic info is nil")
	}

	contract := topicInfo.DataContract
	if contract == "" {
		return fmt.Errorf("contract is empty")
	}

	contractName, version, err := v.ExtractSchemaVersionFromDataContract(contract)
	if err != nil {
		return err
	}

	if !v.HasSchema(contractName, version) {
		return fmt.Errorf("schema for contract %s and version %d not found", contractName, version)
	}

	schema := v.schemas[contractName].GetVersion(version)
	if schema == nil {
		return fmt.Errorf("schema for contract %s and version %d is nil", contractName, version)
	}

	// We now wrap the payload inside the fields object (using direct bytes)

	var fullPath strings.Builder
	if topicInfo.VirtualPath != nil {
		fullPath.WriteString(*topicInfo.VirtualPath)
		fullPath.WriteString(".")
	}
	fullPath.WriteString(topicInfo.Name)
	wrappedPayload := []byte(fmt.Sprintf(`{"fields": %s, "virtual_path": "%s"}`, string(payload), fullPath.String()))

	// Debug print the wrapped payload
	fmt.Printf("Wrapped payload: %s\n", string(wrappedPayload))

	validationResult := schema.Validate(wrappedPayload)

	if validationResult == nil {
		return fmt.Errorf("schema validation result is nil")
	}

	if !validationResult.Valid {
		errors := validationResult.Errors
		validationErrors := []string{}
		for _, err := range errors {
			validationErrors = append(validationErrors, err.Error())
		}
		return fmt.Errorf("schema validation failed: %v", strings.Join(validationErrors, "\n"))
	}

	return nil
}

var schemaVersionRegex = regexp.MustCompile(`^(.+)-v(\d+)$`)

func (v *Validator) ExtractSchemaVersionFromDataContract(contract string) (contractName string, version uint64, err error) {
	// the data contract must end with v<version> (so we can easily regex that)
	matches := schemaVersionRegex.FindStringSubmatch(contract)
	// First match is the full string, second is the version
	if len(matches) != 3 {
		return "", 0, fmt.Errorf("invalid data contract: %s", contract)
	}

	version, err = strconv.ParseUint(matches[2], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid version: %s", matches[2])
	}

	return matches[1], version, nil
}

func (v *Validator) LoadSchema(contractName string, version uint64, schema []byte) error {
	// contract name must start with an underscore (this enables easier matching on the data contract from the topic)
	if !strings.HasPrefix(contractName, "_") {
		return fmt.Errorf("contract name must start with an underscore: %s", contractName)
	}

	v.schemasMutex.Lock()
	defer v.schemasMutex.Unlock()

	if _, ok := v.schemas[contractName]; !ok {
		v.schemas[contractName] = NewSchema(contractName)
	}

	return v.schemas[contractName].AddVersion(version, schema)
}

func (v *Validator) HasSchema(contract string, version uint64) bool {
	v.schemasMutex.RLock()
	defer v.schemasMutex.RUnlock()

	schema, ok := v.schemas[contract]
	if !ok {
		return false
	}

	return schema.HasVersion(version)
}
