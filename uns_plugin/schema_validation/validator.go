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
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

const (
	cacheHitTTL  = 0                // Cache successful schema fetches forever (schemas are immutable)
	cacheMissTTL = 10 * time.Minute // Cache misses for 10 minutes to retry sooner
	httpTimeout  = 30 * time.Second
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

// CacheEntry represents a cached schema entry with TTL
type CacheEntry struct {
	// Schema holds the actual schema if it exists, nil if schema doesn't exist
	Schema *Schema
	// SchemaExists indicates whether the schema exists in the registry
	SchemaExists bool
	// CachedAt is when this entry was cached
	CachedAt time.Time
	// ExpiredAt is when this entry expires
	ExpiresAt time.Time
}

// IsExpired checks if the cache entry has expired
func (ce *CacheEntry) IsExpired() bool {
	// Zero time means never expires (for immutable schema hits)
	if ce.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(ce.ExpiresAt)
}

// SchemaRegistryVersion represents a version response from the schema registry
type SchemaRegistryVersion struct {
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

// Validator manages schema validation for UNS topics with TTL-based caching.
type Validator struct {
	// Cache maps "contractName-v123" to CacheEntry
	schemaCache       map[string]*CacheEntry
	cacheMutex        sync.RWMutex
	schemaRegistryURL string
	httpClient        *http.Client
}

// NewValidator creates a new Validator instance with empty cache.
func NewValidator() *Validator {
	return &Validator{
		schemaCache: make(map[string]*CacheEntry),
		httpClient: &http.Client{
			Timeout: httpTimeout,
		},
	}
}

// NewValidatorWithRegistry creates a new Validator instance with the specified schema registry URL.
func NewValidatorWithRegistry(schemaRegistryURL string) *Validator {
	return &Validator{
		schemaCache:       make(map[string]*CacheEntry),
		schemaRegistryURL: schemaRegistryURL,
		httpClient: &http.Client{
			Timeout: httpTimeout,
		},
	}
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

	// Get schema from cache or fetch synchronously
	schema, schemaExists, err := v.getSchemaWithCache(contractName, version)
	if err != nil {
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: true,
			ContractName:        contractName,
			ContractVersion:     version,
			BypassReason:        fmt.Sprintf("failed to fetch schema: %v", err),
			Error:               nil,
		}
	}

	if !schemaExists {
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: true,
			ContractName:        contractName,
			ContractVersion:     version,
			BypassReason:        fmt.Sprintf("schema for contract '%s' version %d does not exist in registry", contractName, version),
			Error:               nil,
		}
	}

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

	jsonSchema := schema.GetVersion(version)
	if jsonSchema == nil {
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: true,
			ContractName:        contractName,
			ContractVersion:     version,
			BypassReason:        fmt.Sprintf("compiled schema for contract '%s' version %d is nil", contractName, version),
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
	// This implementation is technically not secure, as it allows JSON injection.
	// However, the input we get is coming from benthos and likely safe.
	// Also this is not used for any other purpose than validation, so it's not a security issue.
	wrappedPayload := []byte(fmt.Sprintf(`{"fields": %s, "virtual_path": "%s"}`,
		string(payload), fullPath.String()))

	validationResult := jsonSchema.ValidateJSON(wrappedPayload)
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

// getSchemaWithCache retrieves schema from cache or fetches it synchronously
func (v *Validator) getSchemaWithCache(contractName string, version uint64) (*Schema, bool, error) {
	cacheKey := fmt.Sprintf("%s-v%d", contractName, version)

	// Check cache first
	v.cacheMutex.RLock()
	entry, exists := v.schemaCache[cacheKey]
	v.cacheMutex.RUnlock()

	if exists && !entry.IsExpired() {
		// Cache hit and not expired
		return entry.Schema, entry.SchemaExists, nil
	}

	// Cache miss or expired, fetch synchronously
	return v.fetchSchemaSync(contractName, version)
}

// fetchSchemaSync fetches schema synchronously and updates cache
func (v *Validator) fetchSchemaSync(contractName string, version uint64) (*Schema, bool, error) {
	cacheKey := fmt.Sprintf("%s-v%d", contractName, version)

	// Double-check locking pattern
	v.cacheMutex.Lock()
	defer v.cacheMutex.Unlock()

	// Check if another goroutine already fetched it
	if entry, exists := v.schemaCache[cacheKey]; exists && !entry.IsExpired() {
		return entry.Schema, entry.SchemaExists, nil
	}

	// Fetch from registry
	schemaBytes, schemaExists, err := v.fetchSchemaFromRegistry(contractName, version)
	if err != nil {
		// Cache the error result (schema doesn't exist or fetch failed)
		v.schemaCache[cacheKey] = &CacheEntry{
			Schema:       nil,
			SchemaExists: false,
			CachedAt:     time.Now(),
			ExpiresAt:    time.Now().Add(cacheMissTTL),
		}
		return nil, false, err
	}

	if !schemaExists {
		// Cache the fact that schema doesn't exist
		v.schemaCache[cacheKey] = &CacheEntry{
			Schema:       nil,
			SchemaExists: false,
			CachedAt:     time.Now(),
			ExpiresAt:    time.Now().Add(cacheMissTTL),
		}
		return nil, false, nil
	}

	// Compile the schema
	schema := NewSchema(contractName)
	if err := schema.AddVersion(version, schemaBytes); err != nil {
		// Cache the compilation error
		v.schemaCache[cacheKey] = &CacheEntry{
			Schema:       nil,
			SchemaExists: false,
			CachedAt:     time.Now(),
			ExpiresAt:    time.Now().Add(cacheMissTTL),
		}
		return nil, false, fmt.Errorf("failed to compile schema: %w", err)
	}

	// Cache the successful result - forever since schemas are immutable
	expiresAt := time.Time{} // Zero time means never expires
	if cacheHitTTL > 0 {
		expiresAt = time.Now().Add(cacheHitTTL)
	}
	v.schemaCache[cacheKey] = &CacheEntry{
		Schema:       schema,
		SchemaExists: true,
		CachedAt:     time.Now(),
		ExpiresAt:    expiresAt,
	}

	return schema, true, nil
}

// fetchSchemaFromRegistry fetches schema from the registry
func (v *Validator) fetchSchemaFromRegistry(contractName string, version uint64) ([]byte, bool, error) {
	if v.schemaRegistryURL == "" {
		return nil, false, fmt.Errorf("schema registry URL is not configured")
	}

	url := fmt.Sprintf("%s/subjects/%s/versions/%d", v.schemaRegistryURL, contractName, version)

	resp, err := v.httpClient.Get(url)
	if err != nil {
		return nil, false, fmt.Errorf("failed to fetch schema from registry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil // Schema doesn't exist
	}

	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("schema registry returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read schema response: %w", err)
	}

	var versionResp SchemaRegistryVersion
	if err := json.Unmarshal(body, &versionResp); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal schema response: %w", err)
	}

	return []byte(versionResp.Schema), true, nil
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

	cacheKey := fmt.Sprintf("%s-v%d", contractName, version)

	v.cacheMutex.Lock()
	defer v.cacheMutex.Unlock()

	// Create and compile the schema
	schemaObj := NewSchema(contractName)
	if err := schemaObj.AddVersion(version, schema); err != nil {
		return fmt.Errorf("failed to add schema version %d for contract '%s': %w", version, contractName, err)
	}

	// Cache the schema - forever since schemas are immutable
	expiresAt := time.Time{} // Zero time means never expires
	if cacheHitTTL > 0 {
		expiresAt = time.Now().Add(cacheHitTTL)
	}
	v.schemaCache[cacheKey] = &CacheEntry{
		Schema:       schemaObj,
		SchemaExists: true,
		CachedAt:     time.Now(),
		ExpiresAt:    expiresAt,
	}

	return nil
}

// HasSchema checks if a schema exists for the given contract name and version.
func (v *Validator) HasSchema(contractName string, version uint64) bool {
	cacheKey := fmt.Sprintf("%s-v%d", contractName, version)

	v.cacheMutex.RLock()
	defer v.cacheMutex.RUnlock()

	entry, exists := v.schemaCache[cacheKey]
	if !exists || entry.IsExpired() {
		return false
	}

	return entry.SchemaExists && entry.Schema != nil && entry.Schema.HasVersion(version)
}

// Close cleans up resources (no background processes to stop anymore)
func (v *Validator) Close() {
	// No background processes to stop, just clear the cache
	v.cacheMutex.Lock()
	defer v.cacheMutex.Unlock()

	v.schemaCache = make(map[string]*CacheEntry)
}
