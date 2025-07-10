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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

const (
	cacheHitTTL  = 0                // Cache successful schema fetches forever (schemas are immutable)
	cacheMissTTL = 10 * time.Minute // Cache misses for 10 minutes to retry sooner
	httpTimeout  = 30 * time.Second
	maxCacheSize = 1000 // Maximum number of cache entries to prevent memory leaks
)

// schemaVersionRegex matches data contract names with version suffixes.
// Expected format: "contractname_v123" where 123 is the version number.
var schemaVersionRegex = regexp.MustCompile(`^(.+)_v(\d+)$`)

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

// ContractCacheEntry represents a cached entry for a contract+version combination
// It can hold multiple schemas (with different suffixes) for the same contract+version
type ContractCacheEntry struct {
	// Schemas maps schema subject names to compiled schemas
	Schemas map[string]*Schema
	// SchemaExists indicates whether any schemas exist for this contract+version
	SchemaExists bool
	// CachedAt is when this entry was cached
	CachedAt time.Time
	// ExpiresAt is when this entry expires
	ExpiresAt time.Time
}

// IsExpired checks if the cache entry has expired
func (ce *ContractCacheEntry) IsExpired() bool {
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
	// Cache maps "contractName-v123" to ContractCacheEntry
	contractCache     map[string]*ContractCacheEntry
	cacheMutex        sync.RWMutex
	schemaRegistryURL string
	httpClient        *http.Client
	logger            *service.Logger
}

// NewValidator creates a new Validator instance with empty cache.
func NewValidator() *Validator {
	// Create HTTP client with proper connection limits to prevent leaks
	transport := &http.Transport{
		MaxIdleConns:        10,               // Limit total idle connections
		MaxConnsPerHost:     5,                // Limit connections per host
		MaxIdleConnsPerHost: 2,                // Limit idle connections per host
		IdleConnTimeout:     90 * time.Second, // Close idle connections after 90s
		DisableKeepAlives:   false,            // Allow connection reuse
	}

	return &Validator{
		contractCache: make(map[string]*ContractCacheEntry),
		httpClient: &http.Client{
			Timeout:   httpTimeout,
			Transport: transport,
		},
		logger: nil, // No logger by default
	}
}

// NewValidatorWithRegistry creates a new Validator instance with the specified schema registry URL.
func NewValidatorWithRegistry(schemaRegistryURL string) *Validator {
	// Create HTTP client with proper connection limits to prevent leaks
	transport := &http.Transport{
		MaxIdleConns:        10,               // Limit total idle connections
		MaxConnsPerHost:     5,                // Limit connections per host
		MaxIdleConnsPerHost: 2,                // Limit idle connections per host
		IdleConnTimeout:     90 * time.Second, // Close idle connections after 90s
		DisableKeepAlives:   false,            // Allow connection reuse
	}

	return &Validator{
		contractCache:     make(map[string]*ContractCacheEntry),
		schemaRegistryURL: schemaRegistryURL,
		httpClient: &http.Client{
			Timeout:   httpTimeout,
			Transport: transport,
		},
		logger: nil, // No logger by default
	}
}

// NewValidatorWithRegistryAndLogger creates a new Validator instance with the specified schema registry URL and logger.
func NewValidatorWithRegistryAndLogger(schemaRegistryURL string, logger *service.Logger) *Validator {
	// Create HTTP client with proper connection limits to prevent leaks
	transport := &http.Transport{
		MaxIdleConns:        10,               // Limit total idle connections
		MaxConnsPerHost:     5,                // Limit connections per host
		MaxIdleConnsPerHost: 2,                // Limit idle connections per host
		IdleConnTimeout:     90 * time.Second, // Close idle connections after 90s
		DisableKeepAlives:   false,            // Allow connection reuse
	}

	return &Validator{
		contractCache:     make(map[string]*ContractCacheEntry),
		schemaRegistryURL: schemaRegistryURL,
		httpClient: &http.Client{
			Timeout:   httpTimeout,
			Transport: transport,
		},
		logger: logger,
	}
}

// SetLogger sets the logger for the validator (for backward compatibility)
func (v *Validator) SetLogger(logger *service.Logger) {
	v.logger = logger
}

// debugf logs a debug message if logger is available
func (v *Validator) debugf(format string, args ...interface{}) {
	if v.logger != nil {
		v.logger.Infof(format, args...)
	}
}

// Validate validates the given UNS topic and payload against the registered schemas.
// It extracts the contract and version from the topic, finds all appropriate schemas,
// and validates the payload structure. Returns a ValidationResult with contract information.
func (v *Validator) Validate(unsTopic *topic.UnsTopic, payload []byte) *ValidationResult {
	if unsTopic == nil {
		v.debugf("Validator.Validate: UNS topic is nil")
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: false,
			Error:               fmt.Errorf("UNS topic cannot be nil"),
		}
	}

	topicInfo := unsTopic.Info()
	if topicInfo == nil {
		v.debugf("Validator.Validate: topic info is nil")
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: false,
			Error:               fmt.Errorf("topic info is nil"),
		}
	}

	contract := topicInfo.DataContract
	if contract == "" {
		v.debugf("Validator.Validate: data contract is empty")
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: false,
			Error:               fmt.Errorf("data contract is empty"),
		}
	}

	v.debugf("Validator.Validate: Starting validation for contract='%s', payload length=%d", contract, len(payload))

	contractName, version, err := v.ExtractSchemaVersionFromDataContract(contract)
	if err != nil {
		v.debugf("Validator.Validate: Unversioned contract '%s', bypassing validation", contract)
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

	v.debugf("Validator.Validate: Extracted contractName='%s', version=%d", contractName, version)

	// Get schemas from cache or fetch synchronously
	schemas, schemaExists, err := v.getSchemasWithCache(contractName, version)
	if err != nil {
		v.debugf("Validator.Validate: Failed to get schemas for %s v%d: %v", contractName, version, err)
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: true,
			ContractName:        contractName,
			ContractVersion:     version,
			BypassReason:        fmt.Sprintf("failed to fetch schemas: %v", err),
			Error:               nil,
		}
	}

	if !schemaExists {
		v.debugf("Validator.Validate: No schemas found for contract '%s' version %d", contractName, version)
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: true,
			ContractName:        contractName,
			ContractVersion:     version,
			BypassReason:        fmt.Sprintf("no schemas found for contract '%s' version %d", contractName, version),
			Error:               nil,
		}
	}

	if len(schemas) == 0 {
		v.debugf("Validator.Validate: No schemas available for contract '%s' version %d", contractName, version)
		return &ValidationResult{
			SchemaCheckPassed:   false,
			SchemaCheckBypassed: true,
			ContractName:        contractName,
			ContractVersion:     version,
			BypassReason:        fmt.Sprintf("no schemas available for contract '%s' version %d", contractName, version),
			Error:               nil,
		}
	}

	v.debugf("Validator.Validate: Found %d schemas for contract '%s' version %d", len(schemas), contractName, version)

	// Build the full path for validation
	var fullPath strings.Builder
	if topicInfo.VirtualPath != nil {
		fullPath.WriteString(*topicInfo.VirtualPath)
		fullPath.WriteString(".")
	}
	fullPath.WriteString(topicInfo.Name)

	virtualPath := fullPath.String()
	v.debugf("Validator.Validate: Virtual path='%s'", virtualPath)

	// Wrap the payload with fields and virtual_path for validation
	wrappedPayload := []byte(fmt.Sprintf(`{"fields": %s, "virtual_path": "%s"}`,
		string(payload), virtualPath))

	v.debugf("Validator.Validate: Wrapped payload: %s", string(wrappedPayload))

	// Try to validate against all schemas until one passes
	var lastError error
	schemaCount := 0
	for subjectName, schema := range schemas {
		schemaCount++
		v.debugf("Validator.Validate: Trying schema %d/%d: subject='%s'", schemaCount, len(schemas), subjectName)

		if schema == nil {
			v.debugf("Validator.Validate: Schema is nil for subject '%s'", subjectName)
			continue
		}

		jsonSchema := schema.GetVersion(version)
		if jsonSchema == nil {
			v.debugf("Validator.Validate: No schema version %d found for subject '%s'", version, subjectName)
			continue
		}

		v.debugf("Validator.Validate: Validating against schema for subject '%s'", subjectName)

		validationResult := jsonSchema.ValidateJSON(wrappedPayload)
		if validationResult == nil {
			lastError = fmt.Errorf("schema validation result is nil for subject '%s'", subjectName)
			v.debugf("Validator.Validate: Validation result is nil for subject '%s'", subjectName)
			continue
		}

		if validationResult.Valid {
			v.debugf("Validator.Validate: SUCCESS! Schema validation passed for subject '%s'", subjectName)
			// Found a matching schema
			return &ValidationResult{
				SchemaCheckPassed:   true,
				SchemaCheckBypassed: false,
				ContractName:        contractName,
				ContractVersion:     version,
				Error:               nil,
			}
		}

		// Collect validation errors for debugging
		var validationErrors []string
		if validationResult.Errors != nil {
			for _, validationErr := range validationResult.Errors {
				if validationErr != nil {
					validationErrors = append(validationErrors, validationErr.Error())
				}
			}
		}
		lastError = fmt.Errorf("schema validation failed for subject '%s': %s", subjectName, strings.Join(validationErrors, "; "))
		v.debugf("Validator.Validate: Schema validation failed for subject '%s': %s", subjectName, strings.Join(validationErrors, "; "))
	}

	// None of the schemas matched
	v.debugf("Validator.Validate: FAILED! No schemas matched for contract '%s' version %d. Last error: %v", contractName, version, lastError)
	return &ValidationResult{
		SchemaCheckPassed:   false,
		SchemaCheckBypassed: false,
		ContractName:        contractName,
		ContractVersion:     version,
		Error:               fmt.Errorf("schema validation failed for contract '%s' version %d against all available schemas. Last error: %v", contractName, version, lastError),
	}
}

// getSchemasWithCache retrieves schemas from cache or fetches them synchronously
func (v *Validator) getSchemasWithCache(contractName string, version uint64) (map[string]*Schema, bool, error) {
	cacheKey := fmt.Sprintf("%s-v%d", contractName, version)

	// Check cache first
	v.cacheMutex.RLock()
	entry, exists := v.contractCache[cacheKey]
	v.cacheMutex.RUnlock()

	if exists && entry != nil && !entry.IsExpired() {
		// Cache hit and not expired
		v.debugf("getSchemasWithCache: CACHE HIT for key='%s', schemas=%d", cacheKey, len(entry.Schemas))
		return entry.Schemas, entry.SchemaExists, nil
	}

	v.debugf("getSchemasWithCache: CACHE MISS for key='%s', fetching from registry", cacheKey)

	// Cache miss or expired, fetch synchronously
	return v.fetchSchemasSync(contractName, version)
}

// fetchSchemasSync fetches all schemas matching the contract+version pattern synchronously and updates cache
//
// This function always fetches the LATEST version of each schema subject rather than trying to map
// contract versions to registry versions. This simplifies the architecture and avoids version conflicts
// since schema registry versions are independent of UMH contract versions.
func (v *Validator) fetchSchemasSync(contractName string, version uint64) (map[string]*Schema, bool, error) {
	cacheKey := fmt.Sprintf("%s-v%d", contractName, version)

	v.debugf("fetchSchemasSync: Fetching schemas for contractName='%s', version=%d", contractName, version)

	// Double-check locking pattern
	v.cacheMutex.Lock()
	defer v.cacheMutex.Unlock()

	// Check if another goroutine already fetched it
	if entry, exists := v.contractCache[cacheKey]; exists && entry != nil && !entry.IsExpired() {
		v.debugf("fetchSchemasSync: Another goroutine already fetched key='%s'", cacheKey)
		return entry.Schemas, entry.SchemaExists, nil
	}

	// Fetch all subjects from registry
	subjects, err := v.fetchAllSubjects()
	if err != nil {
		v.debugf("fetchSchemasSync: Failed to fetch subjects from registry: %v", err)
		// Cache the error result
		v.contractCache[cacheKey] = &ContractCacheEntry{
			Schemas:      make(map[string]*Schema),
			SchemaExists: false,
			CachedAt:     time.Now(),
			ExpiresAt:    time.Now().Add(cacheMissTTL),
		}
		return nil, false, err
	}

	v.debugf("fetchSchemasSync: Found %d subjects in registry", len(subjects))

	// Filter subjects that match our pattern: contractName_v{version}_*
	schemaPrefix := fmt.Sprintf("%s_v%d_", contractName, version)
	var matchingSubjects []string
	for _, subject := range subjects {
		if strings.HasPrefix(subject, schemaPrefix) {
			matchingSubjects = append(matchingSubjects, subject)
		}
	}

	v.debugf("fetchSchemasSync: Looking for subjects with prefix='%s', found %d matches: %v", schemaPrefix, len(matchingSubjects), matchingSubjects)

	if len(matchingSubjects) == 0 {
		v.debugf("fetchSchemasSync: No matching subjects found for prefix='%s'", schemaPrefix)
		// No matching schemas found
		v.contractCache[cacheKey] = &ContractCacheEntry{
			Schemas:      make(map[string]*Schema),
			SchemaExists: false,
			CachedAt:     time.Now(),
			ExpiresAt:    time.Now().Add(cacheMissTTL),
		}
		// Evict oldest entries if cache is too large (prevent memory leaks)
		v.evictOldestEntries()
		return nil, false, nil
	}

	// Fetch and compile all matching schemas
	schemas := make(map[string]*Schema)
	for _, subject := range matchingSubjects {
		v.debugf("fetchSchemasSync: Fetching latest schema for subject='%s'", subject)

		// Always fetch the LATEST version of each schema subject, NOT a specific version number.
		//
		// Why? Schema registry versions are independent of UMH contract versions:
		// - Contract "_sensor_data-v2" means "version 2 of the sensor data contract"
		// - But subject "_sensor_data_v2_timeseries-number" might be registered as registry version 1
		//   (because it's the first version of that specific subject)
		// - By fetching "latest", we avoid complex version mapping and always get the current schema
		// - This also automatically picks up schema updates without code changes
		schemaBytes, schemaExists, err := v.fetchLatestSchemaFromRegistry(subject)
		if err != nil {
			v.debugf("fetchSchemasSync: Failed to fetch schema for subject='%s': %v", subject, err)
			// Log error but continue with other schemas
			continue
		}

		if !schemaExists {
			v.debugf("fetchSchemasSync: Schema does not exist for subject='%s'", subject)
			continue
		}

		v.debugf("fetchSchemasSync: Successfully fetched schema for subject='%s', size=%d bytes", subject, len(schemaBytes))

		// Compile the schema
		schema := NewSchema(subject)
		if err := schema.AddVersion(version, schemaBytes); err != nil {
			v.debugf("fetchSchemasSync: Failed to compile schema for subject='%s': %v", subject, err)
			// Log error but continue with other schemas
			continue
		}

		schemas[subject] = schema
		v.debugf("fetchSchemasSync: Successfully compiled schema for subject='%s'", subject)
	}

	v.debugf("fetchSchemasSync: Successfully compiled %d schemas for contractName='%s' version %d", len(schemas), contractName, version)

	// Cache the results
	expiresAt := time.Time{} // Zero time means never expires
	if cacheHitTTL > 0 {
		expiresAt = time.Now().Add(cacheHitTTL)
	}

	v.contractCache[cacheKey] = &ContractCacheEntry{
		Schemas:      schemas,
		SchemaExists: len(schemas) > 0,
		CachedAt:     time.Now(),
		ExpiresAt:    expiresAt,
	}

	// Evict oldest entries if cache is too large (prevent memory leaks)
	v.evictOldestEntries()

	return schemas, len(schemas) > 0, nil
}

// fetchAllSubjects fetches all subjects from the schema registry
func (v *Validator) fetchAllSubjects() ([]string, error) {
	if v.schemaRegistryURL == "" {
		return nil, fmt.Errorf("schema registry URL is not configured")
	}

	url := fmt.Sprintf("%s/subjects", v.schemaRegistryURL)
	v.debugf("fetchAllSubjects: Fetching subjects from URL='%s'", url)

	resp, err := v.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch subjects from registry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		v.debugf("fetchAllSubjects: Registry returned status=%d", resp.StatusCode)
		return nil, fmt.Errorf("schema registry returned status %d for subjects", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read subjects response: %w", err)
	}

	var subjects []string
	if err := json.Unmarshal(body, &subjects); err != nil {
		return nil, fmt.Errorf("failed to unmarshal subjects response: %w", err)
	}

	v.debugf("fetchAllSubjects: Retrieved %d subjects from registry", len(subjects))
	return subjects, nil
}

// fetchLatestSchemaFromRegistry fetches the latest version of a schema subject
//
// This method is preferred over fetchSchemaFromRegistry(subject, version) because:
// 1. Schema registry versions are independent of UMH contract versions
// 2. Always getting "latest" avoids version mapping complexity
// 3. Automatically picks up schema updates without code changes
// 4. Simpler and more robust than trying to guess the right registry version
func (v *Validator) fetchLatestSchemaFromRegistry(subject string) ([]byte, bool, error) {
	if v.schemaRegistryURL == "" {
		return nil, false, fmt.Errorf("schema registry URL is not configured")
	}

	url := fmt.Sprintf("%s/subjects/%s/versions/latest", v.schemaRegistryURL, subject)
	v.debugf("fetchLatestSchemaFromRegistry: Fetching schema from URL='%s'", url)

	resp, err := v.httpClient.Get(url)
	if err != nil {
		return nil, false, fmt.Errorf("failed to fetch latest schema from registry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		v.debugf("fetchLatestSchemaFromRegistry: Schema not found (404) for subject='%s'", subject)
		return nil, false, nil // Schema doesn't exist
	}

	if resp.StatusCode != http.StatusOK {
		v.debugf("fetchLatestSchemaFromRegistry: Registry returned status=%d for subject='%s'", resp.StatusCode, subject)
		return nil, false, fmt.Errorf("schema registry returned status %d for latest version", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read latest schema response: %w", err)
	}

	var versionResp SchemaRegistryVersion
	if err := json.Unmarshal(body, &versionResp); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal latest schema response: %w", err)
	}

	v.debugf("fetchLatestSchemaFromRegistry: Successfully fetched schema for subject='%s', version=%d, id=%d", subject, versionResp.Version, versionResp.ID)
	return []byte(versionResp.Schema), true, nil
}

// ExtractSchemaVersionFromDataContract parses a data contract string to extract
// the base contract name and version number.
// Expected format: "contractname_v123" -> ("contractname", 123, nil)
func (v *Validator) ExtractSchemaVersionFromDataContract(contract string) (contractName string, version uint64, err error) {
	if contract == "" {
		return "", 0, fmt.Errorf("contract string is empty")
	}

	matches := schemaVersionRegex.FindStringSubmatch(contract)
	if len(matches) != 3 {
		return "", 0, fmt.Errorf("invalid data contract format '%s', expected format: 'name_v123'", contract)
	}

	contractName = matches[1]
	version, err = strconv.ParseUint(matches[2], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid version number '%s' in contract '%s': %w", matches[2], contract, err)
	}

	return contractName, version, nil
}

// LoadSchemas loads and compiles multiple schemas for the specified contract name and version.
// The schemas parameter is a map of subject names to schema content.
func (v *Validator) LoadSchemas(contractName string, version uint64, schemas map[string][]byte) error {
	if contractName == "" {
		return fmt.Errorf("contract name cannot be empty")
	}

	if !strings.HasPrefix(contractName, "_") {
		return fmt.Errorf("contract name must start with an underscore, got: '%s'", contractName)
	}

	if len(schemas) == 0 {
		return fmt.Errorf("schemas cannot be empty for contract '%s' version %d", contractName, version)
	}

	cacheKey := fmt.Sprintf("%s-v%d", contractName, version)

	v.cacheMutex.Lock()
	defer v.cacheMutex.Unlock()

	// Create and compile all schemas
	compiledSchemas := make(map[string]*Schema)
	for subjectName, schemaBytes := range schemas {
		if len(schemaBytes) == 0 {
			return fmt.Errorf("schema cannot be empty for subject '%s'", subjectName)
		}

		schemaObj := NewSchema(subjectName)
		if err := schemaObj.AddVersion(version, schemaBytes); err != nil {
			return fmt.Errorf("failed to add schema version %d for subject '%s': %w", version, subjectName, err)
		}

		compiledSchemas[subjectName] = schemaObj
	}

	// Cache the schemas - forever since schemas are immutable
	expiresAt := time.Time{} // Zero time means never expires
	if cacheHitTTL > 0 {
		expiresAt = time.Now().Add(cacheHitTTL)
	}

	v.contractCache[cacheKey] = &ContractCacheEntry{
		Schemas:      compiledSchemas,
		SchemaExists: true,
		CachedAt:     time.Now(),
		ExpiresAt:    expiresAt,
	}

	// Evict oldest entries if cache is too large (prevent memory leaks)
	v.evictOldestEntries()

	return nil
}

// HasSchema checks if schemas exist for the given contract name and version.
func (v *Validator) HasSchema(contractName string, version uint64) bool {
	cacheKey := fmt.Sprintf("%s-v%d", contractName, version)

	v.cacheMutex.RLock()
	defer v.cacheMutex.RUnlock()

	entry, exists := v.contractCache[cacheKey]
	if !exists || entry == nil || entry.IsExpired() {
		return false
	}

	return entry.SchemaExists && len(entry.Schemas) > 0
}

// Close cleans up resources
func (v *Validator) Close() {
	v.cacheMutex.Lock()
	defer v.cacheMutex.Unlock()

	// Clear the cache
	v.contractCache = make(map[string]*ContractCacheEntry)

	// Close HTTP client transport to prevent connection leaks
	if v.httpClient != nil && v.httpClient.Transport != nil {
		if transport, ok := v.httpClient.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
		v.httpClient = nil
	}
}

// evictOldestEntries removes the oldest cache entries if the cache exceeds maxCacheSize
// This prevents unbounded memory growth in long-running space missions
func (v *Validator) evictOldestEntries() {
	if len(v.contractCache) <= maxCacheSize {
		return
	}

	// Find the oldest entries to evict
	type cacheItem struct {
		key       string
		timestamp time.Time
	}

	var items []cacheItem
	for key, entry := range v.contractCache {
		if entry != nil {
			items = append(items, cacheItem{
				key:       key,
				timestamp: entry.CachedAt,
			})
		}
	}

	// Sort by timestamp (oldest first)
	for i := 0; i < len(items)-1; i++ {
		for j := i + 1; j < len(items); j++ {
			if items[i].timestamp.After(items[j].timestamp) {
				items[i], items[j] = items[j], items[i]
			}
		}
	}

	// Remove oldest entries until we're under the limit
	entriesToRemove := len(v.contractCache) - maxCacheSize + 1
	for i := 0; i < entriesToRemove && i < len(items); i++ {
		delete(v.contractCache, items[i].key)
	}
}
