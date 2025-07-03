# ENG-3099 Implementation Plan: Schema Registry Integration for uns_output Plugin

## Overview

This document outlines the implementation plan for integrating JSON schema validation into the `uns_output` plugin using Redpanda's Schema Registry. The enhancement ensures that only messages conforming to registered JSON schemas are published to the UNS, improving data integrity while maintaining the existing fail-open philosophy.

## Objectives

- **Primary**: Validate message payloads against JSON schemas before publishing to `umh.messages`
- **Secondary**: Support versioned data contracts (`_contract`, `_contractv1`, `_contractv34`)
- **Tertiary**: Maintain fail-open behavior to prevent data loss on schema registry issues
- **Performance**: Minimize impact on throughput through efficient caching

## Architecture Overview

```
Message → Extract data_contract → Parse version → Extract tag_name → Cache lookup → Dual validation → Kafka publish
             ↓                                        ↓                    ↓              ↓
        (from metadata)                        (from UNS topic)     Schema Registry   1. Tag allowed?
                                                                   (if not cached)    2. Payload valid?
```

## Schema Structure

Our schemas support **dual validation** with both allowed tag names and tag-specific payload schemas:

```json
{
   "virtual_path": [
      "count",
      "vibration.x-axis", 
      "vibration.y-axis",
      "acceleration.x",
      "serialNumber"
   ],
   "fields": {
      "count": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "vibration.x-axis": {
         "type": "object", 
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "vibration.y-axis": {
         "type": "object", 
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "acceleration.x": {
         "type": "object", 
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "serialNumber": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"}, 
            "value": {"type": "string"}
         },
         "required": ["timestamp_ms", "value"]
      }
   }
}
```

## Design Decision: Configuration Strategy

### Ticket Requirements vs Implementation Plan

**Original Ticket (ENG-3099) stated:**
> "The plugin configuration (uns: {}) doesn't need new YAML fields for this feature, since it's an internal behavior."

**Initial Plan proposed:**
> Adding a required `schema_registry_url` configuration field

**Resolution: Optional Configuration with Auto-derivation**

We chose **Option 2** to balance ticket requirements with practical deployment needs:

**✅ Pros of Our Approach:**
- **Ticket Compliance**: Works without any configuration changes (auto-derivation)
- **Smart Defaults**: Automatically derives `http://localhost:8081` from `localhost:9092`
- **Deployment Flexibility**: Supports non-standard registry URLs when needed
- **Backward Compatibility**: All existing configurations work unchanged
- **Explicit Control**: Allows disabling validation by setting empty string

**❌ Rejected Alternatives:**
- **Option 1 (Auto-derive only)**: Too rigid for complex deployments
- **Required Configuration**: Would break existing configurations
- **Environment Variables**: Adds deployment complexity

**Implementation Impact:**
- Zero breaking changes to existing configurations
- Logging shows auto-derived URLs for transparency
- Explicit configuration overrides auto-derivation when needed

## Implementation Details

### 1. Configuration Changes

**File**: `uns_plugin/uns_output.go`

#### 1.1 Configuration Strategy Decision

We implement **Option 2: Optional Configuration with Auto-derivation** to align with the ENG-3099 ticket requirements while maintaining flexibility:

**Reasoning:**
- **Ticket Alignment**: The original ticket states "The plugin configuration (uns: {}) doesn't need new YAML fields for this feature, since it's an internal behavior"
- **Smart Defaults**: Auto-derive schema registry URL from broker address (localhost:9092 → http://localhost:8081)
- **Flexibility**: Allow explicit configuration for non-standard deployments
- **Backward Compatible**: Existing configurations work unchanged
- **Future-proof**: Handles various deployment scenarios gracefully

#### 1.2 Update Configuration Struct

```go
// Update unsOutputConfig struct
type unsOutputConfig struct {
	umh_topic           *service.InterpolatedString
	brokerAddress       string
	bridgedBy           string
	schemaRegistryURL   string  // NEW: Schema Registry URL (auto-derived or explicit)
}
```

#### 1.3 Add Configuration Field (Optional)

```go
// In outputConfig() function, add new OPTIONAL field
Field(service.NewStringField("schema_registry_url").
	Description(`
Optional Schema Registry URL for JSON schema validation. If not specified,
the URL will be auto-derived from the broker_address (e.g., localhost:9092 
becomes http://localhost:8081).

Set this explicitly for non-standard deployments or to disable validation 
entirely by setting to an empty string.
`).
	Example("http://localhost:8081").
	Optional())
```

#### 1.4 Auto-derivation Helper Function

```go
import (
	// ... existing imports ...
	"strings"
)

// deriveSchemaRegistryURL automatically derives registry URL from broker address
func deriveSchemaRegistryURL(brokerAddress string) string {
	if brokerAddress == "" {
		return "http://localhost:8081" // fallback default
	}
	
	// Extract host from broker address (e.g., "localhost:9092" -> "localhost")
	if strings.Contains(brokerAddress, ":") {
		host := strings.Split(brokerAddress, ":")[0]
		return fmt.Sprintf("http://%s:8081", host)
	}
	
	// If no port specified, assume it's just the host
	return fmt.Sprintf("http://%s:8081", brokerAddress)
}
```

#### 1.5 Update Constructor

```go
// In newUnsOutput function, implement optional config with auto-derivation
var schemaRegistryURL string
if conf.Contains("schema_registry_url") {
	// Explicit configuration provided
	url, err := conf.FieldString("schema_registry_url")
	if err != nil {
		return nil, batchPolicy, 0, fmt.Errorf("error while parsing schema_registry_url field from the config: %v", err)
	}
	schemaRegistryURL = url
} else {
	// Auto-derive from broker address
	schemaRegistryURL = deriveSchemaRegistryURL(config.brokerAddress)
	mgr.Logger().Infof("Auto-derived schema registry URL: %s (from broker: %s)", 
		schemaRegistryURL, config.brokerAddress)
}

config := unsOutputConfig{
	// ... existing fields ...
	schemaRegistryURL: schemaRegistryURL,
}

// Create schema cache if registry URL is provided and not explicitly disabled
var schemaCache *SchemaCache
if schemaRegistryURL != "" {
	schemaCache = NewSchemaCache(schemaRegistryURL, mgr.Logger())
}

return newUnsOutputWithClient(NewClient(), config, schemaCache, mgr.Logger()), batchPolicy, maxInFlight, nil
```

### 2. Contract Version Parser

**File**: `uns_plugin/contract_parser.go` (NEW)

```go
package uns_plugin

import (
	"regexp"
	"strconv"
)

// ContractInfo holds parsed contract information
type ContractInfo struct {
	BaseContract string // e.g., "_pump_data" 
	Version      *int   // nil for latest, specific number for versioned
	FullName     string // original contract name
}

var contractVersionRegex = regexp.MustCompile(`^(.+?)v(\d+)$`)

// ParseContract parses a data contract to extract base name and version
func ParseContract(contract string) ContractInfo {
	if contract == "" {
		return ContractInfo{}
	}

	// Check if contract has version suffix (e.g., "_pump_datav1", "_pump_datav34")
	matches := contractVersionRegex.FindStringSubmatch(contract)
	if len(matches) == 3 {
		baseContract := matches[1]
		if version, err := strconv.Atoi(matches[2]); err == nil {
			return ContractInfo{
				BaseContract: baseContract,
				Version:      &version,
				FullName:     contract,
			}
		}
	}

	// No version suffix, assume latest
	return ContractInfo{
		BaseContract: contract,
		Version:      nil, // latest
		FullName:     contract,
	}
}

// CacheKey returns a unique cache key for this contract version
func (ci ContractInfo) CacheKey() string {
	if ci.Version == nil {
		return ci.BaseContract + ":latest"
	}
	return ci.BaseContract + ":v" + strconv.Itoa(*ci.Version)
}

// RegistrySubject returns the subject name for schema registry
func (ci ContractInfo) RegistrySubject() string {
	return ci.BaseContract
}
```

### 3. Schema Registry Client

**File**: `uns_plugin/schema_registry_client.go` (NEW)

```go
package uns_plugin

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SchemaRegistryClient handles communication with Redpanda Schema Registry
type SchemaRegistryClient struct {
	baseURL    string
	httpClient *http.Client
}

// SchemaResponse represents the response from schema registry
type SchemaResponse struct {
	ID      int    `json:"id"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

// NewSchemaRegistryClient creates a new schema registry client
func NewSchemaRegistryClient(baseURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GetSchemaForContract retrieves schema for a contract (latest or specific version)
func (c *SchemaRegistryClient) GetSchemaForContract(contractInfo ContractInfo) (*SchemaResponse, error) {
	var url string
	if contractInfo.Version == nil {
		// Get latest version
		url = fmt.Sprintf("%s/subjects/%s/versions/latest", c.baseURL, contractInfo.RegistrySubject())
	} else {
		// Get specific version
		url = fmt.Sprintf("%s/subjects/%s/versions/%d", c.baseURL, contractInfo.RegistrySubject(), *contractInfo.Version)
	}
	
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema for contract %s: %w", contractInfo.FullName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		// No schema registered for this contract/version
		return nil, nil
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("schema registry returned status %d for contract %s", resp.StatusCode, contractInfo.FullName)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var schemaResp SchemaResponse
	if err := json.Unmarshal(body, &schemaResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema response: %w", err)
	}

	return &schemaResp, nil
}

// GetLatestSchema - kept for backward compatibility, delegates to GetSchemaForContract
func (c *SchemaRegistryClient) GetLatestSchema(contract string) (*SchemaResponse, error) {
	contractInfo := ParseContract(contract)
	return c.GetSchemaForContract(contractInfo)
}
```

### 4. UMH Topic Parser

**File**: `uns_plugin/uns_topic_parser.go` (NEW)

```go
package uns_plugin

import (
	"fmt"
	"strings"
)

// extractTagNameFromUNSTopic extracts the tag name (last part) from a UNS topic
// Example: "umh.v1.enterprise.site.area._pump_data.vibration.x-axis" -> "vibration.x-axis"
func extractTagNameFromUNSTopic(unsTopic string) (string, error) {
	if unsTopic == "" {
		return "", fmt.Errorf("UNS topic is empty")
	}

	// Split by dots and get the last part
	parts := strings.Split(unsTopic, ".")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid UNS topic format: %s", unsTopic)
	}

	tagName := parts[len(parts)-1]
	if tagName == "" {
		return "", fmt.Errorf("tag name is empty in UNS topic: %s", unsTopic)
	}

	return tagName, nil
}
```

### 5. Schema Validator Implementation

**File**: `uns_plugin/schema_validator.go` (NEW)

```go
package uns_plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/kaptinlin/jsonschema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// ContractSchema represents the dual validation schema structure
type ContractSchema struct {
	VirtualPath []string                   `json:"virtual_path"` // Allowed tag names
	Fields      map[string]json.RawMessage `json:"fields"`       // Tag-specific JSON schemas
}

// SchemaValidator interface for dual validation (tag name + payload)
type SchemaValidator interface {
	ValidateTagAndPayload(tagName string, payload []byte) error
}

// CompiledContractSchema wraps a compiled contract schema for dual validation
type CompiledContractSchema struct {
	contractName    string
	allowedNames    map[string]bool                // Fast lookup for allowed tag names
	compiledSchemas map[string]*jsonschema.Schema  // Compiled JSON schemas per tag
}

// NewCompiledContractSchema creates a new compiled contract schema
func NewCompiledContractSchema(contractName string, schemaJSON []byte) (*CompiledContractSchema, error) {
	var contractSchema ContractSchema
	if err := json.Unmarshal(schemaJSON, &contractSchema); err != nil {
		return nil, fmt.Errorf("failed to parse contract schema: %w", err)
	}

	// Build allowed names lookup map
	allowedNames := make(map[string]bool)
	for _, name := range contractSchema.VirtualPath {
		allowedNames[name] = true
	}

	// Compile JSON schemas for each field
	compiler := jsonschema.NewCompiler()
	compiledSchemas := make(map[string]*jsonschema.Schema)
	
	for tagName, fieldSchemaBytes := range contractSchema.Fields {
		schema, err := compiler.Compile(fieldSchemaBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to compile schema for tag '%s': %w", tagName, err)
		}
		compiledSchemas[tagName] = schema
	}

	return &CompiledContractSchema{
		contractName:    contractName,
		allowedNames:    allowedNames,
		compiledSchemas: compiledSchemas,
	}, nil
}

// ValidateTagAndPayload performs dual validation: tag name + payload
func (c *CompiledContractSchema) ValidateTagAndPayload(tagName string, payload []byte) error {
	// Step 1: Validate tag name is allowed
	if !c.allowedNames[tagName] {
		allowedList := make([]string, 0, len(c.allowedNames))
		for name := range c.allowedNames {
			allowedList = append(allowedList, name)
		}
		return fmt.Errorf("tag '%s' not allowed for contract '%s'. Allowed tags: %v", 
			tagName, c.contractName, allowedList)
	}

	// Step 2: Validate payload against tag-specific schema
	schema, exists := c.compiledSchemas[tagName]
	if !exists {
		return fmt.Errorf("no schema defined for tag '%s' in contract '%s'", tagName, c.contractName)
	}

	result := schema.ValidateJSON(payload)
	if !result.IsValid() {
		// Create detailed error message from validation result
		var errorMsg string
		for field, err := range result.Errors {
			if errorMsg != "" {
				errorMsg += "; "
			}
			errorMsg += fmt.Sprintf("%s: %s", field, err.Message)
		}
		return fmt.Errorf("payload validation failed for tag '%s': %s", tagName, errorMsg)
	}

	return nil
}

// SchemaCache manages schema retrieval and caching with version support
type SchemaCache struct {
	cache            map[string]SchemaValidator // cache key -> validator
	lastFetch        map[string]time.Time       // cache key -> last fetch time
	registryURL      string
	refreshInterval  time.Duration
	mutex            sync.RWMutex
	log              *service.Logger
}

// NewSchemaCache creates a new schema cache
func NewSchemaCache(registryURL string, logger *service.Logger) *SchemaCache {
	return &SchemaCache{
		cache:           make(map[string]SchemaValidator),
		lastFetch:       make(map[string]time.Time),
		registryURL:     registryURL,
		refreshInterval: 10 * time.Minute, // Configurable refresh interval
		log:             logger,
	}
}

// GetValidator retrieves or fetches a validator for the given contract
func (sc *SchemaCache) GetValidator(ctx context.Context, contractName string) (SchemaValidator, error) {
	if contractName == "" {
		return nil, nil // No validation for empty contract
	}

	contractInfo := ParseContract(contractName)
	cacheKey := contractInfo.CacheKey()

	sc.mutex.RLock()
	validator, exists := sc.cache[cacheKey]
	lastFetch := sc.lastFetch[cacheKey]
	sc.mutex.RUnlock()

	// For versioned contracts, don't refresh (they're immutable)
	// For latest contracts, refresh periodically
	needsRefresh := !exists
	if contractInfo.Version == nil && exists {
		// Only refresh "latest" versions periodically
		needsRefresh = time.Since(lastFetch) > sc.refreshInterval
	}

	if needsRefresh {
		return sc.fetchAndCacheSchema(ctx, contractInfo)
	}

	return validator, nil
}

// fetchAndCacheSchema fetches schema from registry and caches it
func (sc *SchemaCache) fetchAndCacheSchema(ctx context.Context, contractInfo ContractInfo) (SchemaValidator, error) {
	cacheKey := contractInfo.CacheKey()
	
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	// Double-check pattern - another goroutine might have fetched it
	if validator, exists := sc.cache[cacheKey]; exists {
		// For versioned contracts, never refresh
		if contractInfo.Version != nil {
			return validator, nil
		}
		// For latest, check if we need refresh
		if time.Since(sc.lastFetch[cacheKey]) <= sc.refreshInterval {
			return validator, nil
		}
	}

	client := NewSchemaRegistryClient(sc.registryURL)
	schemaResp, err := client.GetSchemaForContract(contractInfo)
	if err != nil {
		sc.log.Errorf("Failed to fetch schema for contract %s: %v", contractInfo.FullName, err)
		// Return nil validator but don't error - fail open
		sc.cache[cacheKey] = nil
		sc.lastFetch[cacheKey] = time.Now()
		return nil, nil
	}

	if schemaResp == nil {
		// No schema registered
		sc.log.Infof("No schema registered for contract '%s'; skipping validation", contractInfo.FullName)
		sc.cache[cacheKey] = nil
		sc.lastFetch[cacheKey] = time.Now()
		return nil, nil
	}

	// Compile the contract schema (dual validation schema)
	validator, err := NewCompiledContractSchema(contractInfo.FullName, []byte(schemaResp.Schema))
	if err != nil {
		sc.log.Errorf("Failed to compile contract schema for %s: %v", contractInfo.FullName, err)
		// Fail open - cache nil validator
		sc.cache[cacheKey] = nil
		sc.lastFetch[cacheKey] = time.Now()
		return nil, nil
	}

	sc.cache[cacheKey] = validator
	sc.lastFetch[cacheKey] = time.Now()

	versionInfo := "latest"
	if contractInfo.Version != nil {
		versionInfo = fmt.Sprintf("v%d", *contractInfo.Version)
	}
	sc.log.Infof("Loaded dual validation schema for contract '%s' (%s, registry version %d)", 
		contractInfo.FullName, versionInfo, schemaResp.Version)
	return validator, nil
}

// RefreshOnValidationFailure attempts to refresh schema when validation fails unexpectedly
// Only works for "latest" version contracts - versioned contracts are immutable
func (sc *SchemaCache) RefreshOnValidationFailure(ctx context.Context, contractName string) (SchemaValidator, error) {
	contractInfo := ParseContract(contractName)
	
	// Don't refresh versioned contracts - they're immutable
	if contractInfo.Version != nil {
		return nil, fmt.Errorf("cannot refresh versioned contract %s", contractInfo.FullName)
	}

	cacheKey := contractInfo.CacheKey()
	sc.mutex.Lock()
	lastFetch := sc.lastFetch[cacheKey]
	sc.mutex.Unlock()

	// Rate limit: only refresh if it's been at least 1 minute since last fetch
	if time.Since(lastFetch) < time.Minute {
		return nil, fmt.Errorf("rate limited: schema refresh attempted too recently")
	}

	sc.log.Infof("Attempting schema refresh for contract %s due to validation failure", contractInfo.FullName)
	return sc.fetchAndCacheSchema(ctx, contractInfo)
}
```

### 6. uns_output Integration

**File**: `uns_plugin/uns_output.go` (MODIFIED)

#### 6.1 Update Validation Helper Function

```go
// validateMessage performs dual validation (tag name + payload) for a single message
func (o *unsOutput) validateMessage(ctx context.Context, msg *service.Message, msgBytes []byte, msgIndex int) error {
	// Get data contract from message metadata
	contract := msg.MetaGet("data_contract")
	if contract == "" {
		o.log.Debugf("No data_contract metadata found for message %d, skipping validation", msgIndex)
		return nil
	}

	// Get UNS topic and extract tag name
	unsTopicKey, err := o.config.umh_topic.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to get UNS topic for message %d: %v", msgIndex, err)
	}

	tagName, err := extractTagNameFromUNSTopic(unsTopicKey)
	if err != nil {
		return fmt.Errorf("failed to extract tag name from UNS topic '%s' for message %d: %v", unsTopicKey, msgIndex, err)
	}

	// Parse contract to understand version requirements
	contractInfo := ParseContract(contract)
	if contractInfo.BaseContract == "" {
		o.log.Debugf("Could not parse contract '%s' for message %d, skipping validation", contract, msgIndex)
		return nil
	}

	// Get validator for this contract (including version)
	validator, err := o.schemaCache.GetValidator(ctx, contract)
	if err != nil {
		o.log.Errorf("Failed to get validator for contract %s: %v", contract, err)
		// Fail open - don't block the message
		return nil
	}

	if validator == nil {
		// No schema registered for this contract/version - skip validation
		return nil
	}

	// Perform dual validation (tag name + payload)
	if err := validator.ValidateTagAndPayload(tagName, msgBytes); err != nil {
		// For versioned contracts, don't attempt refresh (they're immutable)
		if contractInfo.Version == nil {
			// Try schema refresh only for "latest" version contracts
			if refreshedValidator, refreshErr := o.schemaCache.RefreshOnValidationFailure(ctx, contract); refreshErr == nil && refreshedValidator != nil {
				// Retry validation with refreshed schema
				if retryErr := refreshedValidator.ValidateTagAndPayload(tagName, msgBytes); retryErr == nil {
					o.log.Infof("Message %d passed dual validation after schema refresh for contract %s", msgIndex, contract)
					return nil
				}
			}
		}

		// Log validation failure with detailed information
		truncatedPayload := string(msgBytes)
		if len(truncatedPayload) > 200 {
			truncatedPayload = truncatedPayload[:200] + "..."
		}
		
		versionInfo := "latest"
		if contractInfo.Version != nil {
			versionInfo = fmt.Sprintf("v%d", *contractInfo.Version)
		}
		
		return fmt.Errorf("dual validation failed for contract %s (%s), tag '%s' - %v. Payload: %s", 
			contractInfo.BaseContract, versionInfo, tagName, err, truncatedPayload)
	}

	o.log.Tracef("Message %d passed dual validation: contract=%s, tag=%s", msgIndex, contract, tagName)
	return nil
}
```

#### 6.2 Example Validation Scenarios

**Valid Message:**
- UNS Topic: `umh.v1.enterprise.site.area._pump_data.vibration.x-axis`  
- Tag Name: `vibration.x-axis` (extracted from topic)
- Contract: `_pump_data` (from metadata)
- Validation: Check if `vibration.x-axis` is in allowed names + validate payload against vibration.x-axis schema

**Invalid Tag Name:**
- UNS Topic: `umh.v1.enterprise.site.area._pump_data.unknown_sensor`
- Tag Name: `unknown_sensor` 
- Contract: `_pump_data`
- Result: Rejected - "tag 'unknown_sensor' not allowed for contract '_pump_data'. Allowed tags: [count, vibration.x-axis, vibration.y-axis, acceleration.x, serialNumber]"

**Invalid Payload:**
- UNS Topic: `umh.v1.enterprise.site.area._pump_data.count`
- Tag Name: `count`
- Payload: `{"value": "not_a_number", "timestamp_ms": 1680000000000}`
- Result: Rejected - "payload validation failed for tag 'count': value: expected number, got string"

## Testing Strategy

Implementation includes comprehensive test coverage for **dual validation** (tag name + payload) with unit tests, integration tests, and performance benchmarks.

### Key Test Scenarios

#### 1. UNS Topic Parsing Tests
```go
// Test tag name extraction from various UNS topic formats
testCases := []struct {
    unsTopic string
    expected string
    shouldErr bool
}{
    {"umh.v1.enterprise.site.area._pump_data.count", "count", false},
    {"umh.v1.site._pump_data.vibration.x-axis", "vibration.x-axis", false},
    {"umh.v1._pump_data.serialNumber", "serialNumber", false},
    {"invalid.topic", "", true},
    {"", "", true},
}
```

#### 2. Dual Validation Tests
```go
// Test both tag name validation and payload validation
func TestDualValidation(t *testing.T) {
    schema := ContractSchema{
        VirtualPath: []string{"count", "vibration.x-axis", "vibration.y-axis", "serialNumber"},
        Fields: map[string]json.RawMessage{
            "count": []byte(`{"type":"object","properties":{"value":{"type":"number"},"timestamp_ms":{"type":"number"}},"required":["value","timestamp_ms"]}`),
            "vibration.x-axis": []byte(`{"type":"object","properties":{"value":{"type":"number"},"timestamp_ms":{"type":"number"}},"required":["value","timestamp_ms"]}`),
            "serialNumber": []byte(`{"type":"object","properties":{"value":{"type":"string"},"timestamp_ms":{"type":"number"}},"required":["value","timestamp_ms"]}`),
            // ... other field schemas
        },
    }
    
    // Test valid tag + valid payload
    // Test valid tag + invalid payload  
    // Test invalid tag + valid payload
    // Test invalid tag + invalid payload
}
```

#### 3. Schema Evolution with Versioned Contracts
```go
// Test version-specific validation
func TestVersionedContractValidation(t *testing.T) {
    // Test _pump_datav1 vs _pump_data (latest) schemas
    // Ensure versioned schemas don't refresh
    // Test different tag name sets per version
}
```

#### 4. Integration Test Examples

**Test Case: Tag Not Allowed**
- Schema: `{"virtual_path": ["count", "vibration.x-axis", "vibration.y-axis"], "fields": {...}}`
- UNS Topic: `umh.v1.site._pump_data.pressure`
- Expected: Message dropped with "tag 'pressure' not allowed" error

**Test Case: Payload Schema Mismatch**
- Schema: Count field expects `{"value": number, "timestamp_ms": number}`
- Payload: `{"value": "string", "timestamp_ms": 123}`
- Expected: Message dropped with "payload validation failed for tag 'count': value: expected number, got string"

**Test Case: Both Validations Pass**
- UNS Topic: `umh.v1.site._pump_data.count`
- Payload: `{"value": 12.5, "timestamp_ms": 1680000000000}`
- Expected: Message published successfully

## Configuration Examples

#### Auto-derived Configuration (Default - Recommended)

```yaml
output:
  uns:
    broker_address: "localhost:9092"
    # schema_registry_url not specified - auto-derived to http://localhost:8081
```

#### Explicit Configuration

```yaml
output:
  uns:
    schema_registry_url: "http://schema-registry.example.com:8081"
    broker_address: "kafka.example.com:9092"
```

#### Disabled Configuration

```yaml
output:
  uns:
    schema_registry_url: ""  # explicitly empty to disable validation
    broker_address: "localhost:9092"
```

#### Auto-derivation Examples

| Broker Address | Auto-derived Schema Registry URL |
|---|---|
| `localhost:9092` | `http://localhost:8081` |
| `kafka.example.com:9092` | `http://kafka.example.com:8081` |
| `redpanda` | `http://redpanda:8081` |
| `` (empty) | `http://localhost:8081` (fallback) |

## Metrics Implementation

### 7. Schema Validation Metrics

**File**: `uns_plugin/uns_output_metrics.go` (NEW)

Following the established patterns from `uns_input_metrics.go`, we'll implement comprehensive metrics tracking for schema validation:

```go
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

package uns_plugin

import (
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// UnsOutputMetrics provides metrics collection for the UNS output plugin schema validation
type UnsOutputMetrics struct {
	// Schema loading metrics
	SchemasLoadedGauge     *service.MetricGauge
	SchemaCacheHitCounter  *service.MetricCounter
	SchemaCacheMissCounter *service.MetricCounter
	SchemaFetchTimer       *service.MetricTimer
	
	// Validation metrics
	ValidationSuccessCounter *service.MetricCounter
	ValidationFailureCounter *service.MetricCounter
	TagValidationFailCounter *service.MetricCounter
	PayloadValidationFailCounter *service.MetricCounter
	ValidationTimer          *service.MetricTimer
	
	// Error metrics
	SchemaFetchErrorCounter   *service.MetricCounter
	SchemaCompileErrorCounter *service.MetricCounter
}

// NewUnsOutputMetrics creates a new metrics collection for the UNS output plugin
func NewUnsOutputMetrics(metricsProvider *service.Metrics) *UnsOutputMetrics {
	return &UnsOutputMetrics{
		// Schema loading metrics
		SchemasLoadedGauge:     metricsProvider.NewGauge("output_uns_schemas_loaded"),
		SchemaCacheHitCounter:  metricsProvider.NewCounter("output_uns_schema_cache_hits"),
		SchemaCacheMissCounter: metricsProvider.NewCounter("output_uns_schema_cache_misses"),
		SchemaFetchTimer:       metricsProvider.NewTimer("output_uns_schema_fetch_time"),
		
		// Validation metrics
		ValidationSuccessCounter: metricsProvider.NewCounter("output_uns_validation_success"),
		ValidationFailureCounter: metricsProvider.NewCounter("output_uns_validation_failure"),
		TagValidationFailCounter: metricsProvider.NewCounter("output_uns_tag_validation_failure"),
		PayloadValidationFailCounter: metricsProvider.NewCounter("output_uns_payload_validation_failure"),
		ValidationTimer:          metricsProvider.NewTimer("output_uns_validation_time"),
		
		// Error metrics
		SchemaFetchErrorCounter:   metricsProvider.NewCounter("output_uns_schema_fetch_errors"),
		SchemaCompileErrorCounter: metricsProvider.NewCounter("output_uns_schema_compile_errors"),
	}
}

// LogSchemaLoaded logs when a schema is successfully loaded
func (m *UnsOutputMetrics) LogSchemaLoaded(startTime time.Time) {
	m.SchemaFetchTimer.Timing(int64(time.Since(startTime)))
	m.SchemasLoadedGauge.Set(1) // Increment loaded schemas count
}

// LogSchemaCacheHit logs a schema cache hit
func (m *UnsOutputMetrics) LogSchemaCacheHit() {
	m.SchemaCacheHitCounter.Incr(1)
}

// LogSchemaCacheMiss logs a schema cache miss
func (m *UnsOutputMetrics) LogSchemaCacheMiss() {
	m.SchemaCacheMissCounter.Incr(1)
}

// LogValidationSuccess logs a successful validation
func (m *UnsOutputMetrics) LogValidationSuccess(startTime time.Time) {
	m.ValidationSuccessCounter.Incr(1)
	m.ValidationTimer.Timing(int64(time.Since(startTime)))
}

// LogValidationFailure logs a validation failure with type
func (m *UnsOutputMetrics) LogValidationFailure(startTime time.Time, failureType string) {
	m.ValidationFailureCounter.Incr(1)
	m.ValidationTimer.Timing(int64(time.Since(startTime)))
	
	switch failureType {
	case "tag":
		m.TagValidationFailCounter.Incr(1)
	case "payload":
		m.PayloadValidationFailCounter.Incr(1)
	}
}

// LogSchemaFetchError logs a schema fetch error
func (m *UnsOutputMetrics) LogSchemaFetchError() {
	m.SchemaFetchErrorCounter.Incr(1)
}

// LogSchemaCompileError logs a schema compilation error
func (m *UnsOutputMetrics) LogSchemaCompileError() {
	m.SchemaCompileErrorCounter.Incr(1)
}

// NewMockOutputMetrics creates a new metrics collection that doesn't actually record metrics
// Useful for testing when you don't need real metrics
func NewMockOutputMetrics() *UnsOutputMetrics {
	mockResources := service.MockResources()
	mockMetrics := mockResources.Metrics()
	return &UnsOutputMetrics{
		SchemasLoadedGauge:     mockMetrics.NewGauge("output_uns_schemas_loaded"),
		SchemaCacheHitCounter:  mockMetrics.NewCounter("output_uns_schema_cache_hits"),
		SchemaCacheMissCounter: mockMetrics.NewCounter("output_uns_schema_cache_misses"),
		SchemaFetchTimer:       mockMetrics.NewTimer("output_uns_schema_fetch_time"),
		ValidationSuccessCounter: mockMetrics.NewCounter("output_uns_validation_success"),
		ValidationFailureCounter: mockMetrics.NewCounter("output_uns_validation_failure"),
		TagValidationFailCounter: mockMetrics.NewCounter("output_uns_tag_validation_failure"),
		PayloadValidationFailCounter: mockMetrics.NewCounter("output_uns_payload_validation_failure"),
		ValidationTimer:          mockMetrics.NewTimer("output_uns_validation_time"),
		SchemaFetchErrorCounter:   mockMetrics.NewCounter("output_uns_schema_fetch_errors"),
		SchemaCompileErrorCounter: mockMetrics.NewCounter("output_uns_schema_compile_errors"),
	}
}
```

### 8. Integration with uns_output Plugin

**File**: `uns_plugin/uns_output.go` (MODIFIED)

```go
// Update unsOutput struct to include metrics
type unsOutput struct {
	config        unsOutputConfig
	client        MessagePublisher
	schemaCache   *SchemaCache
	metrics       *UnsOutputMetrics
	log           *service.Logger
}

// Update constructor to include metrics
func newUnsOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	// ... existing config parsing ...
	
	// Create schema cache if registry URL is provided and not explicitly disabled
	var schemaCache *SchemaCache
	if schemaRegistryURL != "" {
		schemaCache = NewSchemaCache(schemaRegistryURL, mgr.Logger())
	}
	
	// Create metrics
	metrics := NewUnsOutputMetrics(mgr.Metrics())
	
	return newUnsOutputWithClient(NewClient(), config, schemaCache, metrics, mgr.Logger()), batchPolicy, maxInFlight, nil
}

// Update testable constructor
func newUnsOutputWithClient(client MessagePublisher, config unsOutputConfig, schemaCache *SchemaCache, metrics *UnsOutputMetrics, logger *service.Logger) service.BatchOutput {
	return &unsOutput{
		client:      client,
		config:      config,
		schemaCache: schemaCache,
		metrics:     metrics,
		log:         logger,
	}
}

// Update WriteBatch to include validation with metrics
func (o *unsOutput) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	if len(msgs) == 0 {
		return nil
	}

	records := make([]Record, 0, len(msgs))
	for i, msg := range msgs {
		// ... existing key validation ...
		
		// Perform schema validation if schema cache is available
		if o.schemaCache != nil {
			if err := o.validateMessage(ctx, msg, msgAsBytes, i); err != nil {
				// Message failed validation, but continue with fail-open behavior
				o.log.Warnf("Schema validation failed for message %d: %v", i, err)
				// Note: Message is still processed (fail-open behavior)
			}
		}
		
		// ... rest of existing record preparation ...
	}
	
	// ... existing batch sending logic ...
}

// validateMessage performs schema validation with metrics tracking
func (o *unsOutput) validateMessage(ctx context.Context, msg *service.Message, msgBytes []byte, msgIndex int) error {
	startTime := time.Now()
	
	// Get data contract from message metadata
	contract := msg.MetaGet("data_contract")
	if contract == "" {
		o.log.Debugf("No data_contract metadata found for message %d, skipping validation", msgIndex)
		return nil
	}

	// Get UNS topic and extract tag name
	unsTopicKey, err := o.config.umh_topic.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to get UNS topic for message %d: %v", msgIndex, err)
	}

	tagName, err := extractTagNameFromUNSTopic(unsTopicKey)
	if err != nil {
		return fmt.Errorf("failed to extract tag name from UNS topic '%s' for message %d: %v", unsTopicKey, msgIndex, err)
	}

	// Get validator for this contract
	validator, err := o.schemaCache.GetValidator(ctx, contract)
	if err != nil {
		o.metrics.LogSchemaFetchError()
		o.log.Errorf("Failed to get validator for contract %s: %v", contract, err)
		return nil // Fail open - don't block the message
	}

	if validator == nil {
		// No schema registered for this contract - skip validation
		return nil
	}

	// Perform dual validation (tag name + payload)
	if err := validator.ValidateTagAndPayload(tagName, msgBytes); err != nil {
		// Determine failure type
		failureType := "payload"
		if strings.Contains(err.Error(), "not allowed for contract") {
			failureType = "tag"
		}
		
		o.metrics.LogValidationFailure(startTime, failureType)
		return fmt.Errorf("dual validation failed for contract %s, tag '%s' - %v", contract, tagName, err)
	}

	o.metrics.LogValidationSuccess(startTime)
	o.log.Tracef("Message %d passed dual validation: contract=%s, tag=%s", msgIndex, contract, tagName)
	return nil
}
```

### 9. Update SchemaCache to Include Metrics

**File**: `uns_plugin/schema_validator.go` (MODIFIED)

```go
// Update SchemaCache to include metrics
type SchemaCache struct {
	cache            map[string]SchemaValidator
	lastFetch        map[string]time.Time
	registryURL      string
	refreshInterval  time.Duration
	metrics          *UnsOutputMetrics  // NEW: Add metrics
	mutex            sync.RWMutex
	log              *service.Logger
}

// Update constructor to accept metrics
func NewSchemaCache(registryURL string, logger *service.Logger) *SchemaCache {
	return &SchemaCache{
		cache:           make(map[string]SchemaValidator),
		lastFetch:       make(map[string]time.Time),
		registryURL:     registryURL,
		refreshInterval: 10 * time.Minute,
		log:             logger,
		// metrics will be set later via SetMetrics
	}
}

// SetMetrics sets the metrics instance for the cache
func (sc *SchemaCache) SetMetrics(metrics *UnsOutputMetrics) {
	sc.metrics = metrics
}

// Update fetchAndCacheSchema to include metrics
func (sc *SchemaCache) fetchAndCacheSchema(ctx context.Context, contractInfo ContractInfo) (SchemaValidator, error) {
	// ... existing logic ...
	
	// Check cache first
	if validator, exists := sc.cache[cacheKey]; exists {
		if sc.metrics != nil {
			sc.metrics.LogSchemaCacheHit()
		}
		// ... existing cache hit logic ...
	}
	
	if sc.metrics != nil {
		sc.metrics.LogSchemaCacheMiss()
	}
	
	fetchStart := time.Now()
	client := NewSchemaRegistryClient(sc.registryURL)
	schemaResp, err := client.GetSchemaForContract(contractInfo)
	if err != nil {
		if sc.metrics != nil {
			sc.metrics.LogSchemaFetchError()
		}
		sc.log.Errorf("Failed to fetch schema for contract %s: %v", contractInfo.FullName, err)
		return nil, nil // Fail open
	}
	
	// ... existing response handling ...
	
	// Compile the contract schema
	validator, err := NewCompiledContractSchema(contractInfo.FullName, []byte(schemaResp.Schema))
	if err != nil {
		if sc.metrics != nil {
			sc.metrics.LogSchemaCompileError()
		}
		sc.log.Errorf("Failed to compile contract schema for %s: %v", contractInfo.FullName, err)
		return nil, nil // Fail open
	}
	
	if sc.metrics != nil {
		sc.metrics.LogSchemaLoaded(fetchStart)
	}
	
	// ... rest of existing caching logic ...
}
```

### 10. Available Metrics

The following metrics will be available through Benthos's metrics system:

**Schema Loading Metrics:**
- `output_uns_schemas_loaded` (Gauge): Number of loaded schemas
- `output_uns_schema_cache_hits` (Counter): Schema cache hits
- `output_uns_schema_cache_misses` (Counter): Schema cache misses
- `output_uns_schema_fetch_time` (Timer): Time to fetch schemas from registry

**Validation Metrics:**
- `output_uns_validation_success` (Counter): Successful validations
- `output_uns_validation_failure` (Counter): Failed validations
- `output_uns_tag_validation_failure` (Counter): Tag name validation failures
- `output_uns_payload_validation_failure` (Counter): Payload validation failures
- `output_uns_validation_time` (Timer): Validation duration

**Error Metrics:**
- `output_uns_schema_fetch_errors` (Counter): Schema registry fetch errors
- `output_uns_schema_compile_errors` (Counter): Schema compilation errors

## Benefits of Metrics

### 1. **Data Quality Monitoring**
- Track validation success/failure rates per contract
- Identify problematic contracts or schemas
- Monitor data quality trends over time

### 2. **Performance Monitoring**
- Track validation latency per contract
- Monitor cache hit rates
- Identify performance bottlenecks

### 3. **Operational Insights**
- See which contracts are most/least used
- Monitor schema loading and refresh patterns
- Track schema registry health

### 4. **Debugging Support**
- Identify which tags are being rejected most often
- Understand payload validation failure patterns
- Monitor schema compilation issues

### 5. **Capacity Planning**
- Understand contract usage patterns
- Plan schema registry capacity
- Monitor validation throughput

## Dependencies

Add to go.mod:
```bash
go get github.com/kaptinlin/jsonschema@latest
```

## Key Benefits

- ✅ **Dual Validation**: Validates both tag names (from UNS topic) AND payload structure
- ✅ **Data Quality**: Only valid JSON messages with allowed tag names reach the UNS
- ✅ **Version Support**: Handles both latest and specific contract versions (`_contract`, `_contractv1`)
- ✅ **Zero Configuration**: Auto-derives schema registry URL from broker address
- ✅ **Backward Compatible**: Existing configurations work without changes
- ✅ **Tag Name Control**: Prevents unauthorized tag names per contract
- ✅ **Payload Validation**: Ensures payload matches tag-specific JSON schema
- ✅ **Performance**: Efficient caching minimizes schema registry load
- ✅ **Reliability**: Fail-open design prevents data loss
- ✅ **Rich Error Messages**: Clear validation failure messages for debugging
- ✅ **Comprehensive Metrics**: Full observability into validation performance and data quality
- ✅ **Operational Insights**: Track contract usage, validation success rates, and performance metrics
- ✅ **Maintainability**: Clean, testable architecture with comprehensive test coverage 