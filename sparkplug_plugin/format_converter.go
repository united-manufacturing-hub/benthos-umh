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

// Package sparkplug_plugin provides format conversion utilities between UMH-Core and Sparkplug B formats.
//
// This file centralizes all conversion logic that was previously scattered across multiple files.
// It leverages the UNS topic package for standardized topic parsing and construction.
//
// # Format Conversion Overview
//
// UMH-Core uses ISA-95 hierarchical dot notation while Sparkplug B uses colon-separated device IDs:
//   - UMH: "enterprise.site.area.line"
//   - Sparkplug B: "enterprise:site:area:line"
//
// # Key Conversions
//
// **Encoding (UMH → Sparkplug B):**
//   - location_path (dots) → device_id (colons)
//   - virtual_path (dots) → metric_name prefix (colons)
//   - tag_name → metric_name suffix
//   - UMH topic structure → Sparkplug metric naming
//
// **Decoding (Sparkplug B → UMH):**
//   - device_id (colons) → location_path (dots)
//   - metric_name → virtual_path + tag_name parsing
//   - Sparkplug aliases → UMH topic reconstruction
//
// # Usage Examples
//
//	// Encoding UMH to Sparkplug B
//	encoder := NewUMHToSparkplugEncoder()
//	sparkplugData, err := encoder.Encode(umhMessage)
//
//	// Decoding Sparkplug B to UMH
//	decoder := NewSparkplugToUMHDecoder()
//	umhData, err := decoder.Decode(sparkplugMessage)
//
// # Thread Safety
//
// All format converters are stateless and safe for concurrent use.
package sparkplug_plugin

import (
	"fmt"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

// FormatConverter provides bidirectional conversion between UMH-Core and Sparkplug B formats.
// This replaces the scattered conversion logic with a centralized, testable implementation.
type FormatConverter struct {
	// Type converter for value type inference
	typeConverter *TypeConverter
}

// NewFormatConverter creates a new format converter instance.
func NewFormatConverter() *FormatConverter {
	return &FormatConverter{
		typeConverter: NewTypeConverter(),
	}
}

// sanitizeForUMH sanitizes a string to be compatible with UMH topic requirements.
// UMH topics only allow characters: a-z, A-Z, 0-9, dot (.), underscore (_), hyphen (-)
//
// Transformation logic:
// 1. Forward slashes (/) and colons (:) → dots (.) to preserve hierarchical structure
// 2. All other invalid characters → underscores (_)
// 3. Multiple consecutive dots are collapsed to single dots
// 4. Leading and trailing dots are removed
//
// This sanitization happens at the conversion boundary to ensure UMH compatibility
// while preserving the original Sparkplug data structure.
func (fc *FormatConverter) sanitizeForUMH(input string) string {
	if input == "" {
		return ""
	}

	// First pass: convert hierarchy separators to dots
	result := strings.ReplaceAll(input, "/", ".")
	result = strings.ReplaceAll(result, ":", ".")

	// Second pass: replace all other invalid characters with underscores
	// Valid UMH topic characters: a-z, A-Z, 0-9, dot, underscore, hyphen
	var sanitized strings.Builder
	sanitized.Grow(len(result)) // Pre-allocate for performance
	for _, char := range result {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '.' || char == '_' || char == '-' {
			sanitized.WriteRune(char)
		} else {
			sanitized.WriteRune('_')
		}
	}

	// Third pass: collapse multiple consecutive dots
	// Prevents "//" or "::" → ".." which would create invalid UMH topic segments
	finalResult := sanitized.String()
	for strings.Contains(finalResult, "..") {
		finalResult = strings.ReplaceAll(finalResult, "..", ".")
	}

	// Fourth pass: trim leading and trailing dots
	// Prevents topic structure issues in UMH
	finalResult = strings.Trim(finalResult, ".")

	return finalResult
}

// UMHMessage represents a parsed UMH message with structured topic information.
type UMHMessage struct {
	Topic      *topic.UnsTopic   // Parsed UMH topic
	TopicInfo  *proto.TopicInfo  // Topic components
	Value      interface{}       // Message value
	Timestamp  time.Time         // Message timestamp
	Metadata   map[string]string // Additional metadata
	RawPayload interface{}       // Original message payload
}

// SparkplugMessage represents a Sparkplug B message with device and metric information.
type SparkplugMessage struct {
	GroupID     string            // Sparkplug Group ID
	EdgeNodeID  string            // Edge Node ID
	DeviceID    string            // Device ID (converted from location_path)
	MetricName  string            // Full metric name (virtual_path:tag_name)
	MetricAlias uint64            // Sparkplug metric alias
	Value       interface{}       // Metric value
	DataType    string            // Sparkplug data type
	Timestamp   time.Time         // Message timestamp
	Metadata    map[string]string // Additional metadata
}

// EncodeUMHToSparkplug converts a UMH message to Sparkplug B format.
//
// This method handles the core conversion logic:
//   - Parses UMH topic structure using the UNS topic package
//   - Converts location_path dots to device_id colons
//   - Constructs metric names from virtual_path and tag_name
//   - Preserves all metadata and timestamps
//
// Parameters:
//   - msg: Benthos message containing UMH data
//   - groupID: Sparkplug Group ID for the output
//   - edgeNodeID: Edge Node ID for the output
//
// Returns:
//   - *SparkplugMessage: Converted Sparkplug B message
//   - error: Conversion error if any
func (fc *FormatConverter) EncodeUMHToSparkplug(msg *service.Message, groupID, edgeNodeID string) (*SparkplugMessage, error) {
	// Parse UMH message first
	umhMsg, err := fc.parseUMHMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse UMH message: %w", err)
	}

	// Convert location path to Sparkplug device ID
	deviceID := fc.convertLocationPathToDeviceID(umhMsg.TopicInfo)

	// Construct metric name from virtual path and tag name
	metricName := fc.constructSparkplugMetricName(umhMsg.TopicInfo)

	// Infer Sparkplug data type
	dataType := fc.typeConverter.InferMetricType(umhMsg.Value)

	// Build Sparkplug message
	sparkplugMsg := &SparkplugMessage{
		GroupID:    groupID,
		EdgeNodeID: edgeNodeID,
		DeviceID:   deviceID,
		MetricName: metricName,
		Value:      umhMsg.Value,
		DataType:   dataType,
		Timestamp:  umhMsg.Timestamp,
		Metadata:   umhMsg.Metadata,
	}

	return sparkplugMsg, nil
}

// DecodeSparkplugToUMH converts a Sparkplug B message to UMH format.
//
// This method handles the reverse conversion logic:
//   - Converts device_id colons back to location_path dots
//   - Parses metric names to extract virtual_path and tag_name
//   - Reconstructs UMH topic structure
//   - Builds valid UMH topic using the UNS topic package
//
// Parameters:
//   - sparkplugMsg: Sparkplug B message to convert
//   - dataContract: UMH data contract (e.g., "_historian", "_raw")
//
// Returns:
//   - *UMHMessage: Converted UMH message
//   - error: Conversion error if any
func (fc *FormatConverter) DecodeSparkplugToUMH(sparkplugMsg *SparkplugMessage, dataContract string) (*UMHMessage, error) {
	// Convert device ID back to location path
	locationPath := fc.convertDeviceIDToLocationPath(sparkplugMsg.DeviceID)

	// Sanitize location path for UMH compatibility
	locationPath = fc.sanitizeForUMH(locationPath)

	// Parse metric name to extract virtual path and tag name
	virtualPath, tagName, err := fc.parseSparkplugMetricName(sparkplugMsg.MetricName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Sparkplug metric name: %w", err)
	}

	// Sanitize virtual path and tag name for UMH compatibility
	if virtualPath != nil && *virtualPath != "" {
		sanitizedVP := fc.sanitizeForUMH(*virtualPath)
		virtualPath = &sanitizedVP
	}
	tagName = fc.sanitizeForUMH(tagName)

	// Build UMH topic using the UNS topic package
	umhTopic, err := fc.buildUMHTopic(locationPath, dataContract, virtualPath, tagName)
	if err != nil {
		return nil, fmt.Errorf("failed to build UMH topic: %w", err)
	}

	// Build UMH message
	umhMsg := &UMHMessage{
		Topic:     umhTopic,
		TopicInfo: umhTopic.Info(),
		Value:     sparkplugMsg.Value,
		Timestamp: sparkplugMsg.Timestamp,
		Metadata:  sparkplugMsg.Metadata,
	}

	return umhMsg, nil
}

// parseUMHMessage extracts UMH topic information from a Benthos message.
func (fc *FormatConverter) parseUMHMessage(msg *service.Message) (*UMHMessage, error) {
	// Extract structured payload
	payload, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to get structured payload: %w", err)
	}

	// Extract metadata
	metadata := make(map[string]string)
	err = msg.MetaWalk(func(key, value string) error {
		metadata[key] = value
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to extract metadata: %w", err)
	}

	// Try to construct UMH topic from metadata
	umhTopic, err := fc.constructUMHTopicFromMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to construct UMH topic from metadata: %w", err)
	}

	// Extract value - try different common field names
	var value interface{}
	var valueFound bool
	if structMap, ok := payload.(map[string]interface{}); ok {
		// Try common value field names
		for _, fieldName := range []string{"value", "val", "data", "measurement"} {
			if v, exists := structMap[fieldName]; exists {
				value = v
				valueFound = true
				break
			}
		}

		// If no standard field found, check for tag_name field
		if !valueFound {
			if tagName, exists := metadata["tag_name"]; exists {
				if v, exists := structMap[tagName]; exists {
					value = v
					valueFound = true
				}
			}
		}
	}

	if !valueFound {
		// Use entire payload as value only if no value field was found
		value = payload
	}

	// Extract timestamp
	timestamp := time.Now()
	if tsStr, exists := metadata["timestamp"]; exists {
		if parsed, err := time.Parse(time.RFC3339, tsStr); err == nil {
			timestamp = parsed
		}
	}

	umhMsg := &UMHMessage{
		Topic:      umhTopic,
		TopicInfo:  umhTopic.Info(),
		Value:      value,
		Timestamp:  timestamp,
		Metadata:   metadata,
		RawPayload: payload,
	}

	return umhMsg, nil
}

// constructUMHTopicFromMetadata builds a UMH topic from Benthos message metadata.
func (fc *FormatConverter) constructUMHTopicFromMetadata(metadata map[string]string) (*topic.UnsTopic, error) {
	var (
		dc, vp      string
		virtualPath *string
	)

	// Extract required components from metadata
	locationPath, exists := metadata["location_path"]
	if !exists || locationPath == "" {
		return nil, fmt.Errorf("location_path metadata is required for UMH topic construction")
	}

	// Use _historian as default data contract if not specified
	dataContract := "_historian"
	if dc, exists = metadata["data_contract"]; exists && dc != "" {
		dataContract = dc
	}

	// Extract optional virtual path
	if vp, exists = metadata["virtual_path"]; exists && vp != "" {
		virtualPath = &vp
	}

	// Extract tag name (required for topic name)
	tagName, exists := metadata["tag_name"]
	if !exists || tagName == "" {
		return nil, fmt.Errorf("tag_name metadata is required for UMH topic construction")
	}

	// Build topic using UNS topic builder
	builder := topic.NewBuilder()
	builder.SetLocationPath(locationPath)
	builder.SetDataContract(dataContract)

	if virtualPath != nil {
		builder.SetVirtualPath(*virtualPath)
	}

	builder.SetName(tagName)

	umhTopic, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build UMH topic: %w", err)
	}

	return umhTopic, nil
}

// convertLocationPathToDeviceID converts UMH location path (dots) to Sparkplug device ID (colons).
//
// Examples:
//   - "enterprise.site.area" → "enterprise:site:area"
//   - "factory.line1.station2" → "factory:line1:station2"
func (fc *FormatConverter) convertLocationPathToDeviceID(topicInfo *proto.TopicInfo) string {
	if topicInfo.Level0 == "" {
		return ""
	}

	// Build full location path
	locationParts := []string{topicInfo.Level0}
	locationParts = append(locationParts, topicInfo.LocationSublevels...)

	// Join with colons for Sparkplug B device ID (traditional format)
	return strings.Join(locationParts, ":")
}

// convertDeviceIDToLocationPath converts Sparkplug device ID to UMH location path.
// Handles both colon-separated (traditional) and dot-separated (preprocessed) formats.
//
// Examples:
//   - "enterprise:site:area" → "enterprise.site.area"
//   - "enterprise.site.area" → "enterprise.site.area"
func (fc *FormatConverter) convertDeviceIDToLocationPath(deviceID string) string {
	if deviceID == "" {
		return ""
	}

	// Convert colons to dots for UMH location path
	// If already using dots (preprocessed), this is a no-op
	return strings.ReplaceAll(deviceID, ":", ".")
}

// constructSparkplugMetricName builds a Sparkplug metric name from UMH topic components.
//
// Logic:
//   - If virtual_path exists: virtual_path:tag_name (with dots→colons conversion)
//   - If no virtual_path: tag_name only
//
// Examples:
//   - virtual_path="motor.diagnostics", tag_name="temperature" → "motor:diagnostics:temperature"
//   - virtual_path=nil, tag_name="pressure" → "pressure"
func (fc *FormatConverter) constructSparkplugMetricName(topicInfo *proto.TopicInfo) string {
	tagName := topicInfo.Name

	// If no virtual path, return tag name directly
	if topicInfo.VirtualPath == nil || *topicInfo.VirtualPath == "" {
		return tagName
	}

	// Convert dots to colons and append tag name (traditional Sparkplug format)
	virtualPathWithColons := strings.ReplaceAll(*topicInfo.VirtualPath, ".", ":")
	return virtualPathWithColons + ":" + tagName
}

// parseSparkplugMetricName parses a Sparkplug metric name back to virtual_path and tag_name.
//
// Priority order for separators:
//  1. Colons (:) - traditional Sparkplug separator, split on LAST colon
//  2. Forward slashes (/) - hierarchical paths, split on LAST slash
//  3. Dots (.) - fallback for already converted data, split on LAST dot
//  4. No separators - entire string is tag_name
//
// Examples:
//   - "motor:diagnostics:temperature" → virtual_path="motor.diagnostics", tag_name="temperature"
//   - "Refrigeration/Motor 1/Amps" → virtual_path="Refrigeration.Motor 1", tag_name="Amps"
//   - "motor.diagnostics.temperature" → virtual_path="motor.diagnostics", tag_name="temperature"
//   - "pressure" → virtual_path=nil, tag_name="pressure"
func (fc *FormatConverter) parseSparkplugMetricName(metricName string) (virtualPath *string, tagName string, err error) {
	if metricName == "" {
		return nil, "", fmt.Errorf("metric name cannot be empty")
	}

	// Trim leading and trailing slashes/colons/dots for cleaner parsing
	metricName = strings.Trim(metricName, "/:.")
	if metricName == "" {
		return nil, "", fmt.Errorf("metric name cannot be empty after trimming")
	}

	// Priority 1: Check for colons (traditional Sparkplug format)
	colonIndex := strings.LastIndex(metricName, ":")
	if colonIndex != -1 {
		// Split on last colon
		virtualPathStr := metricName[:colonIndex]
		tagName = metricName[colonIndex+1:]

		// Convert colons in virtual path to dots for UMH compatibility
		virtualPathStr = strings.ReplaceAll(virtualPathStr, ":", ".")

		virtualPath = &virtualPathStr
		return virtualPath, tagName, nil
	}

	// Priority 2: Check for forward slashes (hierarchical paths like "Refrigeration/Motor 1/Amps")
	slashIndex := strings.LastIndex(metricName, "/")
	if slashIndex != -1 {
		// Split on last slash
		virtualPathStr := metricName[:slashIndex]
		tagName = metricName[slashIndex+1:]

		// Convert slashes in virtual path to dots for UMH compatibility
		virtualPathStr = strings.ReplaceAll(virtualPathStr, "/", ".")

		virtualPath = &virtualPathStr
		return virtualPath, tagName, nil
	}

	// Priority 3: Check for dots (fallback for already converted data)
	lastDotIndex := strings.LastIndex(metricName, ".")
	if lastDotIndex != -1 {
		// Split at the last dot
		virtualPathStr := metricName[:lastDotIndex]
		tagName = metricName[lastDotIndex+1:]

		virtualPath = &virtualPathStr
		return virtualPath, tagName, nil
	}

	// No separators, entire string is tag name
	return nil, metricName, nil
}

// buildUMHTopic constructs a valid UMH topic using the UNS topic builder.
func (fc *FormatConverter) buildUMHTopic(locationPath, dataContract string, virtualPath *string, tagName string) (*topic.UnsTopic, error) {
	builder := topic.NewBuilder()

	// Set location path
	if locationPath == "" {
		return nil, fmt.Errorf("location_path is required for UMH topic")
	}
	builder.SetLocationPath(locationPath)

	// Set data contract
	if dataContract != "" {
		builder.SetDataContract(dataContract)
	} else {
		builder.SetDataContract("_historian") // Default
	}

	// Set virtual path if provided
	if virtualPath != nil && *virtualPath != "" {
		builder.SetVirtualPath(*virtualPath)
	}

	// Set tag name
	if tagName == "" {
		return nil, fmt.Errorf("tag_name is required for UMH topic")
	}
	builder.SetName(tagName)

	return builder.Build()
}

// ExtractMetricNameFromUMHMessage is a helper function that extracts the metric name
// that should be used in Sparkplug B from a UMH message.
//
// This is useful for the existing Sparkplug plugin code that needs to determine
// metric names for alias assignment.
func (fc *FormatConverter) ExtractMetricNameFromUMHMessage(msg *service.Message) (string, error) {
	umhMsg, err := fc.parseUMHMessage(msg)
	if err != nil {
		return "", err
	}

	return fc.constructSparkplugMetricName(umhMsg.TopicInfo), nil
}

// ExtractDeviceIDFromUMHMessage is a helper function that extracts the Sparkplug device ID
// from a UMH message using the PARRIS method.
//
// This is useful for the existing Sparkplug plugin code that needs to determine
// device IDs for message routing.
func (fc *FormatConverter) ExtractDeviceIDFromUMHMessage(msg *service.Message) (string, error) {
	umhMsg, err := fc.parseUMHMessage(msg)
	if err != nil {
		return "", err
	}

	return fc.convertLocationPathToDeviceID(umhMsg.TopicInfo), nil
}

// ValidateUMHMessage checks if a Benthos message contains valid UMH topic metadata.
func (fc *FormatConverter) ValidateUMHMessage(msg *service.Message) error {
	_, err := fc.parseUMHMessage(msg)
	return err
}

// ConvertSparkplugMetricsToUMHTopics converts a map of Sparkplug metrics back to UMH topic strings.
// This is useful for reverse engineering UMH topics from Sparkplug B messages.
func (fc *FormatConverter) ConvertSparkplugMetricsToUMHTopics(deviceID string, metrics map[string]interface{}, dataContract string) (map[string]string, error) {
	if dataContract == "" {
		dataContract = "_historian"
	}

	result := make(map[string]string)

	for metricName := range metrics {
		virtualPath, tagName, err := fc.parseSparkplugMetricName(metricName)
		if err != nil {
			return nil, fmt.Errorf("failed to parse metric name %s: %w", metricName, err)
		}

		locationPath := fc.convertDeviceIDToLocationPath(deviceID)

		umhTopic, err := fc.buildUMHTopic(locationPath, dataContract, virtualPath, tagName)
		if err != nil {
			return nil, fmt.Errorf("failed to build UMH topic for metric %s: %w", metricName, err)
		}

		result[metricName] = umhTopic.String()
	}

	return result, nil
}
