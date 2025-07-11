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

package stream_processor_plugin

import "time"

// Processing constants
const (
	// Default processing mode
	DefaultMode = "timeseries"

	// Memory allocation constants
	DefaultMetadataMapSize   = 8   // Pre-allocate for common metadata count
	DefaultVariableMapSize   = 16  // Pre-allocate for common variable count
	DefaultStringBuilderSize = 256 // For topic construction

	// JavaScript execution limits
	DefaultJSTimeout = 5 * time.Second // Timeout for JS execution
	MaxStringLength  = 1000            // Limit string length to prevent memory exhaustion
	MaxInputLength   = 10000           // Max input length for fuzz testing
	MaxTopicLength   = 1000            // Max topic length for fuzz testing

	// Topic construction constants
	TopicSeparator     = "."
	DataContractPrefix = "_"

	// Metadata keys
	UMHTopicKey = "umh_topic"

	// Error messages
	ErrMissingMode           = "mode is required"
	ErrUnsupportedMode       = "unsupported mode: %s (only 'timeseries' is supported)"
	ErrMissingOutputTopic    = "output_topic is required"
	ErrMissingModelName      = "model name is required"
	ErrMissingModelVersion   = "model version is required"
	ErrMissingSourceMappings = "at least one source mapping is required"
	ErrMissingTimestamp      = "missing or invalid timestamp_ms"
	ErrMissingValue          = "missing value field"
	ErrInvalidJSON           = "invalid JSON payload: %w"
	ErrInvalidConfiguration  = "invalid configuration: %w"
	ErrMarshalPayload        = "failed to marshal payload: %w"
	ErrGetPayload            = "failed to get message payload: %w"
	ErrAnalyzeMappings       = "failed to analyze mappings: %w"
	ErrPrecompileExpressions = "failed to pre-compile expressions: %w"
)
