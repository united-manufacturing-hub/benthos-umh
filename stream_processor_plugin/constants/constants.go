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

package constants

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

// Constants for JavaScript runtime configuration
const (
	// JavaScript engine configuration
	JSCallStackSize        = 1000                  // Maximum call stack size to prevent stack overflow
	SlowExecutionThreshold = 10 * time.Millisecond // Threshold for logging slow JavaScript execution

	// Object pool configuration
	DefaultByteBufferSize      = 256  // Default byte buffer size for JSON operations
	MaxPooledBufferSize        = 4096 // Maximum buffer size to keep in pool
	MaxPooledStringBuilderSize = 1024 // Maximum string builder size to keep in pool
)

// DangerousGlobals contains a comprehensive list of JavaScript globals that should be disabled
// for security reasons in a sandboxed environment. This combines and consolidates the lists
// from both js_engine.go and pools.go to ensure consistent security configuration.
var DangerousGlobals = []string{
	// ===== CODE EXECUTION - CRITICAL SECURITY RISK =====
	"eval",     // Direct code evaluation - MUST block for security
	"Function", // Function constructor (can execute strings as code)

	// ===== MODULE SYSTEM (Node.js specific - may not exist in Goja) =====
	"require",    // Node.js module loader
	"module",     // Current module object
	"exports",    // Module exports object
	"__dirname",  // Current directory path
	"__filename", // Current file path

	// ===== GLOBAL SCOPE ACCESS =====
	"global",     // Node.js global object
	"globalThis", // ES2020 universal global object reference

	// ===== PROCESS AND ENVIRONMENT (Node.js specific) =====
	"process", // Node.js process object (exit, env vars, etc.)

	// ===== ASYNC OPERATIONS (Browser/Node.js APIs) =====
	"setTimeout",     // Schedule delayed execution
	"setInterval",    // Schedule repeated execution
	"setImmediate",   // Schedule immediate execution (Node.js)
	"clearTimeout",   // Clear scheduled timeout
	"clearInterval",  // Clear scheduled interval
	"clearImmediate", // Clear scheduled immediate (Node.js)

	// ===== I/O AND DEBUGGING =====
	"console", // Console output (already in your built-ins, consider if you want it)

	// ===== PROTOTYPE MANIPULATION - SANDBOX ESCAPE RISKS =====
	"__proto__",        // Prototype chain access (major security risk)
	"__defineGetter__", // Define property getters (deprecated)
	"__defineSetter__", // Define property setters (deprecated)
	"__lookupGetter__", // Lookup property getters (deprecated)
	"__lookupSetter__", // Lookup property setters (deprecated)
	"constructor",      // Constructor property (can be used for prototype manipulation)

	// ===== IMPORT/DYNAMIC LOADING (ES6+ features) =====
	"import",        // Dynamic import (ES2020)
	"importScripts", // Web Worker script import (Browser API)

	// ===== REFLECTION APIS THAT COULD BE DANGEROUS =====
	// Note: These are legitimate ES6 features but could be used for sandbox escapes
	"Reflect", // ES6 Reflect API - consider if you need this
	"Proxy",   // ES6 Proxy API - can intercept operations, consider security implications

	// ===== OBJECT MANIPULATION METHODS =====
	"defineProperty",           // Object.defineProperty access
	"getOwnPropertyDescriptor", // Object.getOwnPropertyDescriptor access
	"getPrototypeOf",           // Object.getPrototypeOf access
	"setPrototypeOf",           // Object.setPrototypeOf access

	// ===== DEPRECATED BUT POTENTIALLY DANGEROUS =====
	"escape",   // Deprecated URL encoding function
	"unescape", // Deprecated URL decoding function
}
