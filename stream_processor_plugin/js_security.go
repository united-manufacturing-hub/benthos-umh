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

import (
	"fmt"
	"time"

	"github.com/dop251/goja"
	"github.com/redpanda-data/benthos/v4/public/service"
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

// ConfigureJSRuntime sets up a Goja runtime with security constraints.
//
// This function consolidates the security configuration that was previously
// duplicated in both JSEngine and ObjectPools. It:
//
// - Sets maximum call stack size to prevent stack overflow from deep recursion
// - Disables dangerous global functions/objects that could be used for code injection
// - Execution timeout (5s) should be handled in executeExpression() to prevent infinite loops
//
// Note: Goja already provides a sandboxed environment without Node.js/browser APIs,
// but this provides additional hardening against potential escape attempts.
func ConfigureJSRuntime(runtime *goja.Runtime, logger *service.Logger) {
	// Set maximum call stack size to prevent stack overflow from deep recursion
	runtime.SetMaxCallStackSize(JSCallStackSize)

	// Create security blocker function
	securityBlocker := CreateSecurityBlocker(runtime, logger)

	// Replace dangerous global objects with security blocker functions
	for _, global := range DangerousGlobals {
		if err := runtime.Set(global, securityBlocker(global)); err != nil {
			// Log warning but continue - this is security hardening, not critical
			if logger != nil {
				logger.Warnf("Failed to disable dangerous global '%s': %v", global, err)
			}
		}
	}

	// Built-in safe objects (Math, JSON, Date, etc.) are available by default
}

// CreateSecurityBlocker creates a security blocker function for dangerous globals.
//
// This function returns a closure that can be used to replace dangerous JavaScript
// globals with functions that log security violations and throw errors when called.
func CreateSecurityBlocker(runtime *goja.Runtime, logger *service.Logger) func(string) func(goja.FunctionCall) goja.Value {
	return func(apiName string) func(goja.FunctionCall) goja.Value {
		return func(call goja.FunctionCall) goja.Value {
			// Log the security violation
			if logger != nil {
				logger.Warnf("Security violation: attempted to call disabled function '%s'", apiName)
			}
			// Throw a TypeError to stop execution
			panic(runtime.NewTypeError(fmt.Sprintf("'%s' is disabled for security reasons in this sandboxed environment", apiName)))
		}
	}
}

// LogSlowExecution logs a warning if JavaScript execution takes longer than the threshold.
//
// This helper function consolidates the slow execution logging that was duplicated
// across multiple methods in JSEngine.
func LogSlowExecution(logger *service.Logger, expression string, duration time.Duration) {
	if duration > SlowExecutionThreshold {
		logger.Warnf("Slow JavaScript execution: %s took %v", expression, duration)
	}
}
