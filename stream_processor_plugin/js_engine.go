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

// JSEngine manages JavaScript runtime for expression evaluation
//
// This engine provides:
// - Sandboxed JavaScript execution using Goja runtime
// - Variable context injection for dynamic mappings
// - Static expression evaluation (no variables needed)
// - Error handling with fail-open policy
// - Expression validation and compilation
//
// SECURITY: The engine is sandboxed and only allows access to:
// - Source variables from the configuration
// - Built-in JavaScript functions (Math, JSON, etc.)
// - No access to system resources or Node.js modules
type JSEngine struct {
	logger *service.Logger

	// Goja runtime instances for expression evaluation
	staticRuntime  *goja.Runtime // For static expressions (no variables)
	dynamicRuntime *goja.Runtime // For dynamic expressions (with variables)

	// Compiled expressions cache for performance
	compiledExpressions map[string]*goja.Program

	// Configuration
	sourceNames []string // Available source variable names
}

// JSExecutionResult represents the result of JavaScript execution
type JSExecutionResult struct {
	Success bool        // Whether execution succeeded
	Value   interface{} // The result value (if successful)
	Error   string      // Error message (if failed)
}

// NewJSEngine creates a new JavaScript engine instance
func NewJSEngine(logger *service.Logger, sourceNames []string) *JSEngine {
	engine := &JSEngine{
		logger:              logger,
		staticRuntime:       goja.New(),
		dynamicRuntime:      goja.New(),
		compiledExpressions: make(map[string]*goja.Program),
		sourceNames:         sourceNames,
	}

	// Configure runtimes
	engine.configureRuntime(engine.staticRuntime)
	engine.configureRuntime(engine.dynamicRuntime)

	return engine
}

// Disable dangerous globals
var dangerousGlobals = []string{
	// Module system - prevents loading external modules/files
	"require", // Node.js module loader
	"module",  // Current module object
	"exports", // Module exports object

	// Process and environment access
	"process",    // Node.js process object (exit, env vars, etc.)
	"__dirname",  // Current directory path
	"__filename", // Current file path

	// Global scope access - prevents escaping sandbox
	"global",     // Node.js global object
	"globalThis", // Universal global object reference

	// Code execution - prevents dynamic code execution
	"Function", // Function constructor (can execute strings as code)
	"eval",     // Direct code evaluation

	// I/O and debugging
	"console", // Console output (potential info leakage)

	// Async operations - prevents background execution
	"setTimeout",     // Schedule delayed execution
	"setInterval",    // Schedule repeated execution
	"setImmediate",   // Schedule immediate execution
	"clearTimeout",   // Clear scheduled timeout
	"clearInterval",  // Clear scheduled interval
	"clearImmediate", // Clear scheduled immediate

	// Prototype manipulation - critical for preventing sandbox escapes
	"__proto__",        // Prototype chain access (major security risk)
	"__defineGetter__", // Define property getters
	"__defineSetter__", // Define property setters
	"__lookupGetter__", // Lookup property getters
	"__lookupSetter__", // Lookup property setters
	"constructor",      // Constructor property access

	// Object manipulation methods that can lead to escapes
	"defineProperty",           // Object.defineProperty equivalent
	"getOwnPropertyDescriptor", // Property descriptor access
	"getPrototypeOf",           // Prototype chain traversal
	"setPrototypeOf",           // Prototype chain modification

	// Legacy/deprecated but potentially dangerous functions
	"escape",   // Legacy escape function
	"unescape", // Legacy unescape function

	// Import/dynamic loading (ES6+ features if supported)
	"import",        // Dynamic import
	"importScripts", // Web Worker script import
}

// configureRuntime sets up a Goja runtime with security constraints
//
// Security measures:
// - Removes eval() and Function() constructor to prevent code injection
// - Sets maximum call stack size to prevent stack overflow from deep recursion
// - Execution timeout (5s) is handled in executeExpression() to prevent infinite loops
//
// Note: Goja already provides a sandboxed environment without Node.js/browser APIs
func (e *JSEngine) configureRuntime(runtime *goja.Runtime) {
	// Set maximum call stack size to prevent stack overflow from deep recursion
	runtime.SetMaxCallStackSize(1000)

	// Create a function that explains why APIs are disabled
	securityBlocker := func(apiName string) func(goja.FunctionCall) goja.Value {
		return func(call goja.FunctionCall) goja.Value {
			panic(runtime.NewTypeError(fmt.Sprintf("'%s' is disabled for security reasons in this sandboxed environment", apiName)))
		}
	}

	// Replace dangerous global objects with security blocker functions
	for _, global := range dangerousGlobals {
		_ = runtime.Set(global, securityBlocker(global))
	}

	// Built-in safe objects (Math, JSON, Date, etc.) are available by default
}

// ValidateExpression validates a JavaScript expression syntax
func (e *JSEngine) ValidateExpression(expression string) error {
	_, err := goja.Compile("validation", expression, false)
	if err != nil {
		return fmt.Errorf("invalid JavaScript expression: %w", err)
	}
	return nil
}

// CompileExpression compiles and caches a JavaScript expression
func (e *JSEngine) CompileExpression(expression string) error {
	// Check if already compiled
	if _, exists := e.compiledExpressions[expression]; exists {
		return nil
	}

	// Compile the expression
	program, err := goja.Compile("expression", expression, false)
	if err != nil {
		return fmt.Errorf("failed to compile expression '%s': %w", expression, err)
	}

	// Cache the compiled program
	e.compiledExpressions[expression] = program
	return nil
}

// EvaluateStatic evaluates a static JavaScript expression (no variables)
//
// Static expressions are those that don't reference any source variables,
// such as:
// - String constants: '"SN-P42-008"'
// - Numeric constants: '42'
// - Date functions: 'Date.now()'
// - Math operations: 'Math.PI * 2'
func (e *JSEngine) EvaluateStatic(expression string) JSExecutionResult {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if duration > 10*time.Millisecond {
			e.logger.Warnf("Slow JavaScript execution: %s took %v", expression, duration)
		}
	}()

	// Compile expression if not already compiled
	if err := e.CompileExpression(expression); err != nil {
		return JSExecutionResult{
			Success: false,
			Error:   fmt.Sprintf("compilation failed: %v", err),
		}
	}

	// Execute the expression
	result, err := e.executeExpression(e.staticRuntime, expression)
	if err != nil {
		e.logger.Warnf("JavaScript execution failed for static expression '%s': %v", expression, err)
		return JSExecutionResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return JSExecutionResult{
		Success: true,
		Value:   result,
	}
}

// EvaluateDynamic evaluates a dynamic JavaScript expression with variable context
//
// Dynamic expressions reference source variables from the configuration,
// such as:
// - Arithmetic: 'press + 4.00001'
// - Conversions: 'tF * 69 / 31'
// - Comparisons: 'press > 100 ? "high" : "normal"'
// - Complex logic: 'press > 0 && run ? press * efficiency : 0'
func (e *JSEngine) EvaluateDynamic(expression string, variables map[string]interface{}) JSExecutionResult {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if duration > 10*time.Millisecond {
			e.logger.Warnf("Slow JavaScript execution: %s took %v", expression, duration)
		}
	}()

	// Compile expression if not already compiled
	if err := e.CompileExpression(expression); err != nil {
		return JSExecutionResult{
			Success: false,
			Error:   fmt.Sprintf("compilation failed: %v", err),
		}
	}

	// Create a new runtime instance for this execution to avoid variable contamination
	runtime := goja.New()
	e.configureRuntime(runtime)

	// Inject variables into the runtime
	for name, value := range variables {
		// Only inject variables that are in the source configuration
		if e.isValidSourceVariable(name) {
			_ = runtime.Set(name, value)
		}
	}

	// Execute the expression
	result, err := e.executeExpression(runtime, expression)
	if err != nil {
		e.logger.Warnf("JavaScript execution failed for dynamic expression '%s': %v", expression, err)
		return JSExecutionResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	return JSExecutionResult{
		Success: true,
		Value:   result,
	}
}

// executeExpression executes a compiled JavaScript expression in the given runtime
func (e *JSEngine) executeExpression(runtime *goja.Runtime, expression string) (interface{}, error) {
	// Get the compiled program
	program, exists := e.compiledExpressions[expression]
	if !exists {
		return nil, fmt.Errorf("expression not compiled: %s", expression)
	}

	// Execute with timeout protection
	done := make(chan struct{})
	var result goja.Value
	var err error

	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("JavaScript execution panic: %v", r)
			}
		}()

		result, err = runtime.RunProgram(program)
	}()

	// Wait for execution with timeout
	select {
	case <-done:
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, fmt.Errorf("JavaScript execution returned nil result")
		}
		return result.Export(), nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("JavaScript execution timeout")
	}
}

// isValidSourceVariable checks if a variable name is in the configured sources
func (e *JSEngine) isValidSourceVariable(name string) bool {
	for _, sourceName := range e.sourceNames {
		if name == sourceName {
			return true
		}
	}
	return false
}

// GetSourceVariables returns the list of available source variable names
func (e *JSEngine) GetSourceVariables() []string {
	return e.sourceNames
}

// Close cleans up the JavaScript engine resources
func (e *JSEngine) Close() error {
	// Clear compiled expressions
	e.compiledExpressions = make(map[string]*goja.Program)

	// Note: Goja runtimes don't need explicit cleanup, they will be garbage collected
	return nil
}
