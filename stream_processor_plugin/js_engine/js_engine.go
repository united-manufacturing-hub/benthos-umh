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

package js_engine

import (
	"fmt"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/js_security"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/pools"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// JSEngine handles JavaScript expression evaluation with caching and pooling
type JSEngine struct {
	logger *service.Logger

	// Goja runtime instances for expression evaluation
	staticRuntime  *goja.Runtime // For static expressions (no variables)
	dynamicRuntime *goja.Runtime // For dynamic expressions (with variables)

	// Compiled expressions cache for performance - OPTIMIZATION: Store compiled programs
	compiledExpressions map[string]*goja.Program
	compiledMutex       sync.RWMutex // Protects compiledExpressions

	// Static expression results cache
	staticExpressionCache map[string]JSExecutionResult
	staticCacheMutex      sync.RWMutex // Protects staticExpressionCache

	// NEW: Pre-compiled programs for static/dynamic mappings
	precompiledPrograms map[string]*goja.Program // Pre-compiled mapping expressions
	precompiledMutex    sync.RWMutex             // Protects precompiledPrograms

	// Object pools for runtime reuse
	pools *pools.ObjectPools

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
func NewJSEngine(logger *service.Logger, sourceNames []string, pools *pools.ObjectPools) *JSEngine {
	engine := &JSEngine{
		logger:                logger,
		staticRuntime:         goja.New(),
		dynamicRuntime:        goja.New(),
		compiledExpressions:   make(map[string]*goja.Program),
		staticExpressionCache: make(map[string]JSExecutionResult),
		precompiledPrograms:   make(map[string]*goja.Program),
		pools:                 pools,
		sourceNames:           sourceNames,
	}

	// Configure runtimes using shared security configuration
	js_security.ConfigureJSRuntime(engine.staticRuntime, logger)
	js_security.ConfigureJSRuntime(engine.dynamicRuntime, logger)

	return engine
}

// ValidateExpression validates a JavaScript expression syntax
func (e *JSEngine) ValidateExpression(expression string) error {
	_, err := goja.Compile("validation", expression, false)
	if err != nil {
		return fmt.Errorf("invalid JavaScript expression '%s': %w. Please check syntax and try again", expression, err)
	}
	return nil
}

// CompileExpression compiles and caches a JavaScript expression
func (e *JSEngine) CompileExpression(expression string) error {
	// Check if already compiled with read lock
	e.compiledMutex.RLock()
	if _, exists := e.compiledExpressions[expression]; exists {
		e.compiledMutex.RUnlock()
		return nil
	}
	e.compiledMutex.RUnlock()

	// Compile the expression
	program, err := goja.Compile("expression", expression, false)
	if err != nil {
		return fmt.Errorf("failed to compile JavaScript expression '%s': %w. Please check syntax and try again", expression, err)
	}

	// Cache the compiled program with write lock
	e.compiledMutex.Lock()
	// Double-check in case another goroutine compiled it while we were compiling
	if _, exists := e.compiledExpressions[expression]; !exists {
		e.compiledExpressions[expression] = program
	}
	e.compiledMutex.Unlock()
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
	// Check cache first with read lock
	e.staticCacheMutex.RLock()
	if cached, exists := e.staticExpressionCache[expression]; exists {
		e.staticCacheMutex.RUnlock()
		return cached
	}
	e.staticCacheMutex.RUnlock()

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		js_security.LogSlowExecution(e.logger, expression, duration)
	}()

	// Compile expression if not already compiled
	if err := e.CompileExpression(expression); err != nil {
		result := JSExecutionResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to compile JavaScript expression '%s': %v. Please check syntax and try again", expression, err),
		}
		// Cache the error result to avoid re-compiling
		e.staticCacheMutex.Lock()
		e.staticExpressionCache[expression] = result
		e.staticCacheMutex.Unlock()
		return result
	}

	// Execute the expression
	result, err := e.executeExpression(e.staticRuntime, expression)
	if err != nil {
		e.logger.Warnf("JavaScript execution failed for static expression '%s': %v", expression, err)
		errorResult := JSExecutionResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to execute JavaScript expression '%s': %v", expression, err),
		}
		// Cache the error result to avoid re-executing
		e.staticCacheMutex.Lock()
		e.staticExpressionCache[expression] = errorResult
		e.staticCacheMutex.Unlock()
		return errorResult
	}

	successResult := JSExecutionResult{
		Success: true,
		Value:   result,
	}
	// Cache the successful result
	e.staticCacheMutex.Lock()
	e.staticExpressionCache[expression] = successResult
	e.staticCacheMutex.Unlock()
	return successResult
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
		js_security.LogSlowExecution(e.logger, expression, duration)
	}()

	// Compile expression if not already compiled
	if err := e.CompileExpression(expression); err != nil {
		return JSExecutionResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to compile JavaScript expression '%s': %v. Please check syntax and try again", expression, err),
		}
	}

	// Get a pooled runtime instance instead of creating new one
	runtime := e.pools.GetJSRuntime()
	runtime.ClearInterrupt() // Clear any previous interrupt state from timeout
	defer e.pools.PutJSRuntime(runtime)

	// Validate all variables first
	for name := range variables {
		if !e.isValidSourceVariable(name) {
			return JSExecutionResult{
				Success: false,
				Error:   fmt.Sprintf("Invalid variable name '%s'. Valid variables are: %v", name, e.sourceNames),
			}
		}
	}

	// Inject variables into the runtime
	for name, value := range variables {
		if err := runtime.Set(name, value); err != nil {
			return JSExecutionResult{
				Success: false,
				Error:   fmt.Sprintf("Failed to set variable '%s' to value '%v': %v", name, value, err),
			}
		}
	}

	// Execute the expression
	result, err := e.executeExpression(runtime, expression)
	if err != nil {
		e.logger.Warnf("JavaScript execution failed for dynamic expression '%s': %v", expression, err)
		return JSExecutionResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to execute JavaScript expression '%s': %v", expression, err),
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
	e.compiledMutex.RLock()
	program, exists := e.compiledExpressions[expression]
	e.compiledMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("JavaScript expression '%s' not compiled. Please call CompileExpression first", expression)
	}

	// Execute with timeout protection
	done := make(chan struct{})
	var result goja.Value
	var err error

	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("JavaScript execution panic: %v. Please check the expression for runtime errors", r)
			}
		}()

		result, err = runtime.RunProgram(program)
	}()

	// Create timeout timer
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	// Wait for execution with timeout
	select {
	case <-done:
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, fmt.Errorf("JavaScript execution returned nil result. Please check that the expression returns a value")
		}
		if result.ExportType() == nil {
			return nil, fmt.Errorf("JavaScript execution returned nil result. Please check that the expression returns a value")
		}
		return result.Export(), nil
	case <-timer.C:
		// Interrupt the runtime to stop execution
		runtime.Interrupt("execution timeout")
		// Wait for goroutine to finish after interrupt with a second timeout
		select {
		case <-done:
			// Goroutine finished after interrupt
		case <-time.After(1 * time.Second):
			// Goroutine didn't finish - log warning about potential resource leak
			e.logger.Warnf("JavaScript execution goroutine may be stuck after timeout")
		}
		return nil, fmt.Errorf("JavaScript execution exceeded 5 second timeout limit. Consider simplifying the expression, avoiding infinite loops, or reducing computational complexity")
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

// ClearStaticCache clears the static expression cache
func (e *JSEngine) ClearStaticCache() {
	e.staticCacheMutex.Lock()
	e.staticExpressionCache = make(map[string]JSExecutionResult)
	e.staticCacheMutex.Unlock()
}

// PrecompileExpressions pre-compiles all static and dynamic mapping expressions for optimal performance
// This is a major optimization that eliminates JavaScript parsing/compilation overhead during runtime
func (e *JSEngine) PrecompileExpressions(staticMappings, dynamicMappings map[string]config.MappingInfo) error {
	e.precompiledMutex.Lock()
	defer e.precompiledMutex.Unlock()

	e.logger.Debug("Pre-compiling JavaScript expressions for performance optimization")

	// Pre-compile static mappings
	for virtualPath, mapping := range staticMappings {
		program, err := goja.Compile(
			fmt.Sprintf("static-%s", virtualPath),
			mapping.Expression,
			false)
		if err != nil {
			return fmt.Errorf("failed to pre-compile static mapping '%s' with expression '%s': %w. Please check syntax and try again", virtualPath, mapping.Expression, err)
		}
		e.precompiledPrograms[mapping.Expression] = program
	}

	// Pre-compile dynamic mappings
	for virtualPath, mapping := range dynamicMappings {
		program, err := goja.Compile(
			fmt.Sprintf("dynamic-%s", virtualPath),
			mapping.Expression,
			false)
		if err != nil {
			return fmt.Errorf("failed to pre-compile dynamic mapping '%s' with expression '%s': %w. Please check syntax and try again", virtualPath, mapping.Expression, err)
		}
		e.precompiledPrograms[mapping.Expression] = program
	}

	totalCompiled := len(staticMappings) + len(dynamicMappings)
	e.logger.Infof("Successfully pre-compiled %d JavaScript expressions for optimal performance", totalCompiled)

	return nil
}

// EvaluateStaticPrecompiled evaluates a static expression using pre-compiled program for optimal performance
func (e *JSEngine) EvaluateStaticPrecompiled(expression string) JSExecutionResult {
	// Check cache first with read lock
	e.staticCacheMutex.RLock()
	cached, exists := e.staticExpressionCache[expression]
	e.staticCacheMutex.RUnlock()

	if exists {
		return cached
	}

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		js_security.LogSlowExecution(e.logger, expression, duration)
	}()

	// Get pre-compiled program
	e.precompiledMutex.RLock()
	program, exists := e.precompiledPrograms[expression]
	e.precompiledMutex.RUnlock()

	if !exists {
		// Fallback to regular compilation (shouldn't happen if precompilation worked)
		e.logger.Warnf("Expression not pre-compiled, falling back to runtime compilation: %s", expression)
		return e.EvaluateStatic(expression)
	}

	// Execute pre-compiled program for optimal performance
	result, err := e.executePrecompiledProgram(e.staticRuntime, program)
	if err != nil {
		e.logger.Warnf("JavaScript execution failed for static expression '%s': %v", expression, err)
		errorResult := JSExecutionResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to execute pre-compiled JavaScript expression '%s': %v", expression, err),
		}
		// Cache the error result
		e.staticCacheMutex.Lock()
		e.staticExpressionCache[expression] = errorResult
		e.staticCacheMutex.Unlock()
		return errorResult
	}

	successResult := JSExecutionResult{
		Success: true,
		Value:   result,
	}
	// Cache the successful result
	e.staticCacheMutex.Lock()
	e.staticExpressionCache[expression] = successResult
	e.staticCacheMutex.Unlock()
	return successResult
}

// EvaluateDynamicPrecompiled evaluates a dynamic expression using pre-compiled program for optimal performance
func (e *JSEngine) EvaluateDynamicPrecompiled(expression string, variables map[string]interface{}) JSExecutionResult {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		js_security.LogSlowExecution(e.logger, expression, duration)
	}()

	// Get pre-compiled program
	e.precompiledMutex.RLock()
	program, exists := e.precompiledPrograms[expression]
	e.precompiledMutex.RUnlock()

	if !exists {
		// Fallback to regular compilation (shouldn't happen if precompilation worked)
		e.logger.Warnf("Expression not pre-compiled, falling back to runtime compilation: %s", expression)
		return e.EvaluateDynamic(expression, variables)
	}

	// Get a pooled runtime for thread safety
	runtime := e.pools.GetJSRuntime()
	runtime.ClearInterrupt() // Clear any previous interrupt state from timeout
	defer e.pools.PutJSRuntime(runtime)

	// Validate all variables first
	for name := range variables {
		if !e.isValidSourceVariable(name) {
			return JSExecutionResult{
				Success: false,
				Error:   fmt.Sprintf("Invalid variable name '%s'. Valid variables are: %v", name, e.sourceNames),
			}
		}
	}

	// Set variables in the runtime
	for name, value := range variables {
		if err := runtime.Set(name, value); err != nil {
			return JSExecutionResult{
				Success: false,
				Error:   fmt.Sprintf("Failed to set variable '%s' to value '%v': %v", name, value, err),
			}
		}
	}

	// Execute pre-compiled program for optimal performance
	result, err := e.executePrecompiledProgram(runtime, program)
	if err != nil {
		e.logger.Warnf("JavaScript execution failed for dynamic expression '%s': %v", expression, err)
		return JSExecutionResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to execute pre-compiled JavaScript expression '%s': %v", expression, err),
		}
	}

	return JSExecutionResult{
		Success: true,
		Value:   result,
	}
}

// executePrecompiledProgram executes a pre-compiled program with timeout protection
func (e *JSEngine) executePrecompiledProgram(runtime *goja.Runtime, program *goja.Program) (interface{}, error) {
	done := make(chan struct{})
	var result goja.Value
	var execErr error

	// Run with timeout
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("JavaScript panic: %v. Please check the expression for runtime errors", r)
			}
		}()

		result, execErr = runtime.RunProgram(program)
	}()

	// Create timeout timer
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case <-done:
		if execErr != nil {
			return nil, execErr
		}
		if result == nil {
			return nil, fmt.Errorf("JavaScript execution returned nil result. Please check that the expression returns a value")
		}
		if result.ExportType() == nil {
			return nil, fmt.Errorf("JavaScript execution returned nil result. Please check that the expression returns a value")
		}
		return result.Export(), nil
	case <-timer.C:
		// Interrupt the runtime to stop execution
		runtime.Interrupt("execution timeout")
		// Wait for goroutine to finish after interrupt with a second timeout
		select {
		case <-done:
			// Goroutine finished after interrupt
		case <-time.After(1 * time.Second):
			// Goroutine didn't finish - log warning about potential resource leak
			e.logger.Warnf("JavaScript execution goroutine may be stuck after timeout (precompiled)")
		}
		return nil, fmt.Errorf("JavaScript execution exceeded 5 second timeout limit. Consider simplifying the expression, avoiding infinite loops, or reducing computational complexity")
	}
}

// Close cleans up the JavaScript engine resources
func (e *JSEngine) Close() error {
	// Clear compiled expressions
	e.compiledMutex.Lock()
	e.compiledExpressions = make(map[string]*goja.Program)
	e.compiledMutex.Unlock()

	// Clear static expression cache
	e.staticCacheMutex.Lock()
	e.staticExpressionCache = make(map[string]JSExecutionResult)
	e.staticCacheMutex.Unlock()

	// Clear pre-compiled programs
	e.precompiledMutex.Lock()
	e.precompiledPrograms = make(map[string]*goja.Program)
	e.precompiledMutex.Unlock()

	// Note: Goja runtimes don't need explicit cleanup, they will be garbage collected
	return nil
}
