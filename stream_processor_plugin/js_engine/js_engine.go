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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/js_runtime"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/js_security"
)

// JSEngine handles JavaScript expression evaluation with caching and pooling
type JSEngine struct {
	logger *service.Logger

	// Shared runtime mechanism (ENG-8128). Harden installs the blocklist the
	// pooled runtimes here always had; ResetGlobals stays off: the old reset
	// cleared only the variables each run bound, which is what the runner does
	// by default.
	runner *js_runtime.Runner

	// Compiled expressions cache for performance - OPTIMIZATION: Store compiled programs
	compiledExpressions map[string]*js_runtime.Program
	compiledMutex       sync.RWMutex // Protects compiledExpressions

	// Static expression results cache
	staticExpressionCache map[string]JSExecutionResult
	staticCacheMutex      sync.RWMutex // Protects staticExpressionCache

	// NEW: Pre-compiled programs for static/dynamic mappings
	precompiledPrograms map[string]*js_runtime.Program // Pre-compiled mapping expressions
	precompiledMutex    sync.RWMutex                   // Protects precompiledPrograms

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
		logger:                logger,
		runner:                js_runtime.NewRunner(js_runtime.Options{Harden: true}),
		compiledExpressions:   make(map[string]*js_runtime.Program),
		staticExpressionCache: make(map[string]JSExecutionResult),
		precompiledPrograms:   make(map[string]*js_runtime.Program),
		sourceNames:           sourceNames,
	}

	return engine
}

// executionTimeout is the per-expression watchdog budget this engine has
// always enforced.
const executionTimeout = 5 * time.Second

var (
	// errExecutionTimeout preserves the exact watchdog error text the
	// engine's callers and tests pin.
	errExecutionTimeout = errors.New("JavaScript execution exceeded 5 second timeout limit. Consider simplifying the expression, avoiding infinite loops, or reducing computational complexity")
	errNilResult        = errors.New("JavaScript execution returned nil result. Please check that the expression returns a value")
)

// compileExpression compiles a mapping expression for the shared runner.
//
// js_runtime.Compile wraps source as (function(){'use strict'; ... })(), whose
// return value is undefined unless the body returns, so a bare expression's
// completion value would be lost. Single expressions — everything mapping
// analysis lets through — compile as "return (<expr>)", which preserves the
// completion value exactly. Sources that are not single expressions (only
// direct engine callers can pass them: the processor's config analysis
// rejects statements) compile as the body of an IIFE instead — the body runs
// the same, only its completion value is discarded.
func compileExpression(name, expression string) (*js_runtime.Program, error) {
	// The opening paren stays on the return's line (no ASI hazard); the
	// closing paren sits on its own line so trailing line comments do not
	// swallow it.
	p, err := js_runtime.Compile(name, "return (\n"+expression+"\n)")
	if err == nil {
		return p, nil
	}
	return js_runtime.Compile(name, "return (function(){\n"+expression+"\n})()")
}

// executeProgram runs prog on the shared runner under the 5-second watchdog:
// the runner interrupts the runtime when the deadline hits, discards runtimes
// that do not stop, and clears the globals this run bound — the reset the old
// pooled runtimes had. The watchdog's exact error text is preserved.
func (e *JSEngine) executeProgram(program *js_runtime.Program, globals map[string]interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), executionTimeout)
	defer cancel()

	val, err := e.runner.Run(ctx, program, globals)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, errExecutionTimeout
		}
		return nil, err
	}
	if val == nil {
		// null and undefined both export to nil; so does a missing
		// completion value. Same check the old ExportType() == nil path made.
		return nil, errNilResult
	}
	return val, nil
}

// ValidateExpression validates a JavaScript expression syntax
func (e *JSEngine) ValidateExpression(expression string) error {
	_, err := compileExpression("validation", expression)
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
	program, err := compileExpression("expression", expression)
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
	result, err := e.runExpression(expression)
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

	// Validate all variables first
	for name := range variables {
		if !e.isValidSourceVariable(name) {
			return JSExecutionResult{
				Success: false,
				Error:   fmt.Sprintf("Invalid variable name '%s'. Valid variables are: %v", name, e.sourceNames),
			}
		}
	}

	// Execute the expression with the variables bound on the shared runner
	result, err := e.runExpressionWithVariables(expression, variables)
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

// runExpression executes a compiled JavaScript expression without variables.
func (e *JSEngine) runExpression(expression string) (interface{}, error) {
	// Get the compiled program
	e.compiledMutex.RLock()
	program, exists := e.compiledExpressions[expression]
	e.compiledMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("JavaScript expression '%s' not compiled. Please call CompileExpression first", expression)
	}

	return e.executeProgram(program, nil)
}

// runExpressionWithVariables executes a compiled JavaScript expression with
// the given variables bound as globals for the run. The runner clears them
// again before pooling the runtime, the way PutJSRuntime used to.
func (e *JSEngine) runExpressionWithVariables(expression string, variables map[string]interface{}) (interface{}, error) {
	// Get the compiled program
	e.compiledMutex.RLock()
	program, exists := e.compiledExpressions[expression]
	e.compiledMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("JavaScript expression '%s' not compiled. Please call CompileExpression first", expression)
	}

	return e.executeProgram(program, variables)
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
func (e *JSEngine) PrecompileExpressions(staticMappings map[string]config.MappingInfo, dynamicMappings map[string]config.MappingInfo) error {
	e.precompiledMutex.Lock()
	defer e.precompiledMutex.Unlock()

	e.logger.Debug("Pre-compiling JavaScript expressions for performance optimization")

	// Pre-compile static mappings
	for virtualPath, mapping := range staticMappings {
		program, err := compileExpression(
			fmt.Sprintf("static-%s", virtualPath),
			mapping.Expression)
		if err != nil {
			return fmt.Errorf("failed to pre-compile static mapping '%s' with expression '%s': %w. Please check syntax and try again", virtualPath, mapping.Expression, err)
		}
		e.precompiledPrograms[mapping.Expression] = program
	}

	// Pre-compile dynamic mappings
	for virtualPath, mapping := range dynamicMappings {
		program, err := compileExpression(
			fmt.Sprintf("dynamic-%s", virtualPath),
			mapping.Expression)
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
	result, err := e.executeProgram(program, nil)
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

	// Validate all variables first
	for name := range variables {
		if !e.isValidSourceVariable(name) {
			return JSExecutionResult{
				Success: false,
				Error:   fmt.Sprintf("Invalid variable name '%s'. Valid variables are: %v", name, e.sourceNames),
			}
		}
	}

	// Execute pre-compiled program for optimal performance; the runner binds
	// the variables for the run and clears them again before pooling
	result, err := e.executeProgram(program, variables)
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

// Close cleans up the JavaScript engine resources
func (e *JSEngine) Close() error {
	// Clear compiled expressions
	e.compiledMutex.Lock()
	e.compiledExpressions = make(map[string]*js_runtime.Program)
	e.compiledMutex.Unlock()

	// Clear static expression cache
	e.staticCacheMutex.Lock()
	e.staticExpressionCache = make(map[string]JSExecutionResult)
	e.staticCacheMutex.Unlock()

	// Clear pre-compiled programs
	e.precompiledMutex.Lock()
	e.precompiledPrograms = make(map[string]*js_runtime.Program)
	e.precompiledMutex.Unlock()

	// Release the runner's pooled runtimes
	return e.runner.Close()
}
