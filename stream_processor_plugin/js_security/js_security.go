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

package js_security

import (
	"fmt"
	"time"

	"github.com/dop251/goja"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/constants"
)

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
	runtime.SetMaxCallStackSize(constants.JSCallStackSize)

	// Create security blocker function
	securityBlocker := CreateSecurityBlocker(runtime, logger)

	// Replace dangerous global objects with security blocker functions
	for _, global := range constants.DangerousGlobals {
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
		return func(_ goja.FunctionCall) goja.Value {
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
	if logger == nil {
		return
	}
	if duration > constants.SlowExecutionThreshold {
		logger.Warnf("Slow JavaScript execution: %s took %v", expression, duration)
	}
}
