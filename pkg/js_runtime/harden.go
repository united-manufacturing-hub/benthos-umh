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

package js_runtime

import (
	"fmt"

	"github.com/dop251/goja"
)

// jsCallStackSize is the maximum goja call stack depth, ported as-is from
// stream_processor_plugin/constants.JSCallStackSize.
const jsCallStackSize = 1000

// dangerousGlobals is the blocklist the stream processor has always installed.
// Ported as-is from stream_processor_plugin/constants.DangerousGlobals
// (ENG-8128). Known, do not rediscover: this blocklist does NOT hold —
// shadowing the global Function still leaves
// Function.prototype.constructor reachable (goja builtin_function.go), and
// the async, generator and async-generator prototypes give three more
// routes. A caller that sets Harden gets exactly what stream_processor had,
// not a sandbox that works.
var dangerousGlobals = []string{
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
	"console", // Console output (the stream processor blocks it; nodered_js does not set Harden)

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

// harden installs the blocklist above on a fresh runtime, replacing each
// listed global with a function that throws a TypeError when called. Ported
// as-is from stream_processor_plugin/js_security.ConfigureJSRuntime, minus
// the log lines (a Runner has no logger; the Options type is frozen and has
// no place for one).
func harden(vm *goja.Runtime) {
	vm.SetMaxCallStackSize(jsCallStackSize)
	for _, name := range dangerousGlobals {
		blocker := func(_ goja.FunctionCall) goja.Value {
			panic(vm.NewTypeError(fmt.Sprintf("'%s' is disabled for security reasons in this sandboxed environment", name)))
		}
		// A Set on the global object only fails on a corrupted runtime; the
		// blocklist is best-effort hardening (see dangerousGlobals above).
		_ = vm.Set(name, blocker)
	}
}
