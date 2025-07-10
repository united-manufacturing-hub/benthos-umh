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
	"bytes"
	"encoding/json"
	"strings"
	"sync"

	"github.com/dop251/goja"
)

// ObjectPools provides pooled objects to reduce memory allocations
type ObjectPools struct {
	// Metadata map pool for message metadata
	metadataPool sync.Pool

	// Variable context pool for JavaScript execution
	variableContextPool sync.Pool

	// Byte buffer pool for JSON operations
	byteBufferPool sync.Pool

	// String builder pool for topic construction
	stringBuilderPool sync.Pool

	// JavaScript runtime pool for dynamic expressions
	jsRuntimePool sync.Pool

	// JSON encoder pool for marshaling operations
	jsonEncoderPool sync.Pool

	// JSON decoder pool for unmarshaling operations
	jsonDecoderPool sync.Pool

	// Topic cache for frequently constructed topics
	topicCache sync.Map

	// Source names for runtime configuration
	sourceNames []string
}

// NewObjectPools creates a new object pools instance
func NewObjectPools(sourceNames []string) *ObjectPools {
	pools := &ObjectPools{
		sourceNames: sourceNames,
	}

	// Initialize metadata map pool
	pools.metadataPool.New = func() interface{} {
		return make(map[string]string, 8) // Pre-allocate for common metadata count
	}

	// Initialize variable context pool
	pools.variableContextPool.New = func() interface{} {
		return make(map[string]interface{}, 16) // Pre-allocate for common variable count
	}

	// Initialize byte buffer pool
	pools.byteBufferPool.New = func() interface{} {
		return make([]byte, 0, 256) // Pre-allocate 256 bytes for typical JSON payloads
	}

	// Initialize string builder pool
	pools.stringBuilderPool.New = func() interface{} {
		return &strings.Builder{}
	}

	// Initialize JavaScript runtime pool
	pools.jsRuntimePool.New = func() interface{} {
		runtime := goja.New()
		pools.configureRuntime(runtime)
		return runtime
	}

	// Initialize JSON encoder pool
	pools.jsonEncoderPool.New = func() interface{} {
		buf := make([]byte, 0, 256)
		return json.NewEncoder(bytes.NewBuffer(buf))
	}

	// Initialize JSON decoder pool
	pools.jsonDecoderPool.New = func() interface{} {
		return json.NewDecoder(bytes.NewReader(nil))
	}

	return pools
}

// configureRuntime sets up a Goja runtime with security constraints
func (p *ObjectPools) configureRuntime(runtime *goja.Runtime) {
	// Set maximum call stack size to prevent stack overflow from deep recursion
	runtime.SetMaxCallStackSize(1000)

	// Disable dangerous globals
	dangerousGlobals := []string{
		"require", "module", "exports", "process", "__dirname", "__filename",
		"global", "globalThis", "Function", "eval", "console",
		"setTimeout", "setInterval", "setImmediate", "clearTimeout", "clearInterval", "clearImmediate",
		"__proto__", "__defineGetter__", "__defineSetter__", "__lookupGetter__", "__lookupSetter__", "constructor",
		"defineProperty", "getOwnPropertyDescriptor", "getPrototypeOf", "setPrototypeOf",
		"escape", "unescape", "import", "importScripts",
	}

	// Create a function that explains why APIs are disabled
	securityBlocker := func(apiName string) func(goja.FunctionCall) goja.Value {
		return func(call goja.FunctionCall) goja.Value {
			panic(runtime.NewTypeError("'" + apiName + "' is disabled for security reasons in this sandboxed environment"))
		}
	}

	// Replace dangerous global objects with security blocker functions
	for _, global := range dangerousGlobals {
		_ = runtime.Set(global, securityBlocker(global))
	}
}

// GetMetadataMap gets a metadata map from the pool
func (p *ObjectPools) GetMetadataMap() map[string]string {
	return p.metadataPool.Get().(map[string]string)
}

// PutMetadataMap returns a metadata map to the pool after clearing it
func (p *ObjectPools) PutMetadataMap(m map[string]string) {
	// Clear the map but keep the underlying storage
	for k := range m {
		delete(m, k)
	}
	p.metadataPool.Put(m)
}

// GetVariableContext gets a variable context map from the pool
func (p *ObjectPools) GetVariableContext() map[string]interface{} {
	return p.variableContextPool.Get().(map[string]interface{})
}

// PutVariableContext returns a variable context to the pool after clearing it
func (p *ObjectPools) PutVariableContext(ctx map[string]interface{}) {
	// Clear the map but keep the underlying storage
	for k := range ctx {
		delete(ctx, k)
	}
	p.variableContextPool.Put(ctx)
}

// GetByteBuffer gets a byte buffer from the pool
func (p *ObjectPools) GetByteBuffer() []byte {
	buf := p.byteBufferPool.Get().([]byte)
	return buf[:0] // Reset length but keep capacity
}

// PutByteBuffer returns a byte buffer to the pool
func (p *ObjectPools) PutByteBuffer(buf []byte) {
	// Only pool buffers that aren't too large to prevent memory bloat
	if cap(buf) <= 4096 {
		p.byteBufferPool.Put(buf)
	}
}

// GetStringBuilder gets a string builder from the pool
func (p *ObjectPools) GetStringBuilder() *strings.Builder {
	sb := p.stringBuilderPool.Get().(*strings.Builder)
	sb.Reset() // Clear previous content
	return sb
}

// PutStringBuilder returns a string builder to the pool
func (p *ObjectPools) PutStringBuilder(sb *strings.Builder) {
	// Only pool builders that aren't too large
	if sb.Cap() <= 1024 {
		p.stringBuilderPool.Put(sb)
	}
}

// GetJSRuntime gets a JavaScript runtime from the pool
func (p *ObjectPools) GetJSRuntime() *goja.Runtime {
	runtime := p.jsRuntimePool.Get().(*goja.Runtime)
	return runtime
}

// PutJSRuntime returns a JavaScript runtime to the pool after clearing variables
func (p *ObjectPools) PutJSRuntime(runtime *goja.Runtime) {
	// Clear all variables from the runtime to prevent contamination
	for _, sourceName := range p.sourceNames {
		_ = runtime.Set(sourceName, goja.Undefined())
	}

	// Also clear common temporary variables that might have been set
	tempVars := []string{"result", "temp", "value", "data", "input", "output"}
	for _, tempVar := range tempVars {
		_ = runtime.Set(tempVar, goja.Undefined())
	}

	p.jsRuntimePool.Put(runtime)
}

// MarshalJSON marshals a value to JSON using pooled buffer - optimized for simple structures
func (p *ObjectPools) MarshalJSON(v interface{}) ([]byte, error) {
	// For simple structures like TimeseriesMessage, direct json.Marshal is often faster
	// than the pooling overhead. Use the standard library.
	return json.Marshal(v)
}

// UnmarshalJSON unmarshals JSON bytes into a value using pooled decoder
func (p *ObjectPools) UnmarshalJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// GetTopic gets a cached topic or constructs a new one
func (p *ObjectPools) GetTopic(outputTopic, dataContract, virtualPath string) string {
	// Create cache key efficiently
	sb := p.GetStringBuilder()
	sb.WriteString(outputTopic)
	sb.WriteByte('.')
	sb.WriteString(dataContract)
	sb.WriteByte('.')
	sb.WriteString(virtualPath)
	cacheKey := sb.String()
	p.PutStringBuilder(sb)

	// Check cache first
	if cached, ok := p.topicCache.Load(cacheKey); ok {
		return cached.(string)
	}

	// Construct and cache the topic
	topic := cacheKey // We already constructed it above
	p.topicCache.Store(cacheKey, topic)
	return topic
}

// ClearTopicCache clears the topic cache (useful for config changes)
func (p *ObjectPools) ClearTopicCache() {
	p.topicCache.Range(func(key, value interface{}) bool {
		p.topicCache.Delete(key)
		return true
	})
}
