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

package pools

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"

	"github.com/dop251/goja"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/constants"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/js_security"
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

	logger *service.Logger
}

// NewObjectPools creates a new object pools instance
func NewObjectPools(sourceNames []string, logger *service.Logger) *ObjectPools {
	pools := &ObjectPools{
		sourceNames: sourceNames,
		logger:      logger,
	}

	// Initialize metadata map pool
	pools.metadataPool.New = func() interface{} {
		return make(map[string]string, 8) // Pre-allocate for common metadata count
	}

	// Initialize variable context pool
	pools.variableContextPool.New = func() interface{} {
		return make(map[string]interface{}, 16) // Pre-allocate for common variable count
	}

	// Initialize byte buffer pool with pointer to avoid allocations
	pools.byteBufferPool.New = func() interface{} {
		buf := make([]byte, 0, constants.DefaultByteBufferSize) // Pre-allocate for typical JSON payloads
		return &buf
	}

	// Initialize string builder pool
	pools.stringBuilderPool.New = func() interface{} {
		return &strings.Builder{}
	}

	// Initialize JavaScript runtime pool
	pools.jsRuntimePool.New = func() interface{} {
		runtime := goja.New()
		js_security.ConfigureJSRuntime(runtime, logger)
		return runtime
	}

	// Initialize JSON encoder pool
	pools.jsonEncoderPool.New = func() interface{} {
		buf := make([]byte, 0, constants.DefaultByteBufferSize)
		return json.NewEncoder(bytes.NewBuffer(buf))
	}

	// Initialize JSON decoder pool
	pools.jsonDecoderPool.New = func() interface{} {
		return json.NewDecoder(bytes.NewReader(nil))
	}

	return pools
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
	bufPtr := p.byteBufferPool.Get().(*[]byte)
	*bufPtr = (*bufPtr)[:0] // Reset length but keep capacity
	return *bufPtr
}

// PutByteBuffer returns a byte buffer to the pool
func (p *ObjectPools) PutByteBuffer(buf []byte) {
	// Only pool buffers that aren't too large to prevent memory bloat
	if cap(buf) <= constants.MaxPooledBufferSize {
		p.byteBufferPool.Put(&buf)
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
	if sb.Cap() <= constants.MaxPooledStringBuilderSize {
		p.stringBuilderPool.Put(sb)
	}
}

// GetJSRuntime gets a JavaScript runtime from the pool
func (p *ObjectPools) GetJSRuntime() *goja.Runtime {
	runtime := p.jsRuntimePool.Get().(*goja.Runtime)
	return runtime
}

// PutJSRuntime returns a JavaScript runtime to the pool after clearing variables
// If cleanup fails, the runtime is discarded to prevent contamination
func (p *ObjectPools) PutJSRuntime(runtime *goja.Runtime) {
	cleanupSuccessful := true

	// Clear all variables from the runtime to prevent contamination
	for _, sourceName := range p.sourceNames {
		if err := runtime.Set(sourceName, goja.Undefined()); err != nil {
			// Log cleanup failure - this is critical for pool hygiene
			if p.logger != nil {
				p.logger.Warnf("Failed to clear source variable '%s' from JS runtime during cleanup: %v", sourceName, err)
			}
			cleanupSuccessful = false
		}
	}

	// Also clear common temporary variables that might have been set
	tempVars := []string{"result", "temp", "value", "data", "input", "output"}
	for _, tempVar := range tempVars {
		if err := runtime.Set(tempVar, goja.Undefined()); err != nil {
			// Log cleanup failure - this is critical for pool hygiene
			if p.logger != nil {
				p.logger.Warnf("Failed to clear temporary variable '%s' from JS runtime during cleanup: %v", tempVar, err)
			}
			cleanupSuccessful = false
		}
	}

	// Clear any interrupt state to ensure clean state for next use
	runtime.ClearInterrupt()

	// Only return to pool if cleanup was successful
	if cleanupSuccessful {
		p.jsRuntimePool.Put(runtime)
	} else {
		// Discard the contaminated runtime - pool will create a new one when needed
		if p.logger != nil {
			p.logger.Warnf("Discarding JS runtime due to cleanup failures - pool will create a new runtime when needed")
		}
		// Don't put the runtime back in the pool - let it be garbage collected
	}
}

// MarshalToJSON marshals a value to JSON using pooled buffer - optimized for simple structures
func (p *ObjectPools) MarshalToJSON(v interface{}) ([]byte, error) {
	// For simple structures like TimeseriesMessage, direct json.Marshal is often faster
	// than the pooling overhead. Use the standard library.
	return json.Marshal(v)
}

// UnmarshalFromJSON unmarshals JSON bytes into a value using pooled decoder
func (p *ObjectPools) UnmarshalFromJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// GetTopic gets a cached topic or constructs a new one
func (p *ObjectPools) GetTopic(outputTopic string, dataContract string, virtualPath string) string {
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
	p.topicCache.Range(func(key, _ interface{}) bool {
		p.topicCache.Delete(key)
		return true
	})
}
