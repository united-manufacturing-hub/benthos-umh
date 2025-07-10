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
	"strings"
	"sync"
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

	// Topic cache for frequently constructed topics
	topicCache sync.Map
}

// NewObjectPools creates a new object pools instance
func NewObjectPools() *ObjectPools {
	pools := &ObjectPools{}

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
