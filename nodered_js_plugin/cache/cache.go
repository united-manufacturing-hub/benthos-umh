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

package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Stats reports the current size of a Cache.
type Stats struct {
	Keys      int64
	DiskBytes int64
}

// Payload carries Watermark so Set can drop replayed / out-of-order writes.
type Payload struct {
	Value     any
	Watermark int64
}

// Cache is used as the caching interface for nodered_js.
type Cache interface {
	// Set writes payload only when payload.Watermark is strictly newer than the stored one.
	Set(ctx context.Context, key string, payload Payload) error
	// Get returns the stored Payload (value + watermark) and whether the key exists.
	Get(ctx context.Context, key string) (Payload, bool)
	// Delete removes the entry for key. No-op when key does not exist.
	Delete(ctx context.Context, key string) error
	// Lock holds a per-cache mutex across a multi-step operation.
	Lock()
	// Unlock releases the mutex acquired by Lock.
	Unlock()
	// Begin opens a batch scope so subsequent Set/Get/Delete run inside one commit.
	Begin(ctx context.Context) error
	// Commit closes the batch, flushing pending writes atomically.
	Commit(ctx context.Context) error
	// Stats reports the current key count and on-disk size.
	Stats(ctx context.Context) (Stats, error)
	// Close releases any resources held by the store.
	Close() error
}

// New resolves a plugin's cache config to a shared Cache instance via the registry.
func New(backend string, name string, path string, ttl time.Duration) (Cache, error) {
	switch backend {
	case "memory":
		if name == "" {
			return NewMemoryStore(ttl), nil
		}
		return Acquire("mem:"+name, func() (Cache, error) {
			return NewMemoryStore(ttl), nil
		})
	case "persistent":
		var absPath string
		if path != "" {
			expanded := path
			if strings.HasPrefix(expanded, "~") {
				home, err := os.UserHomeDir()
				if err != nil {
					return nil, fmt.Errorf("expand cache.path %q: %w", path, err)
				}
				expanded = filepath.Join(home, expanded[1:])
			}
			abs, err := filepath.Abs(expanded)
			if err != nil {
				return nil, fmt.Errorf("resolve cache.path %q: %w", path, err)
			}
			absPath = abs
		}
		key := "bbolt:name:" + name
		if name == "" {
			if absPath == "" {
				return nil, fmt.Errorf("cache.path is required when cache.name is empty")
			}
			key = "bbolt:path:" + absPath
		}
		return Acquire(key, func() (Cache, error) {
			if absPath == "" {
				return nil, fmt.Errorf("cache %q has not been opened yet; the first processor that uses this name must define cache.path", name)
			}
			return NewBboltStore(absPath, ttl)
		})
	default:
		return nil, fmt.Errorf("unsupported cache.backend %q (want 'memory' or 'persistent')", backend)
	}
}
