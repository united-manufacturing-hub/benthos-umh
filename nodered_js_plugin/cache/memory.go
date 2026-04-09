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

import "sync"

// MemoryStore is used as key/value store for the first cache implementation.
type MemoryStore struct {
	mu    sync.RWMutex
	items map[string]any
}

// NewMemoryStore returns a ready-to-use, empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{items: make(map[string]any)}
}

func (m *MemoryStore) Set(key string, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = value
}

func (m *MemoryStore) Get(key string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.items[key]
	return v, ok
}

func (m *MemoryStore) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
}

// Close is a no-op for MemoryStore (nothing to release).
func (m *MemoryStore) Close() error {
	return nil
}
