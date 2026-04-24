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
	"sync"
	"time"
)

// Item wraps a cached value with an optional expiration timestamp.
type Item struct {
	Value      any
	Expiration int64 // UnixNano; 0 means no expiration
}

// Expired returns true if the item has a set expiration that is in the past.
func (item Item) Expired() bool {
	if item.Expiration == 0 {
		return false
	}
	return time.Now().UnixNano() > item.Expiration
}

// MemoryStore is used as key/value store for the first cache implementation.
type MemoryStore struct {
	mu                sync.RWMutex
	items             map[string]Item
	defaultExpiration time.Duration
	janitor           *janitor
	closeOnce         sync.Once
}

var _ Cache = (*MemoryStore)(nil)

// NewMemoryStore returns a ready-to-use, empty MemoryStore.
func NewMemoryStore(defaultExpiration time.Duration) *MemoryStore {
	m := &MemoryStore{
		items:             make(map[string]Item),
		defaultExpiration: defaultExpiration,
	}
	if defaultExpiration > 0 {
		j := newJanitor(1 * time.Hour)
		m.janitor = j
		go j.run(m)
	}
	return m
}

func (m *MemoryStore) Set(_ context.Context, key string, value any) error {
	if key == "" {
		return fmt.Errorf("cache: key must not be empty")
	}
	var expiration int64
	if m.defaultExpiration > 0 {
		expiration = time.Now().Add(m.defaultExpiration).UnixNano()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = Item{
		Value:      value,
		Expiration: expiration,
	}
	return nil
}

func (m *MemoryStore) Get(_ context.Context, key string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item, ok := m.items[key]
	if !ok {
		return nil, false
	}
	if item.Expired() {
		return nil, false
	}
	return item.Value, true
}

func (m *MemoryStore) Exists(ctx context.Context, key string) bool {
	_, ok := m.Get(ctx, key)
	return ok
}

func (m *MemoryStore) Delete(_ context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("cache: key must not be empty")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
	return nil
}

// Close stops the janitor and releases resources.
func (m *MemoryStore) Close() error {
	m.closeOnce.Do(func() {
		if m.janitor != nil {
			close(m.janitor.stop)
		}
	})
	return nil
}

func (m *MemoryStore) deleteExpired() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, item := range m.items {
		if item.Expired() {
			delete(m.items, k)
		}
	}
}

type janitor struct {
	interval time.Duration
	stop     chan struct{}
}

func newJanitor(interval time.Duration) *janitor {
	return &janitor{
		interval: interval,
		stop:     make(chan struct{}),
	}
}

func (j *janitor) run(m *MemoryStore) {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.deleteExpired()
		case <-j.stop:
			return
		}
	}
}
