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

type Item struct {
	Value      any   `json:"value"`
	Expiration int64 `json:"expiration"` // UnixNano; 0 = no expiration
	Watermark  int64 `json:"watermark"`  // caller-supplied; used by Set to reject replays
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
	serialMu          sync.Mutex
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

// Set drops the write silently when entry.Watermark is not strictly newer than the stored one.
func (m *MemoryStore) Set(_ context.Context, key string, entry Payload) error {
	if key == "" {
		return fmt.Errorf("cache: key must not be empty")
	}
	var expiration int64
	if m.defaultExpiration > 0 {
		expiration = time.Now().Add(m.defaultExpiration).UnixNano()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.items[key]; ok && !existing.Expired() {
		if entry.Watermark <= existing.Watermark {
			return &StaleWriteError{Key: key, Incoming: entry.Watermark, Stored: existing.Watermark}
		}
	}
	m.items[key] = Item{
		Value:      entry.Value,
		Expiration: expiration,
		Watermark:  entry.Watermark,
	}
	return nil
}

func (m *MemoryStore) Get(_ context.Context, key string) (Payload, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item, ok := m.items[key]
	if !ok {
		return Payload{}, false
	}
	if item.Expired() {
		return Payload{}, false
	}
	return Payload{Value: item.Value, Watermark: item.Watermark}, true
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

func (m *MemoryStore) Lock() {
	m.serialMu.Lock()
}

func (m *MemoryStore) Unlock() {
	m.serialMu.Unlock()
}

// Begin is a no-op; MemoryStore writes are atomic and the outer Lock scopes the batch.
func (m *MemoryStore) Begin(_ context.Context) error {
	return nil
}

// Commit is a no-op paired with Begin.
func (m *MemoryStore) Commit(_ context.Context) error {
	return nil
}

func (m *MemoryStore) Stats(_ context.Context) (Stats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Stats{Keys: int64(len(m.items))}, nil
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
