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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// bboltBucket is the single bucket used by BboltStore.
var bboltBucket = []byte("cache")

// bboltJanitorInterval is how often expired entries are swept from disk.
const bboltJanitorInterval = 1 * time.Hour

// BboltStore is a disk-backed implementation of Cache using bbolt.
// State survives process restarts. One file per processor instance.
type BboltStore struct {
	db                *bolt.DB
	defaultExpiration time.Duration
	stop              chan struct{}
	closeOnce         sync.Once
}

var _ Cache = (*BboltStore)(nil)

// NewBboltStore opens (or creates) a bbolt file at path and returns a Cache
// backed by it. defaultExpiration of 0 disables expiration entirely.
func NewBboltStore(path string, defaultExpiration time.Duration) (*BboltStore, error) {
	if path == "" {
		return nil, fmt.Errorf("cache: bbolt path must not be empty")
	}

	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("cache: expand home in %q: %w", path, err)
		}
		path = filepath.Join(home, path[1:])
	}

	db, err := bolt.Open(path, 0o600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("cache: open bbolt at %q: %w", path, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(bboltBucket)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("cache: create bucket: %w", err)
	}

	s := &BboltStore{
		db:                db,
		defaultExpiration: defaultExpiration,
		stop:              make(chan struct{}),
	}

	if defaultExpiration > 0 {
		go s.runJanitor()
	}

	return s, nil
}

func (s *BboltStore) Set(ctx context.Context, key string, value any) error {
	err := ctx.Err()
	if err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("cache: key must not be empty")
	}

	var expiration int64
	if s.defaultExpiration > 0 {
		expiration = time.Now().Add(s.defaultExpiration).UnixNano()
	}

	data, err := json.Marshal(Item{Value: value, Expiration: expiration})
	if err != nil {
		return fmt.Errorf("cache: encode value: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bboltBucket)
		if b == nil {
			return fmt.Errorf("cache: bucket missing")
		}
		return b.Put([]byte(key), data)
	})
}

func (s *BboltStore) Get(ctx context.Context, key string) (any, bool) {
	if ctx.Err() != nil {
		return nil, false
	}

	var (
		raw   []byte
		found bool
	)

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bboltBucket)
		if b == nil {
			return nil
		}
		v := b.Get([]byte(key))
		if v == nil {
			return nil
		}
		// Copy bytes out of tx — value is invalid after View returns.
		raw = append(raw, v...)
		found = true
		return nil
	})
	if err != nil || !found {
		return nil, false
	}

	var item Item
	if err := json.Unmarshal(raw, &item); err != nil {
		return nil, false
	}
	if item.Expired() {
		return nil, false
	}
	return item.Value, true
}

func (s *BboltStore) Update(ctx context.Context, key string, fn func(old any, exists bool) (any, error)) error {
	err := ctx.Err()
	if err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("cache: key must not be empty")
	}
	if fn == nil {
		return fmt.Errorf("cache: update fn must not be nil")
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bboltBucket)
		if b == nil {
			return fmt.Errorf("cache: bucket missing")
		}

		var old any
		exists := false
		raw := b.Get([]byte(key))
		if raw != nil {
			var item Item
			err := json.Unmarshal(raw, &item)
			if err == nil && !item.Expired() {
				old = item.Value
				exists = true
			}
		}

		newVal, err := fn(old, exists)
		if err != nil {
			return err
		}

		var expiration int64
		if s.defaultExpiration > 0 {
			expiration = time.Now().Add(s.defaultExpiration).UnixNano()
		}
		data, err := json.Marshal(Item{Value: newVal, Expiration: expiration})
		if err != nil {
			return fmt.Errorf("cache: encode value: %w", err)
		}
		return b.Put([]byte(key), data)
	})
}

func (s *BboltStore) Stats(ctx context.Context) (Stats, error) {
	err := ctx.Err()
	if err != nil {
		return Stats{}, err
	}

	var stats Stats
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bboltBucket)
		if b == nil {
			return nil
		}
		stats.Keys = int64(b.Stats().KeyN)
		return nil
	})
	if err != nil {
		return stats, err
	}

	fi, err := os.Stat(s.db.Path())
	if err == nil {
		stats.DiskBytes = fi.Size()
	}
	return stats, nil
}

func (s *BboltStore) Delete(ctx context.Context, key string) error {
	err := ctx.Err()
	if err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("cache: key must not be empty")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bboltBucket)
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})
}

// Close stops the janitor and closes the underlying bbolt file. Safe to call
// multiple times.
func (s *BboltStore) Close() error {
	var dbErr error
	s.closeOnce.Do(func() {
		close(s.stop)
		dbErr = s.db.Close()
	})
	return dbErr
}

func (s *BboltStore) runJanitor() {
	ticker := time.NewTicker(bboltJanitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.deleteExpired()
		case <-s.stop:
			return
		}
	}
}

func (s *BboltStore) deleteExpired() {
	// Phase 1: collect expired keys in a read-only tx.
	var expired [][]byte
	now := time.Now().UnixNano()

	_ = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bboltBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var item Item
			if err := json.Unmarshal(v, &item); err != nil {
				return nil
			}
			if item.Expiration > 0 && now > item.Expiration {
				kc := make([]byte, len(k))
				copy(kc, k)
				expired = append(expired, kc)
			}
			return nil
		})
	})

	if len(expired) == 0 {
		return
	}

	// Phase 2: delete in a single write tx.
	_ = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bboltBucket)
		if b == nil {
			return nil
		}
		for _, k := range expired {
			_ = b.Delete(k)
		}
		return nil
	})
}
