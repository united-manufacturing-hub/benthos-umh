// Copyright 2026 UMH Systems GmbH
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

package historian_plugin

import (
	lru "github.com/hashicorp/golang-lru/v2"
)

// dedupCacheSize caps the per-process fingerprint cache. The key embeds the
// location/virtual_path, so a source that churns those would grow an unbounded map;
// an LRU gives a hard ceiling. Eviction is safe: an evicted entry just re-emits one
// attribute row, which the SQL ON CONFLICT re-deduplicates.
const dedupCacheSize = 100_000

// DedupCache is the persistent, per-process metadata fingerprint cache. A batch
// gets a working view over it; the view is promoted only after the batch commits.
// The underlying LRU is its own synchronization, so no extra mutex is needed.
type DedupCache struct {
	committed *lru.Cache[string, string] // cache key -> fingerprint
}

func NewDedupCache() *DedupCache {
	c, _ := lru.New[string, string](dedupCacheSize) // err only on size <= 0
	return &DedupCache{committed: c}
}

// Len reports the number of cached fingerprints (exposed for a metrics gauge).
func (c *DedupCache) Len() int { return c.committed.Len() }

// NewBatch returns a working view seeded from the committed cache.
func (c *DedupCache) NewBatch() *BatchView {
	return &BatchView{parent: c, working: make(map[string]string)}
}

// BatchView accumulates this batch's emit decisions. Promote with Commit only
// after the batch transaction commits; discard it (let it be GC'd) on rollback
// so a retried batch re-emits.
type BatchView struct {
	parent  *DedupCache
	working map[string]string
}

// ShouldEmit reports whether this (key, fingerprint) needs an attribute write,
// updating the working set immediately so a later same-key call in this batch
// dedups against the earlier one. The committed cache covers prior batches.
func (v *BatchView) ShouldEmit(key string, fingerprint string) bool {
	if fp, seen := v.working[key]; seen {
		v.working[key] = fingerprint
		return fp != fingerprint
	}
	prior, ok := v.parent.committed.Get(key)
	v.working[key] = fingerprint
	return !ok || prior != fingerprint
}

// Commit promotes every working-set entry into the committed cache.
func (v *BatchView) Commit() {
	for k, fp := range v.working {
		v.parent.committed.Add(k, fp)
	}
}
