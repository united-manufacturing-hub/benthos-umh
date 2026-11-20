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

// dedupCacheSize bounds the per-process fingerprint cache. Eviction is safe: an evicted
// entry re-emits at most one extra attribute row for that tag (at the next message's ts) --
// content-identical and harmless, not a conflict.
const dedupCacheSize = 100_000

// DedupCache is the per-process metadata fingerprint cache (key -> fingerprint). A batch
// works over a view that is promoted into the cache only after the batch commits.
type DedupCache struct {
	committed *lru.Cache[string, string]
}

// NewDedupCache returns an empty, LRU-bounded metadata fingerprint cache.
func NewDedupCache() *DedupCache {
	c, _ := lru.New[string, string](dedupCacheSize) // err only on size <= 0
	return &DedupCache{committed: c}
}

// Len reports the number of committed cache entries (exposed for the dedup-cache-size metric).
func (c *DedupCache) Len() int { return c.committed.Len() }

// Purge drops every committed fingerprint. Connect calls it on each (re)connect so a reconnect to a
// restored or recreated database does not keep suppressing attribute writes against fingerprints
// tied to the previous database. Safe to call concurrently with an in-flight batch: the LRU is
// synchronized, and a batch that commits after the purge simply re-adds its own (valid) entries.
func (c *DedupCache) Purge() { c.committed.Purge() }

// NewBatch starts a BatchView whose emit decisions are promoted into the cache only on Commit.
func (c *DedupCache) NewBatch() *BatchView {
	return &BatchView{parent: c, working: make(map[string]string)}
}

// BatchView accumulates a batch's emit decisions. Promote with Commit only after the
// transaction commits; on rollback it is discarded so a retried batch re-emits.
type BatchView struct {
	parent  *DedupCache
	working map[string]string
}

// ShouldEmit reports whether (key, fingerprint) needs an attribute write, recording it in the
// working set so a later same-key call in this batch dedups against it.
//
// Cross-batch dedup is best-effort. committed is promoted only after the batch commits (see
// Commit), so a rolled-back batch re-emits rather than losing the attribute row. The cost: under
// max_in_flight > 1 two concurrent batches with the same new (key, fingerprint) can both see a
// committed miss and both emit; at distinct ts the attribute PK (topic_id, ts) does not absorb
// them, so a metadata change can leave up to max_in_flight redundant but valid rows before
// committed settles. Deliberately not synchronized: a shared in-flight reservation would
// reintroduce the lost-row-on-rollback hazard that commit-time promotion prevents.
func (v *BatchView) ShouldEmit(key string, fingerprint string) bool {
	if fp, seen := v.working[key]; seen {
		v.working[key] = fingerprint
		return fp != fingerprint
	}
	prior, ok := v.parent.committed.Get(key)
	v.working[key] = fingerprint
	return !ok || prior != fingerprint
}

// Commit promotes this batch's emit decisions into the shared cache. Call it only after the
// transaction commits, so a rolled-back batch re-emits rather than losing an attribute row.
func (v *BatchView) Commit() {
	for k, fp := range v.working {
		v.parent.committed.Add(k, fp)
	}
}
