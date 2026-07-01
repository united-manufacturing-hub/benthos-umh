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
// entry just re-emits one attribute row, which the SQL ON CONFLICT re-deduplicates.
const dedupCacheSize = 100_000

// DedupCache is the per-process metadata fingerprint cache (key -> fingerprint). A batch
// works over a view that is promoted into the cache only after the batch commits.
type DedupCache struct {
	committed *lru.Cache[string, string]
}

func NewDedupCache() *DedupCache {
	c, _ := lru.New[string, string](dedupCacheSize) // err only on size <= 0
	return &DedupCache{committed: c}
}

func (c *DedupCache) Len() int { return c.committed.Len() }

func (c *DedupCache) NewBatch() *BatchView {
	return &BatchView{parent: c, working: make(map[string]string)}
}

// BatchView accumulates a batch's emit decisions. Promote with Commit only after the
// transaction commits; on rollback it is discarded so a retried batch re-emits.
type BatchView struct {
	parent  *DedupCache
	working map[string]string
}

// ShouldEmit reports whether (key, fingerprint) needs an attribute write, recording it in
// the working set so a later same-key call in this batch dedups against it.
//
// Cross-batch dedup is best-effort by design. committed is promoted only after the batch's
// transaction commits (see Commit), so a rolled-back batch re-emits instead of losing the
// attribute row. Under max_in_flight > 1 benthos runs WriteBatch concurrently, so two batches
// carrying the same new (key, fingerprint) can both observe a committed miss before either
// commits and both emit. The attribute PK is (topic_id, ts) with ts the message timestamp, so
// ON CONFLICT does not absorb the second row: a metadata change can produce up to
// max_in_flight redundant -- but valid -- attribute rows before committed settles. This is a
// bounded, self-healing inefficiency, deliberately not synchronized. A shared in-flight
// reservation would reintroduce the lost-row-on-rollback hazard that promoting only after
// commit exists to prevent.
func (v *BatchView) ShouldEmit(key string, fingerprint string) bool {
	if fp, seen := v.working[key]; seen {
		v.working[key] = fingerprint
		return fp != fingerprint
	}
	prior, ok := v.parent.committed.Get(key)
	v.working[key] = fingerprint
	return !ok || prior != fingerprint
}

func (v *BatchView) Commit() {
	for k, fp := range v.working {
		v.parent.committed.Add(k, fp)
	}
}
