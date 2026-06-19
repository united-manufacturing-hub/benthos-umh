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

package timescaledb_historian_plugin

import "sync"

// DedupCache is the persistent, per-process metadata fingerprint cache. A batch
// gets a working view over it; the view is promoted only after the batch commits.
type DedupCache struct {
	mu        sync.Mutex
	committed map[string]string // cache key -> fingerprint
}

func NewDedupCache() *DedupCache {
	return &DedupCache{committed: make(map[string]string)}
}

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
func (v *BatchView) ShouldEmit(key, fingerprint string) bool {
	if fp, seen := v.working[key]; seen {
		v.working[key] = fingerprint
		return fp != fingerprint
	}
	v.parent.mu.Lock()
	prior, ok := v.parent.committed[key]
	v.parent.mu.Unlock()
	v.working[key] = fingerprint
	return !ok || prior != fingerprint
}

// Commit promotes every working-set entry into the committed cache.
func (v *BatchView) Commit() {
	v.parent.mu.Lock()
	defer v.parent.mu.Unlock()
	for k, fp := range v.working {
		v.parent.committed[k] = fp
	}
}
