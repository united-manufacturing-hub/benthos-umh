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

package nodered_js_plugin

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// dedupState remembers the last maxLen message markers to suppress at-least-once retries.
type dedupState struct {
	mu     sync.Mutex
	seen   map[string]struct{}
	order  []string
	maxLen int
}

func newDedupState(maxLen int) *dedupState {
	return &dedupState{
		seen:   make(map[string]struct{}, maxLen),
		order:  make([]string, 0, maxLen),
		maxLen: maxLen,
	}
}

// CheckDedup returns true if the marker has already been seen so the caller can suppress side effects.
func (d *dedupState) CheckDedup(marker string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, ok := d.seen[marker]
	if ok {
		return true
	}

	d.seen[marker] = struct{}{}
	d.order = append(d.order, marker)

	for len(d.order) > d.maxLen {
		head := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, head)
	}

	return false
}

// dedupReplayMetaKey is used by wrapping processors (tag_processor) to persist the dedup verdict on each message so per-stage JS calls can honor it.
const dedupReplayMetaKey = "_umh_dedup_replay"

// markerFor hashes payload + sorted meta so the identity stays stable across Benthos retries.
func markerFor(msg *service.Message) (string, error) {
	payload, err := msg.AsBytes()
	if err != nil {
		return "", err
	}

	var keys []string
	err = msg.MetaWalkMut(func(k string, _ any) error {
		keys = append(keys, k)
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(keys)

	h := sha256.New()
	h.Write(payload)
	for _, k := range keys {
		v, _ := msg.MetaGet(k)
		h.Write([]byte(k))
		h.Write([]byte{'='})
		h.Write([]byte(fmt.Sprintf("%v", v)))
		h.Write([]byte{';'})
	}
	return hex.EncodeToString(h.Sum(nil)[:8]), nil
}
