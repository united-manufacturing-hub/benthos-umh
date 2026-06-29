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

// Acquire returns a refcounted Cache for key, calling build on the first use.
// The underlying store closes when the last reference is Closed.
func Acquire(key string, build func() (Cache, error)) (Cache, error) {
	registryMu.Lock()
	defer registryMu.Unlock()

	e, ok := registry[key]
	if ok {
		e.refs++
		return &shared{Cache: e.cache, entry: e, key: key}, nil
	}

	c, err := build()
	if err != nil {
		return nil, err
	}
	e = &refEntry{cache: c, refs: 1}
	registry[key] = e
	return &shared{Cache: c, entry: e, key: key}, nil
}

var (
	registryMu sync.Mutex
	registry   = map[string]*refEntry{}
)

type refEntry struct {
	cache Cache
	refs  int
}

type shared struct {
	Cache
	entry    *refEntry
	key      string
	closeOne sync.Once
}

func (s *shared) Close() error {
	var err error
	s.closeOne.Do(func() {
		registryMu.Lock()
		defer registryMu.Unlock()
		s.entry.refs--
		if s.entry.refs == 0 {
			delete(registry, s.key)
			err = s.entry.cache.Close()
		}
	})
	return err
}
