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

// Store is used as the caching interface for nodered_js.
type Store interface {
	// Set stores value under key, overwriting any existing entry.
	Set(key string, value any)
	// Get returns the value stored under key and if it even exists.
	Get(key string) (any, bool)
	// Delete removes the entry for key. No-op when key does not exist.
	Delete(key string)
	// Close releases any resources held by the store.
	Close() error
}
