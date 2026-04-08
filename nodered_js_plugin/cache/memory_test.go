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

package cache_test

import (
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/cache"
)

var _ = Describe("MemoryStore", func() {
	var store *cache.MemoryStore

	BeforeEach(func() {
		store = cache.NewMemoryStore()
	})

	Describe("Get on a missing key", func() {
		It("returns false", func() {
			_, ok := store.Get("missing")
			Expect(ok).To(BeFalse())
		})

		It("returns nil value", func() {
			v, _ := store.Get("missing")
			Expect(v).To(BeNil())
		})
	})

	DescribeTable("Set then Get round-trips",
		func(key string, value any, matcher OmegaMatcher) {
			store.Set(key, value)
			v, ok := store.Get(key)
			Expect(ok).To(BeTrue())
			Expect(v).To(matcher)
		},
		Entry("string value", "k", "hello", Equal("hello")),
		Entry("numeric value", "n", float64(42), Equal(float64(42))),
		Entry("boolean true", "b", true, BeTrue()),
		Entry("boolean false", "b2", false, BeFalse()),
		Entry("map value", "obj", map[string]any{"foo": "bar"}, Equal(map[string]any{"foo": "bar"})),
		Entry("explicit nil value", "null", nil, BeNil()),
	)

	It("overwrites an existing key", func() {
		store.Set("k", "first")
		store.Set("k", "second")
		v, ok := store.Get("k")
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal("second"))
	})

	It("deletes an existing key", func() {
		store.Set("k", "v")
		store.Delete("k")
		_, ok := store.Get("k")
		Expect(ok).To(BeFalse())
	})

	It("delete is a no-op for a missing key", func() {
		Expect(func() { store.Delete("nope") }).NotTo(Panic())
	})

	It("is safe for concurrent Set and Get", func() {
		const goroutines = 50
		var wg sync.WaitGroup
		wg.Add(goroutines * 2)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				store.Set("shared", 1)
			}()
			go func() {
				defer wg.Done()
				store.Get("shared") //nolint:errcheck
			}()
		}
		wg.Wait()
	})

	It("satisfies the Store interface", func() {
		var _ cache.Store = cache.NewMemoryStore()
	})
})
