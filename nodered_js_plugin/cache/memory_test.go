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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/cache"
)

var _ = Describe("MemoryStore", func() {
	var store *cache.MemoryStore

	BeforeEach(func() {
		store = cache.NewMemoryStore(0)
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
			Expect(store.Set(key, value)).To(Succeed())
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
		Expect(store.Set("k", "first")).To(Succeed())
		Expect(store.Set("k", "second")).To(Succeed())
		v, ok := store.Get("k")
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal("second"))
	})

	It("deletes an existing key", func() {
		Expect(store.Set("k", "v")).To(Succeed())
		Expect(store.Delete("k")).To(Succeed())
		_, ok := store.Get("k")
		Expect(ok).To(BeFalse())
	})

	It("delete is a no-op for a missing key", func() {
		Expect(store.Delete("nope")).To(Succeed())
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
				store.Get("shared")
			}()
		}
		wg.Wait()
	})

	It("returns error for empty key on Set and Delete", func() {
		err := store.Set("", "value")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("key must not be empty"))

		err = store.Delete("")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("key must not be empty"))
	})

	Describe("expiration", func() {
		It("expires items after the default expiration", func() {
			expStore := cache.NewMemoryStore(50 * time.Millisecond)
			defer expStore.Close()

			Expect(expStore.Set("k", "v")).To(Succeed())

			v, ok := expStore.Get("k")
			Expect(ok).To(BeTrue())
			Expect(v).To(Equal("v"))

			time.Sleep(100 * time.Millisecond)

			_, ok = expStore.Get("k")
			Expect(ok).To(BeFalse())
		})

		It("does not expire items when expiration is 0", func() {
			Expect(store.Set("k", "v")).To(Succeed())
			time.Sleep(10 * time.Millisecond)

			v, ok := store.Get("k")
			Expect(ok).To(BeTrue())
			Expect(v).To(Equal("v"))
		})
	})
})
