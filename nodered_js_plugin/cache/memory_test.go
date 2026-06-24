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
	"context"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/cache"
)

var _ = Describe("MemoryStore", func() {
	var store *cache.MemoryStore
	ctx := context.Background()

	BeforeEach(func() {
		store = cache.NewMemoryStore(0)
	})

	Describe("Get on a missing key", func() {
		It("returns false", func() {
			_, ok := store.Get(ctx, "missing")
			Expect(ok).To(BeFalse())
		})

		It("returns nil value", func() {
			v, _ := store.Get(ctx, "missing")
			Expect(v).To(BeNil())
		})
	})

	DescribeTable("Set then Get round-trips",
		func(key string, value any, matcher OmegaMatcher) {
			Expect(store.Set(context.Background(), key, value)).To(Succeed())
			v, ok := store.Get(context.Background(), key)
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
		Expect(store.Set(ctx, "k", "first")).To(Succeed())
		Expect(store.Set(ctx, "k", "second")).To(Succeed())
		v, ok := store.Get(ctx, "k")
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal("second"))
	})

	It("deletes an existing key", func() {
		Expect(store.Set(ctx, "k", "v")).To(Succeed())
		Expect(store.Delete(ctx, "k")).To(Succeed())
		_, ok := store.Get(ctx, "k")
		Expect(ok).To(BeFalse())
	})

	It("delete is a no-op for a missing key", func() {
		Expect(store.Delete(ctx, "nope")).To(Succeed())
	})

	It("is safe for concurrent Set and Get", func() {
		const goroutines = 50
		var wg sync.WaitGroup
		wg.Add(goroutines * 2)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				store.Set(ctx, "shared", 1)
			}()
			go func() {
				defer wg.Done()
				store.Get(ctx, "shared")
			}()
		}
		wg.Wait()
	})

	It("returns error for empty key on Set and Delete", func() {
		err := store.Set(ctx, "", "value")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("key must not be empty"))

		err = store.Delete(ctx, "")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("key must not be empty"))
	})

	Describe("Update", func() {
		DescribeTable(
			"reads old + writes new atomically",
			func(preset, returnValue, expectStored any) {
				hasPreset := preset != nil
				if hasPreset {
					Expect(store.Set(ctx, "k", preset)).To(Succeed())
				}

				err := store.Update(ctx, "k", func(old any, exists bool) (any, error) {
					Expect(exists).To(Equal(hasPreset))
					if hasPreset {
						Expect(old).To(Equal(preset))
					} else {
						Expect(old).To(BeNil())
					}
					return returnValue, nil
				})
				Expect(err).NotTo(HaveOccurred())

				v, ok := store.Get(ctx, "k")
				Expect(ok).To(BeTrue())
				Expect(v).To(Equal(expectStored))
			},
			Entry("missing key: fn sees nil/false and writes new value",
				nil, float64(1), float64(1)),
			Entry("existing numeric key: fn sees old and writes increment",
				float64(5), float64(6), float64(6)),
			Entry("existing string key: fn sees string and rewrites",
				"before", "after", "after"),
			Entry("existing object: fn returns mutated copy",
				map[string]any{"n": float64(1)},
				map[string]any{"n": float64(2)},
				map[string]any{"n": float64(2)}),
		)

		boom := fmt.Errorf("boom")

		DescribeTable(
			"failure modes leave store unchanged",
			func(useCanceledCtx bool, key string, fn func(any, bool) (any, error), wantErr error, wantSubstr string) {
				Expect(store.Set(ctx, "k", "before")).To(Succeed())

				cctx := ctx
				if useCanceledCtx {
					c, cancel := context.WithCancel(context.Background())
					cancel()
					cctx = c
				}

				err := store.Update(cctx, key, fn)
				Expect(err).To(HaveOccurred())
				if wantErr != nil {
					Expect(err).To(MatchError(wantErr))
				}
				if wantSubstr != "" {
					Expect(err.Error()).To(ContainSubstring(wantSubstr))
				}

				if key == "k" {
					v, ok := store.Get(ctx, "k")
					Expect(ok).To(BeTrue())
					Expect(v).To(Equal("before"))
				}
			},
			Entry("empty key",
				false, "", func(any, bool) (any, error) { return nil, nil },
				nil, "key must not be empty"),
			Entry("nil fn",
				false, "k", (func(any, bool) (any, error))(nil),
				nil, "update fn must not be nil"),
			Entry("fn returns error",
				false, "k", func(any, bool) (any, error) { return "after", boom },
				boom, ""),
			Entry("ctx canceled",
				true, "k", func(any, bool) (any, error) { return "after", nil },
				context.Canceled, ""),
		)

		It("is atomic under concurrent updaters on the same key", func() {
			const goroutines = 200
			var wg sync.WaitGroup
			wg.Add(goroutines)

			for range goroutines {
				go func() {
					defer wg.Done()
					_ = store.Update(ctx, "counter", func(old any, exists bool) (any, error) {
						var n float64
						if exists {
							n, _ = old.(float64)
						}
						return n + 1, nil
					})
				}()
			}
			wg.Wait()

			v, ok := store.Get(ctx, "counter")
			Expect(ok).To(BeTrue())
			Expect(v).To(Equal(float64(goroutines)), "expected exactly N increments, lost updates indicate a race")
		})
	})

	Describe("expiration", func() {
		It("expires items after the default expiration", func() {
			expStore := cache.NewMemoryStore(50 * time.Millisecond)
			defer expStore.Close()

			Expect(expStore.Set(ctx, "k", "v")).To(Succeed())

			v, ok := expStore.Get(ctx, "k")
			Expect(ok).To(BeTrue())
			Expect(v).To(Equal("v"))

			time.Sleep(100 * time.Millisecond)

			_, ok = expStore.Get(ctx, "k")
			Expect(ok).To(BeFalse())
		})

		It("does not expire items when expiration is 0", func() {
			Expect(store.Set(ctx, "k", "v")).To(Succeed())
			time.Sleep(10 * time.Millisecond)

			v, ok := store.Get(ctx, "k")
			Expect(ok).To(BeTrue())
			Expect(v).To(Equal("v"))
		})
	})
})
