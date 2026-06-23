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
	"path/filepath"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/cache"
)

var _ = Describe("BboltStore", func() {
	var (
		store *cache.BboltStore
		path  string
		ctx   context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		path = filepath.Join(GinkgoT().TempDir(), "test.db")
		var err error
		store, err = cache.NewBboltStore(path, 0)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if store != nil {
			_ = store.Close()
		}
	})

	DescribeTable(
		"Set then Get round-trips for JSON-compatible types",
		func(key string, value any, matcher OmegaMatcher) {
			Expect(store.Set(ctx, key, value)).To(Succeed())
			v, ok := store.Get(ctx, key)
			Expect(ok).To(BeTrue())
			Expect(v).To(matcher)
		},
		Entry("string", "s", "hello", Equal("hello")),
		Entry("number", "n", float64(42), Equal(float64(42))),
		Entry("boolean true", "b", true, BeTrue()),
		Entry("boolean false", "b2", false, BeFalse()),
		Entry("map", "obj", map[string]any{"foo": "bar"}, Equal(map[string]any{"foo": "bar"})),
		Entry("nested", "nest", map[string]any{"k": []any{float64(1), float64(2)}},
			Equal(map[string]any{"k": []any{float64(1), float64(2)}})),
		Entry("explicit nil", "null", nil, BeNil()),
	)

	DescribeTable(
		"rejects invalid arguments",
		func(call func(*cache.BboltStore) error, substr string) {
			err := call(store)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(substr))
		},
		Entry("Set with empty key",
			func(s *cache.BboltStore) error { return s.Set(context.Background(), "", "v") },
			"key must not be empty"),
		Entry("Delete with empty key",
			func(s *cache.BboltStore) error { return s.Delete(context.Background(), "") },
			"key must not be empty"),
	)

	It("empty path errors on NewBboltStore", func() {
		_, err := cache.NewBboltStore("", 0)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("path must not be empty"))
	})

	It("Get on missing key returns false", func() {
		_, ok := store.Get(ctx, "missing")
		Expect(ok).To(BeFalse())
	})

	It("overwrites an existing key", func() {
		Expect(store.Set(ctx, "k", "first")).To(Succeed())
		Expect(store.Set(ctx, "k", "second")).To(Succeed())
		v, ok := store.Get(ctx, "k")
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal("second"))
	})

	It("Delete removes key", func() {
		Expect(store.Set(ctx, "k", "v")).To(Succeed())
		Expect(store.Delete(ctx, "k")).To(Succeed())
		_, ok := store.Get(ctx, "k")
		Expect(ok).To(BeFalse())
	})

	It("Delete missing key is a no-op", func() {
		Expect(store.Delete(ctx, "nope")).To(Succeed())
	})

	Describe("file lock", func() {
		It("opening the same file twice in the same process fails", func() {
			second, err := cache.NewBboltStore(path, 0)
			Expect(err).To(HaveOccurred(), "expected flock conflict on duplicate open")
			Expect(second).To(BeNil())
		})

		It("can open file after first store is closed", func() {
			Expect(store.Close()).To(Succeed())
			store = nil

			second, err := cache.NewBboltStore(path, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(second.Close()).To(Succeed())
		})
	})

	It("persists across close + reopen", func() {
		Expect(store.Set(ctx, "k", "persisted")).To(Succeed())
		Expect(store.Close()).To(Succeed())
		store = nil

		reopened, err := cache.NewBboltStore(path, 0)
		Expect(err).NotTo(HaveOccurred())
		defer reopened.Close()

		v, ok := reopened.Get(ctx, "k")
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal("persisted"))
	})

	It("Close is idempotent", func() {
		Expect(store.Close()).To(Succeed())
		Expect(store.Close()).To(Succeed())
		Expect(store.Close()).To(Succeed())
		store = nil
	})

	It("does not panic under parallel writers and readers", func() {
		const goroutines = 50
		var wg sync.WaitGroup
		wg.Add(goroutines * 2)

		for range goroutines {
			go func() {
				defer wg.Done()
				_ = store.Set(ctx, "shared", 1)
			}()
			go func() {
				defer wg.Done()
				store.Get(ctx, "shared")
			}()
		}
		wg.Wait()
	})

	DescribeTable(
		"ctx cancellation",
		func(call func(*cache.BboltStore, context.Context) (any, bool, error), wantErr error, wantOk bool) {
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()

			_, ok, err := call(store, cancelled)
			Expect(ok).To(Equal(wantOk))
			if wantErr == nil {
				Expect(err).NotTo(HaveOccurred())
			} else {
				Expect(err).To(MatchError(wantErr))
			}
		},
		Entry("Set returns ctx.Err",
			func(s *cache.BboltStore, c context.Context) (any, bool, error) {
				return nil, false, s.Set(c, "k", "v")
			},
			context.Canceled, false),
		Entry("Delete returns ctx.Err",
			func(s *cache.BboltStore, c context.Context) (any, bool, error) {
				return nil, false, s.Delete(c, "k")
			},
			context.Canceled, false),
		Entry("Get returns (nil,false) with no error",
			func(s *cache.BboltStore, c context.Context) (any, bool, error) {
				v, ok := s.Get(c, "k")
				return v, ok, nil
			},
			nil, false),
	)

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

				// "before" must still be there — failed Update must not write.
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

	DescribeTable(
		"expiration",
		func(expiration time.Duration, sleep time.Duration, wantOk bool) {
			Expect(store.Close()).To(Succeed())
			store = nil

			expStore, err := cache.NewBboltStore(path, expiration)
			Expect(err).NotTo(HaveOccurred())
			defer expStore.Close()

			Expect(expStore.Set(ctx, "k", "v")).To(Succeed())
			if sleep > 0 {
				time.Sleep(sleep)
			}

			_, ok := expStore.Get(ctx, "k")
			Expect(ok).To(Equal(wantOk))
		},
		Entry("expires after the configured duration",
			50*time.Millisecond, 100*time.Millisecond, false),
		Entry("survives within the configured duration",
			500*time.Millisecond, 10*time.Millisecond, true),
		Entry("0 expiration never expires",
			time.Duration(0), 50*time.Millisecond, true),
	)
})
