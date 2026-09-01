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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/cache"
)

// storeFactory constructs a fresh Cache per spec. Both MemoryStore and BboltStore run every case.
type storeFactory struct {
	name string
	make func(ttl time.Duration) cache.Cache
}

func newFactories() []storeFactory {
	return []storeFactory{
		{
			name: "MemoryStore",
			make: func(ttl time.Duration) cache.Cache {
				return cache.NewMemoryStore(ttl)
			},
		},
		{
			name: "BboltStore",
			make: func(ttl time.Duration) cache.Cache {
				dir, err := os.MkdirTemp("", "cache-*")
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() { _ = os.RemoveAll(dir) })

				s, err := cache.NewBboltStore(filepath.Join(dir, "cache.db"), ttl)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() { _ = s.Close() })
				return s
			},
		},
	}
}

// payload builds a Payload with a fixed Watermark for round-trip specs where timing is not the point.
func payload(v any, ts int64) cache.Payload {
	return cache.Payload{Value: v, Watermark: ts}
}

var _ = Describe("Cache interface", func() {
	ctx := context.Background()

	for _, f := range newFactories() {
		f := f
		Context(f.name, func() {
			var store cache.Cache

			BeforeEach(func() {
				store = f.make(0)
			})

			Describe("Get on missing key", func() {
				It("returns (Payload{}, false)", func() {
					v, ok := store.Get(ctx, "missing")
					Expect(ok).To(BeFalse())
					Expect(v.Value).To(BeNil())
					Expect(v.Watermark).To(Equal(int64(0)))
				})
			})

			DescribeTable("Set + Get round-trips for JSON-compatible types",
				func(key string, value any, matcher OmegaMatcher) {
					Expect(store.Set(ctx, key, payload(value, 1))).To(Succeed())
					v, ok := store.Get(ctx, key)
					Expect(ok).To(BeTrue())
					Expect(v.Value).To(matcher)
					Expect(v.Watermark).To(Equal(int64(1)))
				},
				Entry("string", "s", "hello", Equal("hello")),
				Entry("boolean true", "b1", true, BeTrue()),
				Entry("boolean false", "b2", false, BeFalse()),
				Entry("map", "obj", map[string]any{"foo": "bar"}, Equal(map[string]any{"foo": "bar"})),
				Entry("explicit nil", "null", nil, BeNil()),
			)

			It("stores numeric value (both backends yield the same numeric via toInt)", func() {
				Expect(store.Set(ctx, "n", payload(42, 1))).To(Succeed())
				v, ok := store.Get(ctx, "n")
				Expect(ok).To(BeTrue())
				Expect(toInt(v.Value)).To(Equal(int64(42)))
			})

			It("Delete removes a key", func() {
				Expect(store.Set(ctx, "k", payload("v", 1))).To(Succeed())
				Expect(store.Delete(ctx, "k")).To(Succeed())
				_, ok := store.Get(ctx, "k")
				Expect(ok).To(BeFalse())
			})

			It("Delete on missing key is a no-op", func() {
				Expect(store.Delete(ctx, "nope")).To(Succeed())
			})

			DescribeTable("rejects empty key",
				func(call func(cache.Cache) error) {
					err := call(store)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("key must not be empty"))
				},
				Entry("Set", func(s cache.Cache) error {
					return s.Set(ctx, "", payload("v", 1))
				}),
				Entry("Delete", func(s cache.Cache) error {
					return s.Delete(ctx, "")
				}),
			)

			It("is safe under concurrent Set + Get", func() {
				const goroutines = 50
				var wg sync.WaitGroup
				wg.Add(goroutines * 2)
				for i := 0; i < goroutines; i++ {
					i := i
					go func() {
						defer wg.Done()
						_ = store.Set(ctx, "shared", payload(1, int64(i+1)))
					}()
					go func() {
						defer wg.Done()
						store.Get(ctx, "shared")
					}()
				}
				wg.Wait()
			})

			Describe("Set timestamp ordering", func() {
				It("accepts a monotonic stream and keeps the latest value", func() {
					for i, ts := range []int64{100, 101, 102, 103} {
						Expect(store.Set(ctx, "k", payload(i+1, ts))).To(Succeed())
					}
					v, ok := store.Get(ctx, "k")
					Expect(ok).To(BeTrue())
					Expect(toInt(v.Value)).To(Equal(int64(4)))
					Expect(v.Watermark).To(Equal(int64(103)))
				})

				It("drops an older-timestamp write with ErrOldWatermark", func() {
					Expect(store.Set(ctx, "k", payload("newer", 200))).To(Succeed())
					err := store.Set(ctx, "k", payload("older", 150))
					Expect(err).To(MatchError(cache.ErrOldWatermark))
					var stale *cache.StaleWriteError
					Expect(errors.As(err, &stale)).To(BeTrue())
					Expect(stale.Key).To(Equal("k"))
					Expect(stale.Incoming).To(Equal(int64(150)))
					Expect(stale.Stored).To(Equal(int64(200)))
					v, ok := store.Get(ctx, "k")
					Expect(ok).To(BeTrue())
					Expect(v.Value).To(Equal("newer"))
					Expect(v.Watermark).To(Equal(int64(200)))
				})

				It("drops a replay (equal timestamp) with ErrOldWatermark", func() {
					Expect(store.Set(ctx, "k", payload("first", 500))).To(Succeed())
					err := store.Set(ctx, "k", payload("replay", 500))
					Expect(err).To(MatchError(cache.ErrOldWatermark))
					v, ok := store.Get(ctx, "k")
					Expect(ok).To(BeTrue())
					Expect(v.Value).To(Equal("first"))
				})

				It("gates per-key (different keys are independent)", func() {
					Expect(store.Set(ctx, "a", payload("A@200", 200))).To(Succeed())
					Expect(store.Set(ctx, "b", payload("B@100", 100))).To(Succeed())
					vA, _ := store.Get(ctx, "a")
					vB, _ := store.Get(ctx, "b")
					Expect(vA.Value).To(Equal("A@200"))
					Expect(vB.Value).To(Equal("B@100"))
				})

				It("handles a burst of out-of-order arrivals and settles on the newest", func() {
					arrivals := []struct {
						ts    int64
						value string
					}{
						{300, "v@300"},
						{100, "v@100"},
						{400, "v@400"},
						{200, "v@200"},
						{350, "v@350"},
						{500, "v@500"},
					}
					accepted, dropped := 0, 0
					for _, a := range arrivals {
						err := store.Set(ctx, "k", payload(a.value, a.ts))
						switch {
						case err == nil:
							accepted++
						case errors.Is(err, cache.ErrOldWatermark):
							dropped++
						default:
							Fail("unexpected error: " + err.Error())
						}
					}
					Expect(accepted).To(Equal(3), "300, 400, 500 accepted")
					Expect(dropped).To(Equal(3), "100, 200, 350 dropped")

					v, ok := store.Get(ctx, "k")
					Expect(ok).To(BeTrue())
					Expect(v.Value).To(Equal("v@500"))
					Expect(v.Watermark).To(Equal(int64(500)))
				})

				It("Delete clears the gate: subsequent Set at any timestamp is accepted", func() {
					Expect(store.Set(ctx, "k", payload("high", 1000))).To(Succeed())
					Expect(store.Delete(ctx, "k")).To(Succeed())
					Expect(store.Set(ctx, "k", payload("reset", 50))).To(Succeed())
					v, ok := store.Get(ctx, "k")
					Expect(ok).To(BeTrue())
					Expect(v.Value).To(Equal("reset"))
					Expect(v.Watermark).To(Equal(int64(50)))
				})
			})

			Describe("expiration", func() {
				It("expires items after the configured duration", func() {
					expStore := f.make(50 * time.Millisecond)
					Expect(expStore.Set(ctx, "k", payload("v", 1))).To(Succeed())

					v, ok := expStore.Get(ctx, "k")
					Expect(ok).To(BeTrue())
					Expect(v.Value).To(Equal("v"))

					time.Sleep(100 * time.Millisecond)

					_, ok = expStore.Get(ctx, "k")
					Expect(ok).To(BeFalse())
				})

				It("with 0 expiration never expires", func() {
					Expect(store.Set(ctx, "k", payload("v", 1))).To(Succeed())
					time.Sleep(10 * time.Millisecond)
					v, ok := store.Get(ctx, "k")
					Expect(ok).To(BeTrue())
					Expect(v.Value).To(Equal("v"))
				})
			})
		})
	}
})

// toInt normalises MemoryStore's native ints and BboltStore's json.Number round-trip.
func toInt(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case json.Number:
		i, err := n.Int64()
		Expect(err).NotTo(HaveOccurred())
		return i
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		Expect(err).NotTo(HaveOccurred())
		return i
	default:
		Fail("toInt: unexpected type")
		return 0
	}
}
