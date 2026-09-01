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
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/cache"
)

// Bbolt-only specs. Interface-level behavior (Set/Get/Delete, timestamp gating,
// expiration, empty-key errors) is exercised for both stores in cache_test.go.
var _ = Describe("BboltStore bbolt-specific", func() {
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

	It("empty path errors on NewBboltStore", func() {
		_, err := cache.NewBboltStore("", 0)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("path must not be empty"))
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
		Expect(store.Set(ctx, "k", cache.Payload{Value: "persisted", Watermark: 1})).To(Succeed())
		Expect(store.Close()).To(Succeed())
		store = nil

		reopened, err := cache.NewBboltStore(path, 0)
		Expect(err).NotTo(HaveOccurred())
		defer reopened.Close()

		v, ok := reopened.Get(ctx, "k")
		Expect(ok).To(BeTrue())
		Expect(v.Value).To(Equal("persisted"))
	})

	It("Close is idempotent", func() {
		Expect(store.Close()).To(Succeed())
		Expect(store.Close()).To(Succeed())
		Expect(store.Close()).To(Succeed())
		store = nil
	})

	DescribeTable("ctx cancellation",
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
				return nil, false, s.Set(c, "k", cache.Payload{Value: "v", Watermark: 1})
			},
			context.Canceled, false),
		Entry("Delete returns ctx.Err",
			func(s *cache.BboltStore, c context.Context) (any, bool, error) {
				return nil, false, s.Delete(c, "k")
			},
			context.Canceled, false),
		Entry("Get returns (Payload{},false) with no error",
			func(s *cache.BboltStore, c context.Context) (any, bool, error) {
				v, ok := s.Get(c, "k")
				return v.Value, ok, nil
			},
			nil, false),
	)
})
