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

var _ = Describe("Acquire", func() {
	ctx := context.Background()

	It("returns the same backing store for identical keys", func() {
		path := filepath.Join(GinkgoT().TempDir(), "shared.db")
		key := "bbolt:" + path

		a, err := cache.Acquire(key, func() (cache.Cache, error) {
			return cache.NewBboltStore(path, 0)
		})
		Expect(err).NotTo(HaveOccurred())
		defer a.Close()

		b, err := cache.Acquire(key, func() (cache.Cache, error) {
			Fail("build should not be called when entry already exists")
			return nil, nil
		})
		Expect(err).NotTo(HaveOccurred())
		defer b.Close()

		Expect(a.Set(ctx, "k", "v")).To(Succeed())
		v, ok := b.Get(ctx, "k")
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal("v"))
	})

	It("only closes the underlying store on the last release", func() {
		path := filepath.Join(GinkgoT().TempDir(), "shared.db")
		key := "bbolt:" + path

		a, err := cache.Acquire(key, func() (cache.Cache, error) {
			return cache.NewBboltStore(path, 0)
		})
		Expect(err).NotTo(HaveOccurred())

		b, err := cache.Acquire(key, func() (cache.Cache, error) {
			return nil, nil
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(a.Set(ctx, "k", "v")).To(Succeed())
		Expect(a.Close()).To(Succeed())

		v, ok := b.Get(ctx, "k")
		Expect(ok).To(BeTrue(), "underlying store closed too early")
		Expect(v).To(Equal("v"))

		Expect(b.Close()).To(Succeed())
	})

	It("rebuilds after the last reference is released", func() {
		path := filepath.Join(GinkgoT().TempDir(), "rebuild.db")
		key := "bbolt:" + path

		a, err := cache.Acquire(key, func() (cache.Cache, error) {
			return cache.NewBboltStore(path, 0)
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(a.Close()).To(Succeed())

		builds := 0
		b, err := cache.Acquire(key, func() (cache.Cache, error) {
			builds++
			return cache.NewBboltStore(path, 0)
		})
		Expect(err).NotTo(HaveOccurred())
		defer b.Close()
		Expect(builds).To(Equal(1))
	})

	It("returns the build error without registering an entry", func() {
		key := "bbolt:nonexistent-failure-key"

		_, err := cache.Acquire(key, func() (cache.Cache, error) {
			return cache.NewBboltStore("", 0)
		})
		Expect(err).To(HaveOccurred())

		builds := 0
		c, err := cache.Acquire(key, func() (cache.Cache, error) {
			builds++
			return cache.NewMemoryStore(0), nil
		})
		Expect(err).NotTo(HaveOccurred())
		defer c.Close()
		Expect(builds).To(Equal(1), "previous failure must not leave a stale registry entry")
	})
})
