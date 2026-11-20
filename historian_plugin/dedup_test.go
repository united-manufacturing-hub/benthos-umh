// Copyright 2026 UMH Systems GmbH
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

package historian_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tsh "github.com/united-manufacturing-hub/benthos-umh/historian_plugin"
)

var _ = Describe("DedupCache", func() {
	It("emits first, dedups an identical fingerprint within the same batch", func() {
		v := tsh.NewDedupCache().NewBatch()
		Expect(v.ShouldEmit("k", "fp1")).To(BeTrue())
		Expect(v.ShouldEmit("k", "fp1")).To(BeFalse())
	})
	It("re-emits a changed fingerprint within the same batch", func() {
		v := tsh.NewDedupCache().NewBatch()
		Expect(v.ShouldEmit("k", "fp1")).To(BeTrue())
		Expect(v.ShouldEmit("k", "fp2")).To(BeTrue())
	})
	It("persists across batches only after Commit", func() {
		c := tsh.NewDedupCache()
		v1 := c.NewBatch()
		Expect(v1.ShouldEmit("k", "fp1")).To(BeTrue())
		v1.Commit()
		v2 := c.NewBatch()
		Expect(v2.ShouldEmit("k", "fp1")).To(BeFalse())
	})
	It("re-emits if the batch is discarded (rollback)", func() {
		c := tsh.NewDedupCache()
		v1 := c.NewBatch()
		Expect(v1.ShouldEmit("k", "fp1")).To(BeTrue())
		// no Commit -> rollback
		v2 := c.NewBatch()
		Expect(v2.ShouldEmit("k", "fp1")).To(BeTrue())
	})
})
