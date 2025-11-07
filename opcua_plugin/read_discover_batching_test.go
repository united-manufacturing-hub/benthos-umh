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

package opcua_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
)

var _ = Describe("calculateBatches", func() {
	Context("with Prosys profile MaxBatchSize=800", func() {
		It("should split 1500 nodes into 2 batches [800, 700]", func() {
			batches := CalculateBatches(1500, 800)
			Expect(batches).To(HaveLen(2))
			Expect(batches[0].End - batches[0].Start).To(Equal(800))
			Expect(batches[1].End - batches[1].Start).To(Equal(700))
		})
	})

	Context("with Ignition profile MaxBatchSize=1000", func() {
		It("should split 2500 nodes into 3 batches [1000, 1000, 500]", func() {
			batches := CalculateBatches(2500, 1000)
			Expect(batches).To(HaveLen(3))
			Expect(batches[0].End - batches[0].Start).To(Equal(1000))
			Expect(batches[1].End - batches[1].Start).To(Equal(1000))
			Expect(batches[2].End - batches[2].Start).To(Equal(500))
		})
	})

	Context("edge cases", func() {
		It("should handle exact multiple (2000 nodes, 1000 batch)", func() {
			batches := CalculateBatches(2000, 1000)
			Expect(batches).To(HaveLen(2))
			Expect(batches[0].End - batches[0].Start).To(Equal(1000))
			Expect(batches[1].End - batches[1].Start).To(Equal(1000))
		})

		It("should handle single small batch (50 nodes, 100 batch)", func() {
			batches := CalculateBatches(50, 100)
			Expect(batches).To(HaveLen(1))
			Expect(batches[0].End - batches[0].Start).To(Equal(50))
		})

		It("should handle empty list (0 nodes)", func() {
			batches := CalculateBatches(0, 100)
			Expect(batches).To(HaveLen(0))
		})

		It("should handle single node (1 node)", func() {
			batches := CalculateBatches(1, 100)
			Expect(batches).To(HaveLen(1))
			Expect(batches[0].End - batches[0].Start).To(Equal(1))
		})
	})

	Context("batch range indices", func() {
		It("should produce correct start/end indices for 250 nodes with batch size 100", func() {
			batches := CalculateBatches(250, 100)
			Expect(batches).To(HaveLen(3))

			// Batch 1: [0, 100)
			Expect(batches[0].Start).To(Equal(0))
			Expect(batches[0].End).To(Equal(100))

			// Batch 2: [100, 200)
			Expect(batches[1].Start).To(Equal(100))
			Expect(batches[1].End).To(Equal(200))

			// Batch 3: [200, 250)
			Expect(batches[2].Start).To(Equal(200))
			Expect(batches[2].End).To(Equal(250))
		})

		It("should produce non-overlapping consecutive ranges", func() {
			batches := CalculateBatches(1500, 800)

			// Verify no gaps between batches
			for i := 1; i < len(batches); i++ {
				Expect(batches[i].Start).To(Equal(batches[i-1].End),
					"Batch %d start should equal batch %d end", i, i-1)
			}

			// Verify full coverage
			Expect(batches[0].Start).To(Equal(0))
			Expect(batches[len(batches)-1].End).To(Equal(1500))
		})
	})
})
