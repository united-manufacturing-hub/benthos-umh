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

package algorithms_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

var _ = Describe("ProcessorWrapper", func() {
	var processor *algorithms.ProcessorWrapper
	var baseTime time.Time

	BeforeEach(func() {
		baseTime = time.Now()
	})

	Describe("basic functionality", func() {
		BeforeEach(func() {
			config := algorithms.ProcessorConfig{
				Algorithm: "deadband",
				AlgorithmConfig: map[string]interface{}{
					"threshold": 0.5,
				},
				PassThrough: false,
			}
			var err error
			processor, err = algorithms.NewProcessorWrapper(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle numeric types with conversion", func() {
			// Test various numeric types
			points, err := processor.Ingest(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1), "First point should be kept")

			// Integer conversion
			points, err = processor.Ingest(10, baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(BeEmpty(), "Small change should be dropped")

			// Float32 conversion
			points, err = processor.Ingest(float32(11.0), baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1), "Large change should be kept")
		})

		It("should handle boolean values with change-based logic", func() {
			// First boolean value should be kept
			points, err := processor.Ingest(true, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1))

			// Same boolean value should be dropped
			points, err = processor.Ingest(true, baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(BeEmpty())

			// Changed boolean value should be kept
			points, err = processor.Ingest(false, baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1))
		})

		It("should handle string values with change-based logic", func() {
			// First string value should be kept
			points, err := processor.Ingest("running", baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1))

			// Same string value should be dropped
			points, err = processor.Ingest("running", baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(BeEmpty())

			// Changed string value should be kept
			points, err = processor.Ingest("stopped", baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1))
		})
	})

	Describe("out-of-order handling", func() {
		Context("with PassThrough=false (drop)", func() {
			BeforeEach(func() {
				config := algorithms.ProcessorConfig{
					Algorithm: "deadband",
					AlgorithmConfig: map[string]interface{}{
						"threshold": 0.5,
					},
					PassThrough: false,
				}
				var err error
				processor, err = algorithms.NewProcessorWrapper(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should drop out-of-order data", func() {
				// First point
				points, err := processor.Ingest(10.0, baseTime.Add(2*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))

				// Out-of-order point should be dropped
				points, err = processor.Ingest(11.0, baseTime.Add(1*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(BeEmpty())
			})
		})

		Context("with PassThrough=true", func() {
			BeforeEach(func() {
				config := algorithms.ProcessorConfig{
					Algorithm: "deadband",
					AlgorithmConfig: map[string]interface{}{
						"threshold": 0.5,
					},
					PassThrough: true,
				}
				var err error
				processor, err = algorithms.NewProcessorWrapper(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should pass through out-of-order data to algorithm", func() {
				// First point
				points, err := processor.Ingest(10.0, baseTime.Add(2*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))

				// Out-of-order point should be passed through to algorithm
				_, err = processor.Ingest(11.0, baseTime.Add(1*time.Second))
				Expect(err).NotTo(HaveOccurred())
				// Result depends on algorithm logic, just verify no error
			})
		})
	})

	Describe("metadata", func() {
		BeforeEach(func() {
			config := algorithms.ProcessorConfig{
				Algorithm: "deadband",
				AlgorithmConfig: map[string]interface{}{
					"threshold": 1.0,
				},
				PassThrough: false,
			}
			var err error
			processor, err = algorithms.NewProcessorWrapper(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should include algorithm configuration in metadata", func() {
			config := processor.Config()
			Expect(config).To(ContainSubstring("deadband"))
		})

		It("should return correct algorithm name", func() {
			name := processor.Name()
			Expect(name).To(Equal("deadband"))
		})
	})

	Describe("error handling", func() {
		BeforeEach(func() {
			config := algorithms.ProcessorConfig{
				Algorithm: "deadband",
				AlgorithmConfig: map[string]interface{}{
					"threshold": 0.5,
				},
				PassThrough: false,
			}
			var err error
			processor, err = algorithms.NewProcessorWrapper(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject unsupported types", func() {
			// Test complex number (not supported)
			points, err := processor.Ingest(complex(1, 2), baseTime)
			Expect(err).To(HaveOccurred())
			Expect(points).To(BeEmpty())
		})
	})
})
