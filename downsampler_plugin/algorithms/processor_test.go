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
			keep, err := processor.ProcessPoint(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First point should be kept")

			// Integer conversion
			keep, err = processor.ProcessPoint(10, baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeFalse(), "Small change should be dropped")

			// Float32 conversion
			keep, err = processor.ProcessPoint(float32(11.0), baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Large change should be kept")
		})

		It("should handle boolean values with change-based logic", func() {
			// First boolean value should be kept
			keep, err := processor.ProcessPoint(true, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())

			// Same boolean value should be dropped
			keep, err = processor.ProcessPoint(true, baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeFalse())

			// Changed boolean value should be kept
			keep, err = processor.ProcessPoint(false, baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())
		})

		It("should handle string values with change-based logic", func() {
			// First string value should be kept
			keep, err := processor.ProcessPoint("running", baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())

			// Same string value should be dropped
			keep, err = processor.ProcessPoint("running", baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeFalse())

			// Changed string value should be kept
			keep, err = processor.ProcessPoint("stopped", baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())
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
				keep, err := processor.ProcessPoint(10.0, baseTime.Add(2*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue())

				// Out-of-order point should be dropped
				keep, err = processor.ProcessPoint(11.0, baseTime.Add(1*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeFalse())
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
				keep, err := processor.ProcessPoint(10.0, baseTime.Add(2*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue())

				// Out-of-order point should be passed through to algorithm
				keep, err = processor.ProcessPoint(11.0, baseTime.Add(1*time.Second))
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

		It("should include out-of-order handling in metadata", func() {
			metadata := processor.GetMetadata()
			Expect(metadata).To(ContainSubstring("deadband"))
			Expect(metadata).To(ContainSubstring("out_of_order_handling=drop"))
		})

		It("should return correct algorithm name", func() {
			name := processor.GetName()
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
			keep, err := processor.ProcessPoint(complex(1, 2), baseTime)
			Expect(err).To(HaveOccurred())
			Expect(keep).To(BeFalse())
		})
	})

	Describe("swinging door algorithm integration", func() {
		BeforeEach(func() {
			config := algorithms.ProcessorConfig{
				Algorithm: "swinging_door",
				AlgorithmConfig: map[string]interface{}{
					"threshold": 1.0,
					"min_time":  "100ms",
				},
				PassThrough: false,
			}
			var err error
			processor, err = algorithms.NewProcessorWrapper(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should work with swinging door algorithm", func() {
			// First point should always be kept
			keep, err := processor.ProcessPoint(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())

			// Test integer conversion with SDT
			keep, err = processor.ProcessPoint(11, baseTime.Add(200*time.Millisecond))
			Expect(err).NotTo(HaveOccurred())
			// SDT logic determines result

			// Test boolean handling
			keep, err = processor.ProcessPoint(true, baseTime.Add(300*time.Millisecond))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First boolean should be kept")

			// Test string handling
			keep, err = processor.ProcessPoint("state1", baseTime.Add(400*time.Millisecond))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First string should be kept")

			keep, err = processor.ProcessPoint("state1", baseTime.Add(500*time.Millisecond))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeFalse(), "Repeated string should be dropped")
		})
	})
})
