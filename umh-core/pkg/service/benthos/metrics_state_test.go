package benthos_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
)

var _ = Describe("MetricsState", Label("metrics_state"), func() {
	var (
		state *benthos.BenthosMetricsState
		tick  uint64
	)

	BeforeEach(func() {
		state = benthos.NewBenthosMetricsState()
		tick = 0
	})

	Context("NewBenthosMetricsState", func() {
		It("should initialize with empty processors map", func() {
			Expect(state.Processors).NotTo(BeNil())
			Expect(state.Processors).To(BeEmpty())
			Expect(state.LastTick).To(BeZero())
			Expect(state.IsActive).To(BeFalse())
		})
	})

	Context("UpdateFromMetrics", func() {
		It("should handle first update as baseline", func() {
			metrics := benthos.Metrics{
				Input: benthos.InputMetrics{
					Received: 100,
				},
				Output: benthos.OutputMetrics{
					Sent:      90,
					BatchSent: 10,
				},
			}

			state.UpdateFromMetrics(metrics, tick)
			tick++

			Expect(state.Input.LastCount).To(Equal(int64(100)))
			Expect(state.Input.MessagesPerTick).To(Equal(float64(100)))
			Expect(state.Output.LastCount).To(Equal(int64(90)))
			Expect(state.Output.MessagesPerTick).To(Equal(float64(90)))
			Expect(state.Output.LastBatchCount).To(Equal(int64(10)))
			Expect(state.Output.BatchesPerTick).To(Equal(float64(10)))
			Expect(state.LastTick).To(Equal(uint64(0)))
			Expect(state.IsActive).To(BeTrue())
		})

		It("should handle counter reset", func() {
			// First update
			state.UpdateFromMetrics(benthos.Metrics{
				Input: benthos.InputMetrics{
					Received: 100,
				},
			}, tick)
			tick++

			// Counter reset (new count lower than last count)
			state.UpdateFromMetrics(benthos.Metrics{
				Input: benthos.InputMetrics{
					Received: 50, // Reset to lower value
				},
			}, tick)
			tick++

			Expect(state.Input.LastCount).To(Equal(int64(50)))
			Expect(state.Input.MessagesPerTick).To(Equal(float64(50))) // Should treat as new baseline
		})

		It("should calculate rates correctly over multiple ticks", func() {
			// First update at tick 0
			state.UpdateFromMetrics(benthos.Metrics{
				Input: benthos.InputMetrics{
					Received: 100,
				},
				Output: benthos.OutputMetrics{
					Sent:      90,
					BatchSent: 10,
				},
			}, tick)
			tick++
			tick++

			// Second update with 2 ticks difference
			state.UpdateFromMetrics(benthos.Metrics{
				Input: benthos.InputMetrics{
					Received: 160, // +60 over 2 ticks = 30 per tick
				},
				Output: benthos.OutputMetrics{
					Sent:      140, // +50 over 2 ticks = 25 per tick
					BatchSent: 20,  // +10 over 2 ticks = 5 per tick
				},
			}, tick)
			tick++

			Expect(state.Input.MessagesPerTick).To(Equal(float64(30)))  // (160-100)/2
			Expect(state.Output.MessagesPerTick).To(Equal(float64(25))) // (140-90)/2
			Expect(state.Output.BatchesPerTick).To(Equal(float64(5)))   // (20-10)/2
		})

		It("should handle processor metrics", func() {
			metrics := benthos.Metrics{
				Process: benthos.ProcessMetrics{
					Processors: map[string]benthos.ProcessorMetrics{
						"proc1": {
							Sent:      100,
							BatchSent: 10,
						},
						"proc2": {
							Sent:      200,
							BatchSent: 20,
						},
					},
				},
			}

			state.UpdateFromMetrics(metrics, tick)
			tick++
			Expect(state.Processors).To(HaveLen(2))
			Expect(state.Processors["proc1"].LastCount).To(Equal(int64(100)))
			Expect(state.Processors["proc1"].LastBatchCount).To(Equal(int64(10)))
			Expect(state.Processors["proc2"].LastCount).To(Equal(int64(200)))
			Expect(state.Processors["proc2"].LastBatchCount).To(Equal(int64(20)))
		})

		It("should update activity status correctly", func() {
			// No activity
			state.UpdateFromMetrics(benthos.Metrics{}, tick)
			tick++
			Expect(state.IsActive).To(BeFalse())

			// Input activity
			state.UpdateFromMetrics(benthos.Metrics{
				Input: benthos.InputMetrics{
					Received: 100,
				},
			}, tick)
			tick++
			Expect(state.IsActive).To(BeTrue())

			// No new input activity (same count)
			state.UpdateFromMetrics(benthos.Metrics{
				Input: benthos.InputMetrics{
					Received: 100, // Same as last tick
				},
				Process: benthos.ProcessMetrics{
					Processors: map[string]benthos.ProcessorMetrics{
						"proc1": {
							Sent: 150, // Even with processor activity
						},
					},
				},
			}, tick)
			tick++
			Expect(state.IsActive).To(BeFalse())
			Expect(state.Input.MessagesPerTick).To(BeZero())

			// New input activity
			state.UpdateFromMetrics(benthos.Metrics{
				Input: benthos.InputMetrics{
					Received: 200, // New messages
				},
			}, tick)
			tick++
			Expect(state.IsActive).To(BeTrue())
		})
	})
})
