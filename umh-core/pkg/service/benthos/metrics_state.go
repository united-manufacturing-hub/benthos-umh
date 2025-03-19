package benthos

// ComponentThroughput tracks throughput metrics for a single component
type ComponentThroughput struct {
	// LastTick is the last tick when metrics were updated
	LastTick uint64
	// LastCount is the last message count seen
	LastCount int64
	// LastBatchCount is the last batch count seen
	LastBatchCount int64
	// MessagesPerTick is the number of messages processed per tick
	MessagesPerTick float64
	// BatchesPerTick is the number of batches processed per tick
	BatchesPerTick float64
}

// BenthosMetricsState tracks the state of Benthos metrics over time
type BenthosMetricsState struct {
	// Input tracks input throughput
	Input ComponentThroughput
	// Output tracks output throughput
	Output ComponentThroughput
	// Processors tracks processor throughput
	Processors map[string]ComponentThroughput
	// LastTick is the last tick when metrics were updated
	LastTick uint64
	// IsActive indicates if any component has shown activity in the last tick
	IsActive bool
}

// NewBenthosMetricsState creates a new BenthosMetricsState
func NewBenthosMetricsState() *BenthosMetricsState {
	return &BenthosMetricsState{
		Processors: make(map[string]ComponentThroughput),
	}
}

// UpdateFromMetrics updates the metrics state based on new metrics
func (s *BenthosMetricsState) UpdateFromMetrics(metrics Metrics, tick uint64) {
	// Update input throughput
	s.updateComponentThroughput(&s.Input, metrics.Input.Received, 0, tick)

	// Update output throughput
	s.updateComponentThroughput(&s.Output, metrics.Output.Sent, metrics.Output.BatchSent, tick)

	// Update processor throughput
	for path, processor := range metrics.Process.Processors {
		if _, exists := s.Processors[path]; !exists {
			s.Processors[path] = ComponentThroughput{}
		}
		throughput := s.Processors[path]
		s.updateComponentThroughput(&throughput, processor.Sent, processor.BatchSent, tick)
		s.Processors[path] = throughput
	}

	// Update last tick
	s.LastTick = tick

	// Update activity status
	s.IsActive = s.Input.MessagesPerTick > 0 || s.Output.MessagesPerTick > 0
	if !s.IsActive {
		for _, processor := range s.Processors {
			if processor.MessagesPerTick > 0 {
				s.IsActive = true
				break
			}
		}
	}
}

// updateComponentThroughput updates throughput metrics for a single component
func (s *BenthosMetricsState) updateComponentThroughput(throughput *ComponentThroughput, count, batchCount int64, tick uint64) {
	// If this is the first update or if the counter has reset (new count is lower than last count),
	// treat this as the baseline
	if throughput.LastTick == 0 || count < throughput.LastCount {
		throughput.LastCount = count
		throughput.LastBatchCount = batchCount
		throughput.MessagesPerTick = float64(count)
		throughput.BatchesPerTick = float64(batchCount)
	} else {
		// Calculate messages and batches per tick
		tickDiff := tick - throughput.LastTick
		if tickDiff > 0 {
			throughput.MessagesPerTick = float64(count-throughput.LastCount) / float64(tickDiff)
			throughput.BatchesPerTick = float64(batchCount-throughput.LastBatchCount) / float64(tickDiff)
		}
		throughput.LastCount = count
		throughput.LastBatchCount = batchCount
	}
	throughput.LastTick = tick
}
