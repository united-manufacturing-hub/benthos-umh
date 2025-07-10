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

package stream_processor_plugin

import (
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// ProcessorMetrics holds essential metrics for the stream processor
type ProcessorMetrics struct {
	// Core Processing Metrics
	MessagesProcessed *service.MetricCounter // Total messages processed successfully
	MessagesErrored   *service.MetricCounter // Messages that failed processing
	MessagesDropped   *service.MetricCounter // Messages dropped (no outputs generated)

	// JavaScript Execution Metrics
	JavaScriptErrors        *service.MetricCounter // JavaScript expression errors
	JavaScriptExecutionTime *service.MetricTimer   // Time spent executing JavaScript

	// Output Generation Metrics
	OutputsGenerated *service.MetricCounter // Total output messages generated

	// Performance Timers
	BatchProcessingTime   *service.MetricTimer // Time to process entire batch
	MessageProcessingTime *service.MetricTimer // Time to process individual message

	// Resource Utilization
	ActiveMappings  *service.MetricGauge // Number of active mappings
	ActiveVariables *service.MetricGauge // Number of active variables
}

// NewProcessorMetrics creates a new set of essential processor metrics
func NewProcessorMetrics(metrics *service.Metrics) *ProcessorMetrics {
	return &ProcessorMetrics{
		// Core Processing Metrics
		MessagesProcessed: metrics.NewCounter("messages_processed"),
		MessagesErrored:   metrics.NewCounter("messages_errored"),
		MessagesDropped:   metrics.NewCounter("messages_dropped"),

		// JavaScript Execution Metrics
		JavaScriptErrors:        metrics.NewCounter("javascript_errors"),
		JavaScriptExecutionTime: metrics.NewTimer("javascript_execution_time"),

		// Output Generation Metrics
		OutputsGenerated: metrics.NewCounter("outputs_generated"),

		// Performance Timers
		BatchProcessingTime:   metrics.NewTimer("batch_processing_time"),
		MessageProcessingTime: metrics.NewTimer("message_processing_time"),

		// Resource Utilization
		ActiveMappings:  metrics.NewGauge("active_mappings"),
		ActiveVariables: metrics.NewGauge("active_variables"),
	}
}

// LogMessageProcessed logs successful message processing
func (m *ProcessorMetrics) LogMessageProcessed(processingTime time.Duration) {
	m.MessagesProcessed.Incr(1)
	m.MessageProcessingTime.Timing(int64(processingTime))
}

// LogMessageErrored logs message processing errors
func (m *ProcessorMetrics) LogMessageErrored() {
	m.MessagesErrored.Incr(1)
}

// LogMessageDropped logs messages that were dropped
func (m *ProcessorMetrics) LogMessageDropped() {
	m.MessagesDropped.Incr(1)
}

// LogJavaScriptExecution logs JavaScript execution metrics
func (m *ProcessorMetrics) LogJavaScriptExecution(executionTime time.Duration, success bool) {
	m.JavaScriptExecutionTime.Timing(int64(executionTime))
	if !success {
		m.JavaScriptErrors.Incr(1)
	}
}

// LogOutputGeneration logs output message generation
func (m *ProcessorMetrics) LogOutputGeneration(count int) {
	m.OutputsGenerated.Incr(int64(count))
}

// LogBatchProcessing logs batch processing timing
func (m *ProcessorMetrics) LogBatchProcessing(processingTime time.Duration) {
	m.BatchProcessingTime.Timing(int64(processingTime))
}

// UpdateActiveMetrics updates gauge metrics for active resources
func (m *ProcessorMetrics) UpdateActiveMetrics(totalMappings, totalVariables int64) {
	m.ActiveMappings.Set(totalMappings)
	m.ActiveVariables.Set(totalVariables)
}

// NewMockMetrics creates mock metrics for testing
func NewMockMetrics() *ProcessorMetrics {
	return &ProcessorMetrics{
		MessagesProcessed:       &service.MetricCounter{},
		MessagesErrored:         &service.MetricCounter{},
		MessagesDropped:         &service.MetricCounter{},
		JavaScriptErrors:        &service.MetricCounter{},
		JavaScriptExecutionTime: &service.MetricTimer{},
		OutputsGenerated:        &service.MetricCounter{},
		BatchProcessingTime:     &service.MetricTimer{},
		MessageProcessingTime:   &service.MetricTimer{},
		ActiveMappings:          &service.MetricGauge{},
		ActiveVariables:         &service.MetricGauge{},
	}
}
