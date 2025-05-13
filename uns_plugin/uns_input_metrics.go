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

package uns_plugin

import (
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// UnsInputMetrics provides metrics collection for the UNS input plugin
type UnsInputMetrics struct {
	ConnectedGuage         *service.MetricGauge
	ConnectionErrCounter   *service.MetricCounter
	PollErrCounter         *service.MetricCounter
	RecordsReceivedCounter *service.MetricCounter
	RecordsFilteredCounter *service.MetricCounter
	CommitTimer            *service.MetricTimer
	ConnectionTimer        *service.MetricTimer
	BatchProcessingTimer   *service.MetricTimer
}

// NewUnsInputMetrics creates a new metrics collection for the UNS input plugin
func NewUnsInputMetrics(metricsProvider *service.Metrics) *UnsInputMetrics {
	return &UnsInputMetrics{
		ConnectedGuage:         metricsProvider.NewGauge("input_uns_connected"),
		ConnectionErrCounter:   metricsProvider.NewCounter("input_uns_connection_errors"),
		PollErrCounter:         metricsProvider.NewCounter("input_uns_poll_errors"),
		RecordsReceivedCounter: metricsProvider.NewCounter("input_uns_records_received"),
		RecordsFilteredCounter: metricsProvider.NewCounter("input_uns_records_filtered"),
		CommitTimer:            metricsProvider.NewTimer("input_uns_records_commit_time"),
		ConnectionTimer:        metricsProvider.NewTimer("input_uns_connection_time"),
		BatchProcessingTimer:   metricsProvider.NewTimer("input_uns_batch_processing_time"),
	}
}

// LogConnectionEstablished logs a successful connection and updates metrics
func (m *UnsInputMetrics) LogConnectionEstablished(startTime time.Time) {
	m.ConnectionTimer.Timing(int64(time.Since(startTime)))
	m.ConnectedGuage.Set(1)
}

// LogConnectionClosed logs a successful connection closure and updates metrics
func (m *UnsInputMetrics) LogConnectionClosed() {
	m.ConnectedGuage.Set(0)
}

// LogPollError logs an error during polling and updates metrics
func (m *UnsInputMetrics) LogPollError() {
	m.PollErrCounter.Incr(int64(1))
}

// LogRecordReceived logs a received record and updates metrics
func (m *UnsInputMetrics) LogRecordReceived() {
	m.RecordsReceivedCounter.Incr(int64(1))
}

// LogRecordFiltered logs a filtered record and updates metrics
func (m *UnsInputMetrics) LogRecordFiltered() {
	m.RecordsFilteredCounter.Incr(int64(1))
}

// LogBatchProcessed logs completion of batch processing and updates metrics
func (m *UnsInputMetrics) LogBatchProcessed(startTime time.Time) {
	m.BatchProcessingTimer.Timing(int64(time.Since(startTime)))
}

// LogCommitCompleted logs completion of a commit operation and updates metrics
func (m *UnsInputMetrics) LogCommitCompleted(startTime time.Time) {
	m.CommitTimer.Timing(int64(time.Since(startTime)))
}

// NewMockMetrics creates a new metrics collection that doesn't actually record metrics
// Useful for testing when you don't need real metrics
func NewMockMetrics() *UnsInputMetrics {
	mockResources := service.MockResources()
	mockMetrics := mockResources.Metrics()
	return &UnsInputMetrics{
		ConnectedGuage:         mockMetrics.NewGauge("input_uns_connected"),
		ConnectionErrCounter:   mockMetrics.NewCounter("input_uns_connection_errors"),
		PollErrCounter:         mockMetrics.NewCounter("input_uns_poll_errors"),
		RecordsReceivedCounter: mockMetrics.NewCounter("input_uns_records_received"),
		RecordsFilteredCounter: mockMetrics.NewCounter("input_uns_records_filtered"),
		CommitTimer:            mockMetrics.NewTimer("input_uns_records_commit_time"),
		ConnectionTimer:        mockMetrics.NewTimer("input_uns_connection_time"),
		BatchProcessingTimer:   mockMetrics.NewTimer("input_uns_batch_processing_time"),
	}
}
