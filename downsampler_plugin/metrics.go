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

package downsampler_plugin

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// DownsamplerMetrics provides metrics collection for the downsampler plugin
type DownsamplerMetrics struct {
	MessagesProcessed *service.MetricCounter
	MessagesFiltered  *service.MetricCounter
	MessagesErrored   *service.MetricCounter
	MessagesPassed    *service.MetricCounter
}

// NewDownsamplerMetrics creates a new metrics collection for the downsampler plugin
func NewDownsamplerMetrics(metrics *service.Metrics) *DownsamplerMetrics {
	return &DownsamplerMetrics{
		MessagesProcessed: metrics.NewCounter("messages_processed"),
		MessagesFiltered:  metrics.NewCounter("messages_filtered"),
		MessagesErrored:   metrics.NewCounter("messages_errored"),
		MessagesPassed:    metrics.NewCounter("messages_passed_through"),
	}
}

// IncrementProcessed increments the processed messages counter
func (m *DownsamplerMetrics) IncrementProcessed() {
	m.MessagesProcessed.Incr(1)
}

// IncrementFiltered increments the filtered messages counter
func (m *DownsamplerMetrics) IncrementFiltered() {
	m.MessagesFiltered.Incr(1)
}

// IncrementErrored increments the errored messages counter
func (m *DownsamplerMetrics) IncrementErrored() {
	m.MessagesErrored.Incr(1)
}

// IncrementPassed increments the passed-through messages counter
func (m *DownsamplerMetrics) IncrementPassed() {
	m.MessagesPassed.Incr(1)
}
