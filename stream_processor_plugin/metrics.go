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
	"github.com/redpanda-data/benthos/v4/public/service"
)

// ProcessorMetrics holds all metrics for the stream processor
type ProcessorMetrics struct {
	MessagesProcessed *service.MetricCounter
	MessagesErrored   *service.MetricCounter
	MessagesDropped   *service.MetricCounter
}

// NewProcessorMetrics creates a new set of processor metrics
func NewProcessorMetrics(metrics *service.Metrics) *ProcessorMetrics {
	return &ProcessorMetrics{
		MessagesProcessed: metrics.NewCounter("messages_processed"),
		MessagesErrored:   metrics.NewCounter("messages_errored"),
		MessagesDropped:   metrics.NewCounter("messages_dropped"),
	}
}
