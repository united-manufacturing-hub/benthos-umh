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

package topic_browser_plugin

// updateAdaptiveMetrics updates the CPU-aware Prometheus gauges with current values.
// This method is called during ProcessBatch to expose operational metrics.
func (t *TopicBrowserProcessor) updateAdaptiveMetrics() {
	// Update CPU load gauge (convert percentage to int64)
	if t.adaptiveController != nil && t.cpuLoadGauge != nil {
		cpuPercent := t.adaptiveController.GetCPUPercent()
		t.cpuLoadGauge.Set(int64(cpuPercent))
	}

	// Update active emit interval gauge (convert seconds to milliseconds as int64)
	if t.adaptiveController != nil && t.activeIntervalGauge != nil {
		currentInterval := t.adaptiveController.GetCurrentInterval()
		intervalMs := int64(currentInterval.Milliseconds())
		t.activeIntervalGauge.Set(intervalMs)
	}

	// Update active topics count gauge
	if t.activeTopicsGauge != nil {
		t.bufferMutex.Lock()
		topicCount := int64(len(t.fullTopicMap))
		t.bufferMutex.Unlock()
		t.activeTopicsGauge.Set(topicCount)
	}
}
