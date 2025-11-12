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

package opcua_plugin

import (
	"github.com/gopcua/opcua/ua"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// opcuaSubscriptionFailuresTotal tracks subscription failures by reason
	opcuaSubscriptionFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "opcua_subscription_failures_total",
			Help: "Total number of OPC UA subscription failures by reason",
		},
		[]string{"reason", "node_id"},
	)
)

// RecordSubscriptionFailure increments failure counter with proper labels
func RecordSubscriptionFailure(statusCode ua.StatusCode, nodeID string) {
	reason := classifyFailureReason(statusCode)
	opcuaSubscriptionFailuresTotal.WithLabelValues(reason, nodeID).Inc()
}

// classifyFailureReason maps OPC UA status codes to metric labels
func classifyFailureReason(statusCode ua.StatusCode) string {
	switch statusCode {
	case ua.StatusBadFilterNotAllowed:
		return "filter_not_allowed"
	case ua.StatusBadMonitoredItemFilterUnsupported:
		return "filter_unsupported"
	case ua.StatusBadNodeIDUnknown:
		return "node_id_unknown"
	case ua.StatusBadNodeIDInvalid:
		return "node_id_invalid"
	default:
		return "other"
	}
}

// ResetMetrics resets all OPC UA metrics (for testing)
func ResetMetrics() {
	opcuaSubscriptionFailuresTotal.Reset()
}
