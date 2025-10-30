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
	"testing"

	"github.com/gopcua/opcua/ua"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestSubscriptionFailureMetrics verifies that failed subscriptions
// are tracked with proper labels
func TestSubscriptionFailureMetrics(t *testing.T) {
	// Reset metrics before test
	ResetMetrics()

	tests := []struct {
		name           string
		statusCode     ua.StatusCode
		nodeID         string
		expectedReason string
	}{
		{
			name:           "filter not allowed",
			statusCode:     ua.StatusBadFilterNotAllowed,
			nodeID:         "ns=3;s=ByteString1",
			expectedReason: "filter_not_allowed",
		},
		{
			name:           "filter unsupported",
			statusCode:     ua.StatusBadMonitoredItemFilterUnsupported,
			nodeID:         "ns=3;s=Temperature",
			expectedReason: "filter_unsupported",
		},
		{
			name:           "node id unknown",
			statusCode:     ua.StatusBadNodeIDUnknown,
			nodeID:         "ns=3;s=Missing",
			expectedReason: "node_id_unknown",
		},
		{
			name:           "other error",
			statusCode:     ua.StatusBadInternalError,
			nodeID:         "ns=3;s=Error",
			expectedReason: "other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Record failure
			RecordSubscriptionFailure(tt.statusCode, tt.nodeID)

			// Verify metric incremented
			metric := opcuaSubscriptionFailuresTotal.WithLabelValues(
				tt.expectedReason,
				tt.nodeID,
			)

			count := testutil.ToFloat64(metric)
			if count != 1 {
				t.Errorf("Expected metric count 1, got %f", count)
			}
		})
	}
}
