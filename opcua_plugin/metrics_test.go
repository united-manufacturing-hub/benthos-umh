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
	"os"

	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestSubscriptionFailureMetrics verifies that failed subscriptions
// are tracked with proper labels
var _ = Describe("Subscription Failure Metrics", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	BeforeEach(func() {
		// Reset metrics before each test
		ResetMetrics()
	})

	DescribeTable("tracking subscription failures with proper labels",
		func(statusCode ua.StatusCode, nodeID, expectedReason string) {
			// Record failure
			RecordSubscriptionFailure(statusCode, nodeID)

			// Verify metric incremented
			metric := opcuaSubscriptionFailuresTotal.WithLabelValues(
				expectedReason,
				nodeID,
			)

			count := testutil.ToFloat64(metric)
			Expect(count).To(Equal(float64(1)), "metric count should be incremented")
		},
		Entry("filter not allowed", ua.StatusBadFilterNotAllowed, "ns=3;s=ByteString1", "filter_not_allowed"),
		Entry("filter unsupported", ua.StatusBadMonitoredItemFilterUnsupported, "ns=3;s=Temperature", "filter_unsupported"),
		Entry("node id unknown", ua.StatusBadNodeIDUnknown, "ns=3;s=Missing", "node_id_unknown"),
		Entry("other error", ua.StatusBadInternalError, "ns=3;s=Error", "other"),
	)
})
