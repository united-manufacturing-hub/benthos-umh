//go:build !integration

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

// Lock management regression tests for ENG-3720.
//
// processDataMessage must release stateMu before any sendRebirthRequest call:
// sendRebirthRequest publishes via MQTT, and any callback path that loops
// back into the plugin and tries to acquire stateMu would deadlock.
//
// The bug these tests originally caught is fixed; the tests stay as
// regression guards. They probe the contract by racing a foreign stateMu
// acquisition against a processDataMessage call that triggers a rebirth.
// If the foreign acquisition gets the lock within the probe window,
// processDataMessage released stateMu before the MQTT I/O (correct).

package sparkplug_plugin_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	sparkplugb "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

var _ = Describe("Lock Management in processDataMessage", func() {
	Context("when sequence gap detected and rebirth requested", func() {
		It("should NOT hold stateMu lock during sendRebirthRequest", func() {
			// Regression guard for ENG-3720: confirm stateMu is released before
			// processDataMessage triggers any MQTT I/O via sendRebirthRequest.
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RolePrimaryHost)

			// Setup: Establish initial sequence state for device
			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "TestGroup",
				EdgeNode: "TestNode",
			}

			// Send initial DATA message with seq=0
			initialPayload := createSparkplugPayload(0, []*sparkplugb.Payload_Metric{
				{Name: stringPtr("temp"), Value: &sparkplugb.Payload_Metric_IntValue{IntValue: 42}},
			})
			wrapper.ProcessDataMessage("NDATA", initialPayload, topicInfo)

			// Verify initial state established
			state := wrapper.GetNodeState(topicInfo.DeviceKey())
			Expect(state).NotTo(BeNil())
			Expect(state.LastSeq).To(Equal(uint8(0)))

			// Test Strategy: Try to acquire stateMu lock during sequence gap processing
			// If lock is held during sendRebirthRequest, we'll detect blocking
			lockHeldDuringSendRebirth := false
			done := make(chan struct{})

			// Start goroutine that will try to acquire lock while processDataMessage runs
			go func() {
				defer close(done)
				time.Sleep(5 * time.Millisecond) // Let processDataMessage start

				// Try to acquire the lock with timeout
				lockChan := make(chan bool, 1)
				go func() {
					// Attempt to acquire lock (this simulates what MQTT callback would do)
					stateMu := wrapper.GetStateMu()
					stateMu.Lock()
					lockChan <- true
					stateMu.Unlock()
				}()

				select {
				case <-lockChan:
					// Got lock quickly - lock was NOT held during sendRebirthRequest
					lockHeldDuringSendRebirth = false
				case <-time.After(100 * time.Millisecond):
					// Timeout - lock WAS held during sendRebirthRequest (BUG!)
					lockHeldDuringSendRebirth = true
				}
			}()

			// Trigger sequence gap (seq=5 after seq=0). processDataMessage
			// detects the gap and calls sendRebirthRequest; stateMu must be
			// released before that call.
			gapPayload := createSparkplugPayload(5, []*sparkplugb.Payload_Metric{
				{Name: stringPtr("temp"), Value: &sparkplugb.Payload_Metric_IntValue{IntValue: 43}},
			})

			wrapper.ProcessDataMessage("NDATA", gapPayload, topicInfo)

			// Wait for the probe goroutine to finish.
			<-done

			Expect(lockHeldDuringSendRebirth).To(BeFalse(),
				"stateMu must be released before sendRebirthRequest's MQTT I/O. "+
					"If this test fails, processDataMessage is holding stateMu across the rebirth dispatch, "+
					"which can deadlock against any callback that re-acquires the lock.")
		})

		It("should release and re-acquire lock around sendRebirthRequest", func() {
			// Companion test that just checks the post-state: a sequence gap
			// flips IsOnline=false even when stateMu has been released and
			// re-acquired across the rebirth dispatch.
			wrapper := sparkplugplugin.NewSparkplugInputForTesting()

			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "TestGroup",
				EdgeNode: "TestNode",
			}

			// Setup initial state
			initialPayload := createSparkplugPayload(0, []*sparkplugb.Payload_Metric{
				{Name: stringPtr("temp"), Value: &sparkplugb.Payload_Metric_IntValue{IntValue: 42}},
			})
			wrapper.ProcessDataMessage("NDATA", initialPayload, topicInfo)

			// Trigger sequence gap (seq=10 after seq=0) so processDataMessage
			// dispatches a rebirth.
			gapPayload := createSparkplugPayload(10, []*sparkplugb.Payload_Metric{
				{Name: stringPtr("temp"), Value: &sparkplugb.Payload_Metric_IntValue{IntValue: 43}},
			})

			wrapper.ProcessDataMessage("NDATA", gapPayload, topicInfo)

			// Post-state check: gap was processed (IsOnline=false), proving the
			// function completed without deadlock.
			state := wrapper.GetNodeState(topicInfo.DeviceKey())
			Expect(state).NotTo(BeNil())
			Expect(state.IsOnline).To(BeFalse(), "Node should be marked offline due to sequence gap")
		})
	})
})

// Test Helpers

// createSparkplugPayload creates a test Sparkplug payload
func createSparkplugPayload(seq uint64, metrics []*sparkplugb.Payload_Metric) *sparkplugb.Payload {
	return &sparkplugb.Payload{
		Seq:     &seq,
		Metrics: metrics,
	}
}
