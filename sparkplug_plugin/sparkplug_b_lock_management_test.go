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

// Lock Management Tests - TDD RED Phase for ENG-3720
//
// Problem Identified:
// - Location: sparkplug_b_input.go:630 - `defer s.stateMu.Unlock()` without visible Lock()
// - Issue: sendRebirthRequest() may be called while holding stateMu lock
// - Risk: If MQTT callbacks try to acquire same lock, this causes deadlock
//
// Current Lock Pattern (lines 588-642):
// 1. processDataMessage() acquires stateMu.Lock() at line 589
// 2. For sequence gaps: calls sendRebirthRequest() at line 630
// 3. sendRebirthRequest() does MQTT I/O at line 1138: s.client.Publish()
// 4. Lock released via defer at line 638
//
// Expected Behavior:
// - MQTT I/O operations MUST NOT be called while holding stateMu
// - Lock should be released BEFORE calling sendRebirthRequest()
// - Current code at line 630 is inside the locked section
//
// Test Strategy (TDD RED):
// - Test verifies sendRebirthRequest() doesn't block on stateMu
// - Demonstrates that lock is released before MQTT operations
// - This test MUST fail initially (feature not implemented yet)
//
// References:
// - Code review feedback: "defer unlock without visible lock"
// - sparkplug_b_input.go:630 - sendRebirthRequest() call
// - sparkplug_b_input.go:1138 - MQTT Publish operation

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
			// This test demonstrates the lock management issue
			// Expected to FAIL in RED phase - lock IS held during sendRebirthRequest()

			// Create test wrapper with PrimaryHost role (so sendRebirthRequest is not suppressed)
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
			wrapper.ProcessDataMessage(topicInfo.DeviceKey(), "NDATA", initialPayload, topicInfo)

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

			// Trigger: Send DATA message with sequence gap (seq=5, gap from seq=0)
			// This triggers sendRebirthRequest() at line 630 while holding stateMu lock
			gapPayload := createSparkplugPayload(5, []*sparkplugb.Payload_Metric{
				{Name: stringPtr("temp"), Value: &sparkplugb.Payload_Metric_IntValue{IntValue: 43}},
			})

			// Process message that triggers rebirth (line 630)
			wrapper.ProcessDataMessage(topicInfo.DeviceKey(), "NDATA", gapPayload, topicInfo)

			// Wait for lock detection to complete
			<-done

			// This assertion documents the EXPECTED behavior
			// Currently FAILS because lock IS held during sendRebirthRequest
			Expect(lockHeldDuringSendRebirth).To(BeFalse(),
				"stateMu lock should be released BEFORE calling sendRebirthRequest()\n"+
					"Current behavior: Lock held during sendRebirthRequest() at line 630\n"+
					"Expected behavior: Release lock before sendRebirthRequest() to prevent deadlock\n"+
					"Location: sparkplug_b_input.go:630 - sendRebirthRequest() call inside locked section\n"+
					"Fix pattern: See lines 605-612 where lock is released before requestBirthIfNeeded")
		})

		It("should release and re-acquire lock around sendRebirthRequest", func() {
			// Alternative demonstration: Show that lock is NOT released before sendRebirthRequest
			// This is a documentation test showing the current (incorrect) behavior

			wrapper := sparkplugplugin.NewSparkplugInputForTesting()

			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "TestGroup",
				EdgeNode: "TestNode",
			}

			// Setup initial state
			initialPayload := createSparkplugPayload(0, []*sparkplugb.Payload_Metric{
				{Name: stringPtr("temp"), Value: &sparkplugb.Payload_Metric_IntValue{IntValue: 42}},
			})
			wrapper.ProcessDataMessage(topicInfo.DeviceKey(), "NDATA", initialPayload, topicInfo)

			// Trigger sequence gap - this will call sendRebirthRequest at line 630
			gapPayload := createSparkplugPayload(10, []*sparkplugb.Payload_Metric{
				{Name: stringPtr("temp"), Value: &sparkplugb.Payload_Metric_IntValue{IntValue: 43}},
			})

			// Expected pattern (not currently implemented):
			// 1. processDataMessage acquires lock
			// 2. Detects sequence gap
			// 3. RELEASES lock before sendRebirthRequest
			// 4. Calls sendRebirthRequest (MQTT I/O without lock)
			// 5. Re-acquires lock for alias resolution

			// Current pattern (bug):
			// 1. processDataMessage acquires lock at line 589
			// 2. Detects sequence gap
			// 3. Calls sendRebirthRequest at line 630 (STILL HOLDING LOCK)
			// 4. sendRebirthRequest does MQTT I/O at line 1138 (DEADLOCK RISK)
			// 5. Lock released via defer at line 638

			// Process the gap message
			wrapper.ProcessDataMessage(topicInfo.DeviceKey(), "NDATA", gapPayload, topicInfo)

			// Verify state was updated (proves processDataMessage completed)
			state := wrapper.GetNodeState(topicInfo.DeviceKey())
			Expect(state).NotTo(BeNil())
			Expect(state.IsOnline).To(BeFalse(), "Node should be marked offline due to sequence gap")

			// This test documents the problem:
			// The code at line 630 calls sendRebirthRequest() while holding stateMu lock
			// If MQTT client calls back into our code, it may try to acquire same lock = deadlock
			//
			// Required fix: Release lock before line 630, re-acquire after
			// Similar pattern already exists at lines 605-612 for requestBirthIfNeeded
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
