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

package opcua_plugin_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
)

var _ = Describe("100k Scale Browse Test", Label("100k_scale"), func() {
	Context("When browsing more than 100k nodes", func() {
		It("should discover all 100,001 nodes without deadlock", func() {
			// ARRANGE: Create a tree that will produce >100k variable nodes
			// Structure: 1 root + 11 folders × 10,000 variable children each = 110,000 variable nodes
			// This exceeds the 100k buffer size of nodeChan, which should trigger deadlock
			// if the consumer waits for wg.Wait() before draining the channel
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
			defer cancel()

			// Create root node
			rootNode := createMockNode(1, "RootNode", ua.NodeClassObject)

			// Create 11 folder nodes under root (to get 110k variables total)
			for folderID := 2; folderID <= 12; folderID++ {
				folderNode := createMockNode(uint32(folderID), fmt.Sprintf("Folder_%d", folderID), ua.NodeClassObject)

				// Add 10,000 variable children to each folder
				for childID := 0; childID < 10000; childID++ {
					nodeID := uint32((folderID-2)*10000 + childID + 100)
					childNode := createMockNode(nodeID, fmt.Sprintf("Variable_%d", nodeID), ua.NodeClassVariable)
					folderNode.AddReferenceNode(id.HasComponent, childNode)
				}

				rootNode.AddReferenceNode(id.Organizes, folderNode)
			}

			// Setup channels
			nodeChan := make(chan NodeDef, MaxTagsToBrowse) // 100k buffer
			errChan := make(chan error, MaxTagsToBrowse)
			opcuaBrowserChan := make(chan BrowseDetails, MaxTagsToBrowse)
			var visited sync.Map
			logger := &MockLogger{}

			// ACT: Start browsing
			testProfile := GetProfileByName(ProfileAuto)
			pool := NewGlobalWorkerPool(testProfile, logger)
			pool.SpawnWorkers(testProfile.MinWorkers)
			defer func() {
				_ = pool.Shutdown(TestPoolShutdownTimeout)
			}()

			task := GlobalPoolTask{
				NodeID:       rootNode.ID().String(),
				Ctx:          ctx,
				Node:         rootNode,
				Path:         "",
				Level:        0,
				ParentNodeID: "",
				Visited:      &visited,
				ResultChan:   nodeChan,
				ErrChan:      errChan,
				ProgressChan: opcuaBrowserChan,
			}
			_ = pool.SubmitTask(task)

			// Start concurrent consumer to drain nodeChan as Browse produces nodes
			// This prevents deadlock when Browse workers fill the 100k buffer
			var discoveredNodes []NodeDef
			consumerDone := make(chan struct{})
			go func() {
				for node := range nodeChan {
					discoveredNodes = append(discoveredNodes, node)
				}
				close(consumerDone)
			}()

			// Start concurrent consumer to drain opcuaBrowserChan (ENG-3835)
			// Without this consumer, browse workers block when sending node 100,001
			// since opcuaBrowserChan buffer is only 100k (MaxTagsToBrowse)
			// Browse details are discarded - channel exists to prevent worker deadlock (legacy UI code removed)
			opcuaBrowserConsumerDone := make(chan struct{})
			go func() {
				for range opcuaBrowserChan { //nolint:revive
				}
				close(opcuaBrowserConsumerDone)
			}()

			// Wait for browse to complete with timeout detection
			done := make(chan struct{})
			go func() {
				_ = pool.WaitForCompletion(DefaultBrowseCompletionTimeout)
				close(done)
			}()

			select {
			case <-done:
				// Browse completed successfully
			case <-ctx.Done():
				Fail("Browse operation timed out - likely deadlock at 100k buffer limit")
			}

			// Close channels to signal completion
			close(nodeChan)
			close(errChan)
			close(opcuaBrowserChan)

			// Wait for consumers to finish draining
			<-consumerDone
			<-opcuaBrowserConsumerDone

			// Check for errors
			var errors []error
			for err := range errChan {
				errors = append(errors, err)
			}
			Expect(errors).To(BeEmpty(), "Browse should complete without errors")

			// ASSERT: All 110,000 variable nodes should be discovered
			// Expected: 110,000 variable nodes (11 folders × 10,000 children)
			// The folders themselves (ua.NodeClassObject) are NOT sent to nodeChan
			// Only ua.NodeClassVariable nodes are sent (see line 312-319 in core_browse.go)
			Expect(discoveredNodes).To(HaveLen(110000), "Should discover exactly 110,000 variable nodes without deadlock")
		})
	})
})
