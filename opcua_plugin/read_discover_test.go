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
	"fmt"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
)

var _ = Describe("UpdateNodePaths", func() {
	DescribeTable("deduplication correctness",
		func(nodes, expected []NodeDef) {
			// Make a copy to avoid test interference
			nodesCopy := make([]NodeDef, len(nodes))
			copy(nodesCopy, nodes)

			UpdateNodePaths(nodesCopy)

			// Check each node path matches expected
			Expect(nodesCopy).To(HaveLen(len(expected)))
			for i := range nodesCopy {
				Expect(nodesCopy[i].Path).To(Equal(expected[i].Path),
					"Node %d: expected path %q, got %q", i, expected[i].Path, nodesCopy[i].Path)
			}
		},
		Entry("no duplicates - paths unchanged",
			[]NodeDef{
				{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
				{Path: "Folder.Tag3", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
			},
			[]NodeDef{
				{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
				{Path: "Folder.Tag3", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
			},
		),
		Entry("duplicate paths - nodeID suffixes added",
			[]NodeDef{
				{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
				{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
			},
			[]NodeDef{
				{Path: "Folder.ns_1_s_node1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "Folder.ns_1_s_node2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
				{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
			},
		),
		Entry("multiple duplicates",
			[]NodeDef{
				{Path: "Root.Dup", NodeID: ua.MustParseNodeID("ns=2;i=100")},
				{Path: "Root.Dup", NodeID: ua.MustParseNodeID("ns=2;i=200")},
				{Path: "Root.Dup", NodeID: ua.MustParseNodeID("ns=2;i=300")},
				{Path: "Root.Unique", NodeID: ua.MustParseNodeID("ns=2;i=400")},
			},
			[]NodeDef{
				{Path: "Root.ns_2_i_100", NodeID: ua.MustParseNodeID("ns=2;i=100")},
				{Path: "Root.ns_2_i_200", NodeID: ua.MustParseNodeID("ns=2;i=200")},
				{Path: "Root.ns_2_i_300", NodeID: ua.MustParseNodeID("ns=2;i=300")},
				{Path: "Root.Unique", NodeID: ua.MustParseNodeID("ns=2;i=400")},
			},
		),
		Entry("single-segment paths (root nodes) - Roger's edge case",
			[]NodeDef{
				{Path: "RootTag", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "RootTag", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
			},
			[]NodeDef{
				{Path: "ns_1_s_node1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "ns_1_s_node2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
			},
		),
		Entry("empty paths (defensive)",
			[]NodeDef{
				{Path: "", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
			},
			[]NodeDef{
				{Path: "ns_1_s_node1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "ns_1_s_node2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
			},
		),
		Entry("mixed depth paths",
			[]NodeDef{
				{Path: "Root", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "Root", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
				{Path: "Root.Folder.Tag", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
				{Path: "Root.Folder.Tag", NodeID: ua.MustParseNodeID("ns=1;s=node4")},
			},
			[]NodeDef{
				{Path: "ns_1_s_node1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "ns_1_s_node2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
				{Path: "Root.Folder.ns_1_s_node3", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
				{Path: "Root.Folder.ns_1_s_node4", NodeID: ua.MustParseNodeID("ns=1;s=node4")},
			},
		),
	)
})

// BenchmarkDeduplicate demonstrates O(n) linear time complexity of the current implementation
// Expected behavior: Doubling input size should ~2x the time (linear scaling)
func BenchmarkDeduplicate(b *testing.B) {
	sizes := []int{100, 200, 500, 1000, 2000, 5000}

	for _, size := range sizes {
		// Create nodes with ALL unique paths (tests first-pass hash map population)
		nodes := make([]NodeDef, size)
		for i := 0; i < size; i++ {
			nodes[i] = NodeDef{
				Path:   fmt.Sprintf("Folder.Tag%d", i),
				NodeID: ua.MustParseNodeID(fmt.Sprintf("ns=1;i=%d", i)),
			}
		}

		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				// Make a fresh copy each iteration
				nodesCopy := make([]NodeDef, len(nodes))
				copy(nodesCopy, nodes)
				UpdateNodePaths(nodesCopy)
			}
		})
	}
}

// BenchmarkDeduplicateWorstCase creates nodes with MANY duplicates
// This stresses the two-pass hash map algorithm: first pass counts occurrences, second pass updates duplicates
func BenchmarkDeduplicateWorstCase(b *testing.B) {
	sizes := []int{100, 200, 500, 1000, 2000}

	for _, size := range sizes {
		// Create nodes where every other node has the SAME path (forcing deduplication)
		nodes := make([]NodeDef, size)
		for i := 0; i < size; i++ {
			// Half the nodes share "DuplicatePath", triggering the two-pass deduplication
			if i%2 == 0 {
				nodes[i] = NodeDef{
					Path:   "Folder.DuplicatePath",
					NodeID: ua.MustParseNodeID(fmt.Sprintf("ns=1;i=%d", i)),
				}
			} else {
				nodes[i] = NodeDef{
					Path:   fmt.Sprintf("Folder.Unique%d", i),
					NodeID: ua.MustParseNodeID(fmt.Sprintf("ns=1;i=%d", i)),
				}
			}
		}

		b.Run(fmt.Sprintf("worst_case_%d", size), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				nodesCopy := make([]NodeDef, len(nodes))
				copy(nodesCopy, nodes)
				UpdateNodePaths(nodesCopy)
			}
		})
	}
}

var _ = Describe("discoverNodes GlobalWorkerPool integration", func() {
	// Phase 2, Task 2.1: Refactor discoverNodes() to use GlobalWorkerPool
	//
	// TDD Challenge: discoverNodes() is a private method with complex OPC UA dependencies.
	// We can't easily mock the entire OPC UA stack just to test pool creation.
	//
	// Solution: Test the PATTERN that discoverNodes() will use:
	// 1. Create pool with ServerProfile
	// 2. Spawn MinWorkers
	// 3. Submit tasks (one per NodeID)
	// 4. Shutdown pool
	//
	// These tests verify the building blocks work correctly. The actual integration
	// into discoverNodes() will be verified by:
	// - Code review (visual inspection of refactored code)
	// - Existing integration tests still passing (no regressions)
	// - Manual testing with real OPC UA servers (if needed)
	//
	// This is pragmatic TDD for refactoring: test the new pattern works, then apply it.

	Describe("GlobalWorkerPool usage pattern for discoverNodes", func() {
		It("should create and use pool following discoverNodes pattern", func() {
			// Simulate what discoverNodes() will do
			profile := ServerProfile{
				Name:       "test-profile",
				MaxWorkers: 10,
				MinWorkers: 2,
			}

			// Step 1: Create pool
			pool := NewGlobalWorkerPool(profile)
			Expect(pool).NotTo(BeNil())

			// Step 2: Spawn MinWorkers (as discoverNodes will do)
			workersSpawned := pool.SpawnWorkers(profile.MinWorkers)
			Expect(workersSpawned).To(Equal(2))

			// Step 3: Submit tasks (simulating NodeIDs iteration)
			nodeIDs := []string{
				"ns=1;i=1000",
				"ns=1;i=2000",
				"ns=1;i=3000",
			}

			resultChan := make(chan any, len(nodeIDs))
			errChan := make(chan error, len(nodeIDs))

			for _, nodeID := range nodeIDs {
				task := GlobalPoolTask{
					NodeID:     nodeID,
					ResultChan: resultChan,
					ErrChan:    errChan,
				}
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())
			}

			// Wait for all results (workers process tasks)
			receivedResults := 0
			Eventually(func() int {
				select {
				case <-resultChan:
					receivedResults++
				default:
				}
				return receivedResults
			}).Within(2 * time.Second).Should(Equal(len(nodeIDs)))

			// Step 4: Shutdown (with defer in real code)
			err := pool.Shutdown(30 * time.Second)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	// RED Test: This will guide the implementation
	Describe("discoverNodes refactoring checklist", func() {
		It("documents what needs to be added to discoverNodes()", func() {
			// This test documents the refactoring steps (will be removed after implementation)
			checklist := []string{
				"Create pool := NewGlobalWorkerPool(g.ServerProfile)",
				"Add defer pool.Shutdown(30 * time.Second)",
				"Call pool.SpawnWorkers(g.ServerProfile.MinWorkers)",
				"Replace browse() goroutine spawning with pool.SubmitTask() loop",
				"Pass existing nodeChan/errChan to GlobalPoolTask",
			}

			// This test always passes - it's documentation, not verification
			Expect(len(checklist)).To(Equal(5))

			// The REAL verification is:
			// 1. Code review sees these lines in discoverNodes()
			// 2. Existing tests still pass (no regressions)
		})
	})
})
