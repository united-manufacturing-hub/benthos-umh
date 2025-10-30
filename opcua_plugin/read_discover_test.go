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

	"github.com/gopcua/opcua/ua"
	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
)

// TestDeduplicateCorrectness verifies that UpdateNodePaths correctly deduplicates paths
func TestDeduplicateCorrectness(t *testing.T) {
	tests := []struct {
		name     string
		nodes    []NodeDef
		expected []NodeDef
	}{
		{
			name: "no duplicates - paths unchanged",
			nodes: []NodeDef{
				{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
				{Path: "Folder.Tag3", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
			},
			expected: []NodeDef{
				{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
				{Path: "Folder.Tag3", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
			},
		},
		{
			name: "duplicate paths - nodeID suffixes added",
			nodes: []NodeDef{
				{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
				{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
			},
			expected: []NodeDef{
				{Path: "Folder.ns_1_s_node1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
				{Path: "Folder.ns_1_s_node2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
				{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
			},
		},
		{
			name: "multiple duplicates",
			nodes: []NodeDef{
				{Path: "Root.Dup", NodeID: ua.MustParseNodeID("ns=2;i=100")},
				{Path: "Root.Dup", NodeID: ua.MustParseNodeID("ns=2;i=200")},
				{Path: "Root.Dup", NodeID: ua.MustParseNodeID("ns=2;i=300")},
				{Path: "Root.Unique", NodeID: ua.MustParseNodeID("ns=2;i=400")},
			},
			expected: []NodeDef{
				{Path: "Root.ns_2_i_100", NodeID: ua.MustParseNodeID("ns=2;i=100")},
				{Path: "Root.ns_2_i_200", NodeID: ua.MustParseNodeID("ns=2;i=200")},
				{Path: "Root.ns_2_i_300", NodeID: ua.MustParseNodeID("ns=2;i=300")},
				{Path: "Root.Unique", NodeID: ua.MustParseNodeID("ns=2;i=400")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy to avoid test interference
			nodesCopy := make([]NodeDef, len(tt.nodes))
			copy(nodesCopy, tt.nodes)

			UpdateNodePaths(nodesCopy)

			// Check each node path matches expected
			if len(nodesCopy) != len(tt.expected) {
				t.Fatalf("Expected %d nodes, got %d", len(tt.expected), len(nodesCopy))
			}

			for i := range nodesCopy {
				if nodesCopy[i].Path != tt.expected[i].Path {
					t.Errorf("Node %d: expected path %q, got %q",
						i, tt.expected[i].Path, nodesCopy[i].Path)
				}
			}
		})
	}
}

// BenchmarkDeduplicate demonstrates O(n²) complexity of the current implementation
// Expected behavior: Doubling input size should ~4x the time (quadratic scaling)
func BenchmarkDeduplicate(b *testing.B) {
	sizes := []int{100, 200, 500, 1000, 2000, 5000}

	for _, size := range sizes {
		// Create nodes with ALL unique paths (worst case - no early termination)
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
// This stresses the nested loop path where it must compare against all previous nodes
func BenchmarkDeduplicateWorstCase(b *testing.B) {
	sizes := []int{100, 200, 500, 1000, 2000}

	for _, size := range sizes {
		// Create nodes where every other node has the SAME path (forcing deduplication)
		nodes := make([]NodeDef, size)
		for i := 0; i < size; i++ {
			// Half the nodes share "DuplicatePath", forcing O(n²) comparisons
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
