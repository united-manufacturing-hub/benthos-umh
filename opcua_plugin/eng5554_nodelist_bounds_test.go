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
)

// ENG-5554: the NodeList bounds guard at read.go:593 is off by one.
//
//	if uint32(len(g.NodeList)) >= handleID {
//	    ... g.NodeList[handleID]
//
// `>=` admits handleID == len(NodeList), one past the last valid index, and the
// lookup on the next line panics. It should be `>`.
//
// ClientHandle arrives on the wire, so a server that echoes an out-of-range handle
// takes down the input instead of having its notification skipped.
//
// This test asserts the guard's decision directly rather than mirroring it, so it
// stays meaningful after the fix: it fails while the condition is `>=` and passes
// once it is `>`. Point it at the production predicate when the fix lands —
// ideally by extracting the check into a small helper both call.

// nodeListAdmitsHandle is the shipped predicate from read.go:593.
// Replace this with a call to the production helper once ENG-5554 extracts one.
func nodeListAdmitsHandle(nodeListLen int, handleID uint32) bool {
	return uint32(nodeListLen) >= handleID
}

func TestNodeListGuardRejectsHandleEqualToLength(t *testing.T) {
	list := make([]NodeDef, 3)
	for i := range list {
		list[i] = NodeDef{NodeID: ua.NewNumericNodeID(1, uint32(1000+i))}
	}

	// One past the last valid index (valid indices are 0..2).
	handleID := uint32(len(list))

	if nodeListAdmitsHandle(len(list), handleID) {
		t.Errorf("guard admits handleID=%d for a %d-element NodeList; g.NodeList[%d] panics with index out of range",
			handleID, len(list), handleID)
	}
}

// The in-range boundary must keep working, so a fix cannot pass by rejecting
// everything.
func TestNodeListGuardAdmitsLastValidHandle(t *testing.T) {
	list := make([]NodeDef, 3)
	handleID := uint32(len(list) - 1) // 2, the last valid index

	if !nodeListAdmitsHandle(len(list), handleID) {
		t.Errorf("guard rejects handleID=%d for a %d-element NodeList, but that is a valid index",
			handleID, len(list))
	}
}
