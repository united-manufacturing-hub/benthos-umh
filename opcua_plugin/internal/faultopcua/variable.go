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

package faultopcua

import (
	"fmt"
	"sync/atomic"

	"github.com/gopcua/opcua/ua"
)

// A Variable is one node on the server, and the thing a fault is armed against.
// Holding a handle rather than a name is what makes a fault against a node that does
// not exist unwritable: the old API matched by string, so one typo between adding a
// node and arming a fault produced the fault's whole observable behavior against a
// node that was not there, and the scenario passed end to end.
type Variable struct {
	ns   *faultNamespace
	id   *ua.NodeID
	name string

	reads  atomic.Int64
	writes atomic.Int64
}

// ID is the node's full identifier, the form a client config carries: ns=2;s=Setpoint.
// It comes from the server, so a test never assembles one by hand.
func (v *Variable) ID() string {
	return fmt.Sprintf("ns=%d;s=%s", v.id.Namespace(), v.name)
}

// Value is what the machine currently holds. It is the server-side truth a test
// asserts beside the client's reaction: after AcceptWriteNeverApply, the client
// believes the write landed and this still answers the old value.
//
// It reads the node directly rather than through the service the client uses, so
// it answers the stored value even while a fault is holding the service loop, and
// it is unaffected by anything a fault does to the answer on the wire.
func (v *Variable) Value() any {
	dv := v.ns.NodeNameSpace.Attribute(v.id, ua.AttributeIDValue)
	if dv == nil || dv.Value == nil {
		return nil
	}
	return dv.Value.Value()
}

// Reads and Writes count the requests that reached this node. They prove the request
// physically arrived, which is what separates "the client handled the fault" from
// "the client never got that far".
func (v *Variable) Reads() int  { return int(v.reads.Load()) }
func (v *Variable) Writes() int { return int(v.writes.Load()) }

// sawRead and sawWrite are called from the namespace on every request that reaches
// this node, before any fault decides what to do with it: a request the machine
// then refuses, stalls or drops has still arrived, and a test that watches its
// client time out needs to know that.
func (v *Variable) sawRead()  { v.reads.Add(1) }
func (v *Variable) sawWrite() { v.writes.Add(1) }
