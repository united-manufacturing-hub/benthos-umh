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

// Package faultopcua runs a real OPC UA server that can be told to misbehave.
//
// Everything hard about talking to a machine is the machine behaving badly, and
// the simulators answer correctly by design. opc-plc accepts a write and applies
// it, answers every read, and never drops a session, so a client that handles
// none of those faults passes against it exactly as well as one that handles all
// of them. This package is the instrument beside that control: opc-plc green and
// faultopcua red is a statement about our code.
//
// A test builds a server with [New], gives it nodes with [Server.AddVariable],
// arms one fault with the arm method carrying that fault's own parameters, runs
// its client against [Server.Endpoint], and asserts two things: what the client
// did, and that the fault fired. The second assertion is not optional. A fault
// that silently failed to inject reads exactly like a fault the client handled.
//
// # Example
//
// A machine that accepts a write and never applies it. The client is told the
// write landed; the read-back and the server-side value say otherwise, and
// [Fault.Fired] proves the fault, not luck, is why:
//
//	func TestWriteIsVerified(t *testing.T) {
//		srv := faultopcua.New(t) // Close is registered with t.Cleanup
//		setpoint := srv.AddVariable("Setpoint", int32(7))
//		f := srv.AcceptWriteNeverApply(setpoint)
//		if err := srv.Start(context.Background()); err != nil {
//			t.Fatal(err)
//		}
//
//		client, err := opcua.NewClient(srv.Endpoint(),
//			opcua.SecurityMode(ua.MessageSecurityModeNone),
//			opcua.AutoReconnect(false))
//		if err != nil {
//			t.Fatal(err)
//		}
//		if err := client.Connect(context.Background()); err != nil {
//			t.Fatal(err)
//		}
//		// Cleanups run last-registered-first, so the server closes before
//		// the client, as "One service loop" below asks. New's own Close
//		// runs again afterwards, which Close tolerates.
//		t.Cleanup(func() { _ = client.Close(context.Background()) })
//		t.Cleanup(func() { _ = srv.Close() })
//
//		// ... run the code under test against setpoint.ID() with client ...
//
//		if got := setpoint.Value(); got != int32(7) {
//			t.Errorf("the machine holds %v, want the original 7", got)
//		}
//		if setpoint.Writes() == 0 || !f.Fired() {
//			t.Error("the write never met the fault, so this test proved nothing")
//		}
//	}
//
// # One service loop
//
// gopcua's server handles every service request in a single loop, so a fault
// that blocks -- NeverAnswer, or AnswerLate while it is sleeping -- holds up
// every later request from every client, not only the node it is armed on.
// Two consequences worth knowing before writing a scenario:
//
// A scenario cannot arm NeverAnswer on one node and expect another node to
// answer meanwhile. It will not.
//
// Close the server before the client. Closing the client first sends a
// CloseSession that queues behind the blocked handler, and the client then
// waits out gopcua's 10s default RequestTimeout.
//
// It is not a general OPC UA server. It implements what a fault needs and nothing
// more, and nobody should point a plant at it.
package faultopcua
