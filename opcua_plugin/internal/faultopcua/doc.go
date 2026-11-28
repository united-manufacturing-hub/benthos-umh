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
// waits out gopcua's 10s default RequestTimeout: measured at 10.56s against
// 0.30s the other way round. The helpers in this package's tests register
// their cleanups accordingly, and LIFO makes that the reverse of how it
// reads.
//
// It is not a general OPC UA server. It implements what a fault needs and nothing
// more, and nobody should point a plant at it.
package faultopcua
