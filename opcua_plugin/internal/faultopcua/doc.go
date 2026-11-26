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
// of them. This package is the other half of that pair.
//
// # The control and the instrument
//
// Keep both servers, with different jobs. opc-plc stays in CI as the CONTROL,
// answering "is our client behaving normally against a server that works". This
// package is the INSTRUMENT, answering "does our client do the right thing when
// the machine does the wrong thing".
//
// Read them together. A red test on the instrument with a green control points
// at our code. Both red points at the server underneath, which matters, because
// gopcua's server has its own open bugs.
//
// # A fault must prove it fired
//
// A fault that silently fails to inject looks exactly like a fault the client
// handled: the test passes either way, and the pass means nothing. So every
// fault records itself when it takes effect, and Firings reports what actually
// happened on the wire. A test that asserts only the client's reaction is not
// finished; it has to assert the firing too.
//
// This is a test instrument. It is not an OPC UA server anyone should point a
// plant at, and it implements only what a fault needs.
package faultopcua
