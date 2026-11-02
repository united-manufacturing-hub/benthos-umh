// Copyright 2026 UMH Systems GmbH
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

// Package template_write_plugin is the copy-me template for a device write
// output. It implements a made-up protocol against a simulated PLC, so
// everything in it is runnable and tested, and nothing in it is specific to a
// real library.
//
// It is NOT registered. Register is never called from an init function and
// cmd/benthos/bundle does not import this package, so no "template_write"
// output exists in the binary.
//
// # Adding a write output
//
// Copy this whole folder to <name>_plugin/, rename the package to match, and
// change the files marked "edit" below. The files marked "keep" are generic
// and need no change. This is the same split as the Beckhoff ADS plugin: the
// device library sits behind one interface, so swapping it means writing one
// adapter file.
//
//	keep  protocol.go          contract: Point, Codec, Dialer, Session, optional
//	                           ReadBacker / Batcher / Resolver, WriteError and
//	                           its reasons.
//	keep  output.go            the Benthos output: Connect, Write (one attempt),
//	                           Close, resolving, batching, read-back, reconnect.
//	keep  config.go            the shared config: mappings[] and readBack.
//	keep  values.go            strict value helpers for codecs.
//	edit  client.go            the boundary to the library: Client, Item, errors.
//	edit  client_plclibrary.go the library adapter. Rename to client_<library>.go
//	                           and replace the simulated PLC. The only file
//	                           that imports the library.
//	edit  codec.go             address grammar and value encoding. No I/O.
//	edit  session.go           adapts Client to Session: error classification,
//	                           read-back, batching.
//	edit  write.go             connection fields, address docs, build function
//	                           and Register. Call Register from init().
//
// Rename the types prefixed "device" (devicePoint, deviceCodec, deviceDialer,
// deviceSession) after the protocol, and replace the sample tests in
// write_test.go. output_test.go tests the generic files with a fake protocol;
// keep it with them.
//
// If the plugin already has a Client for its input, reuse it. The input and
// the output of one protocol should share one boundary to the library.
//
// Then add the usual plugin pieces (see CLAUDE.md, "Adding a plugin"): the
// blank import in cmd/benthos/bundle/package.go, docs under docs/output/, a
// test-<name> Make target and a CI workflow.
//
// # What the template decides, so each protocol does not decide it again
//
//   - One attempt per message. Retry and backoff belong to the write flow in
//     umh-core (ENG-5759), not to the plugin.
//   - Errors carry a stable reason as their first word, so the write flow and
//     a fallback output can read it from the fallback_error metadata. The
//     retry signal is Reason.Retryable, ParseReason and IsRetryable in
//     protocol.go: the write flow retries a rejected or readback_mismatch
//     write and gives up on the other reasons. See Reason for the contract.
//   - A lost connection is signaled with service.ErrNotConnected, and
//     Benthos calls Connect again.
//   - Optional capabilities are separate interfaces the session implements:
//     ReadBacker for read-back, Batcher for request limits, Resolver (with a
//     ResolvingCodec) for protocols that address by name, like ADS symbols.
package template_write_plugin
