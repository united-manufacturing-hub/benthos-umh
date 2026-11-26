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

package s7comm_plugin

import "context"

// Client is everything the plugin needs from an S7 library to reach a PLC.
// The plugin uses its own Item, so swapping the library means writing one new
// adapter file (client_<library>.go) and nothing else.
type Client interface {
	// ReadItems reads all items in one request. It fills each item's Data and
	// sets Err on an item the PLC refused. It returns an error when the request
	// itself fails.
	ReadItems(ctx context.Context, items []Item) error
	// PDULength is the PDU size negotiated on connect, for batching.
	PDULength() int
	// CPUInfo describes the CPU, for the log.
	CPUInfo() (string, error)
	// Close ends the connection.
	Close() error
}

// Item is one S7 memory access in protocol terms. Area and WordLength are S7
// protocol codes (areaMap, wordLenMap), not library types.
type Item struct {
	Area       int
	WordLength int
	DBNumber   int
	// ByteOffset is the start byte. For a bit access, Bit is the bit within it.
	ByteOffset int
	Bit        int
	// Amount is how many elements of WordLength to transfer: one for a bit, a
	// word or a real, the number of characters for a string.
	Amount int
	// Data receives the bytes read. Its length is the buffer the caller sizes.
	Data []byte
	// Err is set by the adapter when the PLC refused this item.
	Err error
}

// clientFactory opens a connected Client.
type clientFactory func(ctx context.Context, conn connectionConfig) (Client, error)
