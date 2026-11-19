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

package template_write_plugin

import (
	"context"
	"errors"
)

// Errors a Client returns. The session turns them into the engine errors (see protocol.go) the
// engine understands. A real adapter wraps the library's errors with these,
// so the rest of the plugin never inspects library error types.
var (
	// ErrTransport means the connection broke: timeout, reset, closed socket.
	ErrTransport = errors.New("transport failure")
	// ErrRefused means the device answered and said no: bad address,
	// read-only area, access denied.
	ErrRefused = errors.New("device refused the request")
)

// Item is one address and its raw bytes, in the device's own terms. It is the
// only data type that crosses the library boundary.
type Item struct {
	Address string
	Data    []byte
}

// Client is the boundary to the device library. Our own interface with our
// own structs: a library swap means a new adapter, and the codec, session and
// tests stay.
//
// A Client is used by one goroutine at a time. The engine serializes calls.
// Methods wrap their failures with ErrTransport or ErrRefused.
type Client interface {
	// Connect opens the connection. It honors ctx.
	Connect(ctx context.Context) error
	// WriteItems writes all items in one request.
	WriteItems(ctx context.Context, items []Item) error
	// ReadItems reads the current bytes of the items' addresses. The returned
	// items are in the same order as the input; Data is filled in.
	ReadItems(ctx context.Context, items []Item) ([]Item, error)
	// Close ends the connection. Closing an unconnected Client is not an error.
	Close() error
}
