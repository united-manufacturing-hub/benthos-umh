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
	"fmt"
)

// maxItemsPerRequest is the example device's request limit. S7 allows 20
// items per request and Modbus one register range, so most protocols have one.
const maxItemsPerRequest = 20

// dialer opens sessions. newClient is a field so tests can inject a client
// that fails on demand.
type deviceDialer struct {
	host      string
	newClient func(host string) Client
}

var _ Dialer[devicePoint] = (*deviceDialer)(nil)

// Dial connects a new client. A connect failure is returned as is: Connect
// failures make Benthos back off and try again, so they need no reason.
func (d *deviceDialer) Dial(ctx context.Context) (Session[devicePoint], error) {
	client := d.newClient(d.host)
	if err := client.Connect(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connecting to %s: %w", d.host, err)
	}
	return &deviceSession{client: client}, nil
}

// session adapts Client to Session. It does not know which library
// is behind the Client.
//
// It implements the two optional interfaces, ReadBacker and Batcher. A
// protocol without read-back support leaves ReadBacker out; the engine then
// refuses readBack: true at Connect with a clear error.
type deviceSession struct {
	client Client
}

var (
	_ Session[devicePoint]    = (*deviceSession)(nil)
	_ ReadBacker[devicePoint] = (*deviceSession)(nil)
	_ Batcher[devicePoint]    = (*deviceSession)(nil)
)

// Write sends the points in one request. The engine already split them with
// Batch, so len(points) is within the device's limit.
func (s *deviceSession) Write(ctx context.Context, points []devicePoint) error {
	return classify(s.client.WriteItems(ctx, toItems(points)))
}

// ReadBack reads every point's address and returns what the device holds.
func (s *deviceSession) ReadBack(ctx context.Context, points []devicePoint) ([]devicePoint, error) {
	items, err := s.client.ReadItems(ctx, toItems(points))
	if err != nil {
		return nil, classify(err)
	}
	read := make([]devicePoint, len(items))
	for index, item := range items {
		read[index] = devicePoint{address: item.Address, data: item.Data}
	}
	return read, nil
}

// Batch splits points into requests the device accepts, keeping their order.
func (s *deviceSession) Batch(points []devicePoint) [][]devicePoint {
	batches := make([][]devicePoint, 0, (len(points)+maxItemsPerRequest-1)/maxItemsPerRequest)
	for start := 0; start < len(points); start += maxItemsPerRequest {
		end := min(start+maxItemsPerRequest, len(points))
		batches = append(batches, points[start:end])
	}
	return batches
}

func (s *deviceSession) Close() error { return s.client.Close() }

// classify maps the Client's errors to the engine's:
//
//	ErrTransport -> ErrConnectionLost: the engine drops the session and
//	                Benthos reconnects.
//	ErrRefused   -> ErrRejected: the engine reports a "rejected" WriteError
//	                and keeps the session.
//
// Anything else is returned as is. The engine treats an unclassified error as
// a lost connection, which is the safe default.
func classify(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrTransport):
		return fmt.Errorf("%w: %w", ErrConnectionLost, err)
	case errors.Is(err, ErrRefused):
		return fmt.Errorf("%w: %w", ErrRejected, err)
	default:
		return err
	}
}

func toItems(points []devicePoint) []Item {
	items := make([]Item, len(points))
	for index, p := range points {
		items[index] = Item{Address: p.address, Data: p.data}
	}
	return items
}
