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
	"fmt"
	"sync"
)

// simulatedPLC stands in for a real PLC so the template runs and is tested
// without hardware. Its memory is shared by every client of the PLC, like a
// real PLC's memory, so a test can see what was written. A real plugin has no
// such type: the PLC is the library's peer on the network.
type simulatedPLC struct {
	mutex  sync.Mutex
	memory map[string][]byte
}

func newSimulatedPLC() *simulatedPLC {
	return &simulatedPLC{memory: map[string][]byte{}}
}

// plcLibraryClient is the adapter between Client and the PLC library. Here
// the "library" is the simulatedPLC above. A real adapter has the same shape:
// it holds the library's connection handle, translates Item to the library's
// request types, and wraps the library's errors with ErrTransport or
// ErrRefused. It is the only file that imports the library. Rename this file
// to client_<library>.go, for example client_gos7.go.
type plcLibraryClient struct {
	device    *simulatedPLC
	connected bool
}

var _ Client = (*plcLibraryClient)(nil)

func (c *plcLibraryClient) Connect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrTransport, err)
	}
	c.connected = true
	return nil
}

func (c *plcLibraryClient) WriteItems(_ context.Context, items []Item) error {
	if !c.connected {
		return fmt.Errorf("%w: not connected", ErrTransport)
	}
	c.device.mutex.Lock()
	defer c.device.mutex.Unlock()
	for _, item := range items {
		// Copy at the boundary: the caller keeps its slice.
		c.device.memory[item.Address] = append([]byte(nil), item.Data...)
	}
	return nil
}

func (c *plcLibraryClient) ReadItems(_ context.Context, items []Item) ([]Item, error) {
	if !c.connected {
		return nil, fmt.Errorf("%w: not connected", ErrTransport)
	}
	c.device.mutex.Lock()
	defer c.device.mutex.Unlock()
	read := make([]Item, len(items))
	for index, item := range items {
		data, ok := c.device.memory[item.Address]
		if !ok {
			return nil, fmt.Errorf("%w: nothing at %s", ErrRefused, item.Address)
		}
		read[index] = Item{Address: item.Address, Data: append([]byte(nil), data...)}
	}
	return read, nil
}

func (c *plcLibraryClient) Close() error {
	c.connected = false
	return nil
}
