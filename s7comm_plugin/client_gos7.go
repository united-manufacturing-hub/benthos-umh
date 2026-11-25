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

import (
	"context"
	"errors"
	"fmt"

	"github.com/robinson/gos7"
)

// The gos7 adapter. It is the only file that opens a connection with gos7, and
// every gos7 quirk of the connection is handled here.

// gos7Session is the part of gos7.Client the adapter uses.
type gos7Session interface {
	AGReadMulti(dataItems []gos7.S7DataItem, itemsCount int) error
	GetCPUInfo() (gos7.S7CpuInfo, error)
}

type gos7Client struct {
	session   gos7Session
	pduLength int
	close     func() error
}

// newGos7Client opens an ISO-on-TCP session. gos7 does not take a context, so
// the handler timeout bounds the connect.
func newGos7Client(_ context.Context, conn connectionConfig) (Client, error) {
	handler := gos7.NewTCPClientHandler(conn.tcpDevice, conn.rack, conn.slot)
	handler.Timeout = conn.timeout
	handler.IdleTimeout = conn.timeout
	if err := handler.Connect(); err != nil {
		return nil, err
	}

	return &gos7Client{session: gos7.NewClient(handler), pduLength: handler.PDULength, close: handler.Close}, nil
}

// ReadItems sends the items to gos7 exactly as they are, so the request on the
// wire is the one the input has always sent. gos7 fills the items' Data in
// place and reports a refused item in the item, not as an error.
func (c *gos7Client) ReadItems(_ context.Context, items []Item) error {
	gosItems := make([]gos7.S7DataItem, len(items))
	for index, item := range items {
		gosItems[index] = gos7.S7DataItem{
			Area:     item.Area,
			WordLen:  item.WordLength,
			DBNumber: item.DBNumber,
			Start:    item.ByteOffset,
			Bit:      item.Bit,
			Amount:   item.Amount,
			Data:     item.Data,
		}
	}
	if err := c.session.AGReadMulti(gosItems, len(gosItems)); err != nil {
		return err
	}
	for index, gosItem := range gosItems {
		items[index].Data = gosItem.Data
		if gosItem.Error != "" {
			items[index].Err = errors.New(gosItem.Error)
		}
	}

	return nil
}

func (c *gos7Client) PDULength() int { return c.pduLength }

func (c *gos7Client) CPUInfo() (string, error) {
	info, err := c.session.GetCPUInfo()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s", info), nil
}

func (c *gos7Client) Close() error {
	if c.close == nil {
		return nil
	}

	return c.close()
}
