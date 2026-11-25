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
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// NewGos7ClientForTest wraps a stand-in for the gos7 client in the real
// adapter, so a test checks the request that would go on the wire.
func NewGos7ClientForTest(session gos7Session, pduLength int) Client {
	return &gos7Client{session: session, pduLength: pduLength}
}

// NewGos7ClientWithCloseForTest is NewGos7ClientForTest with the function Close calls.
func NewGos7ClientWithCloseForTest(session gos7Session, pduLength int, closeFn func() error) Client {
	return &gos7Client{session: session, pduLength: pduLength, close: closeFn}
}

// ConnectGos7ForTest opens a connection with the real gos7 adapter.
func ConnectGos7ForTest(ctx context.Context, tcpDevice string, rack int, slot int, timeout time.Duration) (Client, error) {
	return newGos7Client(ctx, connectionConfig{tcpDevice: tcpDevice, rack: rack, slot: slot, timeout: timeout})
}

// UseClientFactoryForTest makes Connect open its client through factory.
func (s *S7CommInput) UseClientFactoryForTest(factory func(tcpDevice string, rack int, slot int, timeout time.Duration) (Client, error)) {
	s.newClient = func(_ context.Context, conn connectionConfig) (Client, error) {
		return factory(conn.tcpDevice, conn.rack, conn.slot, conn.timeout)
	}
}

// ConnectionForTest is the connection fields of a parsed config.
type ConnectionForTest struct {
	TCPDevice string
	Rack      int
	Slot      int
	Timeout   time.Duration
}

// ParseConnectionForTest returns the connection fields of a parsed config.
func ParseConnectionForTest(conf *service.ParsedConfig) (ConnectionForTest, error) {
	conn, err := parseConnectionConfig(conf)
	return ConnectionForTest{TCPDevice: conn.tcpDevice, Rack: conn.rack, Slot: conn.slot, Timeout: conn.timeout}, err
}
