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
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// connectionConfig holds the fields every S7 component needs to reach a PLC.
type connectionConfig struct {
	tcpDevice string
	rack      int
	slot      int
	timeout   time.Duration
}

// parseConnectionConfig reads the connection fields of the input's config.
func parseConnectionConfig(conf *service.ParsedConfig) (connectionConfig, error) {
	tcpDevice, err := conf.FieldString("tcpDevice")
	if err != nil {
		return connectionConfig{}, err
	}
	rack, err := conf.FieldInt("rack")
	if err != nil {
		return connectionConfig{}, err
	}
	slot, err := conf.FieldInt("slot")
	if err != nil {
		return connectionConfig{}, err
	}
	timeoutSeconds, err := conf.FieldInt("timeout")
	if err != nil {
		return connectionConfig{}, err
	}

	return connectionConfig{
		tcpDevice: tcpDevice,
		rack:      rack,
		slot:      slot,
		timeout:   time.Duration(timeoutSeconds) * time.Second,
	}, nil
}
