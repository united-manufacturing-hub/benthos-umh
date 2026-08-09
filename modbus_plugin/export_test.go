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

package modbus_plugin

// This file is only compiled into the test binary. It exposes the constructor's
// per-slave distribution and deduplication to the external modbus_plugin_test
// package so its helpers can call production code instead of re-implementing it.

// BuildPerSlaveAddressesForTest exposes buildPerSlaveAddresses to external tests.
func (m *ModbusInput) BuildPerSlaveAddressesForTest() map[byte][]ModbusDataItemWithAddress {
	return m.buildPerSlaveAddresses()
}

// DedupPerSlaveForTest exposes dedupPerSlave to external tests.
func (m *ModbusInput) DedupPerSlaveForTest(perSlaveAddresses map[byte][]ModbusDataItemWithAddress) map[byte][]ModbusDataItemWithAddress {
	return m.dedupPerSlave(perSlaveAddresses)
}
