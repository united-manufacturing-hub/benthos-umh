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

import "testing"

func TestModbusTagNameFallsBackToLocator(t *testing.T) {
	if got := modbusTagName(modbusTag{name: "", unifiedAddress: "holding.100.INT16"}); got != "holding.100.INT16" {
		t.Fatalf("nameless: got %q, want %q", got, "holding.100.INT16")
	}

	if got := modbusTagName(modbusTag{name: "temperature", unifiedAddress: "temperature.holding.100.INT16"}); got != "temperature" {
		t.Fatalf("named: got %q, want %q", got, "temperature")
	}
}
