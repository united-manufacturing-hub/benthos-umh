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

package bundle

import (
	// Fix for ENG-752
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"

	_ "github.com/RuneRoven/benthosADS"
	_ "github.com/RuneRoven/benthosAlarm"
	_ "github.com/RuneRoven/benthosSMTP"
	_ "github.com/united-manufacturing-hub/benthos-umh/classic_to_core_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/eip_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/modbus_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/s7comm_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/sensorconnect_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/topic_browser_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/uns_plugin"
)
