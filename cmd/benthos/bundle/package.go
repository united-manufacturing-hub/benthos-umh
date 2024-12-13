package bundle

import (
	// Fix for ENG-752
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"

	_ "github.com/RuneRoven/benthosADS"
	_ "github.com/RuneRoven/benthosAlarm"
	_ "github.com/RuneRoven/benthosSMTP"
	_ "github.com/united-manufacturing-hub/benthos-umh/modbus_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/s7comm_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/sensorconnect_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin"
)
