package bundle

import (
	// Fix for ENG-752
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"

	_ "github.com/RuneRoven/benthosADS"
	_ "github.com/RuneRoven/benthosSMTP"
	_ "github.com/united-manufacturing-hub/benthos-umh/v2/opcua_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/v2/s7comm_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/v2/modbus_plugin"
)
