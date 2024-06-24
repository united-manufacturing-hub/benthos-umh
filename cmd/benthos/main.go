// Copyright 2023 UMH Systems GmbH
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

package main

import (
	"context"

	// Fix for ENG-752
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"

	_ "github.com/RuneRoven/benthosADS"
	_ "github.com/RuneRoven/benthosSMTP"
	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/united-manufacturing-hub/benthos-umh/v2/opcua_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/v2/s7comm_plugin"
)

func main() {
	service.RunCLI(context.Background())
}
