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

package plugin

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOPCUAInput_Connect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	input := &OPCUAInput{
		endpoint: "opc.tcp://localhost:4840", // replace with your actual endpoint
		username: "",                         
		password: "",                         
		nodeIDs:  nil,                        // add node IDs if you want to test the browsing
	}

	// Attempt to connect
	err := input.Connect(ctx)
	assert.NoError(t, err)

	// Close connection
	if input.client != nil {
		input.client.Close()
	}
}
