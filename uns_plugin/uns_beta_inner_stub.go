//go:build !connect_patched

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

package uns_plugin

import (
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// errUnsBetaUnavailable is returned in a binary built without the connect
// patches, which lack the redpanda constructor uns_beta needs.
var errUnsBetaUnavailable = errors.New("uns_beta is not included in this benthos-umh build: use an official benthos-umh release, or build with `make build`")

func buildUnsBetaInner(_ *service.Resources, _ []string, _ string, _ string, _ string) (service.BatchInput, error) {
	return nil, errUnsBetaUnavailable
}
