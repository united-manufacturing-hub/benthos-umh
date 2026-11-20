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

// errUnsBetaUnavailable makes newUnsBeta fail in a binary built without the
// connect patches, which lack the forwarder shim that builds the reader.
var errUnsBetaUnavailable = errors.New("uns_beta is not available in this binary: build it with `make build`, which applies the connect patches and sets the connect_patched tag")

// buildUnsBetaInner is never reached, because newUnsBeta fails first.
func buildUnsBetaInner(_ *service.Resources, _ []string, _ string, _ string, _ []string) (service.BatchInput, error) {
	return nil, errUnsBetaUnavailable
}
