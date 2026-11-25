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

// Package js_security holds the JS execution logging the stream processor's
// engine uses. The runtime hardening that used to live here (the dangerous
// globals blocklist and the blocker functions) moved to pkg/js_runtime under
// Options.Harden (ENG-8128), so every embedding plugin shares one mechanism.
package js_security

import (
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/constants"
)

// LogSlowExecution logs a warning if JavaScript execution takes longer than the threshold.
//
// This helper function consolidates the slow execution logging that was duplicated
// across multiple methods in JSEngine.
func LogSlowExecution(logger *service.Logger, expression string, duration time.Duration) {
	if logger == nil {
		return
	}
	if duration > constants.SlowExecutionThreshold {
		logger.Warnf("Slow JavaScript execution: %s took %v", expression, duration)
	}
}
