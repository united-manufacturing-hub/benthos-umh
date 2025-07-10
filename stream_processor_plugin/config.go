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

package stream_processor_plugin

import (
	"fmt"
)

// validateConfig validates the stream processor configuration
func validateConfig(config StreamProcessorConfig) error {
	if config.Mode == "" {
		return fmt.Errorf("mode is required")
	}

	if config.Mode != "timeseries" {
		return fmt.Errorf("unsupported mode: %s (only 'timeseries' is supported)", config.Mode)
	}

	if config.OutputTopic == "" {
		return fmt.Errorf("output_topic is required")
	}

	if config.Model.Name == "" {
		return fmt.Errorf("model name is required")
	}

	if config.Model.Version == "" {
		return fmt.Errorf("model version is required")
	}

	if len(config.Sources) == 0 {
		return fmt.Errorf("at least one source mapping is required")
	}

	return nil
}
