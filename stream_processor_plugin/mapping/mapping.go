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

package mapping

import "fmt"

// FlattenMappings flattens nested mapping structures into dot notation
func FlattenMappings(mapping map[string]interface{}) map[string]string {
	result := make(map[string]string)
	flattenHelper(mapping, "", result)
	return result
}

// flattenHelper recursively flattens nested structures
func flattenHelper(obj map[string]interface{}, prefix string, result map[string]string) {
	for key, value := range obj {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}

		switch v := value.(type) {
		case string:
			result[path] = v
		case map[string]interface{}:
			flattenHelper(v, path, result)
		default:
			result[path] = fmt.Sprintf("%v", v)
		}
	}
}
