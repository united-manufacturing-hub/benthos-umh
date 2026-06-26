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

package topic_browser_plugin

import (
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

// LocationPath returns the full location path by joining level0 with all location sublevels.
//
// Deprecated: use the canonical (*proto.TopicInfo).LocationPath() method directly. This thin
// wrapper remains so existing call sites keep compiling; it delegates to the canonical method.
func LocationPath(t *proto.TopicInfo) string {
	return t.LocationPath()
}
