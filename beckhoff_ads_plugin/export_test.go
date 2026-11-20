// Copyright 2026 UMH Systems GmbH
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

package beckhoff_ads_plugin

import "github.com/redpanda-data/benthos/v4/public/service"

// SanitizeSymbolName exposes the plugin's own tag-name sanitizer to the external
// test package, which asserted against a hand-rolled copy that could drift.
var SanitizeSymbolName = sanitize

// NewLogger wraps a Benthos logger the way the plugin does, for tests that
// build an AdsCommInput by hand.
var NewLogger = classified

// classified returns l with this input's classification, as NewAdsCommInput
// gets it from the Benthos resources.
func classified(l *service.Logger) *service.Logger {
	return classifier.Logger(service.MockResources(service.MockResourcesOptUseLogger(l)))
}

// kindUnclassified is "ads::unclassified", which Classify returns for an error
// no rule knows, and for no error at all.
var kindUnclassified = classifier.Classify(nil)
