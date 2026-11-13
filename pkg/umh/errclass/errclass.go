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

// Package errclass gives every Warn and Error log line of a plugin an
// error_id and an error_class, so that whoever reads the log can tell causes
// apart without parsing message text. The contract, with each plugin's
// error_ids:
// https://linear.app/united-manufacturing-hub/document/plugin-error-classes-contract-between-benthos-umh-and-umh-core-e51f0744258b
//
// A plugin uses the package in this order:
//
//  1. Declare a Plugin and Define one Kind per cause, as package variables.
//  2. Write Rules that map the errors the plugin sees onto those Kinds, and put
//     them in a Classifier.
//  3. Log through the Logger that Classifier.Logger returns. Every Warn and
//     Error method takes a Kind, so a line cannot be written without one.
//  4. Wrap the error the plugin returns from Connect with Classifier.WithPrefix,
//     so the line Benthos logs for it carries the same error_id.
//
// The forbidigo rule in .golangci.yaml forbids calling Warn and Error on a
// *service.Logger directly.
package errclass

// Class is the classification of errors
type Class string

const (
	// Bug: User can't fix it themselves.
	Bug Class = "bug"
	// Known: Something the user can fix themselves e.g. bad config or network issues.
	Known Class = "known"
	// Unknown: nothing matched, so it is reported until it gets a Kind of its own.
	Unknown Class = "unknown"
)
