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

package errclass

// Class says where the cause behind a Kind lies. It is logged as error_class,
// and umh-core reports only Bug and Unknown to Sentry.
type Class string

const (
	// Known: the cause is outside UMH's code, such as configuration, device
	// state or the network. This includes faults that recover by themselves,
	// such as a dropped connection.
	Known Class = "known"
	// Bug: only UMH can fix it.
	Bug Class = "bug"
	// Unknown: no Kind matched. Every plugin has exactly one Kind of this class,
	// Plugin.Unclassified, and such lines are reported until the cause gets a
	// Kind of its own.
	Unknown Class = "unknown"
)
