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

// Owner says who has to act on the cause behind a Kind. It is logged as
// error_owner.
type Owner string

const (
	// User: the cause is outside UMH's code, such as configuration, device
	// state or the network. The user can fix it, or it recovers by itself,
	// such as a dropped connection.
	User Owner = "user"
	// UMH: only UMH can fix it, even where a ticket for it exists already.
	UMH Owner = "umh"
	// Unknown: no Kind matched. Every plugin has exactly one Kind with this
	// owner, "<plugin>::unclassified", and such lines are reported until the
	// cause gets a Kind of its own.
	Unknown Owner = "unknown"
)
