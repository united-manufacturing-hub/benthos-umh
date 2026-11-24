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

// owner says who has to act on the cause behind a Kind. It is logged as
// error_owner.
type owner string

const (
	// ownerUser: the cause is outside UMH's code, such as the configuration,
	// the device or the network. The user can fix it, or it recovers by
	// itself, such as a dropped connection.
	ownerUser owner = "user"
	// ownerUMH: only UMH can fix the cause, also when a ticket for it
	// already exists.
	ownerUMH owner = "umh"
	// ownerUnknown: no rule matched. Every component has exactly one Kind
	// with this owner, "<component>::unclassified". A line with this owner
	// shows a cause that does not have a Kind of its own yet.
	ownerUnknown owner = "unknown"
)
