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

package historian_plugin

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// disposition is how WriteBatch should handle a write error. It splits on two axes:
// (1) is the error caused by the payload itself (deterministic, never succeeds on retry)?
// -- that is the only axis that drops data. (2) for everything we keep NACKing, is it a
// transient blip or a standing fault that needs an operator? -- that only changes signaling.
type disposition int

const (
	// dispRetryTransient: NACK and let benthos retry, quietly. Connection loss,
	// serialization/deadlock, lock contention, operator intervention, and every error
	// that carries no SQLSTATE (network reset, context deadline, pool timeout).
	dispRetryTransient disposition = iota
	// dispRetryStanding: NACK (never drop good data) but log loudly -- a standing fault
	// that will not clear without an operator (disk full, permission, misconfig) or a code
	// we do not recognize. Defaulting the unknown here is the safe choice for a historian:
	// a stall is recoverable, a silent drop is not.
	dispRetryStanding
	// dispDropPoison: the payload itself is the problem and no retry can ever succeed.
	// Drop the offending row (loud log + metric) so it stops head-of-line-blocking the
	// stream. THIS is the seam write-flow on_error (ENG-5224) later re-routes: instead of
	// dropping, the plugin will surface a classified error the write flow diverts to a DLQ.
	dispDropPoison
)

// classify maps a write error to its disposition. Unknown SQLSTATEs and non-SQLSTATE errors
// are handled conservatively so good data is never dropped by surprise.
func classify(err error) disposition {
	var pg *pgconn.PgError
	if !errors.As(err, &pg) {
		return dispRetryTransient // no SQLSTATE: network reset, ctx deadline, pool timeout, ErrNotConnected
	}
	// P0001 (plpgsql RAISE) is treated as poison because the ONLY sanctioned sources are the
	// plugin's own deterministic guards -- raise_pk_conflict (append-only value/attribute conflict)
	// and tag_value_type_guard (datatype flip), both defined in sql.go. INVARIANT: no schema object
	// this plugin installs may RAISE P0001 for a retryable/operational reason (a new trigger,
	// migration, or function must signal those via a proper SQLSTATE), or that data would be
	// silently dropped here instead of held for retry.
	if pg.Code == "P0001" {
		return dispDropPoison
	}
	// Note: 21000 (cardinality_violation) is NOT mapped here. Its only expected source is the batched
	// insert's "cannot affect row a second time", which writeBatchFast detects at the call site and
	// turns into errIntraBatchConflict. A 21000 reaching classify() from anywhere else is unexpected,
	// so it falls through to the conservative default (retry/hold) rather than a silent drop.
	switch class(pg.Code) {
	case "22", "23": // data exception / integrity constraint -- deterministic per payload
		return dispDropPoison
	case "08", "40", "55", "57": // connection / serialization+deadlock / lock / operator
		return dispRetryTransient
	case "53": // disk_full, out_of_memory, too_many_connections -- retry, but a standing fault
		return dispRetryStanding
	default: // 42xxx (incl. 42501 permission), 3D/3F/28/0P, and any unrecognized code
		return dispRetryStanding
	}
}

// class returns the two-character SQLSTATE class, or "" if the code is malformed.
func class(code string) string {
	if len(code) < 2 {
		return ""
	}
	return code[:2]
}

// pgSQLState returns the 5-character SQLSTATE carried by err, or "none".
func pgSQLState(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code
	}
	return "none"
}
