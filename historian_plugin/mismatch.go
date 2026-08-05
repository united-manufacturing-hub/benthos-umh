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
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	startupGrace         = 30 * time.Second
	mismatchLogInterval  = 2 * time.Minute
	maxReportedContracts = 5
)

func suggestedTopicPattern(contract string) string {
	return `^umh\.v1(?:\.[^._][^.]*)+\._` + contract + `(_v\d+)?\..+$`
}

func reportedContracts(seen map[string]struct{}) []string {
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	if len(out) > maxReportedContracts {
		out = out[:maxReportedContracts]
	}
	return out
}

func mismatchMessage(contract string, everStored bool, total int, mismatched int, contracts []string, example string) string {
	seen := "[" + strings.Join(contracts, ", ") + "]"
	if everStored {
		return fmt.Sprintf("TimescaleDB historian: subscription is over-broad (reason=%s) -- %d of %d message(s) in this batch carry other data contracts %s (example: %s), so the whole batch is refused and even its matching message(s) are not written. Narrow umh_topics to %s",
			DropContractMismatch, mismatched, total, seen, example, suggestedTopicPattern(contract))
	}
	return fmt.Sprintf("TimescaleDB historian: no message carries data contract _%s (reason=%s); the subscription selects %s (example: %s) and every batch is refused. Either set data_contract_name to a contract that is published, or narrow umh_topics to %s",
		contract, DropContractMismatch, seen, example, suggestedTopicPattern(contract))
}

func nackMessage(contract string, total int, mismatched int) string {
	return fmt.Sprintf("TimescaleDB historian: batch refused, %d of %d message(s) do not carry data contract _%s (reason=%s)",
		mismatched, total, contract, DropContractMismatch)
}

func (o *historianOutput) pastStartupGrace() bool {
	return o.now().Sub(o.startedAt) >= startupGrace
}

func (o *historianOutput) noteContractMismatch(total int, mismatched int, seen map[string]struct{}, example string, sawMatching bool) {
	o.logStateMu.Lock()
	defer o.logStateMu.Unlock()
	now := o.now()
	if !o.lastMismatchLog.IsZero() && now.Sub(o.lastMismatchLog) < mismatchLogInterval {
		return
	}
	o.lastMismatchLog = now
	o.logger.Errorf("%s", mismatchMessage(o.contract, o.everStored.Load() || sawMatching, total, mismatched, reportedContracts(seen), example))
}
