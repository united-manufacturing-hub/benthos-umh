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

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

const mismatchLogInterval = 2 * time.Minute

// maxReportedContracts caps how many contracts the error names, and how many topics are parsed to
// find them: past the cap the fix is the same whether 6 arrived or 600.
const maxReportedContracts = 5

// contractOfTopic returns the data-contract segment of a umh_topic. The second result is false when
// the topic does not parse.
func contractOfTopic(umhTopic string) (string, bool) {
	ut, err := topic.NewUnsTopic(umhTopic)
	if err != nil {
		return "", false
	}
	return ut.Info().DataContract, true
}

// noteArrivedContract records a contract the batch carried instead of the configured one, up to
// maxReportedContracts.
func noteArrivedContract(seen map[string]struct{}, umhTopic string) {
	if len(seen) > maxReportedContracts {
		return
	}
	if contract, ok := contractOfTopic(umhTopic); ok {
		seen[contract] = struct{}{}
	}
}

// reportedContracts formats the arrived contracts for the error, sorted so repeated reports match.
func reportedContracts(seen map[string]struct{}) string {
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	if len(out) > maxReportedContracts {
		out = append(out[:maxReportedContracts], "and others")
	}
	return "[" + strings.Join(out, ", ") + "]"
}

func suggestedTopicPattern(contract string) string {
	return `^umh\.v1(?:\.[^._][^.]*)+\._` + contract + `(_v\d+)?\..+$`
}

func mismatchMessage(contract string, contractIsPublished bool, total int, mismatched int, arrived string) string {
	if contractIsPublished {
		return fmt.Sprintf("TimescaleDB historian: subscription is over-broad, %d of %d message(s) carry another data contract %s (reason=%s). Narrow umh_topics to %s",
			mismatched, total, arrived, DropContractMismatch, suggestedTopicPattern(contract))
	}
	return fmt.Sprintf("TimescaleDB historian: no message carries data contract _%s, %d of %d message(s) carry %s instead (reason=%s). Either set data_contract_name to a published contract, or narrow umh_topics to %s",
		contract, mismatched, total, arrived, DropContractMismatch, suggestedTopicPattern(contract))
}

func nackMessage(contract string, total int, mismatched int) string {
	return fmt.Sprintf("TimescaleDB historian: batch refused, %d of %d message(s) do not carry data contract _%s (reason=%s)",
		mismatched, total, contract, DropContractMismatch)
}

func (o *historianOutput) noteContractMismatch(now time.Time, total int, mismatched int, batchCarriedConfiguredContract bool, arrived map[string]struct{}) {
	if !o.claimMismatchLogSlot(now) {
		return
	}
	contractIsPublished := o.everStored.Load() || batchCarriedConfiguredContract
	o.logger.Errorf("%s", mismatchMessage(o.contract, contractIsPublished, total, mismatched, reportedContracts(arrived)))
}

// claimMismatchLogSlot reports whether the caller may log now, taking the slot if so. Mismatches
// repeat every batch, so without this a misconfigured subscription floods the log.
func (o *historianOutput) claimMismatchLogSlot(now time.Time) bool {
	o.mismatchLogMu.Lock()
	defer o.mismatchLogMu.Unlock()
	if !o.lastMismatchLog.IsZero() && now.Sub(o.lastMismatchLog) < mismatchLogInterval {
		return false
	}
	o.lastMismatchLog = now
	return true
}
