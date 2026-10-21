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

package fieldrule

import (
	"fmt"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type DurationRule struct {
	name         string
	allowEmpty   bool
	minimum      time.Duration
	hasMinimum   bool
	wholeSeconds bool
}

func Duration(name string) *DurationRule {
	return &DurationRule{name: name}
}

func (r *DurationRule) AllowEmpty() *DurationRule {
	r.allowEmpty = true
	return r
}

func (r *DurationRule) Min(minimum time.Duration) *DurationRule {
	r.minimum = minimum
	r.hasMinimum = true
	return r
}

func (r *DurationRule) WholeSeconds() *DurationRule {
	r.wholeSeconds = true
	return r
}

func (r *DurationRule) Field() *service.ConfigField {
	return service.NewDurationField(r.name).LintRule(r.lintRule())
}

func (r *DurationRule) lintRule() string {
	cases := []string{
		fmt.Sprintf(`this == "" => %s`, r.emptyVerdict()),
		fmt.Sprintf(`this.type() != "string" => [ %q ]`, r.notADurationMessage()),
		fmt.Sprintf(`this.parse_duration().catch(null) == null => [ %q ]`, r.notADurationMessage()),
	}
	if r.hasMinimum {
		cases = append(cases, fmt.Sprintf(
			`this.parse_duration() < %d => [ %q ]`,
			r.minimum.Nanoseconds(),
			fmt.Sprintf("%s must be at least %s", r.name, r.minimum),
		))
	}
	if r.wholeSeconds {
		cases = append(cases, fmt.Sprintf(
			`this.parse_duration() %% %d != 0 => [ %q ]`,
			time.Second.Nanoseconds(),
			fmt.Sprintf("%s must be a whole number of seconds", r.name),
		))
	}
	cases = append(cases, `_ => []`)

	return "root = match {\n  " + strings.Join(cases, ",\n  ") + ",\n}"
}

func (r *DurationRule) emptyVerdict() string {
	if r.allowEmpty {
		return "[]"
	}
	return fmt.Sprintf("[ %q ]", r.notADurationMessage())
}

func (r *DurationRule) notADurationMessage() string {
	return fmt.Sprintf(`%s must be a duration with a unit, like "168h", "30m" or "45s"`, r.name)
}
