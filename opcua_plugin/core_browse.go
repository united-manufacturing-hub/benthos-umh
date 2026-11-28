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

package opcua_plugin

import (
	"regexp"
	"strings"
	"time"

	"github.com/gopcua/opcua/ua"

	compat "github.com/united-manufacturing-hub/benthos-umh/pkg/compat"
)

const (
	MaxTagsToBrowse = 100_000
	// StaleTime is used to define whether a node that was discovered is marked
	// as stale to rediscover -> if maybe an attribute changed from the previous run
	StaleTime = 15 * time.Minute
)

var sanitizeRegex = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

type NodeDef struct {
	NodeID       *ua.NodeID
	NodeClass    ua.NodeClass
	BrowseName   string
	Description  string
	AccessLevel  ua.AccessLevelType
	DataType     string    // String representation for metadata
	DataTypeID   ua.TypeID // TypeID for filter compatibility checking
	ParentNodeID string    // custom, not an official opcua attribute
	Path         string    // custom, not an official opcua attribute
}

// BrowseDetails represents browse operation progress sent to prevent worker deadlock.
//
// USAGE:
// - GlobalWorkerPool sends updates to ProgressChan during 100k+ node browses
// - If channel is nil (server metadata detection) or no consumer, updates are skipped
// - Tests and large-scale browses must provide channel + consumer to prevent deadlock
//
// DEADLOCK PREVENTION:
// - Buffer capacity: 100k items (see GlobalWorkerPool implementation)
// - Without consumer: Workers block when buffer fills → deadlock
// - With nil channel: Workers skip sending → no deadlock risk for small browses
//
// See core_browse_global_pool.go for GlobalWorkerPool-based browsing architecture.
type BrowseDetails struct {
	NodeDef               NodeDef
	TaskCount             int64
	WorkerCount           int64
	AvgServerResponseTime time.Duration
}

// join concatenates two strings with a dot separator.
//
// This function is used to construct hierarchical paths by joining parent and child
// node names. If the parent string (`a`) is empty, it returns the child string (`b`)
// without adding a dot, ensuring that paths do not start with an unnecessary separator.
func join(a string, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}

// sanitize cleans a string by replacing invalid characters with underscores.
//
// OPC UA node names may contain characters that are not suitable for certain contexts
// (e.g., identifiers in code, file systems). This function ensures that the string
// only contains alphanumeric characters, hyphens, or underscores by replacing any
// invalid character with an underscore. This sanitization helps prevent errors and
// ensures consistency when using node names in various parts of the application.
func sanitize(s string) string {
	return sanitizeRegex.ReplaceAllString(s, "_")
}

// splitLast splits path at its last dot into the parent path and the last
// segment. A path without a dot has an empty parent.
func splitLast(path string) (string, string) {
	if i := strings.LastIndex(path, "."); i >= 0 {
		return path[:i], path[i+1:]
	}
	return "", path
}

// tagTopicParts holds the values of the opcua_tag_path, opcua_tag_name and
// opcua_tag_group metadata of a node. They are the keys that place the tag in
// the UNS topic: tag processor configs map path and name to the virtual path
// and the name of the topic
// (https://docs.umh.app/usage/unified-namespace/topic-convention).
type tagTopicParts struct {
	path  string
	name  string
	group string
}

// compatibility is the set of compatibility flags the OPC UA input
// recognizes inside its `compatibility` config block.
var compatibility = compat.New()

// correctTagPath is the compatibility flag correct_tag_path: publish a tag
// under its browse path without the tag's own name, which opcua_tag_name
// already carries. Off by default because switching it moves affected tags
// to a new topic and leaves their history behind on the old one (ENG-7953).
var correctTagPath = compatibility.Declare("correct_tag_path",
	"Publishes tags whose browse name also appears earlier in their browse path under their correct topic, including tags inside a folder of the same name, which do not publish at all today. Off by default: switching it on moves the affected tags to a new topic and leaves their history behind on the old one.")

// tagTopicPartsOf returns the topic parts of n in this input's compatibility
// mode: the corrected calculation when the compatibility flag correct_tag_path
// is enabled, the legacy one otherwise.
func (g *OPCUAInput) tagTopicPartsOf(n NodeDef) tagTopicParts {
	if g.CorrectTagPath {
		return correctedTagTopicPartsOf(n)
	}
	return legacyTagTopicPartsOf(n)
}

// legacyTagTopicPartsOf returns the topic parts of n as every benthos-umh
// release so far has computed them: it cuts the first occurrence of the
// browse name anywhere in the path, not the last segment. A tag whose name
// also appears earlier in its browse path therefore lands in a misspelled
// folder (element 1 of array arr in data block DB1 publishes to DB.arr.1.1
// instead of DB1.arr.1), and a tag inside a folder of the same name does not
// publish at all.
//
// This is frozen behavior that bridges depend on: never change it. It stays
// available as the default of the compatibility flag correct_tag_path
// (ENG-7953).
func legacyTagTopicPartsOf(n NodeDef) tagTopicParts {
	name := sanitize(n.BrowseName)
	group := strings.TrimSuffix(strings.Replace(n.Path, n.BrowseName, "", 1), ".")
	if group == "" {
		group = name
	}
	path := group
	if group == name {
		path = ""
	}
	return tagTopicParts{path: path, name: name, group: group}
}

// correctedTagTopicPartsOf returns the topic parts of n under the corrected
// tag-path calculation, selected by the compatibility flag correct_tag_path
// (ENG-7953): the tag path is the node's browse path without the node's own
// name, which opcua_tag_name already carries.
func correctedTagTopicPartsOf(n NodeDef) tagTopicParts {
	name := sanitize(n.BrowseName)
	path := tagPathOf(n.Path, n.BrowseName)
	group := path
	if group == "" {
		group = name
	}
	return tagTopicParts{path: path, name: name, group: group}
}

// tagPathOf returns the opcua_tag_path of the node at path: its browse path
// without the node's own name, which opcua_tag_name already carries. Browse
// builds path from sanitized names; browseName is the node's BrowseName before
// sanitize.
func tagPathOf(path string, browseName string) string {
	parent, last := splitLast(path)

	// Compared before sanitize, so a name that sanitize changes keeps its
	// segment: "Pump 1" at Line.Pump_1 gets the tag path Line.Pump_1. Comparing
	// after sanitize would change the topic of those tags (ENG-7956).
	if last == browseName {
		return parent
	}

	// Root nodes whose name sanitize changes already publish with an empty
	// tag path. Keep them there (ENG-7956).
	if parent == "" && last == sanitize(browseName) {
		return ""
	}

	// The last segment is a name that sanitize changed (see above), or a NodeID
	// that UpdateNodePaths put there because two nodes had the same path.
	// Cutting the NodeID would give both nodes the same topic again.
	return path
}

// topic returns the UNS topic the tag publishes under: the tag path joined
// with the tag name.
func (t tagTopicParts) topic() string {
	return join(t.path, t.name)
}

type Logger interface {
	Debugf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
}

type VisitedNodeInfo struct {
	Def             NodeDef
	LastSeen        time.Time
	FullyDiscovered bool
}
