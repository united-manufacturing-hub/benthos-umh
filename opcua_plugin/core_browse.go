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

// tagMeta is the opcua_tag_path, opcua_tag_name and opcua_tag_group metadata
// of a node. Tag processor configs map path and name to the virtual path and
// the name of the UNS topic
// (https://docs.umh.app/usage/unified-namespace/topic-convention).
type tagMeta struct {
	path  string
	name  string
	group string
}

// tagMetaOf returns the tag metadata of n. The group is the path, or the name
// where the path is empty.
func tagMetaOf(n NodeDef) tagMeta {
	name := sanitize(n.BrowseName)
	group := strings.TrimSuffix(strings.Replace(n.Path, n.BrowseName, "", 1), ".")
	if group == "" {
		group = name
	}
	path := group
	if group == name {
		path = ""
	}
	return tagMeta{path: path, name: name, group: group}
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
