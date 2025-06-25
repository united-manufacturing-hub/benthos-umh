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

package sparkplug_plugin

import "time"

// MQTT transport configuration
type MQTT struct {
	URLs           []string      `yaml:"urls"`
	ClientID       string        `yaml:"client_id"`
	Credentials    Credentials   `yaml:"credentials"`
	QoS            byte          `yaml:"qos"`
	KeepAlive      time.Duration `yaml:"keep_alive"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	CleanSession   bool          `yaml:"clean_session"`
}

// MQTT credentials
type Credentials struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// Sparkplug identity configuration
type Identity struct {
	GroupID      string `yaml:"group_id"`      // Required: Stable business grouping (e.g., "FactoryA")
	EdgeNodeID   string `yaml:"edge_node_id"`  // Required: Static Edge Node ID for session consistency (Sparkplug B compliance)
	LocationPath string `yaml:"location_path"` // Optional: UMH location path for PARRIS Method conversion to device_id
	DeviceID     string `yaml:"device_id"`     // Optional: Static device ID override. If empty and location_path provided, auto-generated via PARRIS
}

// Subscription configuration for primary_host role
type Subscription struct {
	Groups []string `yaml:"groups"` // Groups to subscribe to. Empty means all groups (+)
}

// Role defines the Sparkplug behavior mode
type Role string

const (
	RoleEdgeNode    Role = "edge_node"    // Publish only its own topics
	RolePrimaryHost Role = "primary_host" // Subscribe to all groups
	RoleHybrid      Role = "hybrid"       // Do both (rare, but useful)
)

// Behaviour contains plugin-specific configuration toggles
type Behaviour struct {
	// INPUT-side flags
	AutoSplitMetrics      bool `yaml:"auto_split_metrics"`
	DataMessagesOnly      bool `yaml:"data_messages_only"`
	DataOnly              bool `yaml:"data_only"`
	EnableRebirthReq      bool `yaml:"enable_rebirth_req"`
	DropBirthMessages     bool `yaml:"drop_birth_messages"`
	StrictTopicValidation bool `yaml:"strict_topic_validation"`
	AutoExtractValues     bool `yaml:"auto_extract_values"`

	// OUTPUT-side flags (for future output plugin)
	RetainLastValues bool `yaml:"retain_last_values"`
	BirthOnConnect   bool `yaml:"birth_on_connect"`
}

// Config is the complete Sparkplug B configuration structure
type Config struct {
	MQTT         MQTT         `yaml:"mqtt"`
	Identity     Identity     `yaml:"identity"`
	Role         Role         `yaml:"role"`
	Subscription Subscription `yaml:"subscription"`
	Behaviour    Behaviour    `yaml:"behaviour"`
}

// GetSubscriptionTopics returns the MQTT topics to subscribe to based on role
func (c *Config) GetSubscriptionTopics() []string {
	switch c.Role {
	case RoleEdgeNode:
		// Edge nodes only listen to their own group
		return []string{"spBv1.0/" + c.Identity.GroupID + "/#"}
	case RolePrimaryHost:
		// Primary hosts can filter by specific groups or listen to all
		if len(c.Subscription.Groups) > 0 {
			var topics []string
			for _, group := range c.Subscription.Groups {
				topics = append(topics, "spBv1.0/"+group+"/#")
			}
			return topics
		}
		// Default: listen to all groups for complete visibility
		return []string{"spBv1.0/+/#"}
	case RoleHybrid:
		// Hybrid mode uses same logic as primary_host
		if len(c.Subscription.Groups) > 0 {
			var topics []string
			for _, group := range c.Subscription.Groups {
				topics = append(topics, "spBv1.0/"+group+"/#")
			}
			return topics
		}
		// Default: listen to all groups
		return []string{"spBv1.0/+/#"}
	default:
		// Default to primary host behavior
		return []string{"spBv1.0/+/#"}
	}
}

// GetStateTopic returns the STATE topic for this identity
func (c *Config) GetStateTopic() string {
	return "spBv1.0/" + c.Identity.GroupID + "/STATE/" + c.Identity.EdgeNodeID
}

// IsNodeLevel returns true if this is a node-level identity (empty device_id)
func (c *Config) IsNodeLevel() bool {
	return c.Identity.DeviceID == ""
}

// GetDeviceKey returns the device key for state tracking
func (c *Config) GetDeviceKey() string {
	if c.IsNodeLevel() {
		return c.Identity.GroupID + "/" + c.Identity.EdgeNodeID
	}
	return c.Identity.GroupID + "/" + c.Identity.EdgeNodeID + "/" + c.Identity.DeviceID
}

// GetPublishTopicPrefix returns the topic prefix for publishing messages
func (c *Config) GetPublishTopicPrefix() string {
	if c.IsNodeLevel() {
		return "spBv1.0/" + c.Identity.GroupID + "/"
	}
	return "spBv1.0/" + c.Identity.GroupID + "/" + c.Identity.EdgeNodeID + "/"
}
