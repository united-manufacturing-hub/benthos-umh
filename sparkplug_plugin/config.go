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

import (
	"fmt"
	"time"
)

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

// Role defines the Sparkplug behavior mode for INPUT plugin (Host-only)
type Role string

const (
	// INPUT plugin roles - Three-mode system for Sparkplug B Hosts
	RoleSecondaryPassive Role = "secondary_passive" // Secondary Host Passive (default): Read-only consumer, no rebirth commands, safe for brownfield
	RoleSecondaryActive  Role = "secondary_active"  // Secondary Host Active: Can send rebirth commands, no STATE publishing
	RolePrimaryHost      Role = "primary"           // Primary Host: Full host with STATE publishing and session management

	// OUTPUT plugin role (internal use only)
	RoleEdgeNode Role = "edge_node" // Edge Node: Publishes NBIRTH/NDATA
)

// Config is the complete Sparkplug B configuration structure
type Config struct {
	MQTT         MQTT         `yaml:"mqtt"`
	Identity     Identity     `yaml:"identity"`
	Subscription Subscription `yaml:"subscription"`

	// Internal field - auto-detected based on configuration
	Role Role `yaml:"-"`

	// Discovery REBIRTH configuration (secondary_active/primary only)
	RequestBirthOnConnect bool          `yaml:"request_birth_on_connect"` // Send REBIRTH requests to newly discovered nodes
	BirthRequestThrottle  time.Duration `yaml:"birth_request_throttle"`   // Minimum time between REBIRTH requests
}

// AutoDetectRole determines the role based on configuration (Host-only for INPUT plugin)
func (c *Config) AutoDetectRole() {
	// Only auto-detect if role is not explicitly set
	if c.Role == "" {
		// Default to Secondary Passive (muted, safe for brownfield deployments)
		c.Role = RoleSecondaryPassive
	}
}

// Validate validates the configuration and returns an error if invalid
func (c *Config) Validate() error {
	if c.MQTT.QoS > 2 {
		return fmt.Errorf("invalid QoS value %d: must be 0, 1, or 2", c.MQTT.QoS)
	}
	if c.Identity.GroupID == "" {
		return fmt.Errorf("group_id is required")
	}

	// Auto-detect role before validation
	c.AutoDetectRole()

	// Validate role values
	switch c.Role {
	case RoleSecondaryPassive, RoleSecondaryActive, RolePrimaryHost, RoleEdgeNode:
		// Valid roles
	default:
		return fmt.Errorf("invalid role '%s': must be 'secondary_passive', 'secondary_active', 'primary', or 'edge_node'", c.Role)
	}

	// host_id (using edge_node_id field) is required for Primary Host (to publish STATE messages)
	// Note: Primary Host uses edge_node_id as host_id for STATE topic: spBv1.0/STATE/<host_id>
	if c.Identity.EdgeNodeID == "" && c.Role == RolePrimaryHost {
		return fmt.Errorf("edge_node_id is required for Primary Host role as host_id to publish STATE messages on spBv1.0/STATE/<host_id>")
	}
	return nil
}

// GetSubscriptionTopics returns the MQTT topics to subscribe to based on role
func (c *Config) getHostSubscriptionTopics() []string {
	if len(c.Subscription.Groups) > 0 {
		topics := make([]string, 0, len(c.Subscription.Groups))
		for _, group := range c.Subscription.Groups {
			topics = append(topics, "spBv1.0/"+group+"/#")
		}
		return topics
	}
	// Default: listen to all groups
	return []string{"spBv1.0/+/#"}
}

func (c *Config) GetSubscriptionTopics() []string {
	switch c.Role {
	case RoleSecondaryPassive, RoleSecondaryActive, RolePrimaryHost:
		return c.getHostSubscriptionTopics()
	case RoleEdgeNode:
		// Edge nodes only listen to their own group (for OUTPUT plugin only)
		return []string{"spBv1.0/" + c.Identity.GroupID + "/#"}
	default:
		// Default to secondary passive behavior (safe)
		return c.getHostSubscriptionTopics()
	}
}

// GetStateTopic returns the STATE topic for Primary Host (Sparkplug v3.0 format)
// Primary Host publishes on: spBv1.0/STATE/<host_id> (no group_id)
func (c *Config) GetStateTopic() string {
	return "spBv1.0/STATE/" + c.Identity.EdgeNodeID // EdgeNodeID is used as host_id for Primary Host
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
