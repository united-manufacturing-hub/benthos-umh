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
	"strings"
)

// Profile name constants
const (
	ProfileAuto            = "auto"
	ProfileHighPerformance = "high-performance"
	ProfileIgnition        = "ignition"
	ProfileKepware         = "kepware"
	ProfileS71200          = "siemens-s7-1200"
	ProfileS71500          = "siemens-s7-1500"
	ProfileProsys          = "prosys"
)

// ServerProfile defines OPC UA server optimization parameters
type ServerProfile struct {
	Name         string
	DisplayName  string
	Description  string
	MaxBatchSize int
	MaxWorkers   int
	MinWorkers   int
}

// Profile instances
var (
	profileAuto = ServerProfile{
		Name:         ProfileAuto,
		DisplayName:  "Auto (Defensive Defaults)",
		Description:  "Safe defaults that work with any OPC UA server, including resource-constrained embedded devices. System will auto-detect known servers and optimize automatically.",
		MaxBatchSize: 50,
		MaxWorkers:   5,
		MinWorkers:   1,
	}

	profileHighPerformance = ServerProfile{
		Name:         ProfileHighPerformance,
		DisplayName:  "High-Performance (VM Servers)",
		Description:  "Aggressive profile for high-performance OPC UA servers running on VM infrastructure. Use when you know your server can handle high concurrency.",
		MaxBatchSize: 1000,
		MaxWorkers:   50,
		MinWorkers:   10,
	}

	profileIgnition = ServerProfile{
		Name:         ProfileIgnition,
		DisplayName:  "Ignition Gateway",
		Description:  "Optimized for Inductive Automation Ignition Gateway (Eclipse Milo). Handles 64 concurrent operations per session.",
		MaxBatchSize: 1000,
		MaxWorkers:   20,
		MinWorkers:   5,
	}

	profileKepware = ServerProfile{
		Name:         ProfileKepware,
		DisplayName:  "Kepware KEPServerEX",
		Description:  "Optimized for PTC Kepware KEPServerEX. Supports up to 128 OPC UA sessions (default, configurable to 4000).",
		MaxBatchSize: 1000,
		MaxWorkers:   40,
		MinWorkers:   5,
	}

	profileS71200 = ServerProfile{
		Name:         ProfileS71200,
		DisplayName:  "Siemens S7-1200 PLC",
		Description:  "Optimized for Siemens S7-1200 PLCs (Firmware V4.4+). Limited to 10 concurrent sessions and 1000 total monitored items.",
		MaxBatchSize: 100,
		MaxWorkers:   10,
		MinWorkers:   3,
	}

	profileS71500 = ServerProfile{
		Name:         ProfileS71500,
		DisplayName:  "Siemens S7-1500 PLC",
		Description:  "Optimized for Siemens S7-1500 PLCs (CPU 1511-1513, Firmware V3.1+). Supports 32 concurrent sessions and 4000 total monitored items.",
		MaxBatchSize: 500,
		MaxWorkers:   20,
		MinWorkers:   5,
	}

	profileProsys = ServerProfile{
		Name:         ProfileProsys,
		DisplayName:  "Prosys Simulation Server",
		Description:  "Optimized for Prosys OPC UA Simulation Server. Supports 100+ concurrent sessions and high-throughput simulation.",
		MaxBatchSize: 800,
		MaxWorkers:   60,
		MinWorkers:   5,
	}

	profileUnknown = ServerProfile{
		Name:         "unknown",
		DisplayName:  "Unknown Server (Fallback)",
		Description:  "Conservative fallback used when server vendor cannot be detected. Same as Auto profile.",
		MaxBatchSize: 50,
		MaxWorkers:   5,
		MinWorkers:   1,
	}
)

// DetectServerProfile analyzes ServerInfo and returns matching profile
func DetectServerProfile(serverInfo *ServerInfo) ServerProfile {
	if serverInfo == nil {
		return profileUnknown
	}

	// Case-insensitive matching
	manufacturer := strings.ToLower(serverInfo.ManufacturerName)
	product := strings.ToLower(serverInfo.ProductName)

	// Ignition Gateway (Inductive Automation / Eclipse Milo)
	if strings.Contains(manufacturer, "inductive automation") ||
		strings.Contains(product, "ignition") ||
		strings.Contains(product, "eclipse milo") {
		return profileIgnition
	}

	// Kepware KEPServerEX (PTC)
	if strings.Contains(manufacturer, "ptc") ||
		strings.Contains(manufacturer, "kepware") ||
		strings.Contains(product, "kepserverex") ||
		strings.Contains(product, "kepware") {
		return profileKepware
	}

	// Siemens (check for S7-1200 first, then S7-1500)
	if strings.Contains(manufacturer, "siemens") {
		// Prioritize S7-1200 detection
		if strings.Contains(product, "s7-1200") ||
			strings.Contains(product, "1200") {
			return profileS71200
		}
		// S7-1500 detection
		if strings.Contains(product, "s7-1500") ||
			strings.Contains(product, "1500") {
			return profileS71500
		}
	}

	// Prosys Simulation Server
	if strings.Contains(manufacturer, "prosys") {
		return profileProsys
	}

	// Unknown - use defensive fallback
	return profileUnknown
}

// GetProfileByName returns profile by name constant
func GetProfileByName(name string) ServerProfile {
	switch name {
	case ProfileAuto:
		return profileAuto
	case ProfileHighPerformance:
		return profileHighPerformance
	case ProfileIgnition:
		return profileIgnition
	case ProfileKepware:
		return profileKepware
	case ProfileS71200:
		return profileS71200
	case ProfileS71500:
		return profileS71500
	case ProfileProsys:
		return profileProsys
	default:
		return profileUnknown
	}
}
