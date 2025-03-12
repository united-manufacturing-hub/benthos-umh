package mgmtconfig

import (
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"

	uuidlib "github.com/google/uuid"
	kubernetes2 "github.com/united-manufacturing-hub/ManagementConsole/shared/kubernetes"
	"github.com/united-manufacturing-hub/ManagementConsole/shared/models/ctypes"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/errors"
)

type Connection struct {
	Name        string                          `yaml:"name"`
	Ip          string                          `yaml:"ip"`
	Type        ctypes.DatasourceConnectionType `yaml:"type"`
	Notes       string                          `yaml:"notes"`
	Port        int                             `yaml:"port"`
	LastUpdated uint64                          `yaml:"lastUpdated"`
	Uuid        uuidlib.UUID                    `yaml:"uuid"`
	Location    *ConnectionLocation             `yaml:"location"` // This is optional for backward compatibility
}

type ConnectionLocation struct {
	Enterprise          string  `yaml:"enterprise" json:"enterprise"` // Always inherited
	Site                *string `yaml:"site" json:"site"`
	SiteIsInherited     bool    `yaml:"siteIsInherited" json:"siteIsInherited"`
	Area                *string `yaml:"area" json:"area"`
	AreaIsInherited     bool    `yaml:"areaIsInherited" json:"areaIsInherited"`
	Line                *string `yaml:"line" json:"line"`
	LineIsInherited     bool    `yaml:"lineIsInherited" json:"lineIsInherited"`
	WorkCell            *string `yaml:"workCell" json:"workCell"`
	WorkCellIsInherited bool    `yaml:"workCellIsInherited" json:"workCellIsInherited"`
}

const retries = 5

// GetConnections gets the list of existing connections
func GetConnections(cache *kubernetes2.Cache, clientSet kubernetes.Interface) ([]Connection, error) {

	configmap, err := GetMgmtConfig(cache)
	if err != nil {
		return nil, err
	}

	// Get the connections
	var connections []Connection
	if data, ok := configmap.Data["connections"]; ok {
		if err := yaml.Unmarshal([]byte(data), &connections); err != nil {
			return nil, err
		}
	}
	return connections, nil
}

// AddConnection adds a new connection to the ConfigMap by appending it to the Connections slice.
// It will retry up to 5 times if the ConfigMap is modified by another process.
func AddConnection(newConnection Connection, cache *kubernetes2.Cache, clientSet kubernetes.Interface) error {
	newConnection.LastUpdated = uint64(time.Now().UnixNano())

	var retryCount = 0
	for retryCount < retries {
		configmap, err := GetMgmtConfig(cache)
		if err != nil {
			return err
		}

		// Get the connections
		var connections []Connection
		if data, ok := configmap.Data["connections"]; ok {
			if err := yaml.Unmarshal([]byte(data), &connections); err != nil {
				return err
			}

		}

		// Check if the connection already exists
		for _, connection := range connections {
			if connection.Uuid == newConnection.Uuid {
				return fmt.Errorf("connection with UUID %s already exists", newConnection.Uuid)
			}
		}

		// Add the new connection
		connections = append(connections, newConnection)

		// Marshal the connections
		yamlData, err := yaml.Marshal(connections)
		if err != nil {
			return err
		}

		// Update the configmap
		configmap.Data["connections"] = string(yamlData)
		err = kubernetes2.SetConfigMap(clientSet, &configmap)
		if err != nil {
			if errors.IsConflict(err) {
				retryCount++
				continue
			}
			return fmt.Errorf("failed to update ConfigMap: %v", err)
		}
		// zap.S().Debuff("Written configmap:%+v", configmap)
		break
	}

	if retryCount == retries {
		return fmt.Errorf("failed to update configmap after %d retries", retries)
	}

	return nil
}

// UpdateConnection overwrites an existing connection in the ConfigMap with the given connection.
// It will retry up to 5 times if the ConfigMap is modified by another process.
func UpdateConnection(connection Connection, cache *kubernetes2.Cache, clientSet kubernetes.Interface) error {
	connection.LastUpdated = uint64(time.Now().UnixNano())

	var retryCount = 0
	for retryCount < retries {
		configmap, err := GetMgmtConfig(cache)
		if err != nil {
			return err
		}

		// Get the connections
		var connections []Connection
		if data, ok := configmap.Data["connections"]; ok {
			if err := yaml.Unmarshal([]byte(data), &connections); err != nil {
				return err
			}

		}

		// Check if the connection does not exist
		var found bool
		for _, c := range connections {
			if c.Uuid == connection.Uuid {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("connection with UUID %s does not exist", connection.Uuid)
		}

		// Update the connection
		for i, c := range connections {
			if c.Uuid == connection.Uuid {
				connections[i] = connection
				break
			}
		}

		// Marshal the connections
		yamlData, err := yaml.Marshal(connections)
		if err != nil {
			return err
		}

		// Update the configmap
		configmap.Data["connections"] = string(yamlData)
		err = kubernetes2.SetConfigMap(clientSet, &configmap)
		if err != nil {
			if errors.IsConflict(err) {
				retryCount++
				continue
			}
			return fmt.Errorf("failed to update ConfigMap: %v", err)
		}

		break
	}

	if retryCount == retries {
		return fmt.Errorf("failed to update configmap after %d retries", retries)
	}

	return nil
}

func DeleteConnection(uuid uuidlib.UUID, cache *kubernetes2.Cache, clientSet kubernetes.Interface) error {
	var retryCount = 0
	for retryCount < retries {
		configmap, err := GetMgmtConfig(cache)
		if err != nil {
			return err
		}

		// Get the connections
		var connections []Connection
		if data, ok := configmap.Data["connections"]; ok {
			if err := yaml.Unmarshal([]byte(data), &connections); err != nil {
				return err
			}

		}

		// Check if the connection does not exist
		var found bool
		var idx int
		for i, c := range connections {
			if c.Uuid == uuid {
				found = true
				idx = i
				break
			}
		}
		if !found {
			return fmt.Errorf("connection with UUID %s does not exist", uuid)
		}

		// Delete the connection
		// FIXME: This operation is very inefficient, as it moves all elements after the deleted element one index to the left.
		//        If order is not important, we should just swap the element to delete with the last element and then truncate the slice.
		//        This would be O(1) instead of O(n).
		//        connections[i] = connections[len(connections)-1]
		//        connections = connections[:len(connections)-1]
		connections = append(connections[:idx], connections[idx+1:]...)

		// Marshal the connections
		yamlData, err := yaml.Marshal(connections)
		if err != nil {
			return err
		}

		// Update the configmap
		configmap.Data["connections"] = string(yamlData)
		err = kubernetes2.SetConfigMap(clientSet, &configmap)
		if err != nil {
			if errors.IsConflict(err) {
				retryCount++
				continue
			}
			return fmt.Errorf("failed to update ConfigMap: %v", err)
		}

		break
	}

	if retryCount == retries {
		return fmt.Errorf("failed to update configmap after %d retries", retries)
	}

	return nil
}
