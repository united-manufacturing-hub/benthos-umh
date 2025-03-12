package mgmtconfig

import (
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"

	kubernetes2 "github.com/united-manufacturing-hub/ManagementConsole/shared/kubernetes"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/errors"
)

type MQTTBroker struct {
	Ip          string `json:"ip"`
	Port        uint32 `json:"port"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	LastUpdated uint64 `yaml:"lastUpdated"`
}

// UpdateMQTTBroker updates the MQTT broker in the ConfigMap with the given MQTT broker.
// It will retry up to 5 times if the ConfigMap is modified by another process.
func UpdateMQTTBroker(mqttBroker MQTTBroker, cache *kubernetes2.Cache, clientSet kubernetes.Interface) error {
	mqttBroker.LastUpdated = uint64(time.Now().UnixNano())

	var retryCount = 0
	for retryCount < retries {
		configmap, err := GetMgmtConfig(cache)
		if err != nil {
			return err
		}

		// get "brokers" from configmap.Data as Brokers
		var brokers Brokers
		err = yaml.Unmarshal([]byte(configmap.Data["brokers"]), &brokers)
		if err != nil {
			return fmt.Errorf("failed to unmarshal brokers: %v", err)
		}
		brokers.MQTT = mqttBroker

		// Convert back to yaml and set it in configmap.Data
		brokersYaml, err := yaml.Marshal(brokers)
		if err != nil {
			return fmt.Errorf("failed to marshal brokers: %v", err)
		}
		configmap.Data["brokers"] = string(brokersYaml)

		err = kubernetes2.SetConfigMap(clientSet, &configmap)
		if err != nil {
			if errors.IsConflict(err) {
				time.Sleep(time.Millisecond * 500)
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
