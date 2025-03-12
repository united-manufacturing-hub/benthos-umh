package mgmtconfig

import (
	"fmt"

	"k8s.io/client-go/kubernetes"

	kubernetes2 "github.com/united-manufacturing-hub/ManagementConsole/shared/kubernetes"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/errors"
)

type Brokers struct {
	MQTT  MQTTBroker  `json:"mqtt"`
	Kafka KafkaBroker `json:"kafka"`
}

func UpdateBrokers(brokers Brokers, cache *kubernetes2.Cache, clientSet kubernetes.Interface) error {
	var retryCount = 0
	for retryCount < retries {
		configmap, err := GetMgmtConfig(cache)
		if err != nil {
			return err
		}

		// Marshal the brokers
		yamlData, err := yaml.Marshal(brokers)
		if err != nil {
			return err
		}

		// Update the configmap
		configmap.Data["brokers"] = string(yamlData)
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
