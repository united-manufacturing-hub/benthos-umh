package mgmtconfig

import (
	"fmt"

	"k8s.io/client-go/kubernetes"

	kubernetes2 "github.com/united-manufacturing-hub/ManagementConsole/shared/kubernetes"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/errors"
)

type ReleaseChannel string

const (
	Unset      = ""
	Enterprise = "enterprise"
	Stable     = "stable"
	Nightly    = "nightly"
)

type UmhConfig struct {
	Version               string         `yaml:"version"`
	HelmChartAddon        string         `yaml:"helm_chart_addon"`
	UmhMergePoint         int            `yaml:"umh_merge_point"`
	LastUpdated           uint64         `yaml:"lastUpdated"`
	Enabled               bool           `yaml:"enabled"`
	ConvertingActionsDone bool           `yaml:"convertingActionsDone"`
	ReleaseChannel        ReleaseChannel `yaml:"releaseChannel"`

	// DisableHardwareStatusCheck is a flag to disable the hardware status check inside the status message
	// It is used for testing purposes
	DisableHardwareStatusCheck *bool `yaml:"disableHardwareStatusCheck"`
}

func UpdateConvertingActionsDone(convertingActionsDone bool, cache *kubernetes2.Cache, clientSet kubernetes.Interface) error {
	var retryCount = 0
	for retryCount < retries {
		configmap, err := GetMgmtConfig(cache)
		if err != nil {
			return err
		}

		// Get the connections
		var umhConfig UmhConfig
		if data, ok := configmap.Data["umh"]; ok {
			if err := yaml.Unmarshal([]byte(data), &umhConfig); err != nil {
				return err
			}

		}

		umhConfig.ConvertingActionsDone = convertingActionsDone

		// Marshal the brokers
		yamlData, err := yaml.Marshal(umhConfig)
		if err != nil {
			return err
		}

		// Update the configmap
		configmap.Data["umh"] = string(yamlData)
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
