package mgmtconfig

import (
	"fmt"
	"github.com/united-manufacturing-hub/ManagementConsole/shared/ptr"

	"github.com/united-manufacturing-hub/ManagementConsole/shared/constants"
	"github.com/united-manufacturing-hub/ManagementConsole/shared/kubernetes"

	"github.com/Masterminds/semver/v3"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	v1core "k8s.io/api/core/v1"
)

type Network struct {
	ExternalIP              string `yaml:"externalIP"`
	ExternalGateway         string `yaml:"externalGateway"`
	ExternalDeviceInterface string `yaml:"externalDeviceInterface"`
	DNSServer               string `yaml:"dnsServer"`
}

type OS struct {
	Architecture string `yaml:"architecture"`
}

type Helm struct {
	ChartVersion *semver.Version `yaml:"chartVersion"`
	// SameTopicExperience is a feature that maps 1 MQTT topic to 1 Kafka topic
	SameTopicExperience *bool `yaml:"sameTopicExperience"`
	// SkipSafetyChecks is a feature that skips our disk and uptime checks (For testing purposes)
	SkipSafetyChecks *bool `yaml:"skipSafetyChecks"`
}

type MgmtConfig struct {
	Connections          []Connection         `yaml:"connections"`
	Tls                  TlsConfig            `yaml:"tls"`
	Umh                  UmhConfig            `yaml:"umh"`
	Debug                DebugConfig          `yaml:"debug"`
	FeatureFlags         map[string]bool      `yaml:"flags,omitempty"`
	Location             Location             `yaml:"location"`
	Brokers              Brokers              `yaml:"brokers"`
	ProtocolConverterMap ProtocolConverterMap `yaml:"protocolConverterMap"`
	DataFlowComponents   DataFlowComponents   `yaml:"dataFlowComponents"`
	Network              Network              `yaml:"network"`
	OS                   OS                   `yaml:"os"`
	Helm                 Helm                 `yaml:"helm"`
}

// GetMgmtConfig retrieves the ConfigMap from the Kubernetes cluster.
// NOTE: to prevent data race conditions and ensure atomicity and isolation, only call GetMgmtConfig() once per algorithm
func GetMgmtConfig(cache *kubernetes.Cache) (mgmtConfig v1core.ConfigMap, err error) {
	mgmtConfig, err = kubernetes.GetConfigMapByName(cache, constants.NamespaceMgmtCompanion, constants.ConfigMapName)
	if err != nil {
		zap.S().Errorf("Error getting configmap %s: %s", constants.ConfigMapName, err)
	}
	// zap.S().Debuff("Got configmap %+v", mgmtConfig)

	return mgmtConfig, err
}

// GetParsedMgmtConfig requires a GetMgmtConfig before.
// This separation should enforce that there should only be one GetMgmtConfig() call per algorithm
func GetParsedMgmtConfigFromConfigmap(mgmtConfig v1core.ConfigMap) (parsedConfig MgmtConfig, err error) {
	// Extract and parse the "umh" field from the ConfigMap.
	parsedConfig.Umh, err = GetUMHFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		zap.S().Errorf("Error parsing 'umh' field: %s", err)
		return parsedConfig, err
	}
	//	zap.S().Debugf("Parsed UMH field: %+v", parsedConfig.Umh)

	// Extract and parse the "connections" field from the ConfigMap.
	parsedConfig.Connections, err = GetConnectionsFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		zap.S().Errorf("Error parsing 'connections' field: %s", err)
		return parsedConfig, err
	}

	// Extract and parse the "location" field from the ConfigMap.
	parsedConfig.Location, err = GetLocationFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		zap.S().Errorf("Error parsing 'location' field: %s", err)
		return parsedConfig, err
	}

	// Extract and parse the "broker" field from the ConfigMap.
	parsedConfig.Brokers, err = GetBrokersFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		zap.S().Errorf("Error parsing 'broker' field: %s", err)
		return parsedConfig, err
	}

	// Extract and parse the "tls" field from the ConfigMap.
	parsedConfig.Tls, err = GetTlsFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		zap.S().Errorf("Error parsing 'tls' field: %s", err)
		return parsedConfig, err
	}

	// Extract and parse the "tls" field from the ConfigMap.
	parsedConfig.Debug, err = GetDebugFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		zap.S().Errorf("Error parsing 'debug' field: %s", err)
		return parsedConfig, err
	}
	parsedConfig.FeatureFlags = GetFlagsFieldFromMgmtConfig(mgmtConfig)

	parsedConfig.Network, err = GetNetworkFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		zap.S().Infof("Error parsing 'network' field in the mgmtConfig: %s", err)
		// TODO: Right now network is set by install script 0.5.1 and the error is ignored here
		// since customers running older versions of the mgmt companion will not have the network field set.
		// Returning error would break the backward compatibility

		parsedConfig.Network = Network{}
		// return parsedConfig, err
	}

	parsedConfig.OS, err = GetOSFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		//zap.S().Infof("Error parsing 'os' field in the mgmtConfig: %s", err)
		// TODO: Right now OS is set by install script 0.5.1 and the error is ignored here
		// since customers running older versions of the mgmt companion will not have the OS field set.
		// Returning error would break the backward compatibility

		parsedConfig.OS = OS{}
		// return parsedConfig, err
	}

	// Extract and parse the "protocolConverterMap" field from the ConfigMap.
	protocolConverterMap, err := GetProtocolConverterMapFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		if err.Error() == "protocolConverterMap field not found in ConfigMap" {
			parsedConfig.ProtocolConverterMap = make(ProtocolConverterMap)
		} else {
			return parsedConfig, err
		}
	} else {
		parsedConfig.ProtocolConverterMap = protocolConverterMap
	}

	// Extract and parse the "dataFlowComponents" field from the ConfigMap.
	dataFlowComponents, err := GetDataFlowComponentsFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		if err.Error() == "dataFlowComponents field not found in ConfigMap" {
			parsedConfig.DataFlowComponents = make(DataFlowComponents)
		} else {
			return parsedConfig, err
		}
	} else {
		parsedConfig.DataFlowComponents = dataFlowComponents
	}

	// Extract and parse the "helm" field from the ConfigMap.
	helm, err := GetHelmFieldFromMgmtConfig(mgmtConfig)
	if err != nil {
		if err.Error() == "helm field not found in ConfigMap" {
			parsedConfig.Helm = Helm{}
		} else {
			return parsedConfig, err
		}
	} else {
		parsedConfig.Helm = helm
	}

	return parsedConfig, nil
}

func GetNetworkFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (Network, error) {
	return extractField[Network](mgmtConfig, "network")
}

func GetOSFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (OS, error) {
	return extractField[OS](mgmtConfig, "os")
}

func GetDataFlowComponentsFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (DataFlowComponents, error) {
	return extractField[DataFlowComponents](mgmtConfig, "dataFlowComponents")
}

func GetProtocolConverterMapFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (ProtocolConverterMap, error) {
	data := mgmtConfig.Data["protocolConverterMap"]
	return ProtocolConverterMapFromBinary([]byte(data))
}

// GetUMHFieldFromMgmtConfig extracts the "umh" field from the provided ConfigMap.
func GetUMHFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (umhField UmhConfig, err error) {
	umh, err := extractField[UmhConfig](mgmtConfig, "umh")
	if err != nil {
		return umh, err
	}
	if umh.ReleaseChannel == "" {
		umh.ReleaseChannel = Stable
	}
	if umh.DisableHardwareStatusCheck == nil {
		umh.DisableHardwareStatusCheck = ptr.FalsePtr()
	}
	return umh, nil
}

// GetConnectionsFieldFromMgmtConfig extracts the "connections" field from the provided ConfigMap.
func GetConnectionsFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (connectionsField []Connection, err error) {
	return extractField[[]Connection](mgmtConfig, "connections")
}

// GetLocationFieldFromMgmtConfig extracts the "location" field from the provided ConfigMap.
func GetLocationFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (locationField Location, err error) {
	location, err := extractField[Location](mgmtConfig, "location")
	if err != nil {
		return Location{}, nil

	}
	return location, nil
}

// GetBrokersFieldFromMgmtConfig extracts the "brokers" field from the provided ConfigMap.
func GetBrokersFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (brokers Brokers, err error) {
	brokers, err = extractField[Brokers](mgmtConfig, "brokers")
	if err != nil {
		//zap.S().Infof("No broker config found, using kafka fallback")
		return Brokers{
			Kafka: KafkaBroker{
				Ip:   "united-manufacturing-hub-kafka.united-manufacturing-hub.svc.cluster.local",
				Port: 9092,
			},
		}, nil
	}
	if brokers.Kafka.Port == 0 && brokers.MQTT.Port == 0 {
		//zap.S().Infof("No broker config found, using kafka fallback")
		brokers.Kafka.Port = 9092
		brokers.Kafka.Ip = "united-manufacturing-hub-kafka.united-manufacturing-hub.svc.cluster.local"
	}
	return brokers, nil
}

// GetTlsFieldFromMgmtConfig extracts the "tls" field from the provided ConfigMap.
func GetTlsFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (tlsField TlsConfig, err error) {
	tlsField, err = extractField[TlsConfig](mgmtConfig, "tls")
	if err != nil { // the field does probably not exist
		tlsField := TlsConfig{
			InsecureSkipTLSVerify: false,
		}
		return tlsField, nil
	}

	// if the field exists, return it
	return tlsField, nil
}

// GetDebugFieldFromMgmtConfig extracts the "debug" field from the provided ConfigMap.
func GetDebugFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (debugField DebugConfig, err error) {
	debugField, err = extractField[DebugConfig](mgmtConfig, "debug")
	if err != nil { // field does probably not exist
		debugField := DebugConfig{
			DisableBackendConnection: false,
			UpdateTagOverwrite:       "latest",
		}
		return debugField, nil
	}

	// if the field exists, return it
	return debugField, nil
}

// GetFlagsFieldFromMgmtConfig extracts the "flags" field from the provided ConfigMap.
func GetFlagsFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (umhField map[string]bool) {
	flags, err := extractField[map[string]bool](mgmtConfig, "flags")
	if err != nil {
		return make(map[string]bool)
	}
	//	zap.S().Debugf("Parsed feature flags: %+v", flags)
	return flags
}

// GetHelmFieldFromMgmtConfig extracts the "helm" field from the provided ConfigMap.
func GetHelmFieldFromMgmtConfig(mgmtConfig v1core.ConfigMap) (helmField Helm, err error) {
	return extractField[Helm](mgmtConfig, "helm")
}

// extractField is a generic helper function that unmarshals a field from a ConfigMap into a given struct type.
func extractField[T any](mgmtConfig v1core.ConfigMap, fieldName string) (field T, err error) {
	if fieldData, ok := mgmtConfig.Data[fieldName]; ok {
		if err := yaml.Unmarshal([]byte(fieldData), &field); err != nil {
			zap.S().Errorf("Error unmarshalling %s field: %s", fieldName, err)
			return field, err
		}
		return field, nil
	}
	err = fmt.Errorf("%s field not found in ConfigMap", fieldName)
	return field, err
}
