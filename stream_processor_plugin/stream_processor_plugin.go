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

package stream_processor_plugin

import (
	"fmt"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/processor"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	spec := service.NewConfigSpec().
		Version("1.0.0").
		Summary("A processor for collecting timeseries data from multiple UNS sources and generating transformed messages").
		Description(`The stream_processor is a specialized Benthos processor designed to collect timeseries data from multiple UNS sources,
maintain state for variable mappings, and generate transformed messages using JavaScript expressions. It operates exclusively with UNS input/output.

The processor implements dependency-based evaluation:
- Static mappings (no variable dependencies) are evaluated on every incoming message
- Dynamic mappings are only evaluated when their dependent variables are received

Configuration structure:
- mode: Processing mode (currently only "timeseries" is supported)
- model: Model name and version for data contract generation
- output_topic: Base topic for output messages
- sources: Map of variable aliases to UNS topic paths
- mapping: JavaScript expressions for field transformations

Output topics are constructed as: <output_topic>.<data_contract>.<virtual_path>
Where data_contract is "_<model_name>_<model_version>" and virtual_path is the mapping field path.`).
		Field(service.NewStringField("mode").
			Description("Processing mode").
			Default("timeseries")).
		Field(service.NewObjectField("model",
			service.NewStringField("name").Description("Model name for data contract generation"),
			service.NewStringField("version").Description("Model version for data contract generation"),
		).Description("Model configuration for data contract")).
		Field(service.NewStringField("output_topic").
			Description("Base topic for output messages")).
		Field(service.NewStringMapField("sources").
			Description("Map of variable aliases to UNS topic paths")).
		Field(service.NewObjectField("mapping").
			Description("JavaScript expressions for field transformations").
			Optional())

	err := service.RegisterBatchProcessor(
		"stream_processor",
		spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			// Parse configuration
			mode, err := conf.FieldString("mode")
			if err != nil {
				return nil, err
			}

			modelConf, err := conf.FieldObjectMap("model")
			if err != nil {
				return nil, err
			}

			modelName, err := modelConf["name"].FieldString()
			if err != nil {
				return nil, err
			}

			modelVersion, err := modelConf["version"].FieldString()
			if err != nil {
				return nil, err
			}

			outputTopic, err := conf.FieldString("output_topic")
			if err != nil {
				return nil, err
			}

			sources, err := conf.FieldStringMap("sources")
			if err != nil {
				return nil, err
			}

			var mapping map[string]interface{}
			if conf.Contains("mapping") {
				mappingAny, err := conf.FieldAny("mapping")
				if err != nil {
					return nil, err
				}
				if m, ok := mappingAny.(map[string]interface{}); ok {
					mapping = m
				} else {
					return nil, fmt.Errorf("mapping field must be an object")
				}
			}

			cfg := config.StreamProcessorConfig{
				Mode:        mode,
				Model:       config.ModelConfig{Name: modelName, Version: modelVersion},
				OutputTopic: outputTopic,
				Sources:     sources,
				Mapping:     mapping,
			}

			return processor.NewStreamProcessor(cfg, mgr.Logger(), mgr.Metrics())
		})
	if err != nil {
		panic(err)
	}
}
