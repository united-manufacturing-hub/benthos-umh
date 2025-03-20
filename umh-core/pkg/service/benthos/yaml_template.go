package benthos

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

// processOutput specially handles the output section to ensure empty maps are rendered as {}
func processOutput(output interface{}) (string, error) {
	if output == nil {
		return " []", nil
	}

	outputMap, ok := output.(map[string]interface{})
	if !ok {
		return " []", nil
	}

	if len(outputMap) == 0 {
		return " []", nil
	}

	var result strings.Builder
	result.WriteString("\n")

	for component, config := range outputMap {
		result.WriteString(fmt.Sprintf("  %s:\n", component))

		configMap, ok := config.(map[string]interface{})
		if !ok || len(configMap) == 0 {
			result.WriteString("    {}\n")
			continue
		}

		// Marshal the config to YAML
		data, err := yaml.Marshal(configMap)
		if err != nil {
			return "", fmt.Errorf("failed to marshal config to YAML: %w", err)
		}

		// Indent the YAML with 4 spaces
		lines := strings.Split(string(data), "\n")
		// Remove empty line at the end
		if len(lines) > 0 && lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}

		for _, line := range lines {
			if line != "" {
				result.WriteString(fmt.Sprintf("    %s\n", line))
			}
		}
	}

	return result.String(), nil
}

// processInput renders the input section with proper formatting
func processInput(input interface{}) (string, error) {
	if input == nil {
		return " []", nil
	}

	inputMap, ok := input.(map[string]interface{})
	if !ok || len(inputMap) == 0 {
		return " []", nil
	}

	var result strings.Builder
	result.WriteString("\n")

	for component, config := range inputMap {
		result.WriteString(fmt.Sprintf("  %s:", component))

		configMap, ok := config.(map[string]interface{})
		if !ok {
			// Handle scalar values
			result.WriteString(fmt.Sprintf(" %v\n", config))
			continue
		}

		if len(configMap) == 0 {
			result.WriteString(" {}\n")
			continue
		}

		result.WriteString("\n")

		// Marshal the config to YAML
		data, err := yaml.Marshal(configMap)
		if err != nil {
			return "", fmt.Errorf("failed to marshal config to YAML: %w", err)
		}

		// Indent the YAML with 4 spaces
		lines := strings.Split(string(data), "\n")
		// Remove empty line at the end
		if len(lines) > 0 && lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}

		for _, line := range lines {
			if line != "" {
				result.WriteString(fmt.Sprintf("    %s\n", line))
			}
		}
	}

	return result.String(), nil
}

// processPipeline renders the pipeline section with proper formatting for processors
func processPipeline(pipeline interface{}) (string, error) {
	if pipeline == nil {
		return " []", nil
	}

	pipelineMap, ok := pipeline.(map[string]interface{})
	if !ok || len(pipelineMap) == 0 {
		return " []", nil
	}

	var result strings.Builder
	result.WriteString("\n")

	// Special handling for processors
	for component, config := range pipelineMap {
		result.WriteString(fmt.Sprintf("  %s:", component))

		if component == "processors" {
			processors, ok := config.([]interface{})
			if !ok || len(processors) == 0 {
				result.WriteString(" []\n")
				continue
			}

			result.WriteString("\n")
			for _, proc := range processors {
				procMap, ok := proc.(map[string]interface{})
				if !ok || len(procMap) == 0 {
					continue
				}

				for procType, procConfig := range procMap {
					result.WriteString(fmt.Sprintf("    - %s:", procType))

					// Handle different types of processor config
					switch v := procConfig.(type) {
					case string:
						// For string values like bloblang mappings
						result.WriteString(fmt.Sprintf(" |\n        %s\n", v))
					case map[string]interface{}:
						// For nested config objects
						if len(v) == 0 {
							result.WriteString(" {}\n")
							continue
						}
						result.WriteString("\n")
						data, err := yaml.Marshal(v)
						if err != nil {
							return "", fmt.Errorf("failed to marshal processor config to YAML: %w", err)
						}
						lines := strings.Split(string(data), "\n")
						if len(lines) > 0 && lines[len(lines)-1] == "" {
							lines = lines[:len(lines)-1]
						}
						for _, line := range lines {
							if line != "" {
								result.WriteString(fmt.Sprintf("        %s\n", line))
							}
						}
					default:
						// For primitive values
						result.WriteString(fmt.Sprintf(" %v\n", procConfig))
					}
				}
			}
		} else {
			// Regular config settings
			configMap, ok := config.(map[string]interface{})
			if !ok {
				// Handle scalar values
				result.WriteString(fmt.Sprintf(" %v\n", config))
				continue
			}

			if len(configMap) == 0 {
				result.WriteString(" {}\n")
				continue
			}

			result.WriteString("\n")

			// Marshal the config to YAML
			data, err := yaml.Marshal(configMap)
			if err != nil {
				return "", fmt.Errorf("failed to marshal config to YAML: %w", err)
			}

			// Indent the YAML with 4 spaces
			lines := strings.Split(string(data), "\n")
			// Remove empty line at the end
			if len(lines) > 0 && lines[len(lines)-1] == "" {
				lines = lines[:len(lines)-1]
			}

			for _, line := range lines {
				if line != "" {
					result.WriteString(fmt.Sprintf("    %s\n", line))
				}
			}
		}
	}

	return result.String(), nil
}

// processResources renders resource sections (cache and rate_limit)
func processResources(resources interface{}) (string, error) {
	if resources == nil {
		return " []", nil
	}

	resourcesSlice, ok := resources.([]interface{})
	if !ok || len(resourcesSlice) == 0 {
		return " []", nil
	}

	var result strings.Builder
	result.WriteString("\n")

	for _, resource := range resourcesSlice {
		resourceMap, ok := resource.(map[string]interface{})
		if !ok || len(resourceMap) == 0 {
			continue
		}

		for resType, resConfig := range resourceMap {
			result.WriteString(fmt.Sprintf("  - %s:", resType))

			configMap, ok := resConfig.(map[string]interface{})
			if !ok {
				// Handle scalar values
				result.WriteString(fmt.Sprintf(" %v\n", resConfig))
				continue
			}

			if len(configMap) == 0 {
				result.WriteString(" {}\n")
				continue
			}

			result.WriteString("\n")

			// Marshal the config to YAML
			data, err := yaml.Marshal(configMap)
			if err != nil {
				return "", fmt.Errorf("failed to marshal resource config to YAML: %w", err)
			}

			// Indent the YAML with 6 spaces
			lines := strings.Split(string(data), "\n")
			// Remove empty line at the end
			if len(lines) > 0 && lines[len(lines)-1] == "" {
				lines = lines[:len(lines)-1]
			}

			for _, line := range lines {
				if line != "" {
					result.WriteString(fmt.Sprintf("      %s\n", line))
				}
			}
		}
	}

	return result.String(), nil
}

// processBuffer renders the buffer section
func processBuffer(buffer interface{}) (string, error) {
	if buffer == nil {
		return " []", nil
	}

	bufferMap, ok := buffer.(map[string]interface{})
	if !ok || len(bufferMap) == 0 {
		return " []", nil
	}

	var result strings.Builder
	result.WriteString("\n")

	for component, config := range bufferMap {
		result.WriteString(fmt.Sprintf("  %s:", component))

		configMap, ok := config.(map[string]interface{})
		if !ok {
			// Handle scalar values
			result.WriteString(fmt.Sprintf(" %v\n", config))
			continue
		}

		if len(configMap) == 0 {
			result.WriteString(" {}\n")
			continue
		}

		result.WriteString("\n")

		// Marshal the config to YAML
		data, err := yaml.Marshal(configMap)
		if err != nil {
			return "", fmt.Errorf("failed to marshal config to YAML: %w", err)
		}

		// Indent the YAML with 4 spaces
		lines := strings.Split(string(data), "\n")
		// Remove empty line at the end
		if len(lines) > 0 && lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}

		for _, line := range lines {
			if line != "" {
				result.WriteString(fmt.Sprintf("    %s\n", line))
			}
		}
	}

	return result.String(), nil
}

// templateData represents the data structure expected by the simplified Benthos YAML template
type templateData struct {
	Input              string
	Output             string
	Pipeline           string
	CacheResources     string
	RateLimitResources string
	Buffer             string
	MetricsPort        int
	LogLevel           string
}

// simplifiedTemplate is a much simpler template that just places pre-rendered YAML blocks
var simplifiedTemplate = `input:{{.Input}}

output:{{.Output}}

pipeline:{{.Pipeline}}

cache_resources:{{.CacheResources}}

rate_limit_resources:{{.RateLimitResources}}

buffer:{{.Buffer}}

http:
  address: "0.0.0.0:{{.MetricsPort}}"

logger:
  level: "{{.LogLevel}}"
`

var benthosYamlTemplate = template.Must(template.New("benthos").Parse(simplifiedTemplate))

// RenderBenthosYAML renders a Benthos config to YAML using a struct-based approach
func RenderBenthosYAML(input, output, pipeline, cacheResources, rateLimitResources, buffer interface{}, metricsPort int, logLevel string) (string, error) {
	// Create the top-level config struct
	config := BenthosConfig{
		Input:  PossiblyArray{},
		Output: PossiblyArray{},
		Buffer: PossiblyEmptyObject{"none": map[string]interface{}{}}, // Default to none buffer
		HTTP: HTTPConfig{
			Address: fmt.Sprintf("0.0.0.0:%d", metricsPort),
		},
		Logger: LoggerConfig{
			Level: logLevel,
		},
	}

	// Handle input if provided
	if input != nil {
		if inputMap, ok := input.(map[string]interface{}); ok {
			config.Input = PossiblyArray(inputMap)
		}
	}

	// Handle output if provided
	if output != nil {
		if outputMap, ok := output.(map[string]interface{}); ok {
			config.Output = PossiblyArray(outputMap)
		}
	}

	// Handle buffer if provided
	if buffer != nil {
		if bufferMap, ok := buffer.(map[string]interface{}); ok && len(bufferMap) > 0 {
			config.Buffer = PossiblyEmptyObject(bufferMap) // Use provided buffer config
		}
		// Otherwise keep the default none buffer
	}

	// Handle cache resources if provided
	if cacheResources != nil {
		if cacheArray, ok := cacheResources.([]map[string]interface{}); ok {
			config.CacheResources = PossiblyResourceArray(cacheArray)
		}
	}

	// Handle rate limit resources if provided
	if rateLimitResources != nil {
		if rateArray, ok := rateLimitResources.([]map[string]interface{}); ok {
			config.RateLimitResources = PossiblyResourceArray(rateArray)
		}
	}

	// Handle pipeline processors if provided
	if pipeline != nil {
		if pipelineMap, ok := pipeline.(map[string]interface{}); ok {
			// Try multiple approaches to extract processors based on the type
			if processors, ok := pipelineMap["processors"]; ok {
				// Try as []interface{}
				if procArray, ok := processors.([]interface{}); ok {
					extractProcessorsFromInterface(procArray, &config.Pipeline.Processors)
				} else if typedArray, ok := processors.([]map[string]interface{}); ok {
					// Try as []map[string]interface{}
					for _, procMap := range typedArray {
						for procType, procConfig := range procMap {
							processor := Processor{
								Type:   procType,
								Config: procConfig,
							}
							config.Pipeline.Processors = append(config.Pipeline.Processors, processor)
						}
					}
				}
			}
		}
	}

	// Marshal the config to YAML
	var buf bytes.Buffer
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)

	if err := encoder.Encode(config); err != nil {
		return "", fmt.Errorf("failed to marshal Benthos config: %w", err)
	}

	return buf.String(), nil
}

// extractProcessorsFromInterface handles extracting processors from a []interface{} value
func extractProcessorsFromInterface(processors []interface{}, dest *ProcessorList) {
	for _, p := range processors {
		switch val := p.(type) {
		case map[string]interface{}:
			// Extract processor type and config
			for procType, procConfig := range val {
				processor := Processor{
					Type:   procType,
					Config: procConfig,
				}
				*dest = append(*dest, processor)
			}
		}
	}
}
