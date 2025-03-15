package benthos

import (
	"text/template"
)

var benthosYamlTemplate = template.Must(template.New("benthos").Parse(`
input:
  {{.Input}}
output:
  {{.Output}}
pipeline:
  {{.Pipeline}}
cache_resources:
  {{.CacheResources}}
rate_limit_resources:
  {{.RateLimitResources}}
buffer:
  {{.Buffer}}

http:
  address: "0.0.0.0:{{.MetricsPort}}"

logger:
  level: "{{.LogLevel}}"
`))
