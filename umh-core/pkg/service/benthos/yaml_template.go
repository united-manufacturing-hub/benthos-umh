package benthos

import (
	"text/template"
)

var benthosYamlTemplate = template.Must(template.New("benthos").Parse(`input:{{if .Input}}
{{- range $component, $config := .Input }}
  {{ $component }}:
{{- range $key, $value := $config }}
    {{ $key }}: {{ $value }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

output:{{if .Output}}
{{- range $component, $config := .Output }}
  {{ $component }}:
{{- range $key, $value := $config }}
    {{ $key }}: {{ $value }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

pipeline:{{if .Pipeline}}
{{- range $component, $config := .Pipeline }}
  {{ $component }}:
{{- if eq $component "processors" }}
{{- range $proc := $config }}
    - {{ range $pk, $pv := $proc }}{{ $pk }}:
{{- range $k, $v := $pv }}
        {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- else }}
{{- range $k, $v := $config }}
    {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

cache_resources:{{if .CacheResources}}
{{- range $resource := .CacheResources }}
{{- range $key, $value := $resource }}
  - {{ $key }}:
{{- range $k, $v := $value }}
      {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

rate_limit_resources:{{if .RateLimitResources}}
{{- range $resource := .RateLimitResources }}
{{- range $key, $value := $resource }}
  - {{ $key }}:
{{- range $k, $v := $value }}
      {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

buffer:{{if .Buffer}}
{{- range $component, $config := .Buffer }}
  {{ $component }}:
{{- range $key, $value := $config }}
    {{ $key }}: {{ $value }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

http:
  address: "0.0.0.0:{{ .MetricsPort }}"

logger:
  level: "{{ .LogLevel }}"`))
