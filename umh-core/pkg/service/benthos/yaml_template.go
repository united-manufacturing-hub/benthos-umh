package benthos

import (
	"text/template"
)

var benthosYamlTemplate = template.Must(template.New("benthos").Parse(`input:{{if .Input}}
{{- range .Input }}
{{- range $key, $value := . }}
  {{ $key }}:
{{- range $k, $v := $value }}
    {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

output:{{if .Output}}
{{- range .Output }}
{{- range $key, $value := . }}
  {{ $key }}:
{{- range $k, $v := $value }}
    {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

pipeline:{{if .Pipeline}}
{{- range .Pipeline }}
{{- range $key, $value := . }}
  {{ $key }}:
{{- if eq $key "processors" }}
{{- range $proc := $value }}
    - {{ range $pk, $pv := $proc }}{{ $pk }}:
{{- range $k, $v := $pv }}
        {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- else }}
{{- range $k, $v := $value }}
    {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

cache_resources:{{if .CacheResources}}
{{- range .CacheResources }}
{{- range $key, $value := . }}
  - {{ $key }}:
{{- range $k, $v := $value }}
      {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

rate_limit_resources:{{if .RateLimitResources}}
{{- range .RateLimitResources }}
{{- range $key, $value := . }}
  - {{ $key }}:
{{- range $k, $v := $value }}
      {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

buffer:{{if .Buffer}}
{{- range .Buffer }}
{{- range $key, $value := . }}
  {{ $key }}:
{{- range $k, $v := $value }}
    {{ $k }}: {{ $v }}
{{- end }}
{{- end }}
{{- end }}
{{- else }} []{{end}}

http:
  address: "0.0.0.0:{{ .MetricsPort }}"

logger:
  level: "{{ .LogLevel }}"`))
