package graph

import (
	"text/template"
)

var digraphTemplate = template.Must(template.New("digraph").Parse(digraphTemplateText))

const digraphTemplateText = `digraph {
	compound = "true"
	newrank = "true"
	subgraph "root" {
{{ range $from, $toList := $}}{{ range $_, $to := $toList}}		"{{$from}}" -> "{{$to}}"
{{ end }}{{ end }}        }
}`
