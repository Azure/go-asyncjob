package graph

import (
	"text/template"
)

// https://www.graphviz.org/docs/
// http://magjac.com/graphviz-visual-editor/

var digraphTemplate = template.Must(template.New("digraph").Parse(digraphTemplateText))

const digraphTemplateText = `digraph {
	compound = "true"
	newrank = "true"
	subgraph "root" {
{{ range $from, $toList := $}}{{ range $_, $to := $toList}}		"{{$from}}" -> "{{$to}}"
{{ end }}{{ end }}        }
}`

const digraphTemplateTextV2 = `digraph {
	compound = "true"
	newrank = "true"
	subgraph "root" {
{{ range $node := $.Nodes}}		"{{$node.Name}}" [label="{{$node.Name}}", shape="{{node.Shape}}"]
{{ end }}{{ end }}        }
}`
