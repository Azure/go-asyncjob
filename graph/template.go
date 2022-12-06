package graph

import (
	"text/template"
)

// https://www.graphviz.org/docs/
// http://magjac.com/graphviz-visual-editor/

var digraphTemplate = template.Must(template.New("digraph").Parse(digraphTemplateText))

type templateRef struct {
	Nodes []*DotNodeSpec
	Edges []*DotEdgeSpec
}

const digraphTemplateText = `digraph {
	newrank = "true"
{{ range $node := $.Nodes}}		"{{$node.Name}}" [label="{{$node.DisplayName}}" shape={{$node.Shape}} style={{$node.Style}} tooltip="{{$node.Tooltip}}" fillcolor={{$node.FillColor}}] 
{{ end }}        
{{ range $edge := $.Edges}}		"{{$edge.FromNodeName}}" -> "{{$edge.ToNodeName}}" [style={{$edge.Style}} tooltip="{{$edge.Tooltip}}" color={{$edge.Color}}] 
{{ end }}
}`
