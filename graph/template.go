package graph

import (
	"text/template"
)

// https://www.graphviz.org/docs/
// http://magjac.com/graphviz-visual-editor/

var digraphTemplate = template.Must(template.New("digraph").Parse(digraphTemplateText))

const digraphTemplateText = `digraph {
	newrank = "true"
{{ range $node := $.Nodes}}		{{$node.ID}} [label="{{$node.Name}}" shape={{$node.Shape}} style={{$node.Style}} tooltip="{{$node.Tooltip}}" fillcolor={{$node.FillColor}}] 
{{ end }}        
{{ range $edge := $.Edges}}		{{$edge.FromNodeID}} -> {{$edge.ToNodeID}} [style={{$edge.Style}} tooltip="{{$edge.Tooltip}}" color={{$edge.Color}}] 
{{ end }}
}`
