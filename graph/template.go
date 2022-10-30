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

/* ideal output
digraph G {
  jobroot [shape=triangle style=filled fillcolor=gold tooltip="State: Failed\nDuration:5s"]
  param_servername [label="servername" shape=doublecircle style=filled fillcolor=green tooltip="Value: dummy.server.io"]
  param_table1 [label="table1" shape=doublecircle style=filled fillcolor=green tooltip="Value: table1"]
  param_query1 [label="query1" shape=doublecircle style=filled fillcolor=green tooltip="Value: select * from table1"]
  param_table2 [label="table2" shape=doublecircle style=filled fillcolor=green tooltip="Value: table2"]
  param_query2 [label="query2" shape=doublecircle style=filled fillcolor=green tooltip="Value: select * from table2"]
  jobroot -> param_servername [tooltip="time:2022-10-28T21:16:07Z"]
  param_servername -> func_getConnection
  func_getConnection [label="getConnection" shape=ellipse style=filled fillcolor=green  tooltip="State: Finished\nDuration:1s"]
  func_query1 [label="query1" shape=ellipse style=filled fillcolor=green tooltip="State: Finished\nDuration:2s"]
  func_query2 [label="query2" shape=ellipse style=filled fillcolor=red tooltip="State: Failed\nDuration:2s"]
  jobroot -> param_table1 [style=bold color=green tooltip="time:2022-10-28T21:16:07Z"]
  param_table1 -> func_query1 [tooltip="time:2022-10-28T21:16:07Z"]
  jobroot -> param_query1 [tooltip="time:2022-10-28T21:16:07Z"]
  param_query1 -> func_query1
  jobroot -> param_table2 [tooltip="time:2022-10-28T21:16:07Z"]
  param_table2 -> func_query2
  jobroot -> param_query2 [tooltip="time:2022-10-28T21:16:07Z"]
  param_query2 -> func_query2
  func_getConnection -> func_query1
  func_query1 -> func_summarize
  func_getConnection -> func_query2
  func_query2 -> func_summarize [color=red]
  func_summarize [label="summarize" shape=ellipse style=filled fillcolor=red tooltip="State: Blocked"]
  func_email [label="email" shape=ellipse style=filled tooltip="State: Pending"]
  func_summarize -> func_email [style=dotted]
}
*/
