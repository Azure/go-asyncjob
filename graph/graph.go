package graph

import (
	"bytes"
)

type GraphErrorCode string

const (
	ErrDuplicateNode          GraphErrorCode = "node with same key already exists in this graph"
	ErrConnectNotExistingNode GraphErrorCode = "node to connect does not exist in this graph"
)

func (ge GraphErrorCode) Error() string {
	return string(ge)
}

type NodeConstrain interface {
	// Name of the node, used as key in the graph, so should be unique.
	DotSpec() DotNodeSpec
}

type Edge[NT NodeConstrain] struct {
	From NT
	To   NT
}

type DotNodeSpec struct {
	ID      string
	Name    string
	Tooltip string
	Shape   string
	Style   string
}

type Graph[NT NodeConstrain] struct {
	nodes     map[string]NT
	nodeEdges map[string][]*Edge[NT]
}

func NewGraph[NT NodeConstrain]() *Graph[NT] {
	return &Graph[NT]{
		nodes:     make(map[string]NT),
		nodeEdges: make(map[string][]*Edge[NT]),
	}
}

func (g *Graph[NT]) AddNode(n NT) error {
	nodeKey := n.DotSpec().ID
	if _, ok := g.nodes[nodeKey]; ok {
		return ErrDuplicateNode
	}
	g.nodes[nodeKey] = n

	return nil
}

func (g *Graph[NT]) Connect(from, to string) error {
	var nodeFrom, nodeTo NT
	var ok bool
	if nodeFrom, ok = g.nodes[from]; !ok {
		return ErrConnectNotExistingNode
	}

	if nodeTo, ok = g.nodes[to]; !ok {
		return ErrConnectNotExistingNode
	}

	g.nodeEdges[from] = append(g.nodeEdges[from], &Edge[NT]{From: nodeFrom, To: nodeTo})
	return nil
}

// https://en.wikipedia.org/wiki/DOT_(graph_description_language)
func (g *Graph[NT]) ToDotGraph() (string, error) {
	edges := make(map[string][]string)
	for _, nodeEdges := range g.nodeEdges {
		for _, edge := range nodeEdges {
			edges[edge.From.DotSpec().ID] = append(edges[edge.From.DotSpec().ID], edge.To.DotSpec().ID)
		}
	}

	buf := new(bytes.Buffer)
	err := digraphTemplate.Execute(buf, edges)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
