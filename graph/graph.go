package graph

import (
	"bytes"
	"fmt"
)

// NodeConstrain is a constraint for a node in a graph
type NodeConstrain interface {
	// Name of the node, should be unique in the graph
	GetName() string
	// DotSpec returns the dot spec for this node
	DotSpec() *DotNodeSpec
}

// EdgeSpecFunc is a function that returns the DOT specification for an edge.
type EdgeSpecFunc[T NodeConstrain] func(from, to T) *DotEdgeSpec

type Edge[NT NodeConstrain] struct {
	From NT
	To   NT
}

// DotNodeSpec is the specification for a node in a DOT graph
type DotNodeSpec struct {
	// id of the node
	Name string
	// display text of the node
	DisplayName string
	Tooltip     string
	Shape       string
	Style       string
	FillColor   string
}

// DotEdgeSpec is the specification for an edge in DOT graph
type DotEdgeSpec struct {
	FromNodeName string
	ToNodeName   string
	Tooltip      string
	Style        string
	Color        string
}

// Graph hold the nodes and edges of a graph
type Graph[NT NodeConstrain] struct {
	nodes        map[string]NT
	nodeEdges    map[string][]*Edge[NT]
	edgeSpecFunc EdgeSpecFunc[NT]
}

// NewGraph creates a new graph
func NewGraph[NT NodeConstrain](edgeSpecFunc EdgeSpecFunc[NT]) *Graph[NT] {
	return &Graph[NT]{
		nodes:        make(map[string]NT),
		nodeEdges:    make(map[string][]*Edge[NT]),
		edgeSpecFunc: edgeSpecFunc,
	}
}

// AddNode adds a node to the graph
func (g *Graph[NT]) AddNode(n NT) error {
	nodeKey := n.GetName()
	if _, ok := g.nodes[nodeKey]; ok {
		return NewGraphError(ErrDuplicateNode, fmt.Sprintf("node with key %s already exists in this graph", nodeKey))
	}
	g.nodes[nodeKey] = n

	return nil
}

func (g *Graph[NT]) Connect(from, to NT) error {
	fromNodeKey := from.GetName()
	toNodeKey := to.GetName()
	var ok bool
	if from, ok = g.nodes[fromNodeKey]; !ok {
		return NewGraphError(ErrConnectNotExistingNode, fmt.Sprintf("cannot connect node %s, it's not added in this graph yet", fromNodeKey))
	}

	if to, ok = g.nodes[toNodeKey]; !ok {
		return NewGraphError(ErrConnectNotExistingNode, fmt.Sprintf("cannot connect node %s, it's not added in this graph yet", toNodeKey))
	}

	g.nodeEdges[fromNodeKey] = append(g.nodeEdges[fromNodeKey], &Edge[NT]{From: from, To: to})
	return nil
}

// https://en.wikipedia.org/wiki/DOT_(graph_description_language)
func (g *Graph[NT]) ToDotGraph() (string, error) {
	nodes := make([]*DotNodeSpec, 0)
	for _, node := range g.nodes {
		nodes = append(nodes, node.DotSpec())
	}

	edges := make([]*DotEdgeSpec, 0)
	for _, nodeEdges := range g.nodeEdges {
		for _, edge := range nodeEdges {
			edges = append(edges, g.edgeSpecFunc(edge.From, edge.To))
		}
	}

	buf := new(bytes.Buffer)
	err := digraphTemplate.Execute(buf, templateRef{Nodes: nodes, Edges: edges})
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (g *Graph[NT]) TopologicalSort() []NT {
	visited := make(map[string]bool)
	stack := make([]NT, 0)

	for _, node := range g.nodes {
		if !visited[node.GetName()] {
			g.topologicalSortInternal(node, &visited, &stack)
		}
	}
	return stack
}

func (g *Graph[NT]) topologicalSortInternal(node NT, visited *map[string]bool, stack *[]NT) {
	(*visited)[node.GetName()] = true
	for _, edge := range g.nodeEdges[node.GetName()] {
		if !(*visited)[edge.To.GetName()] {
			g.topologicalSortInternal(edge.To, visited, stack)
		}
	}
	*stack = append([]NT{node}, *stack...)
}

type orderedset struct {
	indexes map[string]int
	items   []string
	length  int
}
