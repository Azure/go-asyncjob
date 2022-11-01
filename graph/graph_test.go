package graph_test

import (
	"fmt"
	"testing"

	"github.com/Azure/go-asyncjob/graph"
)

func TestSimpleJob(t *testing.T) {
	g := graph.NewGraph[*testNode](edgeSpecFromConnection)
	root := &testNode{Name: "root"}
	g.AddNode(root)
	calc1 := &testNode{Name: "calc1"}
	g.AddNode(calc1)
	calc2 := &testNode{Name: "calc2"}
	g.AddNode(calc2)
	summary := &testNode{Name: "summary"}
	g.AddNode(summary)

	g.Connect(root.DotSpec().ID, calc1.DotSpec().ID)
	g.Connect(root.DotSpec().ID, calc2.DotSpec().ID)
	g.Connect(calc1.DotSpec().ID, summary.DotSpec().ID)
	g.Connect(calc2.DotSpec().ID, summary.DotSpec().ID)

	graph, err := g.ToDotGraph()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(graph)
}

type testNode struct {
	Name string
}

func (tn *testNode) DotSpec() *graph.DotNodeSpec {
	return &graph.DotNodeSpec{
		ID:        tn.Name,
		Name:      tn.Name,
		Tooltip:   tn.Name,
		Shape:     "box",
		Style:     "filled",
		FillColor: "green",
	}
}

func edgeSpecFromConnection(from, to *testNode) *graph.DotEdgeSpec {
	return &graph.DotEdgeSpec{
		FromNodeID: from.DotSpec().ID,
		ToNodeID:   to.DotSpec().ID,
		Tooltip:    fmt.Sprintf("%s -> %s", from.DotSpec().Name, to.DotSpec().Name),
		Style:      "solid",
		Color:      "black",
	}
}
