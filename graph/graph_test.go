package graph_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Azure/go-asyncjob/graph"
	"github.com/stretchr/testify/assert"
)

func TestSimpleGraph(t *testing.T) {
	g := graph.NewGraph(edgeSpecFromConnection)
	root := &testNode{Name: "root"}
	g.AddNode(root)
	calc1 := &testNode{Name: "calc1"}
	g.AddNode(calc1)
	calc2 := &testNode{Name: "calc2"}
	g.AddNode(calc2)
	summary := &testNode{Name: "summary"}
	g.AddNode(summary)

	g.Connect(root, calc1)
	g.Connect(root, calc2)
	g.Connect(calc1, summary)
	g.Connect(calc2, summary)

	graphStr, err := g.ToDotGraph()
	if err != nil {
		assert.NoError(t, err)
	}
	t.Log(graphStr)

	err = g.AddNode(calc1)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, graph.ErrDuplicateNode))

	calc3 := &testNode{Name: "calc3"}
	err = g.Connect(root, calc3)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, graph.ErrConnectNotExistingNode))
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
