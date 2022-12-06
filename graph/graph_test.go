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

	sortedNodes := g.TopologicalSort()
	assert.Equal(t, 4, len(sortedNodes))
	assert.Equal(t, root, sortedNodes[0])
	assert.Equal(t, summary, sortedNodes[3])

	err = g.AddNode(calc1)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, graph.ErrDuplicateNode))

	calc3 := &testNode{Name: "calc3"}
	err = g.Connect(root, calc3)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, graph.ErrConnectNotExistingNode))
}

func TestDemoGraph(t *testing.T) {
	g := graph.NewGraph(edgeSpecFromConnection)
	root := &testNode{Name: "root"}
	g.AddNode(root)

	paramServerName := &testNode{Name: "param_serverName"}
	g.AddNode(paramServerName)
	g.Connect(root, paramServerName)
	connect := &testNode{Name: "func_getConnection"}
	g.AddNode(connect)
	g.Connect(paramServerName, connect)
	checkAuth := &testNode{Name: "func_checkAuth"}
	g.AddNode(checkAuth)

	paramTable1 := &testNode{Name: "param_table1"}
	g.AddNode(paramTable1)
	g.Connect(root, paramTable1)
	tableClient1 := &testNode{Name: "func_getTableClient1"}
	g.AddNode(tableClient1)
	g.Connect(connect, tableClient1)
	g.Connect(paramTable1, tableClient1)
	paramQuery1 := &testNode{Name: "param_query1"}
	g.AddNode(paramQuery1)
	g.Connect(root, paramQuery1)
	queryTable1 := &testNode{Name: "func_queryTable1"}
	g.AddNode(queryTable1)
	g.Connect(paramQuery1, queryTable1)
	g.Connect(tableClient1, queryTable1)
	g.Connect(checkAuth, queryTable1)

	paramTable2 := &testNode{Name: "param_table2"}
	g.AddNode(paramTable2)
	g.Connect(root, paramTable2)
	tableClient2 := &testNode{Name: "func_getTableClient2"}
	g.AddNode(tableClient2)
	g.Connect(connect, tableClient2)
	g.Connect(paramTable2, tableClient2)
	paramQuery2 := &testNode{Name: "param_query2"}
	g.AddNode(paramQuery2)
	g.Connect(root, paramQuery2)
	queryTable2 := &testNode{Name: "func_queryTable2"}
	g.AddNode(queryTable2)
	g.Connect(paramQuery2, queryTable2)
	g.Connect(tableClient2, queryTable2)
	g.Connect(checkAuth, queryTable2)

	summary := &testNode{Name: "func_summarize"}
	g.AddNode(summary)
	g.Connect(queryTable1, summary)
	g.Connect(queryTable2, summary)

	email := &testNode{Name: "func_email"}
	g.AddNode(email)
	g.Connect(summary, email)

	sortedNodes := g.TopologicalSort()
	for _, n := range sortedNodes {
		fmt.Println(n.GetName())
	}
}

type testNode struct {
	Name string
}

func (tn *testNode) GetName() string {
	return tn.Name
}

func (tn *testNode) DotSpec() *graph.DotNodeSpec {
	return &graph.DotNodeSpec{
		Name:        tn.Name,
		DisplayName: tn.Name,
		Tooltip:     tn.Name,
		Shape:       "box",
		Style:       "filled",
		FillColor:   "green",
	}
}

func edgeSpecFromConnection(from, to *testNode) *graph.DotEdgeSpec {
	return &graph.DotEdgeSpec{
		FromNodeName: from.GetName(),
		ToNodeName:   to.GetName(),
		Tooltip:      fmt.Sprintf("%s -> %s", from.DotSpec().Name, to.DotSpec().Name),
		Style:        "solid",
		Color:        "black",
	}
}
