package asyncjob_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Azure/go-asyncjob"
	"github.com/stretchr/testify/assert"
)

func TestSimpleJob(t *testing.T) {
	t.Parallel()
	sb := &SqlSummaryJobLib{
		Params: nil,
	}

	jb := sb.BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{})
	renderGraph(jb)
	jobInstance := jb.Start(context.Background(), &SqlSummaryJobParameters{
		Table1: "table1",
		Query1: "query1",
		Table2: "table2",
		Query2: "query2",
	})
	jobErr := jobInstance.Wait(context.Background())
	assert.NoError(t, jobErr)
}

func renderGraph[T any](jb *asyncjob.JobDefinition[T]) error {
	graphStr, err := jb.Visualize()
	if err != nil {
		return err
	}

	fmt.Println(graphStr)
	return nil
}
