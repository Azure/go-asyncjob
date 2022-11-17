package asyncjob_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Azure/go-asyncjob"
	"github.com/stretchr/testify/assert"
)

func TestSimpleJobV2(t *testing.T) {
	t.Parallel()
	sb := &SqlSummaryJobLib{
		Params: nil,
	}

	jb := sb.BuildJobV2(context.Background(), map[string]asyncjob.RetryPolicy{})
	renderGraphV2(jb) // got bug in stepBuilderV2

	jobInstance := jb.Start(context.Background(), &SqlSummaryJobParameters{
		Table1: "table1",
		Query1: "query1",
		Table2: "table2",
		Query2: "query2",
	})
	jobErr := jobInstance.Wait(context.Background())
	assert.NoError(t, jobErr)
}

func renderGraphV2[T any](jb *asyncjob.JobDefinition[T]) error {
	graphStr, err := jb.Visualize()
	if err != nil {
		return err
	}

	fmt.Println(graphStr)
	return nil
}
