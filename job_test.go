package asyncjob_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Azure/go-asyncjob"
	"github.com/stretchr/testify/assert"
)

func TestSimpleJob(t *testing.T) {
	t.Parallel()
	sb := &SqlSummaryJobLib{}

	jb := sb.BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{})
	renderGraph(jb)
	jobInstance := jb.Start(context.Background(), &SqlSummaryJobParameters{
		ServerName: "server1",
		Table1:     "table1",
		Query1:     "query1",
		Table2:     "table2",
		Query2:     "query2",
	})
	jobErr := jobInstance.Wait(context.Background())
	assert.NoError(t, jobErr)
}

func TestJobError(t *testing.T) {
	t.Parallel()
	sb := &SqlSummaryJobLib{}

	jb := sb.BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{})
	renderGraph(jb)

	ctx := context.WithValue(context.Background(), "error-injection.server1.table1", fmt.Errorf("table1 not exists"))
	jobInstance := jb.Start(ctx, &SqlSummaryJobParameters{
		ServerName: "server1",
		Table1:     "table1",
		Query1:     "query1",
		Table2:     "table2",
		Query2:     "query2",
	})

	err := jobInstance.Wait(context.Background())
	assert.Error(t, err)

	jobErr := &asyncjob.JobError{}
	errors.As(err, &jobErr)
	assert.Equal(t, jobErr.Code, asyncjob.ErrStepFailed)
	assert.Equal(t, jobErr.StepName, "getTableClient1")
}

func renderGraph[T any](jb *asyncjob.JobDefinition[T]) error {
	graphStr, err := jb.Visualize()
	if err != nil {
		return err
	}

	fmt.Println(graphStr)
	return nil
}
