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

	jd := sb.BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{})
	renderGraph(t, jd)

	jobInstance := jd.Start(context.Background(), &SqlSummaryJobParameters{
		ServerName: "server1",
		Table1:     "table1",
		Query1:     "query1",
		Table2:     "table2",
		Query2:     "query2",
	})
	jobErr := jobInstance.Wait(context.Background())
	assert.NoError(t, jobErr)

	renderGraph(t, jobInstance)
}

func TestJobError(t *testing.T) {
	t.Parallel()
	sb := &SqlSummaryJobLib{}

	jd := sb.BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{})
	renderGraph(t, jd)

	ctx := context.WithValue(context.Background(), "error-injection.server1.table1", fmt.Errorf("table1 not exists"))
	jobInstance := jd.Start(ctx, &SqlSummaryJobParameters{
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
	assert.Equal(t, "getTableClient1", jobErr.StepName)

	renderGraph(t, jobInstance)
}

func TestJobPanic(t *testing.T) {
	t.Parallel()
	sb := &SqlSummaryJobLib{}

	jd := sb.BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{})
	renderGraph(t, jd)

	ctx := context.WithValue(context.Background(), "panic-injection.server1.table2", true)
	jobInstance := jd.Start(ctx, &SqlSummaryJobParameters{
		ServerName: "server1",
		Table1:     "table1",
		Query1:     "query1",
		Table2:     "table2",
		Query2:     "query2",
	})

	err := jobInstance.Wait(context.Background())
	assert.Error(t, err)

	/*  panic is out of reach of jobError, but planning to catch panic in the future
	jobErr := &asyncjob.JobError{}
	assert.True(t, errors.As(err, &jobErr))
	assert.Equal(t, jobErr.Code, asyncjob.ErrStepFailed)
	assert.Equal(t, jobErr.StepName, "getTableClient1")*/

	renderGraph(t, jobInstance)
}

func renderGraph(t *testing.T, jb GraphRender) {
	graphStr, err := jb.Visualize()
	assert.NoError(t, err)

	t.Log(graphStr)
}

type GraphRender interface {
	Visualize() (string, error)
}
