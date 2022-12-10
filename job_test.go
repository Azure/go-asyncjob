package asyncjob_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-asyncjob/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleJob(t *testing.T) {
	t.Parallel()

	jd, err := BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{})
	assert.NoError(t, err)
	renderGraph(t, jd)

	jobInstance := jd.Start(context.WithValue(context.Background(), testLoggingContextKey, t), &SqlSummaryJobLibAdvanced{
		Params: &SqlSummaryJobParameters{
			ServerName: "server1",
			Table1:     "table1",
			Query1:     "query1",
			Table2:     "table2",
			Query2:     "query2",
		},
		SqlSummaryJobLib: SqlSummaryJobLib{},
	})
	jobErr := jobInstance.Wait(context.Background())
	assert.NoError(t, jobErr)
	renderGraph(t, jobInstance)

	jobInstance2 := jd.Start(context.WithValue(context.Background(), testLoggingContextKey, t), &SqlSummaryJobLibAdvanced{
		Params: &SqlSummaryJobParameters{
			ServerName: "server2",
			Table1:     "table3",
			Query1:     "query3",
			Table2:     "table4",
			Query2:     "query4",
		},
		SqlSummaryJobLib: SqlSummaryJobLib{},
	})
	jobErr = jobInstance2.Wait(context.Background())
	assert.NoError(t, jobErr)
	renderGraph(t, jobInstance2)
}

func TestJobError(t *testing.T) {
	t.Parallel()

	jd, err := BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{})
	assert.NoError(t, err)

	ctx := context.WithValue(context.Background(), testLoggingContextKey, t)
	ctx = context.WithValue(ctx, "error-injection.server1.table1", fmt.Errorf("table1 not exists"))
	jobInstance := jd.Start(ctx, &SqlSummaryJobLibAdvanced{
		Params: &SqlSummaryJobParameters{
			ServerName: "server1",
			Table1:     "table1",
			Query1:     "query1",
			Table2:     "table2",
			Query2:     "query2",
		},
		SqlSummaryJobLib: SqlSummaryJobLib{},
	})

	err = jobInstance.Wait(context.Background())
	assert.Error(t, err)

	jobErr := &asyncjob.JobError{}
	errors.As(err, &jobErr)
	assert.Equal(t, jobErr.Code, asyncjob.ErrStepFailed)
	assert.Equal(t, "GetTableClient1", jobErr.StepInstance.GetName())
}

func TestJobPanic(t *testing.T) {
	t.Parallel()
	jd, err := BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{})
	assert.NoError(t, err)

	ctx := context.WithValue(context.Background(), testLoggingContextKey, t)
	ctx = context.WithValue(ctx, "panic-injection.server1.table2", true)
	jobInstance := jd.Start(ctx, &SqlSummaryJobLibAdvanced{
		Params: &SqlSummaryJobParameters{
			ServerName: "server1",
			Table1:     "table1",
			Query1:     "query1",
			Table2:     "table2",
			Query2:     "query2",
		},
		SqlSummaryJobLib: SqlSummaryJobLib{},
	})

	err = jobInstance.Wait(context.Background())
	assert.Error(t, err)

	/*  panic is out of reach of jobError, but planning to catch panic in the future
	jobErr := &asyncjob.JobError{}
	assert.True(t, errors.As(err, &jobErr))
	assert.Equal(t, jobErr.Code, asyncjob.ErrStepFailed)
	assert.Equal(t, jobErr.StepName, "getTableClient1")*/
}

func TestJobStepRetry(t *testing.T) {
	t.Parallel()
	jd, err := BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{"QueryTable1": newLinearRetryPolicy(time.Millisecond*3, 3)})
	assert.NoError(t, err)

	ctx := context.WithValue(context.Background(), testLoggingContextKey, t)
	ctx = context.WithValue(ctx, "error-injection.server1.table1.query1", fmt.Errorf("query exeeded memory limit"))
	jobInstance := jd.Start(ctx, &SqlSummaryJobLibAdvanced{
		Params: &SqlSummaryJobParameters{
			ServerName: "server1",
			Table1:     "table1",
			Query1:     "query1",
			Table2:     "table2",
			Query2:     "query2",
		},
		SqlSummaryJobLib: SqlSummaryJobLib{},
	})

	err = jobInstance.Wait(context.Background())
	assert.Error(t, err)

	jobErr := &asyncjob.JobError{}
	errors.As(err, &jobErr)
	assert.Equal(t, jobErr.Code, asyncjob.ErrStepFailed)
	assert.Equal(t, "QueryTable1", jobErr.StepInstance.GetName())

	exeData := jobErr.StepInstance.ExecutionData()
	assert.Equal(t, exeData.Retried.Count, 3)

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
