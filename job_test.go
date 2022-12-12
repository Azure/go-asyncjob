package asyncjob_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-asyncjob"
	"github.com/stretchr/testify/assert"
)

func TestSimpleJob(t *testing.T) {
	t.Parallel()

	jobInstance1 := SqlSummaryAsyncJobDefinition.Start(context.WithValue(context.Background(), testLoggingContextKey, t), &SqlSummaryJobLib{
		Params: &SqlSummaryJobParameters{
			ServerName: "server1",
			Table1:     "table1",
			Query1:     "query1",
			Table2:     "table2",
			Query2:     "query2",
		},
	}, asyncjob.WithJobId("jobInstance1"))

	jobInstance2 := SqlSummaryAsyncJobDefinition.Start(context.WithValue(context.Background(), testLoggingContextKey, t), &SqlSummaryJobLib{
		Params: &SqlSummaryJobParameters{
			ServerName: "server2",
			Table1:     "table3",
			Query1:     "query3",
			Table2:     "table4",
			Query2:     "query4",
		},
	}, asyncjob.WithJobId("jobInstance2"))

	jobInstance3 := SqlSummaryAsyncJobDefinition.Start(context.WithValue(context.Background(), testLoggingContextKey, t), &SqlSummaryJobLib{
		Params: &SqlSummaryJobParameters{
			ServerName: "server3",
			Table1:     "table5",
			Query1:     "query5",
			Table2:     "table6",
			Query2:     "query6",
		},
	}, asyncjob.WithSequentialExecution())

	jobErr := jobInstance1.Wait(context.Background())
	assert.NoError(t, jobErr)
	renderGraph(t, jobInstance1)

	jobErr = jobInstance2.Wait(context.Background())
	assert.NoError(t, jobErr)
	renderGraph(t, jobInstance2)

	jobErr = jobInstance3.Wait(context.Background())
	assert.NoError(t, jobErr)
	renderGraph(t, jobInstance3)

	jobResult, jobErr := jobInstance1.Result(context.Background())
	assert.NoError(t, jobErr)
	assert.Equal(t, jobResult.QueryResult1["serverName"], "server1")
	assert.Equal(t, jobResult.QueryResult1["tableName"], "table1")
	assert.Equal(t, jobResult.QueryResult1["queryName"], "query1")
	assert.Equal(t, jobResult.QueryResult2["serverName"], "server1")
	assert.Equal(t, jobResult.QueryResult2["tableName"], "table2")
	assert.Equal(t, jobResult.QueryResult2["queryName"], "query2")

	jobResult3, jobErr := jobInstance3.Result(context.Background())
	assert.NoError(t, jobErr)
	assert.Equal(t, jobResult3.QueryResult1["serverName"], "server3")
	assert.Equal(t, jobResult3.QueryResult1["tableName"], "table5")
	assert.Equal(t, jobResult3.QueryResult1["queryName"], "query5")
	assert.Equal(t, jobResult3.QueryResult2["serverName"], "server3")
	assert.Equal(t, jobResult3.QueryResult2["tableName"], "table6")
	assert.Equal(t, jobResult3.QueryResult2["queryName"], "query6")
}

func TestJobError(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), testLoggingContextKey, t)
	jobInstance := SqlSummaryAsyncJobDefinition.Start(ctx, &SqlSummaryJobLib{
		Params: &SqlSummaryJobParameters{
			ServerName: "server1",
			Table1:     "table1",
			Query1:     "query1",
			Table2:     "table2",
			Query2:     "query2",
			ErrorInjection: map[string]func() error{
				"GetTableClient.server1.table1": func() error { return fmt.Errorf("table1 not exists") },
			},
		},
	})

	err := jobInstance.Wait(context.Background())
	assert.Error(t, err)

	jobErr := &asyncjob.JobError{}
	errors.As(err, &jobErr)
	assert.Equal(t, jobErr.Code, asyncjob.ErrStepFailed)
	assert.Equal(t, "GetTableClient1", jobErr.StepInstance.GetName())
}

func TestJobPanic(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), testLoggingContextKey, t)
	jobInstance := SqlSummaryAsyncJobDefinition.Start(ctx, &SqlSummaryJobLib{
		Params: &SqlSummaryJobParameters{
			ServerName: "server1",
			Table1:     "table1",
			Query1:     "query1",
			Table2:     "table2",
			Query2:     "query2",
			PanicInjection: map[string]bool{
				"GetTableClient.server1.table2": true,
			},
		},
	})

	err := jobInstance.Wait(context.Background())
	assert.Error(t, err)

	jobErr := &asyncjob.JobError{}
	assert.True(t, errors.As(err, &jobErr))
	assert.Equal(t, jobErr.Code, asyncjob.ErrStepFailed)
	assert.Equal(t, jobErr.StepInstance.GetName(), "GetTableClient2")
}

func TestJobStepRetry(t *testing.T) {
	t.Parallel()
	jd, err := BuildJob(context.Background(), map[string]asyncjob.RetryPolicy{"QueryTable1": newLinearRetryPolicy(time.Millisecond*3, 3)})
	assert.NoError(t, err)

	invalidStep := &asyncjob.StepDefinition[string]{}
	_, err = asyncjob.JobWithResult(jd, invalidStep)
	assert.Error(t, err)

	// newly created job definition should not be sealed
	assert.False(t, jd.Sealed())

	ctx := context.WithValue(context.Background(), testLoggingContextKey, t)
	ctx = context.WithValue(ctx, "error-injection.server1.table1.query1", fmt.Errorf("query exeeded memory limit"))
	jobInstance := jd.Start(ctx, &SqlSummaryJobLib{
		Params: &SqlSummaryJobParameters{
			ServerName: "server1",
			Table1:     "table1",
			Query1:     "query1",
			Table2:     "table2",
			Query2:     "query2",
			ErrorInjection: map[string]func() error{
				"ExecuteQuery.server1.table1.query1": func() error { return fmt.Errorf("query exeeded memory limit") },
			},
		},
	})

	// once Start() is triggered, job definition should be sealed
	assert.True(t, jd.Sealed())

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

func TestDefinitionGraph(t *testing.T) {
	t.Parallel()

	renderGraph(t, SqlSummaryAsyncJobDefinition)

	SqlSummaryAsyncJobDefinition.Seal()

	_, err := asyncjob.AddStep(context.Background(), SqlSummaryAsyncJobDefinition.JobDefinition, "EmailNotification2", emailNotificationStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.Error(t, err)
}

func renderGraph(t *testing.T, jb GraphRender) {
	graphStr, err := jb.Visualize()
	assert.NoError(t, err)

	t.Log(graphStr)
}

type GraphRender interface {
	Visualize() (string, error)
}
