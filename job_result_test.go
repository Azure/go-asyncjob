package asyncjob_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleJobWithResult(t *testing.T) {
	t.Parallel()

	jobInstance := SqlSummaryAsyncJobDefinition.Start(context.WithValue(context.Background(), testLoggingContextKey, t), &SqlSummaryJobLib{
		Params: &SqlSummaryJobParameters{
			ServerName: "server2",
			Table1:     "table3",
			Query1:     "query3",
			Table2:     "table4",
			Query2:     "query4",
		},
	})
	jobErr := jobInstance.Wait(context.Background())
	assert.NoError(t, jobErr)
	renderGraph(t, jobInstance)

	jobResult, jobErr := jobInstance.Result(context.Background())
	assert.NoError(t, jobErr)
	assert.NotNil(t, jobResult)
}
