package asyncjob_test

import (
	"context"
	"testing"

	"github.com/Azure/go-asyncjob/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleJobWithResult(t *testing.T) {
	t.Parallel()

	jd, err := BuildJobWithResult(context.Background(), map[string]asyncjob.RetryPolicy{})
	assert.NoError(t, err)
	renderGraph(t, jd)

	jobInstance := jd.Start(context.WithValue(context.Background(), testLoggingContextKey, t), &SqlSummaryJobLib{
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
