package asyncjob_test

import (
	"context"
	"testing"

	"github.com/Azure/go-asyncjob/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleJobWithResult(t *testing.T) {
	t.Parallel()
	sb := &SqlSummaryJobLib{}

	jd, err := sb.BuildJobWithResult(context.Background(), map[string]asyncjob.RetryPolicy{})
	assert.NoError(t, err)
	renderGraph(t, jd)

	jobInstance := jd.Start(context.WithValue(context.Background(), testLoggingContextKey, t), &SqlSummaryJobParameters{
		ServerName: "server1",
		Table1:     "table1",
		Query1:     "query1",
		Table2:     "table2",
		Query2:     "query2",
	})
	jobErr := jobInstance.Wait(context.Background())
	assert.NoError(t, jobErr)
	renderGraph(t, jobInstance)

	jobResult, jobErr := jobInstance.Result(context.Background())
	assert.NoError(t, jobErr)
	assert.NotNil(t, jobResult)
}
