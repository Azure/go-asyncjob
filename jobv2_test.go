package asyncjob_test

import (
	"context"
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

	jobInstance := jb.Start(context.Background(), &SqlSummaryJobParameters{
		Table1: "table1",
		Query1: "query1",
		Table2: "table2",
		Query2: "query2",
	})
	jobErr := jobInstance.Wait(context.Background())
	assert.NoError(t, jobErr)
}
