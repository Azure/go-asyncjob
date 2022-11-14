package asyncjob_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-asyncjob"
	"github.com/goccy/go-graphviz"
	"github.com/stretchr/testify/assert"
)

func TestSimpleJob(t *testing.T) {
	t.Parallel()
	sb := &SqlSummaryJobLib{
		Table1:        "table1",
		Query1:        "query1",
		Table2:        "table2",
		Query2:        "query2",
		RetryPolicies: map[string]asyncjob.RetryPolicy{},
	}
	jb := sb.BuildJob(context.Background())

	jb.Start(context.Background())
	jobErr := jb.Wait(context.Background())
	assert.NoError(t, jobErr)

	renderErr := renderGraph(jb)
	assert.NoError(t, renderErr)
}

func TestSimpleJobError(t *testing.T) {
	t.Parallel()
	sb := &SqlSummaryJobLib{
		Table1:         "table1",
		Query1:         "query1",
		Table2:         "table2",
		Query2:         "query2",
		ErrorInjection: map[string]func() error{"ExecuteQuery.query2": getErrorFunc(fmt.Errorf("table2 schema error"), 1)},
		RetryPolicies:  map[string]asyncjob.RetryPolicy{},
	}
	jb := sb.BuildJob(context.Background())

	jb.Start(context.Background())
	jb.Wait(context.Background())
	jobErr := jb.Wait(context.Background())
	if jobErr != nil {
		assert.Error(t, jobErr)
	}

	renderErr := renderGraph(jb)
	assert.NoError(t, renderErr)
}

func TestSimpleJobPanic(t *testing.T) {
	t.Parallel()
	linearRetry := newLinearRetryPolicy(10*time.Millisecond, 2)
	sb := &SqlSummaryJobLib{
		Table1: "table1",
		Query1: "panicQuery1",
		Table2: "table2",
		Query2: "query2",
		ErrorInjection: map[string]func() error{
			"CheckAuth":                getErrorFunc(fmt.Errorf("auth transient error"), 1),
			"GetConnection":            getErrorFunc(fmt.Errorf("InternalServerError"), 1),
			"ExecuteQuery.panicQuery1": getPanicFunc(4),
		},
		RetryPolicies: map[string]asyncjob.RetryPolicy{
			"CheckAuth":     linearRetry, // coverage for AddStep
			"GetConnection": linearRetry, // coverage for StepAfter
			"QueryTable1":   linearRetry, // coverage for StepAfterBoth
		},
	}
	jb := sb.BuildJob(context.Background())

	jb.Start(context.Background())
	jobErr := jb.Wait(context.Background())
	if jobErr != nil {
		assert.Error(t, jobErr)
	}

	renderErr := renderGraph(jb)
	assert.NoError(t, renderErr)
}

func renderGraph(jb *asyncjob.Job) error {
	graphStr, err := jb.Visualize()
	if err != nil {
		return err
	}

	fmt.Println(graphStr)

	_, err = graphviz.ParseBytes([]byte(graphStr))
	if err != nil {
		return err
	}

	return nil
}

func getErrorFunc(err error, count int) func() error {
	return func() error {
		if count > 0 {
			count--
			return err
		}
		return nil
	}
}

func getPanicFunc(count int) func() error {
	return func() error {
		if count > 0 {
			count--
			panic("panic")
		}
		return nil
	}
}
