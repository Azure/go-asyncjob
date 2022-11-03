package asyncjob

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSimpleJob(t *testing.T) {
	sb := &SqlSummaryJobLib{
		Table1: "table1",
		Query1: "query1",
		Table2: "table2",
		Query2: "query2",
	}
	jb := sb.BuildJob(context.Background(), nil)

	jb.Start(context.Background())
	jb.Wait(context.Background())

	dotGraph, err := jb.Visualize()
	if err != nil {
		t.FailNow()
	}
	fmt.Println(dotGraph)
}

func TestSimpleJobError(t *testing.T) {
	sb := &SqlSummaryJobLib{
		Table1:         "table1",
		Query1:         "query1",
		Table2:         "table2",
		Query2:         "query2",
		ErrorInjection: map[string]func() error{"ExecuteQuery.query2": getErrorFunc(fmt.Errorf("table2 schema error"), 1)},
	}
	jb := sb.BuildJob(context.Background(), nil)

	jb.Start(context.Background())
	jb.Wait(context.Background())

	dotGraph, err := jb.Visualize()
	if err != nil {
		t.FailNow()
	}
	fmt.Println(dotGraph)
}

func TestSimpleJobPanic(t *testing.T) {
	sb := &SqlSummaryJobLib{
		Table1:         "table1",
		Query1:         "panicQuery1",
		Table2:         "table2",
		Query2:         "query2",
		ErrorInjection: map[string]func() error{"ExecuteQuery.panicQuery1": getPanicFunc(1)},
	}
	jb := sb.BuildJob(context.Background(), NewLinearRetryPolicy(10*time.Millisecond, 3))

	jb.Start(context.Background())
	err := jb.Wait(context.Background())
	fmt.Print(err)

	dotGraph, err := jb.Visualize()
	if err != nil {
		t.FailNow()
	}
	fmt.Println(dotGraph)
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
