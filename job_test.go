package asyncjob

import (
	"context"
	"fmt"
	"github.com/Azure/go-asynctask"
	"testing"
)

func TestSimpleJob(t *testing.T) {
	sb := &SqlJobBuilder{
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
	sb := &SqlJobBuilder{
		Table1: "table1",
		Query1: "query1",
		Table2: "table2",
		Query2: "query2",
	}
	jb := sb.BuildJob(context.Background(), map[string]error{"table2": fmt.Errorf("table2 schema error")})

	jb.Start(context.Background())
	jb.Wait(context.Background())

	dotGraph, err := jb.Visualize()
	if err != nil {
		t.FailNow()
	}
	fmt.Println(dotGraph)
}

type SqlJobBuilder struct {
	ServerName string
	Table1     string
	Query1     string
	Table2     string
	Query2     string
}

func (sjb *SqlJobBuilder) BuildJob(bCtx context.Context, errorInjections map[string]error) *Job {
	job := NewJob("sqlSummaryJob")
	jobLib := &SqlSummaryJobLib{ErrorInjection: errorInjections}

	serverNameParamTask := InputParam(bCtx, job, "serverName", &sjb.ServerName)
	connTsk, _ := StepAfter(bCtx, job, "getConnection", serverNameParamTask, jobLib.GetConnection)

	table1ParamTsk := InputParam(bCtx, job, "table1", &sjb.Table1)
	table1ClientTsk, _ := StepAfterBoth(bCtx, job, "getTableClient1", connTsk, table1ParamTsk, jobLib.GetTableClient)
	query1ParamTsk := InputParam(bCtx, job, "query1", &sjb.Query1)
	qery1ResultTsk, _ := StepAfterBoth(bCtx, job, "queryTable1", table1ClientTsk, query1ParamTsk, jobLib.ExecuteQuery)

	table2ParamTsk := InputParam(bCtx, job, "table2", &sjb.Table2)
	table2ClientTsk, _ := StepAfterBoth(bCtx, job, "getTableClient2", connTsk, table2ParamTsk, jobLib.GetTableClient)
	query2ParamTsk := InputParam(bCtx, job, "query2", &sjb.Query2)
	qery2ResultTsk, _ := StepAfterBoth(bCtx, job, "queryTable2", table2ClientTsk, query2ParamTsk, jobLib.ExecuteQuery)

	summaryTsk, _ := StepAfterBoth(bCtx, job, "summarize", qery1ResultTsk, qery2ResultTsk, jobLib.SummarizeQueryResult)
	AddStep(bCtx, job, "emailNotification", asynctask.ActionToFunc(jobLib.EmailNotification), ExecuteAfter(summaryTsk))
	return job
}
