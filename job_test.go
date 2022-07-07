package asyncjob

import (
	"context"
	"fmt"
	"testing"
)

func TestSimpleJob(t *testing.T) {
	sb := &SqlJobBuilder{
		Table1: "table1",
		Query1: "query1",
		Table2: "table2",
		Query2: "query2",
	}
	jb := sb.BuildJob(context.Background())

	dotGraph := jb.Visualize()
	fmt.Println(dotGraph)

	jb.Start(context.Background())
	jb.Wait(context.Background())
}

var _ JobBuilder = &SqlJobBuilder{}

type SqlJobBuilder struct {
	ServerName string
	Table1     string
	Query1     string
	Table2     string
	Query2     string
}

func (sjb *SqlJobBuilder) BuildJob(bCtx context.Context) *Job {
	job := NewJob("sqlSummaryJob")
	jobLib := &SqlSummaryJobLib{}

	serverNameParamTask := InputParam(bCtx, job, "param_serverName", &sjb.ServerName)
	connTsk, _ := StepAfter(bCtx, job, "getConnection", serverNameParamTask, jobLib.GetConnection)

	// TODO: handle error during BuildJob

	table1ParamTsk := InputParam(bCtx, job, "param_table1", &sjb.Table1)
	table1ClientTsk, _ := StepAfterBoth(bCtx, job, "getTableClient1", connTsk, table1ParamTsk, jobLib.GetTableClient)
	query1ParamTsk := InputParam(bCtx, job, "param_query1", &sjb.Query1)
	qery1ResultTsk, _ := StepAfterBoth(bCtx, job, "queryTable1", table1ClientTsk, query1ParamTsk, jobLib.ExecuteQuery)

	table2ParamTsk := InputParam(bCtx, job, "param_table2", &sjb.Table2)
	table2ClientTsk, _ := StepAfterBoth(bCtx, job, "getTableClient2", connTsk, table2ParamTsk, jobLib.GetTableClient)
	query2ParamTsk := InputParam(bCtx, job, "param_query2", &sjb.Query2)
	qery2ResultTsk, _ := StepAfterBoth(bCtx, job, "queryTable2", table2ClientTsk, query2ParamTsk, jobLib.ExecuteQuery)

	StepAfterBoth(bCtx, job, "summarize", qery1ResultTsk, qery2ResultTsk, jobLib.SummarizeQueryResult)
	return job
}
