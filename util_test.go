package asyncjob_test

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/go-asyncjob"
	"github.com/Azure/go-asynctask"
)

type SqlSummaryJobLib struct {
	ServerName     string
	Table1         string
	Query1         string
	Table2         string
	Query2         string
	ErrorInjection map[string]func() error
	RetryPolicies  map[string]asyncjob.RetryPolicy
}

type SqlConnection struct {
	ServerName string
}

type SqlTableClient struct {
	ServerName string
	TableName  string
}

type SqlQueryResult struct {
	Data map[string]interface{}
}

type SummarizedResult struct {
	Data1 map[string]interface{}
	Data2 map[string]interface{}
}

func (sql *SqlSummaryJobLib) GetConnection(ctx context.Context, serverName *string) (*SqlConnection, error) {
	fmt.Println("GetConnection")
	if sql.ErrorInjection != nil {
		if errFunc, ok := sql.ErrorInjection[fmt.Sprintf("GetConnection.%s", *serverName)]; ok {
			if err := errFunc(); err != nil {
				return nil, err
			}
		}
	}
	return &SqlConnection{ServerName: *serverName}, nil
}

func (sql *SqlSummaryJobLib) GetTableClient(ctx context.Context, conn *SqlConnection, tableName *string) (*SqlTableClient, error) {
	fmt.Println("GetTableClient with tableName:", *tableName)
	if sql.ErrorInjection != nil {
		if errFunc, ok := sql.ErrorInjection[fmt.Sprintf("GetTableClient.%s", *tableName)]; ok {
			if err := errFunc(); err != nil {
				return nil, err
			}
		}
	}
	return &SqlTableClient{ServerName: conn.ServerName, TableName: *tableName}, nil
}

func (sql *SqlSummaryJobLib) CheckAuth(ctx context.Context) error {
	if sql.ErrorInjection != nil {
		if errFunc, ok := sql.ErrorInjection["CheckAuth"]; ok {
			if err := errFunc(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sql *SqlSummaryJobLib) ExecuteQuery(ctx context.Context, tableClient *SqlTableClient, queryString *string) (*SqlQueryResult, error) {
	fmt.Println("ExecuteQuery: ", *queryString)
	if sql.ErrorInjection != nil {
		if errFunc, ok := sql.ErrorInjection[fmt.Sprintf("ExecuteQuery.%s", *queryString)]; ok {
			if err := errFunc(); err != nil {
				return nil, err
			}
		}
	}

	return &SqlQueryResult{Data: map[string]interface{}{"serverName": tableClient.ServerName, "tableName": tableClient.TableName, "queryName": *queryString}}, nil
}

func (sql *SqlSummaryJobLib) SummarizeQueryResult(ctx context.Context, result1 *SqlQueryResult, result2 *SqlQueryResult) (*SummarizedResult, error) {
	fmt.Println("SummarizeQueryResult")
	if sql.ErrorInjection != nil {
		if errFunc, ok := sql.ErrorInjection["SummarizeQueryResult"]; ok {
			if err := errFunc(); err != nil {
				return nil, err
			}
		}
	}
	return &SummarizedResult{Data1: result1.Data, Data2: result2.Data}, nil
}

func (sql *SqlSummaryJobLib) EmailNotification(ctx context.Context) error {
	fmt.Println("EmailNotification")
	return nil
}

func (sql *SqlSummaryJobLib) BuildJob(bCtx context.Context) *asyncjob.Job {
	job := asyncjob.NewJob("sqlSummaryJob")

	serverNameParamTask := asyncjob.InputParam(bCtx, job, "serverName", &sql.ServerName)
	connTsk, _ := asyncjob.StepAfter(bCtx, job, "GetConnection", serverNameParamTask, sql.GetConnection, asyncjob.WithRetry(sql.RetryPolicies["GetConnection"]))

	checkAuthTask, _ := asyncjob.AddStep(bCtx, job, "CheckAuth", asynctask.ActionToFunc(sql.CheckAuth), asyncjob.WithRetry(sql.RetryPolicies["CheckAuth"]))

	table1ParamTsk := asyncjob.InputParam(bCtx, job, "table1", &sql.Table1)
	table1ClientTsk, _ := asyncjob.StepAfterBoth(bCtx, job, "getTableClient1", connTsk, table1ParamTsk, sql.GetTableClient)
	query1ParamTsk := asyncjob.InputParam(bCtx, job, "query1", &sql.Query1)
	qery1ResultTsk, _ := asyncjob.StepAfterBoth(bCtx, job, "QueryTable1", table1ClientTsk, query1ParamTsk, sql.ExecuteQuery, asyncjob.WithRetry(sql.RetryPolicies["QueryTable1"]), asyncjob.ExecuteAfter(checkAuthTask))

	table2ParamTsk := asyncjob.InputParam(bCtx, job, "table2", &sql.Table2)
	table2ClientTsk, _ := asyncjob.StepAfterBoth(bCtx, job, "getTableClient2", connTsk, table2ParamTsk, sql.GetTableClient)
	query2ParamTsk := asyncjob.InputParam(bCtx, job, "query2", &sql.Query2)
	qery2ResultTsk, _ := asyncjob.StepAfterBoth(bCtx, job, "QueryTable2", table2ClientTsk, query2ParamTsk, sql.ExecuteQuery, asyncjob.WithRetry(sql.RetryPolicies["QueryTable2"]), asyncjob.ExecuteAfter(checkAuthTask))

	summaryTsk, _ := asyncjob.StepAfterBoth(bCtx, job, "summarize", qery1ResultTsk, qery2ResultTsk, sql.SummarizeQueryResult)
	asyncjob.AddStep(bCtx, job, "emailNotification", asynctask.ActionToFunc(sql.EmailNotification), asyncjob.ExecuteAfter(summaryTsk))
	return job
}

type linearRetryPolicy struct {
	sleepInterval time.Duration
	maxRetryCount int
	tried         int
}

func newLinearRetryPolicy(sleepInterval time.Duration, maxRetryCount int) asyncjob.RetryPolicy {
	return &linearRetryPolicy{
		sleepInterval: sleepInterval,
		maxRetryCount: maxRetryCount,
	}
}

func (lrp *linearRetryPolicy) SleepInterval() time.Duration {
	lrp.tried++
	return lrp.sleepInterval
}

func (lrp *linearRetryPolicy) ShouldRetry(error) bool {
	if lrp.tried < lrp.maxRetryCount {
		return true
	}
	return false
}
