package asyncjob_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-asyncjob"
	"github.com/Azure/go-asynctask"
)

const testLoggingContextKey = "test-logging"

// SqlSummaryAsyncJobDefinition is the job definition for the SqlSummaryJobLib
//   JobDefinition fit perfectly in init() function
var SqlSummaryAsyncJobDefinition *asyncjob.JobDefinitionWithResult[SqlSummaryJobLib, SummarizedResult]

func init() {
	var err error
	SqlSummaryAsyncJobDefinition, err = BuildJobWithResult(map[string]asyncjob.RetryPolicy{})
	if err != nil {
		panic(err)
	}

	SqlSummaryAsyncJobDefinition.Seal()
}

type SqlSummaryJobLib struct {
	Params *SqlSummaryJobParameters
}

func serverNameStepFunc(sql *SqlSummaryJobLib) asynctask.AsyncFunc[string] {
	return func(ctx context.Context) (*string, error) {
		return &sql.Params.ServerName, nil
	}
}

func table1NameStepFunc(sql *SqlSummaryJobLib) asynctask.AsyncFunc[string] {
	return func(ctx context.Context) (*string, error) {
		return &sql.Params.Table1, nil
	}
}

func table2NameStepFunc(sql *SqlSummaryJobLib) asynctask.AsyncFunc[string] {
	return func(ctx context.Context) (*string, error) {
		return &sql.Params.Table2, nil
	}
}

func query1ParamStepFunc(sql *SqlSummaryJobLib) asynctask.AsyncFunc[string] {
	return func(ctx context.Context) (*string, error) {
		return &sql.Params.Query1, nil
	}
}

func query2ParamStepFunc(sql *SqlSummaryJobLib) asynctask.AsyncFunc[string] {
	return func(ctx context.Context) (*string, error) {
		return &sql.Params.Query2, nil
	}
}

func connectionStepFunc(sql *SqlSummaryJobLib) asynctask.AsyncFunc[SqlConnection] {
	return func(ctx context.Context) (*SqlConnection, error) {
		return sql.GetConnection(ctx, &sql.Params.ServerName)
	}
}

func checkAuthStepFunc(sql *SqlSummaryJobLib) asynctask.AsyncFunc[interface{}] {
	return asynctask.ActionToFunc(func(ctx context.Context) error {
		return sql.CheckAuth(ctx)
	})
}

func tableClient1StepFunc(sql *SqlSummaryJobLib) asynctask.ContinueFunc[SqlConnection, SqlTableClient] {
	return func(ctx context.Context, conn *SqlConnection) (*SqlTableClient, error) {
		return sql.GetTableClient(ctx, conn, &sql.Params.Table1)
	}
}

func tableClient2StepFunc(sql *SqlSummaryJobLib) asynctask.ContinueFunc[SqlConnection, SqlTableClient] {
	return func(ctx context.Context, conn *SqlConnection) (*SqlTableClient, error) {
		return sql.GetTableClient(ctx, conn, &sql.Params.Table2)
	}
}

func queryTable1StepFunc(sql *SqlSummaryJobLib) asynctask.ContinueFunc[SqlTableClient, SqlQueryResult] {
	return func(ctx context.Context, tableClient *SqlTableClient) (*SqlQueryResult, error) {
		return sql.ExecuteQuery(ctx, tableClient, &sql.Params.Query1)
	}
}

func queryTable2StepFunc(sql *SqlSummaryJobLib) asynctask.ContinueFunc[SqlTableClient, SqlQueryResult] {
	return func(ctx context.Context, tableClient *SqlTableClient) (*SqlQueryResult, error) {
		return sql.ExecuteQuery(ctx, tableClient, &sql.Params.Query2)
	}
}

func summarizeQueryResultStepFunc(sql *SqlSummaryJobLib) asynctask.AfterBothFunc[SqlQueryResult, SqlQueryResult, SummarizedResult] {
	return func(ctx context.Context, query1Result *SqlQueryResult, query2Result *SqlQueryResult) (*SummarizedResult, error) {
		return sql.SummarizeQueryResult(ctx, query1Result, query2Result)
	}
}

func emailNotificationStepFunc(sql *SqlSummaryJobLib) asynctask.AsyncFunc[interface{}] {
	return asynctask.ActionToFunc(func(ctx context.Context) error {
		return sql.EmailNotification(ctx)
	})
}

func BuildJob(retryPolicies map[string]asyncjob.RetryPolicy) (*asyncjob.JobDefinition[SqlSummaryJobLib], error) {
	job := asyncjob.NewJobDefinition[SqlSummaryJobLib]("sqlSummaryJob")

	connTsk, err := asyncjob.AddStep(job, "GetConnection", connectionStepFunc, asyncjob.WithRetry(retryPolicies["GetConnection"]), asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step GetConnection: %w", err)
	}

	checkAuthTask, err := asyncjob.AddStep(job, "CheckAuth", checkAuthStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step CheckAuth: %w", err)
	}

	table1ClientTsk, err := asyncjob.StepAfter(job, "GetTableClient1", connTsk, tableClient1StepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step GetTableClient1: %w", err)
	}

	qery1ResultTsk, err := asyncjob.StepAfter(job, "QueryTable1", table1ClientTsk, queryTable1StepFunc, asyncjob.WithRetry(retryPolicies["QueryTable1"]), asyncjob.ExecuteAfter(checkAuthTask), asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step QueryTable1: %w", err)
	}

	table2ClientTsk, err := asyncjob.StepAfter(job, "GetTableClient2", connTsk, tableClient2StepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step GetTableClient2: %w", err)
	}

	qery2ResultTsk, err := asyncjob.StepAfter(job, "QueryTable2", table2ClientTsk, queryTable2StepFunc, asyncjob.WithRetry(retryPolicies["QueryTable2"]), asyncjob.ExecuteAfter(checkAuthTask), asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step QueryTable2: %w", err)
	}

	summaryTsk, err := asyncjob.StepAfterBoth(job, "Summarize", qery1ResultTsk, qery2ResultTsk, summarizeQueryResultStepFunc, asyncjob.WithRetry(retryPolicies["Summarize"]), asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step Summarize: %w", err)
	}

	_, err = asyncjob.AddStep(job, "EmailNotification", emailNotificationStepFunc, asyncjob.ExecuteAfter(summaryTsk), asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step EmailNotification: %w", err)
	}
	return job, nil
}

func BuildJobWithResult(retryPolicies map[string]asyncjob.RetryPolicy) (*asyncjob.JobDefinitionWithResult[SqlSummaryJobLib, SummarizedResult], error) {
	job, err := BuildJob(retryPolicies)
	if err != nil {
		return nil, err
	}

	summaryStepMeta, ok := job.GetStep("Summarize")
	if !ok {
		return nil, fmt.Errorf("step Summarize not found")
	}
	summaryStep, ok := summaryStepMeta.(*asyncjob.StepDefinition[SummarizedResult])
	if !ok {
		return nil, fmt.Errorf("step Summarize have different generic type parameter: %T", summaryStepMeta)
	}
	return asyncjob.JobWithResult(job, summaryStep)
}

type SqlSummaryJobParameters struct {
	ServerName     string
	Table1         string
	Query1         string
	Table2         string
	Query2         string
	ErrorInjection map[string]func() error
	PanicInjection map[string]bool
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
	QueryResult1 map[string]interface{}
	QueryResult2 map[string]interface{}
}

func (sql *SqlSummaryJobLib) GetConnection(ctx context.Context, serverName *string) (*SqlConnection, error) {
	sql.Logging(ctx, "GetConnection")
	if sql.Params.ErrorInjection != nil {
		if errFunc, ok := sql.Params.ErrorInjection["GetConnection"]; ok {
			return nil, errFunc()
		}
	}
	return &SqlConnection{ServerName: *serverName}, nil
}

func (sql *SqlSummaryJobLib) GetTableClient(ctx context.Context, conn *SqlConnection, tableName *string) (*SqlTableClient, error) {
	sql.Logging(ctx, fmt.Sprintf("GetTableClient with tableName: %s", *tableName))
	injectionKey := fmt.Sprintf("GetTableClient.%s.%s", conn.ServerName, *tableName)
	if sql.Params.PanicInjection != nil {
		if shouldPanic, ok := sql.Params.PanicInjection[injectionKey]; ok && shouldPanic {
			panic("as you wish")
		}
	}
	if sql.Params.ErrorInjection != nil {
		if errFunc, ok := sql.Params.ErrorInjection[injectionKey]; ok {
			return nil, errFunc()
		}
	}
	return &SqlTableClient{ServerName: conn.ServerName, TableName: *tableName}, nil
}

func (sql *SqlSummaryJobLib) CheckAuth(ctx context.Context) error {
	sql.Logging(ctx, "CheckAuth")
	injectionKey := "CheckAuth"
	if sql.Params.PanicInjection != nil {
		if shouldPanic, ok := sql.Params.PanicInjection[injectionKey]; ok && shouldPanic {
			panic("as you wish")
		}
	}
	if sql.Params.ErrorInjection != nil {
		if errFunc, ok := sql.Params.ErrorInjection[injectionKey]; ok {
			return errFunc()
		}
	}
	return nil
}

func (sql *SqlSummaryJobLib) ExecuteQuery(ctx context.Context, tableClient *SqlTableClient, queryString *string) (*SqlQueryResult, error) {
	sql.Logging(ctx, fmt.Sprintf("ExecuteQuery: %s", *queryString))
	injectionKey := fmt.Sprintf("ExecuteQuery.%s.%s.%s", tableClient.ServerName, tableClient.TableName, *queryString)
	if sql.Params.PanicInjection != nil {
		if shouldPanic, ok := sql.Params.PanicInjection[injectionKey]; ok && shouldPanic {
			panic("as you wish")
		}
	}
	if sql.Params.ErrorInjection != nil {
		if errFunc, ok := sql.Params.ErrorInjection[injectionKey]; ok {
			return nil, errFunc()
		}
	}

	return &SqlQueryResult{Data: map[string]interface{}{"serverName": tableClient.ServerName, "tableName": tableClient.TableName, "queryName": *queryString}}, nil
}

func (sql *SqlSummaryJobLib) SummarizeQueryResult(ctx context.Context, result1 *SqlQueryResult, result2 *SqlQueryResult) (*SummarizedResult, error) {
	sql.Logging(ctx, "SummarizeQueryResult")
	injectionKey := "SummarizeQueryResult"
	if sql.Params.PanicInjection != nil {
		if shouldPanic, ok := sql.Params.PanicInjection[injectionKey]; ok && shouldPanic {
			panic("as you wish")
		}
	}
	if sql.Params.ErrorInjection != nil {
		if errFunc, ok := sql.Params.ErrorInjection[injectionKey]; ok {
			return nil, errFunc()
		}
	}
	return &SummarizedResult{QueryResult1: result1.Data, QueryResult2: result2.Data}, nil
}

func (sql *SqlSummaryJobLib) EmailNotification(ctx context.Context) error {
	sql.Logging(ctx, "EmailNotification")
	return nil
}

func (sql *SqlSummaryJobLib) Logging(ctx context.Context, msg string) {
	if tI := ctx.Value(testLoggingContextKey); tI != nil {
		t := tI.(*testing.T)

		jobName := ctx.Value("asyncjob.jobName")
		jobId := ctx.Value("asyncjob.jobId")
		stepName := ctx.Value("asyncjob.stepName")

		t.Logf("[Job: %s-%s, Step: %s] %s", jobName, jobId, stepName, msg)

	} else {
		fmt.Println(msg)
	}
}

func EnrichContext(ctx context.Context, instanceMeta asyncjob.StepInstanceMeta) context.Context {
	ctx = context.WithValue(ctx, "asyncjob.jobName", instanceMeta.GetJobInstance().GetJobDefinition().GetName())
	ctx = context.WithValue(ctx, "asyncjob.jobId", instanceMeta.GetJobInstance().GetJobInstanceId())
	ctx = context.WithValue(ctx, "asyncjob.stepName", instanceMeta.GetStepDefinition().GetName())
	return ctx
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

func (lrp *linearRetryPolicy) ShouldRetry(error) (bool, time.Duration) {
	if lrp.tried < lrp.maxRetryCount {
		lrp.tried++
		return true, lrp.sleepInterval
	}

	return false, time.Duration(0)
}
