package asyncjob_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-asyncjob/v2"
	"github.com/Azure/go-asynctask"
)

const testLoggingContextKey = "test-logging"

type SqlSummaryJobLibAdvanced struct {
	SqlSummaryJobLib
	Params *SqlSummaryJobParameters
}

func serverNameStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.AsyncFunc[string] {
	return func(ctx context.Context) (*string, error) {
		return &sql.Params.ServerName, nil
	}
}

func table1NameStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.AsyncFunc[string] {
	return func(ctx context.Context) (*string, error) {
		return &sql.Params.Table1, nil
	}
}

func table2NameStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.AsyncFunc[string] {
	return func(ctx context.Context) (*string, error) {
		return &sql.Params.Table2, nil
	}
}

func query1ParamStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.AsyncFunc[string] {
	return func(ctx context.Context) (*string, error) {
		return &sql.Params.Query1, nil
	}
}

func query2ParamStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.AsyncFunc[string] {
	return func(ctx context.Context) (*string, error) {
		return &sql.Params.Query2, nil
	}
}

func connectionStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.ContinueFunc[string, SqlConnection] {
	return func(ctx context.Context, serverName *string) (*SqlConnection, error) {
		return sql.GetConnection(ctx, serverName)
	}
}

func checkAuthStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.AsyncFunc[interface{}] {
	return asynctask.ActionToFunc(func(ctx context.Context) error {
		return sql.CheckAuth(ctx)
	})
}

func tableClientStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.AfterBothFunc[SqlConnection, string, SqlTableClient] {
	return func(ctx context.Context, conn *SqlConnection, tableName *string) (*SqlTableClient, error) {
		return sql.GetTableClient(ctx, conn, tableName)
	}
}

func queryTableStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.AfterBothFunc[SqlTableClient, string, SqlQueryResult] {
	return func(ctx context.Context, tableClient *SqlTableClient, query *string) (*SqlQueryResult, error) {
		return sql.ExecuteQuery(ctx, tableClient, query)
	}
}

func summarizeQueryResultStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.AfterBothFunc[SqlQueryResult, SqlQueryResult, SummarizedResult] {
	return func(ctx context.Context, query1Result *SqlQueryResult, query2Result *SqlQueryResult) (*SummarizedResult, error) {
		return sql.SummarizeQueryResult(ctx, query1Result, query2Result)
	}
}

func emailNotificationStepFunc(sql *SqlSummaryJobLibAdvanced) asynctask.AsyncFunc[interface{}] {
	return asynctask.ActionToFunc(func(ctx context.Context) error {
		return sql.EmailNotification(ctx)
	})
}

func BuildJob(bCtx context.Context, retryPolicies map[string]asyncjob.RetryPolicy) (*asyncjob.JobDefinition[SqlSummaryJobLibAdvanced], error) {
	job := asyncjob.NewJobDefinition[SqlSummaryJobLibAdvanced]("sqlSummaryJob")
	serverNameParamTask, err := asyncjob.StepFromJobInputMethod(bCtx, job, "ServerNameParam", serverNameStepFunc)
	if err != nil {
		return nil, fmt.Errorf("error adding step ServerNameParam: %w", err)
	}

	connTsk, err := asyncjob.StepAfterFromJobInputMethod(bCtx, job, "GetConnection", serverNameParamTask, connectionStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step GetConnection: %w", err)
	}

	checkAuthTask, err := asyncjob.StepFromJobInputMethod(bCtx, job, "CheckAuth", checkAuthStepFunc)
	if err != nil {
		return nil, fmt.Errorf("error adding step CheckAuth: %w", err)
	}

	table1ParamTsk, err := asyncjob.StepFromJobInputMethod(bCtx, job, "Table1Param", table1NameStepFunc)
	if err != nil {
		return nil, fmt.Errorf("error adding step Table1Param: %w", err)
	}

	table1ClientTsk, err := asyncjob.StepAfterBothFromJobInputMethod(bCtx, job, "GetTableClient1", connTsk, table1ParamTsk, tableClientStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step GetTableClient1: %w", err)
	}

	query1ParamTsk, err := asyncjob.StepFromJobInputMethod(bCtx, job, "Query1Param", query1ParamStepFunc)
	if err != nil {
		return nil, fmt.Errorf("error adding step Query1Param: %w", err)
	}

	qery1ResultTsk, err := asyncjob.StepAfterBothFromJobInputMethod(bCtx, job, "QueryTable1", table1ClientTsk, query1ParamTsk, queryTableStepFunc, asyncjob.WithRetry(retryPolicies["QueryTable1"]), asyncjob.ExecuteAfter(checkAuthTask), asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step QueryTable1: %w", err)
	}

	table2ParamTsk, err := asyncjob.StepFromJobInputMethod(bCtx, job, "Table2NameParam", table2NameStepFunc)
	if err != nil {
		return nil, fmt.Errorf("error adding step Table2NameParam: %w", err)
	}

	table2ClientTsk, err := asyncjob.StepAfterBothFromJobInputMethod(bCtx, job, "GetTableClient2", connTsk, table2ParamTsk, tableClientStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step GetTableClient2: %w", err)
	}

	query2ParamTsk, err := asyncjob.StepFromJobInputMethod(bCtx, job, "Query2Param", query2ParamStepFunc)
	if err != nil {
		return nil, fmt.Errorf("error adding step Query2Param: %w", err)
	}

	qery2ResultTsk, err := asyncjob.StepAfterBothFromJobInputMethod(bCtx, job, "QueryTable2", table2ClientTsk, query2ParamTsk, queryTableStepFunc, asyncjob.WithRetry(retryPolicies["QueryTable2"]), asyncjob.ExecuteAfter(checkAuthTask), asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step QueryTable2: %w", err)
	}

	summaryTsk, err := asyncjob.StepAfterBothFromJobInputMethod(bCtx, job, "Summarize", qery1ResultTsk, qery2ResultTsk, summarizeQueryResultStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step Summarize: %w", err)
	}

	_, err = asyncjob.StepFromJobInputMethod(bCtx, job, "EmailNotification", emailNotificationStepFunc, asyncjob.ExecuteAfter(summaryTsk), asyncjob.WithContextEnrichment(EnrichContext))
	if err != nil {
		return nil, fmt.Errorf("error adding step EmailNotification: %w", err)
	}
	return job, nil
}

type SqlSummaryJobLib struct {
}

type SqlSummaryJobParameters struct {
	ServerName     string
	Table1         string
	Query1         string
	Table2         string
	Query2         string
	ErrorInjection map[string]func() error
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
	if v := ctx.Value(fmt.Sprintf("error-injection.%s", *serverName)); v != nil {
		if err, ok := v.(error); ok {
			return nil, err
		}
	}
	return &SqlConnection{ServerName: *serverName}, nil
}

func (sql *SqlSummaryJobLib) GetTableClient(ctx context.Context, conn *SqlConnection, tableName *string) (*SqlTableClient, error) {
	sql.Logging(ctx, fmt.Sprintf("GetTableClient with tableName: %s", *tableName))
	if v := ctx.Value(fmt.Sprintf("error-injection.%s.%s", conn.ServerName, *tableName)); v != nil {
		if err, ok := v.(error); ok {
			return nil, err
		}
	}

	if v := ctx.Value(fmt.Sprintf("panic-injection.%s.%s", conn.ServerName, *tableName)); v != nil {
		if shouldPanic := v.(bool); shouldPanic {
			panic("as you wish")
		}
	}
	return &SqlTableClient{ServerName: conn.ServerName, TableName: *tableName}, nil
}

func (sql *SqlSummaryJobLib) CheckAuth(ctx context.Context) error {
	if v := ctx.Value("error-injection.checkAuth"); v != nil {
		if err, ok := v.(error); ok {
			return err
		}
	}

	return nil
}

func (sql *SqlSummaryJobLib) ExecuteQuery(ctx context.Context, tableClient *SqlTableClient, queryString *string) (*SqlQueryResult, error) {
	sql.Logging(ctx, fmt.Sprintf("ExecuteQuery: %s", *queryString))
	if v := ctx.Value(fmt.Sprintf("error-injection.%s.%s.%s", tableClient.ServerName, tableClient.TableName, *queryString)); v != nil {
		if err, ok := v.(error); ok {
			return nil, err
		}
	}

	return &SqlQueryResult{Data: map[string]interface{}{"serverName": tableClient.ServerName, "tableName": tableClient.TableName, "queryName": *queryString}}, nil
}

func (sql *SqlSummaryJobLib) SummarizeQueryResult(ctx context.Context, result1 *SqlQueryResult, result2 *SqlQueryResult) (*SummarizedResult, error) {
	sql.Logging(ctx, "SummarizeQueryResult")
	if v := ctx.Value("error-injection.summarize"); v != nil {
		if err, ok := v.(error); ok {
			return nil, err
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
		stepName := ctx.Value("asyncjob.stepName")

		t.Logf("[Job: %s, Step: %s] %s", jobName, stepName, msg)

	} else {
		fmt.Println(msg)
	}
}

func (sql *SqlSummaryJobLib) BuildJob(bCtx context.Context, retryPolicies map[string]asyncjob.RetryPolicy) *asyncjob.JobDefinition[SqlSummaryJobParameters] {
	job := asyncjob.NewJobDefinition[SqlSummaryJobParameters]("sqlSummaryJob")
	serverNameParamTask, _ := asyncjob.StepFromJobInput(bCtx, job, "serverName", func(_ context.Context, input *SqlSummaryJobParameters) (*string, error) {
		return &input.ServerName, nil
	})
	connTsk, _ := asyncjob.StepAfter(bCtx, job, "GetConnection", serverNameParamTask, sql.GetConnection, asyncjob.WithContextEnrichment(EnrichContext))
	checkAuthTask, _ := asyncjob.AddStep(bCtx, job, "CheckAuth", asynctask.ActionToFunc(sql.CheckAuth))

	table1ParamTsk, _ := asyncjob.StepFromJobInput(bCtx, job, "table1", func(_ context.Context, input *SqlSummaryJobParameters) (*string, error) {
		return &input.Table1, nil
	})
	table1ClientTsk, _ := asyncjob.StepAfterBoth(bCtx, job, "getTableClient1", connTsk, table1ParamTsk, sql.GetTableClient, asyncjob.WithContextEnrichment(EnrichContext))
	query1ParamTsk, _ := asyncjob.StepFromJobInput(bCtx, job, "query1", func(_ context.Context, input *SqlSummaryJobParameters) (*string, error) {
		return &input.Query1, nil
	})
	qery1ResultTsk, _ := asyncjob.StepAfterBoth(bCtx, job, "QueryTable1", table1ClientTsk, query1ParamTsk, sql.ExecuteQuery, asyncjob.WithRetry(retryPolicies["QueryTable1"]), asyncjob.ExecuteAfter(checkAuthTask), asyncjob.WithContextEnrichment(EnrichContext))

	table2ParamTsk, _ := asyncjob.StepFromJobInput(bCtx, job, "table2", func(_ context.Context, input *SqlSummaryJobParameters) (*string, error) {
		return &input.Table2, nil
	})
	table2ClientTsk, _ := asyncjob.StepAfterBoth(bCtx, job, "getTableClient2", connTsk, table2ParamTsk, sql.GetTableClient, asyncjob.WithContextEnrichment(EnrichContext))
	query2ParamTsk, _ := asyncjob.StepFromJobInput(bCtx, job, "query2", func(_ context.Context, input *SqlSummaryJobParameters) (*string, error) {
		return &input.Query2, nil
	})
	qery2ResultTsk, _ := asyncjob.StepAfterBoth(bCtx, job, "QueryTable2", table2ClientTsk, query2ParamTsk, sql.ExecuteQuery, asyncjob.WithRetry(retryPolicies["QueryTable2"]), asyncjob.ExecuteAfter(checkAuthTask), asyncjob.WithContextEnrichment(EnrichContext))

	summaryTsk, _ := asyncjob.StepAfterBoth(bCtx, job, "summarize", qery1ResultTsk, qery2ResultTsk, sql.SummarizeQueryResult, asyncjob.WithContextEnrichment(EnrichContext))
	asyncjob.AddStep(bCtx, job, "emailNotification", asynctask.ActionToFunc(sql.EmailNotification), asyncjob.ExecuteAfter(summaryTsk), asyncjob.WithContextEnrichment(EnrichContext))
	return job
}

func (sql *SqlSummaryJobLib) BuildJobWithResult(bCtx context.Context, retryPolicies map[string]asyncjob.RetryPolicy) (*asyncjob.JobDefinitionWithResult[SqlSummaryJobParameters, SummarizedResult], error) {
	job := sql.BuildJob(bCtx, retryPolicies)

	summaryStepMeta, _ := job.GetStep("summarize")
	summaryStep, _ := summaryStepMeta.(*asyncjob.StepDefinition[SummarizedResult])
	return asyncjob.JobWithResult(job, summaryStep)
}

func EnrichContext(ctx context.Context, instanceMeta asyncjob.StepInstanceMeta) context.Context {
	ctx = context.WithValue(ctx, "asyncjob.jobName", instanceMeta.GetJobInstance().GetJobDefinition().GetName())
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
