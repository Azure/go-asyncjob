package asyncjob

import (
	"context"
	"fmt"
)

type SqlSummaryJobLib struct {
}

type SqlConnection struct {
}

type SqlTableClient struct {
}

type SqlQueryResult struct {
}

type SummarizedResult struct{}

func (sql *SqlSummaryJobLib) GetConnection(ctx context.Context) (*SqlConnection, error) {
	fmt.Println("GetConnection")
	return &SqlConnection{}, nil
}

func (sql *SqlSummaryJobLib) GetTableClient(ctx context.Context, conn *SqlConnection, tableName *string) (*SqlTableClient, error) {
	fmt.Println("GetTableClient with tableName:", *tableName)
	return &SqlTableClient{}, nil
}

func (sql *SqlSummaryJobLib) ExecuteQuery(ctx context.Context, tableClient *SqlTableClient, queryName *string) (*SqlQueryResult, error) {
	fmt.Println("ExecuteQuery:", *queryName)
	return &SqlQueryResult{}, nil
}

func (sql *SqlSummaryJobLib) SummarizeQueryResult(ctx context.Context, result1 *SqlQueryResult, result2 *SqlQueryResult) (*SummarizedResult, error) {
	fmt.Println("SummarizeQueryResult")
	return &SummarizedResult{}, nil
}
