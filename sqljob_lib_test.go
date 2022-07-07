package asyncjob

import (
	"context"
	"fmt"
)

type SqlSummaryJobLib struct {
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
	return &SqlConnection{ServerName: *serverName}, nil
}

func (sql *SqlSummaryJobLib) GetTableClient(ctx context.Context, conn *SqlConnection, tableName *string) (*SqlTableClient, error) {
	fmt.Println("GetTableClient with tableName:", *tableName)
	return &SqlTableClient{ServerName: conn.ServerName, TableName: *tableName}, nil
}

func (sql *SqlSummaryJobLib) ExecuteQuery(ctx context.Context, tableClient *SqlTableClient, queryString *string) (*SqlQueryResult, error) {
	fmt.Println("ExecuteQuery:", *queryString)
	return &SqlQueryResult{Data: map[string]interface{}{"serverName": tableClient.ServerName, "tableName": tableClient.TableName, "queryName": *queryString}}, nil
}

func (sql *SqlSummaryJobLib) SummarizeQueryResult(ctx context.Context, result1 *SqlQueryResult, result2 *SqlQueryResult) (*SummarizedResult, error) {
	fmt.Println("SummarizeQueryResult")
	return &SummarizedResult{Data1: result1.Data, Data2: result2.Data}, nil
}
