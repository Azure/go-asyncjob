package asyncjob_test

import (
	"testing"

	"github.com/Azure/go-asyncjob"
	"github.com/stretchr/testify/assert"
)

func TestDefinitionRendering(t *testing.T) {
	t.Parallel()

	renderGraph(t, SqlSummaryAsyncJobDefinition)
}

func TestDefinitionBuilder(t *testing.T) {
	t.Parallel()

	job := asyncjob.NewJobDefinition[SqlSummaryJobLib]("sqlSummaryJob")
	notExistingTask := &asyncjob.StepDefinition[any]{}

	_, err := asyncjob.AddStep(job, "GetConnection", connectionStepFunc, asyncjob.ExecuteAfter(notExistingTask), asyncjob.WithContextEnrichment(EnrichContext))
	assert.EqualError(t, err, "RefStepNotInJob: trying to reference to step \"\", but it is not registered in job")

	connTsk, err := asyncjob.AddStep(job, "GetConnection", connectionStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.NoError(t, err)

	_, err = asyncjob.AddStep(job, "GetConnection", connectionStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.EqualError(t, err, "AddExistingStep: trying to add step \"GetConnection\" to job definition, but it already exists")

	table1ClientTsk, err := asyncjob.StepAfter(job, "GetTableClient1", connTsk, tableClient1StepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.NoError(t, err)

	_, err = asyncjob.StepAfter(job, "GetTableClient1", connTsk, tableClient1StepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.EqualError(t, err, "AddExistingStep: trying to add step \"GetTableClient1\" to job definition, but it already exists")

	table2ClientTsk, err := asyncjob.StepAfter(job, "GetTableClient2", connTsk, tableClient2StepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.NoError(t, err)

	_, err = asyncjob.StepAfter(job, "QueryTable1", table1ClientTsk, queryTable1StepFunc, asyncjob.ExecuteAfter(notExistingTask), asyncjob.WithContextEnrichment(EnrichContext))
	assert.EqualError(t, err, "RefStepNotInJob: trying to reference to step \"\", but it is not registered in job")

	query1Task, err := asyncjob.StepAfter(job, "QueryTable1", table1ClientTsk, queryTable1StepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.NoError(t, err)
	query2Task, err := asyncjob.StepAfter(job, "QueryTable2", table2ClientTsk, queryTable2StepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.NoError(t, err)

	_, err = asyncjob.StepAfterBoth(job, "Summarize", query1Task, query2Task, summarizeQueryResultStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.NoError(t, err)

	_, err = asyncjob.StepAfterBoth(job, "Summarize", query1Task, query2Task, summarizeQueryResultStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.EqualError(t, err, "AddExistingStep: trying to add step \"Summarize\" to job definition, but it already exists")

	_, err = asyncjob.StepAfterBoth(job, "Summarize1", query1Task, query1Task, summarizeQueryResultStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.EqualError(t, err, "DuplicateInputParentStep: at least 2 input parentSteps are same")

	query3Task := &asyncjob.StepDefinition[SqlQueryResult]{}
	_, err = asyncjob.StepAfterBoth(job, "Summarize2", query1Task, query3Task, summarizeQueryResultStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.EqualError(t, err, "RefStepNotInJob: trying to reference to step \"\", but it is not registered in job")

	assert.False(t, job.Sealed())
	job.Seal()
	assert.True(t, job.Sealed())
	job.Seal()
	assert.True(t, job.Sealed())

	_, err = asyncjob.AddStep(job, "GetConnectionAgain", connectionStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.EqualError(t, err, "AddStepInSealedJob: trying to add step \"GetConnectionAgain\" to a sealed job definition")

	_, err = asyncjob.StepAfter(job, "QueryTable1Again", table1ClientTsk, queryTable1StepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.EqualError(t, err, "AddStepInSealedJob: trying to add step \"QueryTable1Again\" to a sealed job definition")

	_, err = asyncjob.StepAfterBoth(job, "SummarizeAgain", query1Task, query2Task, summarizeQueryResultStepFunc, asyncjob.WithContextEnrichment(EnrichContext))
	assert.EqualError(t, err, "AddStepInSealedJob: trying to add step \"SummarizeAgain\" to a sealed job definition")
}
