package asyncjob

type JobDefinition struct {
}

type JobInstance struct {
}

type StepDefinition[T any] struct {
	name            string
	stepType        stepType
	executionOption *StepExecutionOptions
	stepFunc
}

type StepInstance[T any] struct {
	Definition    StepDefinition[T]
	task          *asynctask.Task[T]
	state         StepState
	executionData *StepExecutionData
}
