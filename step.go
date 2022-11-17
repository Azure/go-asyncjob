package asyncjob

import (
	"context"

	"github.com/Azure/go-asynctask"
)

type StepState string

const StepStatePending StepState = "pending"
const StepStateRunning StepState = "running"
const StepStateFailed StepState = "failed"
const StepStateCompleted StepState = "completed"

type stepType string

const stepTypeTask stepType = "task"
const stepTypeRoot stepType = "root"
const stepTypeParam stepType = "param"

type StepMeta interface {
	GetName() string
	GetState() StepState
	DependsOn() []string
	Wait(context.Context) error
	Waitable() asynctask.Waitable
	ExecutionPolicy() *StepExecutionOptions
	ExecutionData() *StepExecutionData
	getType() stepType
}

type StepInfo[T any] struct {
	name             string
	task             *asynctask.Task[T]
	state            StepState
	executionOptions *StepExecutionOptions
	executionData    *StepExecutionData
	stepType         stepType
}

func newStepInfo[T any](stepName string, stepType stepType, optionDecorators ...ExecutionOptionPreparer) *StepInfo[T] {
	step := &StepInfo[T]{
		name:             stepName,
		state:            StepStatePending,
		executionOptions: &StepExecutionOptions{},
		executionData:    &StepExecutionData{},
		stepType:         stepType,
	}

	for _, decorator := range optionDecorators {
		step.executionOptions = decorator(step.executionOptions)
	}

	return step
}

// compiler check
var _ StepMeta = &StepInfo[string]{}

func (si *StepInfo[T]) GetName() string {
	return si.name
}

func (si *StepInfo[T]) GetState() StepState {
	return si.state
}

func (si *StepInfo[T]) DependsOn() []string {
	return si.executionOptions.DependOn
}

func (si *StepInfo[T]) Wait(ctx context.Context) error {
	return si.task.Wait(ctx)
}

func (si *StepInfo[T]) Waitable() asynctask.Waitable {
	return si.task
}

func (si *StepInfo[T]) ExecutionPolicy() *StepExecutionOptions {
	return si.executionOptions
}

func (si *StepInfo[T]) ExecutionData() *StepExecutionData {
	return si.executionData
}

func (si *StepInfo[T]) getType() stepType {
	return si.stepType
}
