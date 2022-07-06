package asyncjob

import (
	"context"
	"time"

	"github.com/Azure/go-asynctask"
)

type StepState string

const StepStatePending StepState = "pending"
const StepStateRunning StepState = "running"
const StepStateFailed StepState = "failed"
const StepStateCompleted StepState = "completed"

type StepExecutionOptions struct {
	Timeout     time.Duration
	ErrorPolicy StepErrorPolicy
	RetryPolicy StepRetryPolicy
}

type StepErrorPolicy struct{}

type StepRetryPolicy struct{}

type ExecutionOptionPreparer func(*StepExecutionOptions) *StepExecutionOptions

type StepMeta interface {
	Name() string
	GetState() StepState
	DependsOn() []string
	Wait(context.Context) error
	Waitable() asynctask.Waitable
	ExecutionPolicy() *StepExecutionOptions
}

type StepInfo[T any] struct {
	name             string
	task             *asynctask.Task[T]
	state            StepState
	dependOn         []string
	executionOptions *StepExecutionOptions
}

func newStepInfo[T any](stepName string, dependOn []string, optionDecorators ...ExecutionOptionPreparer) *StepInfo[T] {
	step := &StepInfo[T]{
		name:             stepName,
		dependOn:         dependOn,
		state:            StepStatePending,
		executionOptions: &StepExecutionOptions{},
	}

	for _, decorator := range optionDecorators {
		step.executionOptions = decorator(step.executionOptions)
	}

	return step
}

// compiler check
var _ StepMeta = &StepInfo[string]{}

func (si *StepInfo[T]) Name() string {
	return si.name
}

func (si *StepInfo[T]) GetState() StepState {
	return si.state
}

func (si *StepInfo[T]) DependsOn() []string {
	return si.dependOn
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
