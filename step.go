package asyncjob

import (
	"context"

	"github.com/Azure/go-asyncjob/graph"
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

type StepDefinitionMeta interface {
	GetName() string
	DependsOn() []string
	ExecutionPolicy() *StepExecutionOptions
	DotSpec() *graph.DotNodeSpec
	CreateStepInstance(context.Context, JobInstanceMeta) StepInstanceMeta
}

type StepDefinition[T any] struct {
	name             string
	stepType         stepType
	executionOptions *StepExecutionOptions
	instanceCreator  func(context.Context, JobInstanceMeta) StepInstanceMeta
}

func newStepDefinition[T any](stepName string, stepType stepType, optionDecorators ...ExecutionOptionPreparer) *StepDefinition[T] {
	step := &StepDefinition[T]{
		name:             stepName,
		executionOptions: &StepExecutionOptions{},
		stepType:         stepType,
	}

	for _, decorator := range optionDecorators {
		step.executionOptions = decorator(step.executionOptions)
	}

	return step
}

func (sd *StepDefinition[T]) GetName() string {
	return sd.name
}

func (sd *StepDefinition[T]) DependsOn() []string {
	return sd.executionOptions.DependOn
}

func (sd *StepDefinition[T]) ExecutionPolicy() *StepExecutionOptions {
	return sd.executionOptions
}

func (sd *StepDefinition[T]) CreateStepInstance(ctx context.Context, jobInstance JobInstanceMeta) StepInstanceMeta {
	return sd.instanceCreator(ctx, jobInstance)
}

func (sd *StepDefinition[T]) DotSpec() *graph.DotNodeSpec {
	return &graph.DotNodeSpec{
		Name:        sd.GetName(),
		DisplayName: sd.GetName(),
		Shape:       "box",
		Style:       "filled",
		FillColor:   "gray",
		Tooltip:     "",
	}
}

func connectStepDefinition(stepFrom, stepTo StepDefinitionMeta) *graph.DotEdgeSpec {
	edgeSpec := &graph.DotEdgeSpec{
		FromNodeName: stepFrom.GetName(),
		ToNodeName:   stepTo.GetName(),
		Color:        "black",
		Style:        "bold",
	}

	return edgeSpec
}

type StepInstanceMeta interface {
	GetName() string
	Waitable() asynctask.Waitable
}

type StepInstance[T any] struct {
	Definition    *StepDefinition[T]
	task          *asynctask.Task[T]
	state         StepState
	executionData *StepExecutionData
}

func newStepInstance[T any](stepDefinition *StepDefinition[T]) *StepInstance[T] {
	return &StepInstance[T]{
		Definition:    stepDefinition,
		executionData: &StepExecutionData{},
		state:         StepStatePending,
	}
}

func (si *StepInstance[T]) Waitable() asynctask.Waitable {
	return si.task
}

func (si *StepInstance[T]) GetName() string {
	return si.Definition.GetName()
}
