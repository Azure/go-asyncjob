package asyncjob

import (
	"context"

	"github.com/Azure/go-asyncjob/graph"
	"github.com/Azure/go-asynctask"
)

type StepDefinitionMeta interface {
	GetName() string
	DependsOn() []string
	ExecutionPolicy() *StepExecutionOptions
	DotSpec() *graph.DotNodeSpec
}

type StepDefinition[T any] struct {
	name             string
	stepType         stepType
	executionOptions *StepExecutionOptions
	taskCreator      func(context.Context, JobInstanceMeta, *StepInstance[T]) *asynctask.Task[T]
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

func (sd *StepDefinition[T]) DotSpec() *graph.DotNodeSpec {
	return &graph.DotNodeSpec{
		ID:        sd.GetName(),
		Name:      sd.GetName(),
		Shape:     "box",
		Style:     "filled",
		FillColor: "gray",
		Tooltip:   "",
	}
}

func (sd *StepDefinition[T]) Instantiate(ji JobInstanceMeta) *StepInstance[T] {
	// TODO : create asynctask
	si := &StepInstance[T]{
		Definition:    sd,
		executionData: &StepExecutionData{},
		state:         StepStatePending,
	}
	si.task = sd.taskCreator(context.Background(), ji, si)
	return si
}

func connectStepDefinition(stepFrom, stepTo StepDefinitionMeta) *graph.DotEdgeSpec {
	edgeSpec := &graph.DotEdgeSpec{
		FromNodeID: stepFrom.GetName(),
		ToNodeID:   stepTo.GetName(),
		Color:      "black",
		Style:      "bold",
	}

	return edgeSpec
}

type StepInstanceMeta interface {
	Waitable() asynctask.Waitable
}

type StepInstance[T any] struct {
	Definition    *StepDefinition[T]
	task          *asynctask.Task[T]
	state         StepState
	executionData *StepExecutionData
}

func (si *StepInstance[T]) Waitable() asynctask.Waitable {
	return si.task
}
