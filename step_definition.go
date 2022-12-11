package asyncjob

import (
	"context"

	"github.com/Azure/go-asyncjob/graph"
)

type stepType string

const stepTypeTask stepType = "task"
const stepTypeRoot stepType = "root"

// StepDefinitionMeta is the interface for a step definition
type StepDefinitionMeta interface {

	// GetName return name of the step
	GetName() string

	// DependsOn return the list of step names that this step depends on
	DependsOn() []string

	// ExecutionPolicy return the execution policy of the step
	ExecutionPolicy() *StepExecutionOptions

	// DotSpec used for generating graphviz graph
	DotSpec() *graph.DotNodeSpec

	// Instantiate a new step instance
	createStepInstance(context.Context, JobInstanceMeta) StepInstanceMeta
}

// StepDefinition defines a step and it's dependencies in a job definition.
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

func (sd *StepDefinition[T]) createStepInstance(ctx context.Context, jobInstance JobInstanceMeta) StepInstanceMeta {
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
