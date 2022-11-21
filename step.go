package asyncjob

import (
	"context"
	"fmt"
	"time"

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

// StepInstanceMeta is the interface for a step instance
type StepInstanceMeta interface {
	GetName() string
	Waitable() asynctask.Waitable
	DotSpec() *graph.DotNodeSpec
	ExecutionData() *StepExecutionData
	GetState() StepState
}

// StepInstance is the instance of a step, within a job instance.
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

func (si *StepInstance[T]) GetState() StepState {
	return si.state
}

func (si *StepInstance[T]) ExecutionData() *StepExecutionData {
	return si.executionData
}

func (si *StepInstance[T]) DotSpec() *graph.DotNodeSpec {
	shape := "hexagon"
	if si.Definition.stepType == stepTypeRoot {
		shape = "triangle"
	}

	color := "gray"
	switch si.state {
	case StepStatePending:
		color = "gray"
	case StepStateRunning:
		color = "yellow"
	case StepStateCompleted:
		color = "green"
	case StepStateFailed:
		color = "red"
	}

	tooltip := ""
	if si.state != StepStatePending && si.executionData != nil {
		tooltip = fmt.Sprintf("State: %s\\nStartAt: %s\\nDuration: %s", si.state, si.executionData.StartTime.Format(time.RFC3339Nano), si.executionData.Duration)
	}

	return &graph.DotNodeSpec{
		Name:        si.GetName(),
		DisplayName: si.GetName(),
		Shape:       shape,
		Style:       "filled",
		FillColor:   color,
		Tooltip:     tooltip,
	}
}

func connectStepInstance(stepFrom, stepTo StepInstanceMeta) *graph.DotEdgeSpec {
	edgeSpec := &graph.DotEdgeSpec{
		FromNodeName: stepFrom.GetName(),
		ToNodeName:   stepTo.GetName(),
		Color:        "black",
		Style:        "bold",
	}

	// update edge color, tooltip if NodeTo is started already.
	if stepTo.GetState() != StepStatePending {
		executionData := stepTo.ExecutionData()
		edgeSpec.Tooltip = fmt.Sprintf("Time: %s", executionData.StartTime.Format(time.RFC3339Nano))
	}

	fromNodeState := stepFrom.GetState()
	if fromNodeState == StepStateCompleted {
		edgeSpec.Color = "green"
	} else if fromNodeState == StepStateFailed {
		edgeSpec.Color = "red"
	}

	return edgeSpec
}
