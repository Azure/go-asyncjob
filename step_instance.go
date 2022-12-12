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

// StepInstanceMeta is the interface for a step instance
type StepInstanceMeta interface {
	GetName() string
	ExecutionData() *StepExecutionData
	GetState() StepState
	GetJobInstance() JobInstanceMeta
	GetStepDefinition() StepDefinitionMeta
	Waitable() asynctask.Waitable

	DotSpec() *graph.DotNodeSpec
}

// StepInstance is the instance of a step, within a job instance.
type StepInstance[T any] struct {
	Definition  *StepDefinition[T]
	JobInstance JobInstanceMeta

	task          *asynctask.Task[T]
	state         StepState
	executionData *StepExecutionData
}

func newStepInstance[T any](stepDefinition *StepDefinition[T], jobInstance JobInstanceMeta) *StepInstance[T] {
	return &StepInstance[T]{
		Definition:    stepDefinition,
		JobInstance:   jobInstance,
		executionData: &StepExecutionData{},
		state:         StepStatePending,
	}
}

func (si *StepInstance[T]) GetJobInstance() JobInstanceMeta {
	return si.JobInstance
}

func (si *StepInstance[T]) GetStepDefinition() StepDefinitionMeta {
	return si.Definition
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

func (si *StepInstance[T]) EnrichContext(ctx context.Context) (result context.Context) {
	result = ctx
	if si.Definition.executionOptions.ContextPolicy != nil {
		// TODO: bubble up the error somehow
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in EnrichContext", r)
			}
		}()
		result = si.Definition.executionOptions.ContextPolicy(ctx, si)
	}

	return result
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
