package asyncjob

import (
	"context"
	"github.com/Azure/go-asynctask"
	"sync"

	"github.com/Azure/go-asyncjob/graph"
)

type JobDefinitionMeta interface {
	GetStep(stepName string) (StepDefinitionMeta, bool) // switch bool to error
	AddStep(step StepDefinitionMeta, precedingSteps ...StepDefinitionMeta)
	RootStep() StepDefinitionMeta
}

type JobDefinition[T any] struct {
	Name     string
	steps    map[string]StepDefinitionMeta
	stepsDag *graph.Graph[StepDefinitionMeta]
	rootStep *StepDefinition[T]
}

func NewJobDefinition[T any](name string) *JobDefinition[T] {
	j := &JobDefinition[T]{
		Name:     name,
		steps:    make(map[string]StepDefinitionMeta),
		stepsDag: graph.NewGraph[StepDefinitionMeta](connectStepDefinition),
	}

	rootStep := newStepDefinition[T](name, stepTypeRoot)
	j.rootStep = rootStep

	j.steps[j.rootStep.GetName()] = j.rootStep
	j.stepsDag.AddNode(j.rootStep)

	return j
}

func (jd *JobDefinition[T]) Start(ctx context.Context, input *T) *JobInstance[T] {
	// TODO: build job instance
	ji := &JobInstance[T]{
		definition: jd,
		input:      input,
		state:      JobStateRunning,
		steps:      map[string]StepInstanceMeta{},
	}
	ji.rootStep = newStepInstance(jd.rootStep)
	ji.rootStep.task = asynctask.NewCompletedTask[T](input)
	ji.steps[ji.rootStep.GetName()] = ji.rootStep

	for stepDefName, stepDef := range jd.steps {
		if stepDefName == jd.Name {
			continue
		}
		ji.steps[stepDefName] = stepDef.CreateStepInstance(ctx, ji)
	}

	return ji
}

func (jd *JobDefinition[T]) RootStep() StepDefinitionMeta {
	return jd.rootStep
}

func (jd *JobDefinition[T]) RootStepStrongTyped() *StepDefinition[T] {
	return jd.rootStep
}

func (jd *JobDefinition[T]) GetStep(stepName string) (StepDefinitionMeta, bool) {
	stepMeta, ok := jd.steps[stepName]
	return stepMeta, ok
}

func (jd *JobDefinition[T]) AddStep(step StepDefinitionMeta, precedingSteps ...StepDefinitionMeta) {
	// TODO: check conflict
	jd.steps[step.GetName()] = step
	jd.stepsDag.AddNode(step)
	for _, precedingStep := range precedingSteps {
		jd.stepsDag.Connect(precedingStep, step)
	}
}

type JobInstanceMeta interface {
	GetStepInstance(stepName string) (StepInstanceMeta, bool)
	AddStepInstance(step StepInstanceMeta, precedingSteps ...StepInstanceMeta)
	Wait(context.Context) error
}

type JobInstance[T any] struct {
	input      *T
	definition *JobDefinition[T]
	state      JobState
	jobStart   *sync.WaitGroup
	rootStep   *StepInstance[T]
	steps      map[string]StepInstanceMeta
	// stepdag
}

func (ji *JobInstance[T]) GetStepInstance(stepName string) (StepInstanceMeta, bool) {
	stepMeta, ok := ji.steps[stepName]
	return stepMeta, ok
}

func (ji *JobInstance[T]) AddStepInstance(step StepInstanceMeta, precedingSteps ...StepInstanceMeta) {
	// TODO: check conflict
	ji.steps[step.GetName()] = step
	/*
		ji.stepsDag.AddNode(step)
		for _, precedingStep := range precedingSteps {
			jd.stepsDag.Connect(precedingStep, step)
		}
	*/
}

func (ji *JobInstance[T]) Wait(context.Context) error {
	return nil
}
