package asyncjob

import (
	"context"
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

	rootStep := newStepDefinition[T](rootStepName, stepTypeRoot)
	j.rootStep = rootStep

	j.steps[j.rootStep.GetName()] = j.rootStep
	j.stepsDag.AddNode(j.rootStep)

	return j
}

func (jd *JobDefinition[T]) Start(ctx context.Context, params T) *JobInstance[T] {
	// TODO: build job instance
	return &JobInstance[T]{
		definition: jd,
		input:      params,
		state:      JobStateRunning,
	}
}

func (jd *JobDefinition[T]) RootStep() StepDefinitionMeta {
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
	Wait(context.Context) error
}

type JobInstance[T any] struct {
	input      T
	definition *JobDefinition[T]
	state      JobState
	jobStart   *sync.WaitGroup
	rootStep   StepInstanceMeta
	Steps      map[string]StepInstanceMeta
}

func (ji *JobInstance[T]) GetStepInstance(stepName string) (StepInstanceMeta, bool) {
	stepMeta, ok := ji.Steps[stepName]
	return stepMeta, ok
}
