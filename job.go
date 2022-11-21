package asyncjob

import (
	"context"
	"sync"

	"github.com/Azure/go-asyncjob/graph"
	"github.com/Azure/go-asynctask"
)

type JobState string

const JobStatePending JobState = "pending"
const JobStateRunning JobState = "running"
const JobStateCompleted JobState = "completed"

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

	orderedSteps := jd.stepsDag.TopologicalSort()
	for _, stepDef := range orderedSteps {
		if stepDef.GetName() == jd.Name {
			continue
		}
		ji.steps[stepDef.GetName()] = stepDef.CreateStepInstance(ctx, ji)

	}

	return ji
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

func (jd *JobDefinition[T]) Visualize() (string, error) {
	return jd.stepsDag.ToDotGraph()
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

func (ji *JobInstance[T]) Wait(ctx context.Context) error {
	var tasks []asynctask.Waitable
	for _, step := range ji.steps {
		tasks = append(tasks, step.Waitable())
	}
	return asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, tasks...)
}
