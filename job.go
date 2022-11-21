package asyncjob

import (
	"context"
	"errors"
	"sync"

	"github.com/Azure/go-asyncjob/graph"
	"github.com/Azure/go-asynctask"
)

// Interface for a job definition
type JobDefinitionMeta interface {
	GetStep(stepName string) (StepDefinitionMeta, bool) // TODO: switch bool to error

	// not exposing for now.
	addStep(step StepDefinitionMeta, precedingSteps ...StepDefinitionMeta)
	getRootStep() StepDefinitionMeta
}

// JobDefinition defines a job with child steps, and step is organized in a Directed Acyclic Graph (DAG).
type JobDefinition[T any] struct {
	Name     string
	steps    map[string]StepDefinitionMeta
	stepsDag *graph.Graph[StepDefinitionMeta]
	rootStep *StepDefinition[T]
}

// Create new JobDefinition
//   it is suggest to build jobDefinition statically on process start, and reuse it for each job instance.
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

// Start execution of the job definition.
//   this will create and return new instance of the job
//   caller will then be able to wait for the job instance
func (jd *JobDefinition[T]) Start(ctx context.Context, input *T) *JobInstance[T] {
	ji := &JobInstance[T]{
		Definition: jd,
		input:      input,
		steps:      map[string]StepInstanceMeta{},
		stepsDag:   graph.NewGraph[StepInstanceMeta](connectStepInstance),
	}

	// create root step instance
	ji.rootStep = newStepInstance(jd.rootStep)
	ji.rootStep.task = asynctask.NewCompletedTask[T](input)
	ji.rootStep.state = StepStateCompleted
	ji.steps[ji.rootStep.GetName()] = ji.rootStep
	ji.stepsDag.AddNode(ji.rootStep)

	// construct job instance graph, with TopologySort ordering
	orderedSteps := jd.stepsDag.TopologicalSort()
	for _, stepDef := range orderedSteps {
		if stepDef.GetName() == jd.Name {
			continue
		}
		ji.steps[stepDef.GetName()] = stepDef.createStepInstance(ctx, ji)

	}

	return ji
}

func (jd *JobDefinition[T]) getRootStep() StepDefinitionMeta {
	return jd.rootStep
}

// GetStep returns the stepDefinition by name
func (jd *JobDefinition[T]) GetStep(stepName string) (StepDefinitionMeta, bool) {
	stepMeta, ok := jd.steps[stepName]
	return stepMeta, ok
}

// AddStep adds a step to the job definition, with optional preceding steps
func (jd *JobDefinition[T]) addStep(step StepDefinitionMeta, precedingSteps ...StepDefinitionMeta) {
	jd.steps[step.GetName()] = step
	jd.stepsDag.AddNode(step)
	for _, precedingStep := range precedingSteps {
		jd.stepsDag.Connect(precedingStep, step)
	}
}

// Visualize the job definition in graphviz dot format
func (jd *JobDefinition[T]) Visualize() (string, error) {
	return jd.stepsDag.ToDotGraph()
}

type JobInstanceMeta interface {
	GetStepInstance(stepName string) (StepInstanceMeta, bool)
	Wait(context.Context) error

	// not exposing for now
	addStepInstance(step StepInstanceMeta, precedingSteps ...StepInstanceMeta)

	// future considering:
	//  - return result of given step
}

// JobInstance is the instance of a jobDefinition
type JobInstance[T any] struct {
	input      *T
	Definition *JobDefinition[T]
	jobStart   *sync.WaitGroup
	rootStep   *StepInstance[T]
	steps      map[string]StepInstanceMeta
	stepsDag   *graph.Graph[StepInstanceMeta]
}

// GetStepInstance returns the stepInstance by name
func (ji *JobInstance[T]) GetStepInstance(stepName string) (StepInstanceMeta, bool) {
	stepMeta, ok := ji.steps[stepName]
	return stepMeta, ok
}

func (ji *JobInstance[T]) addStepInstance(step StepInstanceMeta, precedingSteps ...StepInstanceMeta) {
	ji.steps[step.GetName()] = step

	ji.stepsDag.AddNode(step)
	for _, precedingStep := range precedingSteps {
		ji.stepsDag.Connect(precedingStep, step)
	}
}

// Wait for all steps in the job to finish.
func (ji *JobInstance[T]) Wait(ctx context.Context) error {
	var tasks []asynctask.Waitable
	for _, step := range ji.steps {
		tasks = append(tasks, step.Waitable())
	}

	err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, tasks...)

	// return rootCaused error if possible
	if err != nil {
		jobErr := &JobError{}
		if errors.As(err, &jobErr) {
			return jobErr.RootCause()
		}

		return err
	}

	return nil
}

// Visualize the job instance in graphviz dot format
func (jd *JobInstance[T]) Visualize() (string, error) {
	return jd.stepsDag.ToDotGraph()
}
