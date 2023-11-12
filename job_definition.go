package asyncjob

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/go-asyncjob/graph"
)

// Interface for a job definition
type JobDefinitionMeta interface {
	GetName() string
	GetStep(stepName string) (StepDefinitionMeta, bool) // TODO: switch bool to error
	Seal()
	Sealed() bool
	Visualize() (string, error)

	// not exposing for now.
	addStep(step StepDefinitionMeta, precedingSteps ...StepDefinitionMeta) error
	getRootStep() StepDefinitionMeta
}

// JobDefinition defines a job with child steps, and step is organized in a Directed Acyclic Graph (DAG).
type JobDefinition[T any] struct {
	name string

	sealed   bool
	steps    map[string]StepDefinitionMeta
	stepsDag *graph.Graph[StepDefinitionMeta]
	rootStep *StepDefinition[T]
}

// Create new JobDefinition
//
//	it is suggest to build jobDefinition statically on process start, and reuse it for each job instance.
func NewJobDefinition[T any](name string) *JobDefinition[T] {
	j := &JobDefinition[T]{
		name:     name,
		steps:    make(map[string]StepDefinitionMeta),
		stepsDag: graph.NewGraph(connectStepDefinition),
	}

	rootStep := newStepDefinition[T](name, stepTypeRoot)
	j.rootStep = rootStep

	j.steps[j.rootStep.GetName()] = j.rootStep
	j.stepsDag.AddNode(j.rootStep)

	return j
}

// Start execution of the job definition.
//
//	this will create and return new instance of the job
//	caller will then be able to wait for the job instance
func (jd *JobDefinition[T]) Start(ctx context.Context, input T, jobOptions ...JobOptionPreparer) *JobInstance[T] {
	if !jd.Sealed() {
		jd.Seal()
	}

	ji := newJobInstance(jd, input, jobOptions...)
	ji.start(ctx)

	return ji
}

func (jd *JobDefinition[T]) getRootStep() StepDefinitionMeta {
	return jd.rootStep
}

func (jd *JobDefinition[T]) GetName() string {
	return jd.name
}

func (jd *JobDefinition[T]) Seal() {
	if jd.sealed {
		return
	}
	jd.sealed = true
}

func (jd *JobDefinition[T]) Sealed() bool {
	return jd.sealed
}

// GetStep returns the stepDefinition by name
func (jd *JobDefinition[T]) GetStep(stepName string) (StepDefinitionMeta, bool) {
	stepMeta, ok := jd.steps[stepName]
	return stepMeta, ok
}

// AddStep adds a step to the job definition, with optional preceding steps
func (jd *JobDefinition[T]) addStep(step StepDefinitionMeta, precedingSteps ...StepDefinitionMeta) error {
	jd.steps[step.GetName()] = step
	jd.stepsDag.AddNode(step)
	for _, precedingStep := range precedingSteps {
		if err := jd.stepsDag.Connect(precedingStep, step); err != nil {
			if errors.Is(err, graph.ErrConnectNotExistingNode) {
				return ErrRefStepNotInJob.WithMessage(fmt.Sprintf("referenced step %s not found", precedingStep.GetName()))
			}

			return err
		}
	}

	return nil
}

// Visualize the job definition in graphviz dot format
func (jd *JobDefinition[T]) Visualize() (string, error) {
	return jd.stepsDag.ToDotGraph()
}
