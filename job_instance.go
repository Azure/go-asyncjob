package asyncjob

import (
	"context"
	"errors"

	"github.com/Azure/go-asyncjob/graph"
	"github.com/Azure/go-asynctask"
	"github.com/google/uuid"
)

type JobInstanceMeta interface {
	GetJobInstanceId() string
	GetJobDefinition() JobDefinitionMeta
	GetStepInstance(stepName string) (StepInstanceMeta, bool)
	Wait(context.Context) error
	Visualize() (string, error)

	// not exposing for now
	addStepInstance(step StepInstanceMeta, precedingSteps ...StepInstanceMeta)
}

type JobExecutionOptions struct {
	Id              string
	RunSequentially bool
}

type JobOptionPreparer func(*JobExecutionOptions) *JobExecutionOptions

func WithJobId(jobId string) JobOptionPreparer {
	return func(options *JobExecutionOptions) *JobExecutionOptions {
		options.Id = jobId
		return options
	}
}

func WithSequentialExecution() JobOptionPreparer {
	return func(options *JobExecutionOptions) *JobExecutionOptions {
		options.RunSequentially = true
		return options
	}
}

// JobInstance is the instance of a jobDefinition
type JobInstance[T any] struct {
	jobOptions *JobExecutionOptions
	input      T
	Definition *JobDefinition[T]
	rootStep   *StepInstance[T]
	steps      map[string]StepInstanceMeta
	stepsDag   *graph.Graph[StepInstanceMeta]
}

func newJobInstance[T any](jd *JobDefinition[T], input T, jobInstanceOptions ...JobOptionPreparer) *JobInstance[T] {
	ji := &JobInstance[T]{
		Definition: jd,
		input:      input,
		steps:      map[string]StepInstanceMeta{},
		stepsDag:   graph.NewGraph(connectStepInstance),
		jobOptions: &JobExecutionOptions{},
	}

	for _, decorator := range jobInstanceOptions {
		ji.jobOptions = decorator(ji.jobOptions)
	}

	if ji.jobOptions.Id == "" {
		ji.jobOptions.Id = uuid.New().String()
	}

	return ji
}

func (ji *JobInstance[T]) start(ctx context.Context) {
	// create root step instance
	ji.rootStep = newStepInstance(ji.Definition.rootStep, ji)
	ji.rootStep.task = asynctask.NewCompletedTask(ji.input)
	ji.rootStep.state = StepStateCompleted
	ji.steps[ji.rootStep.GetName()] = ji.rootStep
	ji.stepsDag.AddNode(ji.rootStep)

	// construct job instance graph, with TopologySort ordering
	orderedSteps := ji.Definition.stepsDag.TopologicalSort()
	for _, stepDef := range orderedSteps {
		if stepDef.GetName() == ji.Definition.GetName() {
			continue
		}
		ji.steps[stepDef.GetName()] = stepDef.createStepInstance(ctx, ji)

		if ji.jobOptions.RunSequentially {
			ji.steps[stepDef.GetName()].Waitable().Wait(ctx)
		}
	}
}

func (ji *JobInstance[T]) GetJobInstanceId() string {
	return ji.jobOptions.Id
}

func (ji *JobInstance[T]) GetJobDefinition() JobDefinitionMeta {
	return ji.Definition
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
