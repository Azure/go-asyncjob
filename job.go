package asyncjob

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/go-asyncjob/graph"
	"github.com/Azure/go-asynctask"
)

type JobState string

const JobStatePending JobState = "pending"
const JobStateRunning JobState = "running"
const JobStateCompleted JobState = "completed"

const rootStepName = "$job"

type JobInterface interface {
	GetStep(stepName string) (StepMeta, bool) // switch bool to error
	AddStep(step StepMeta, precedingSteps ...StepMeta)
	RootStep() *StepInfo[interface{}]

	Start(ctx context.Context) error

	RuntimeContext() context.Context
}

type Job struct {
	Name  string
	Steps map[string]StepMeta

	state    JobState
	rootStep *StepInfo[interface{}]
	jobStart *sync.WaitGroup
	stepsDag *graph.Graph[*stepNode]

	// runtimeCtx is captured to separate build context and runtime context.
	// golang not recommending to store context in the struct. I don't have better idea.
	runtimeCtx context.Context
}

var _ JobInterface = &Job{}

func NewJob(name string) *Job {
	jobStart := sync.WaitGroup{}
	jobStart.Add(1)
	j := &Job{
		Name:  name,
		Steps: make(map[string]StepMeta),

		jobStart: &jobStart,
		state:    JobStatePending,
		stepsDag: graph.NewGraph[*stepNode](stepConn),
	}

	rootStep := newStepInfo[interface{}](rootStepName, stepTypeRoot)
	rootJobFunc := func(ctx context.Context) (*interface{}, error) {
		// this will pause all steps from starting, until Start() method is called.
		jobStart.Wait()
		j.rootStep.executionData.StartTime = time.Now()
		j.state = JobStateRunning
		j.rootStep.state = StepStateCompleted
		j.rootStep.executionData.Duration = time.Since(j.rootStep.executionData.StartTime)
		return nil, nil
	}

	rootStep.task = asynctask.Start(context.Background(), rootJobFunc)
	j.rootStep = rootStep

	j.Steps[j.rootStep.GetName()] = j.rootStep
	j.stepsDag.AddNode(newStepNode(j.rootStep))

	return j
}

func (j *Job) RootStep() *StepInfo[interface{}] {
	return j.rootStep
}

func (j *Job) GetStep(stepName string) (StepMeta, bool) {
	stepMeta, ok := j.Steps[stepName]
	return stepMeta, ok
}

func (j *Job) AddStep(step StepMeta, precedingSteps ...StepMeta) {
	// TODO: check conflict
	j.Steps[step.GetName()] = step
	stepNode := newStepNode(step)
	j.stepsDag.AddNode(stepNode)
	for _, precedingStep := range precedingSteps {
		j.stepsDag.Connect(precedingStep.getID(), step.getID())
	}
}

func (j *Job) Start(ctx context.Context) error {
	// TODO: lock Steps, no modification to job execution graph
	j.jobStart.Done()
	j.runtimeCtx = ctx
	if err := j.rootStep.Wait(ctx); err != nil {
		return fmt.Errorf("root job %s failed: %w", j.rootStep.name, err)
	}
	return nil
}

func (j *Job) RuntimeContext() context.Context {
	return j.runtimeCtx
}

func (j *Job) Wait(ctx context.Context) error {
	var tasks []asynctask.Waitable
	for _, step := range j.Steps {
		tasks = append(tasks, step.Waitable())
	}
	return asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, tasks...)
}

// Visualize return a DAG of the job execution graph
func (j *Job) Visualize() (string, error) {
	return j.stepsDag.ToDotGraph()
}
