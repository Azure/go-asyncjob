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

const rootStepName = "job"

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

func InputParam[T any](bCtx context.Context, j *Job, stepName string, value *T) *StepInfo[T] {
	step := newStepInfo[T](stepName, stepTypeParam)

	instrumentedFunc := func(ctx context.Context) (*T, error) {
		j.rootStep.Wait(ctx)
		step.executionData.StartTime = time.Now()
		step.state = StepStateCompleted
		step.executionData.Duration = time.Since(j.rootStep.executionData.StartTime)
		return value, nil
	}
	step.task = asynctask.Start(bCtx, instrumentedFunc)

	j.Steps[stepName] = step
	j.registerStepInGraph(step, j.rootStep)

	return step
}

func AddStep[T any](bCtx context.Context, j *Job, stepName string, stepFunc asynctask.AsyncFunc[T], optionDecorators ...ExecutionOptionPreparer) (*StepInfo[T], error) {
	step := newStepInfo[T](stepName, stepTypeTask, optionDecorators...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingTasks []asynctask.Waitable
	var precedingSteps []StepMeta
	for _, depStepName := range step.DependsOn() {
		if depStep, ok := j.Steps[depStepName]; ok {
			precedingTasks = append(precedingTasks, depStep.Waitable())
			precedingSteps = append(precedingSteps, depStep)
		} else {
			return nil, fmt.Errorf("step [%s] not found", depStepName)
		}
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingTasks) == 0 {
		precedingSteps = append(precedingSteps, j.rootStep)
		precedingTasks = append(precedingTasks, j.rootStep.Waitable())
	}

	// instrument to :
	//     replaceRuntimeContext,
	//     trackStepState
	//     retryHandling (TODO)
	//     errorHandling (TODO)
	//     timeoutHandling (TODO)
	instrumentedFunc := func(ctx context.Context) (*T, error) {
		if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
			/* this only work on ExecuteAfter from input, asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
			   we need to be consistent on how to set state of dependent step.
			step.executionData.StartTime = time.Now()
			step.state = StepStateFailed
			step.executionData.Duration = 0 */
			return nil, err
		}
		step.executionData.StartTime = time.Now()
		step.state = StepStateRunning
		result, err := stepFunc(j.runtimeCtx)
		if err != nil {
			step.state = StepStateFailed
		} else {
			step.state = StepStateCompleted
		}
		step.executionData.Duration = time.Since(step.executionData.StartTime)
		return result, err
	}

	step.task = asynctask.Start(bCtx, instrumentedFunc)

	j.Steps[stepName] = step
	j.registerStepInGraph(step, precedingSteps...)

	return step, nil
}

func StepAfter[T, S any](bCtx context.Context, j *Job, stepName string, parentStep *StepInfo[T], stepFunc asynctask.ContinueFunc[T, S], optionDecorators ...ExecutionOptionPreparer) (*StepInfo[S], error) {
	// check parentStepT is in this job
	if get, ok := j.Steps[parentStep.GetName()]; !ok || get != parentStep {
		return nil, fmt.Errorf("step [%s] not found in job", parentStep.GetName())
	}

	step := newStepInfo[S](stepName, stepTypeTask, append(optionDecorators, ExecuteAfter(parentStep))...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingSteps []StepMeta
	var precedingTasks []asynctask.Waitable
	for _, depStepName := range step.DependsOn() {
		if depStep, ok := j.Steps[depStepName]; ok {
			precedingSteps = append(precedingSteps, depStep)
			precedingTasks = append(precedingTasks, depStep.Waitable())
		} else {
			return nil, fmt.Errorf("step [%s] not found", depStepName)
		}
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingTasks) == 0 {
		precedingSteps = append(precedingSteps, j.rootStep)
		precedingTasks = append(precedingTasks, j.rootStep.Waitable())
	}

	// instrument to :
	//     replaceRuntimeContext
	//     trackStepState
	//     retryHandling (TODO)
	//     errorHandling (TODO)
	//     timeoutHandling (TODO)
	instrumentedFunc := func(ctx context.Context, t *T) (*S, error) {
		if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
			/* this only work on ExecuteAfter from input, asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
			   we need to be consistent on how to set state of dependent step.
			step.executionData.StartTime = time.Now()
			step.state = StepStateFailed
			step.executionData.Duration = 0 */
			return nil, err
		}
		step.executionData.StartTime = time.Now()
		step.state = StepStateRunning
		result, err := stepFunc(j.runtimeCtx, t)
		if err != nil {
			step.state = StepStateFailed
		} else {
			step.state = StepStateCompleted
		}
		step.executionData.Duration = time.Since(step.executionData.StartTime)
		return result, err
	}

	step.task = asynctask.ContinueWith(bCtx, parentStep.task, instrumentedFunc)

	j.Steps[stepName] = step
	j.registerStepInGraph(step, precedingSteps...)
	return step, nil
}

func StepAfterBoth[T, S, R any](bCtx context.Context, j *Job, stepName string, parentStepT *StepInfo[T], parentStepS *StepInfo[S], stepFunc asynctask.AfterBothFunc[T, S, R], optionDecorators ...ExecutionOptionPreparer) (*StepInfo[R], error) {
	// check parentStepT is in this job
	if get, ok := j.Steps[parentStepT.GetName()]; !ok || get != parentStepT {
		return nil, fmt.Errorf("step [%s] not found in job", parentStepT.GetName())
	}
	if get, ok := j.Steps[parentStepS.GetName()]; !ok || get != parentStepS {
		return nil, fmt.Errorf("step [%s] not found in job", parentStepS.GetName())
	}

	step := newStepInfo[R](stepName, stepTypeTask, append(optionDecorators, ExecuteAfter(parentStepT), ExecuteAfter(parentStepS))...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingSteps []StepMeta
	var precedingTasks []asynctask.Waitable
	for _, depStepName := range step.DependsOn() {
		if depStep, ok := j.Steps[depStepName]; ok {
			precedingSteps = append(precedingSteps, depStep)
			precedingTasks = append(precedingTasks, depStep.Waitable())
		} else {
			return nil, fmt.Errorf("step [%s] not found", depStepName)
		}
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingTasks) == 0 {
		precedingSteps = append(precedingSteps, j.rootStep)
		precedingTasks = append(precedingTasks, j.rootStep.Waitable())
	}
	// instrument to :
	//     replaceRuntimeContext
	//     trackStepState
	//     retryHandling (TODO)
	//     errorHandling (TODO)
	//     timeoutHandling (TODO)
	instrumentedFunc := func(ctx context.Context, t *T, s *S) (*R, error) {
		if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
			/* this only work on ExecuteAfter from input, asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
			   we need to be consistent on how to set state of dependent step.
			step.executionData.StartTime = time.Now()
			step.state = StepStateFailed
			step.executionData.Duration = 0 */
			return nil, err
		}

		step.executionData.StartTime = time.Now()
		step.state = StepStateRunning
		result, err := stepFunc(j.runtimeCtx, t, s)
		if err != nil {
			step.state = StepStateFailed
		} else {
			step.state = StepStateCompleted
		}
		step.executionData.Duration = time.Since(step.executionData.StartTime)
		return result, err
	}

	step.task = asynctask.AfterBoth(bCtx, parentStepT.task, parentStepS.task, instrumentedFunc)

	j.Steps[stepName] = step
	j.registerStepInGraph(step, precedingSteps...)

	return step, nil
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

func (j *Job) Wait(ctx context.Context) error {
	var tasks []asynctask.Waitable
	for _, step := range j.Steps {
		tasks = append(tasks, step.Waitable())
	}
	return asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, tasks...)
}

func (j *Job) registerStepInGraph(step StepMeta, precedingSteps ...StepMeta) error {
	stepNode := newStepNode(step)
	j.stepsDag.AddNode(stepNode)
	for _, precedingStep := range precedingSteps {
		j.stepsDag.Connect(precedingStep.getID(), step.getID())
	}

	return nil
}

// Visualize return a DAG of the job execution graph
func (j *Job) Visualize() (string, error) {
	return j.stepsDag.ToDotGraph()
}
