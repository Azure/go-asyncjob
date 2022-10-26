package asyncjob

import (
	"context"
	"fmt"
	"sync"

	"github.com/Azure/go-asynctask"
	"github.com/hashicorp/terraform/dag"
)

type JobState string

const JobStatePending JobState = "pending"
const JobStateRunning JobState = "running"
const JobStateCompleted JobState = "completed"

type Job struct {
	Name  string
	Steps map[string]StepMeta

	state    JobState
	rootJob  *StepInfo[interface{}]
	jobStart *sync.WaitGroup
	stepsDag *dag.AcyclicGraph

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
		stepsDag: &dag.AcyclicGraph{},
	}

	j.rootJob = &StepInfo[interface{}]{
		name: "[Start]",
		task: asynctask.Start(context.Background(), func(fctx context.Context) (*interface{}, error) {
			fmt.Println("RootJob Added")
			// this will pause all steps from starting, until Start() method is called.
			jobStart.Wait()
			j.state = JobStateRunning
			return nil, nil
		}),
	}

	j.Steps[j.rootJob.GetName()] = j.rootJob
	j.stepsDag.Add(j.rootJob.GetName())

	return j
}

func InputParam[T any](bCtx context.Context, j *Job, stepName string, value *T) *StepInfo[T] {
	step := newStepInfo[T](stepName)

	instrumentedFunc := func(ctx context.Context) (*T, error) {
		j.rootJob.Wait(ctx)
		return value, nil
	}
	step.task = asynctask.Start(bCtx, instrumentedFunc)

	j.Steps[stepName] = step
	j.registerStepInGraph(stepName, j.rootJob.GetName())

	return step
}

func AddStep[T any](bCtx context.Context, j *Job, stepName string, stepFunc asynctask.AsyncFunc[T], optionDecorators ...ExecutionOptionPreparer) (*StepInfo[T], error) {
	step := newStepInfo[T](stepName, optionDecorators...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingStepNames = step.DependsOn()
	var precedingTasks []asynctask.Waitable
	for _, stepName := range precedingStepNames {
		if step, ok := j.Steps[stepName]; ok {
			precedingTasks = append(precedingTasks, step.Waitable())
		} else {
			return nil, fmt.Errorf("step [%s] not found", stepName)
		}
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingTasks) == 0 {
		precedingStepNames = append(precedingStepNames, j.rootJob.GetName())
		precedingTasks = append(precedingTasks, j.rootJob.Waitable())
	}

	// instrument to :
	//     replaceRuntimeContext,
	//     trackStepState
	//     retryHandling (TODO)
	//     errorHandling (TODO)
	//     timeoutHandling (TODO)
	instrumentedFunc := func(ctx context.Context) (*T, error) {
		if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
			return nil, err
		}
		step.state = StepStateRunning
		result, err := stepFunc(j.runtimeCtx)
		if err != nil {
			step.state = StepStateFailed
		} else {
			step.state = StepStateCompleted
		}
		return result, err
	}

	step.task = asynctask.Start(bCtx, instrumentedFunc)

	j.Steps[stepName] = step
	j.registerStepInGraph(stepName, precedingStepNames...)

	return step, nil
}

func StepAfter[T, S any](bCtx context.Context, j *Job, stepName string, parentStep *StepInfo[T], stepFunc asynctask.ContinueFunc[T, S], optionDecorators ...ExecutionOptionPreparer) (*StepInfo[S], error) {
	// check parentStepT is in this job
	if get, ok := j.Steps[parentStep.GetName()]; !ok || get != parentStep {
		return nil, fmt.Errorf("step [%s] not found in job", parentStep.GetName())
	}

	step := newStepInfo[S](stepName, append(optionDecorators, ExecuteAfter(parentStep))...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingStepNames = step.DependsOn()
	var precedingTasks []asynctask.Waitable
	for _, stepName := range precedingStepNames {
		if step, ok := j.Steps[stepName]; ok {
			precedingTasks = append(precedingTasks, step.Waitable())
		} else {
			return nil, fmt.Errorf("step [%s] not found", stepName)
		}
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingTasks) == 0 {
		precedingTasks = append(precedingTasks, j.rootJob.Waitable())
	}

	// instrument to :
	//     replaceRuntimeContext
	//     trackStepState
	//     retryHandling (TODO)
	//     errorHandling (TODO)
	//     timeoutHandling (TODO)
	instrumentedFunc := func(ctx context.Context, t *T) (*S, error) {
		if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
			return nil, err
		}
		step.state = StepStateRunning
		result, err := stepFunc(j.runtimeCtx, t)
		if err != nil {
			step.state = StepStateFailed
		} else {
			step.state = StepStateCompleted
		}
		return result, err
	}

	step.task = asynctask.ContinueWith(bCtx, parentStep.task, instrumentedFunc)

	j.Steps[stepName] = step
	j.registerStepInGraph(stepName, precedingStepNames...)
	if err := j.stepsDag.Validate(); err != nil {
		return nil, fmt.Errorf("cycle dependency detected: %s", err)
	}
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

	step := newStepInfo[R](stepName, append(optionDecorators, ExecuteAfter(parentStepT), ExecuteAfter(parentStepS))...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingStepNames = step.DependsOn()
	var precedingTasks []asynctask.Waitable
	for _, stepName := range precedingStepNames {
		if step, ok := j.Steps[stepName]; ok {
			precedingTasks = append(precedingTasks, step.Waitable())
		} else {
			return nil, fmt.Errorf("step [%s] not found", stepName)
		}
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingTasks) == 0 {
		precedingTasks = append(precedingTasks, j.rootJob.Waitable())
	}
	// instrument to :
	//     replaceRuntimeContext
	//     trackStepState
	//     retryHandling (TODO)
	//     errorHandling (TODO)
	//     timeoutHandling (TODO)
	instrumentedFunc := func(_ context.Context, t *T, s *S) (*R, error) {
		step.state = StepStateRunning
		result, err := stepFunc(j.runtimeCtx, t, s)
		if err != nil {
			step.state = StepStateFailed
		} else {
			step.state = StepStateCompleted
		}
		return result, err
	}

	step.task = asynctask.AfterBoth(bCtx, parentStepT.task, parentStepS.task, instrumentedFunc)

	j.Steps[stepName] = step
	j.registerStepInGraph(stepName, precedingStepNames...)

	return step, nil
}

func (j *Job) Start(ctx context.Context) error {
	// TODO: lock Steps, no modification to job execution graph
	j.jobStart.Done()
	j.runtimeCtx = ctx
	if err := j.rootJob.Wait(ctx); err != nil {
		return fmt.Errorf("job [Start] failed: %w", err)
	}

	j.rootJob.state = StepStateCompleted
	return nil
}

func (j *Job) Wait(ctx context.Context) error {
	var tasks []asynctask.Waitable
	for _, step := range j.Steps {
		tasks = append(tasks, step.Waitable())
	}
	return asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, tasks...)
}

func (j *Job) registerStepInGraph(stepName string, precedingStep ...string) error {
	j.stepsDag.Add(stepName)
	for _, precedingStepName := range precedingStep {
		j.stepsDag.Connect(dag.BasicEdge(precedingStepName, stepName))
		if err := j.stepsDag.Validate(); err != nil {
			return fmt.Errorf("failed to add step %q depend on %q, likely a cycle dependency. %w", stepName, precedingStepName, err)
		}
	}

	return nil
}

// Visualize return a DAG of the job execution graph
func (j *Job) Visualize() string {
	opts := &dag.DotOpts{MaxDepth: 42}
	actual := j.stepsDag.Dot(opts)
	return string(actual)
}
