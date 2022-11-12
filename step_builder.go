package asyncjob

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/go-asynctask"
)

func InputParam[T any](bCtx context.Context, j JobInterface, stepName string, value *T) *StepInfo[T] {
	step := newStepInfo[T](stepName, stepTypeParam)

	instrumentedFunc := func(ctx context.Context) (*T, error) {
		j.RootStep().Wait(ctx)
		step.executionData.StartTime = time.Now()
		step.state = StepStateCompleted
		step.executionData.Duration = 0 * time.Second
		return value, nil
	}
	step.task = asynctask.Start(bCtx, instrumentedFunc)

	j.AddStep(step, j.RootStep())

	return step
}

func AddStep[T any](bCtx context.Context, j JobInterface, stepName string, stepFunc asynctask.AsyncFunc[T], optionDecorators ...ExecutionOptionPreparer) (*StepInfo[T], error) {
	step := newStepInfo[T](stepName, stepTypeTask, optionDecorators...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingTasks []asynctask.Waitable
	var precedingSteps []StepMeta
	for _, depStepName := range step.DependsOn() {
		if depStep, ok := j.GetStep(depStepName); ok {
			precedingTasks = append(precedingTasks, depStep.Waitable())
			precedingSteps = append(precedingSteps, depStep)
		} else {
			return nil, fmt.Errorf("step [%s] not found", depStepName)
		}
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingTasks) == 0 {
		precedingSteps = append(precedingSteps, j.RootStep())
		precedingTasks = append(precedingTasks, j.RootStep().Waitable())
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
			return nil, newJobError(ErrPrecedentStepFailure, "")
		}
		step.executionData.StartTime = time.Now()
		step.state = StepStateRunning

		var result *T
		var err error
		if step.executionOptions.RetryPolicy != nil {
			step.executionData.Retried = &RetryReport{}
			result, err = newRetryer(step.executionOptions.RetryPolicy, step.executionData.Retried, func() (*T, error) { return stepFunc(j.RuntimeContext()) }).Run()
		} else {
			result, err = stepFunc(j.RuntimeContext())
		}

		step.executionData.Duration = time.Since(step.executionData.StartTime)

		if err != nil {
			step.state = StepStateFailed
			return nil, newStepError(stepName, err)
		} else {
			step.state = StepStateCompleted
			return result, nil
		}
	}

	step.task = asynctask.Start(bCtx, instrumentedFunc)

	j.AddStep(step, precedingSteps...)

	return step, nil
}

func StepAfter[T, S any](bCtx context.Context, j JobInterface, stepName string, parentStep *StepInfo[T], stepFunc asynctask.ContinueFunc[T, S], optionDecorators ...ExecutionOptionPreparer) (*StepInfo[S], error) {
	// check parentStepT is in this job
	if get, ok := j.GetStep(parentStep.GetName()); !ok || get != parentStep {
		return nil, fmt.Errorf("step [%s] not found in job", parentStep.GetName())
	}

	step := newStepInfo[S](stepName, stepTypeTask, append(optionDecorators, ExecuteAfter(parentStep))...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingSteps []StepMeta
	var precedingTasks []asynctask.Waitable
	for _, depStepName := range step.DependsOn() {
		if depStep, ok := j.GetStep(depStepName); ok {
			precedingSteps = append(precedingSteps, depStep)
			precedingTasks = append(precedingTasks, depStep.Waitable())
		} else {
			return nil, fmt.Errorf("step [%s] not found", depStepName)
		}
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
			return nil, newJobError(ErrPrecedentStepFailure, "")
		}
		step.executionData.StartTime = time.Now()
		step.state = StepStateRunning
		var result *S
		var err error
		if step.executionOptions.RetryPolicy != nil {
			step.executionData.Retried = &RetryReport{}
			result, err = newRetryer(step.executionOptions.RetryPolicy, step.executionData.Retried, func() (*S, error) { return stepFunc(j.RuntimeContext(), t) }).Run()
		} else {
			result, err = stepFunc(j.RuntimeContext(), t)
		}

		step.executionData.Duration = time.Since(step.executionData.StartTime)

		if err != nil {
			step.state = StepStateFailed
			return nil, newStepError(stepName, err)
		} else {
			step.state = StepStateCompleted
			return result, nil
		}
	}

	step.task = asynctask.ContinueWith(bCtx, parentStep.task, instrumentedFunc)

	j.AddStep(step, precedingSteps...)
	return step, nil
}

func StepAfterBoth[T, S, R any](bCtx context.Context, j JobInterface, stepName string, parentStepT *StepInfo[T], parentStepS *StepInfo[S], stepFunc asynctask.AfterBothFunc[T, S, R], optionDecorators ...ExecutionOptionPreparer) (*StepInfo[R], error) {
	// check parentStepT is in this job
	if get, ok := j.GetStep(parentStepT.GetName()); !ok || get != parentStepT {
		return nil, fmt.Errorf("step [%s] not found in job", parentStepT.GetName())
	}
	if get, ok := j.GetStep(parentStepS.GetName()); !ok || get != parentStepS {
		return nil, fmt.Errorf("step [%s] not found in job", parentStepS.GetName())
	}

	step := newStepInfo[R](stepName, stepTypeTask, append(optionDecorators, ExecuteAfter(parentStepT), ExecuteAfter(parentStepS))...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingSteps []StepMeta
	var precedingTasks []asynctask.Waitable
	for _, depStepName := range step.DependsOn() {
		if depStep, ok := j.GetStep(depStepName); ok {
			precedingSteps = append(precedingSteps, depStep)
			precedingTasks = append(precedingTasks, depStep.Waitable())
		} else {
			return nil, fmt.Errorf("step [%s] not found", depStepName)
		}
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
			return nil, newJobError(ErrPrecedentStepFailure, "")
		}

		step.executionData.StartTime = time.Now()
		step.state = StepStateRunning

		var result *R
		var err error
		if step.executionOptions.RetryPolicy != nil {
			step.executionData.Retried = &RetryReport{}
			result, err = newRetryer(step.executionOptions.RetryPolicy, step.executionData.Retried, func() (*R, error) { return stepFunc(j.RuntimeContext(), t, s) }).Run()
		} else {
			result, err = stepFunc(j.RuntimeContext(), t, s)
		}

		step.executionData.Duration = time.Since(step.executionData.StartTime)

		if err != nil {
			step.state = StepStateFailed
			return nil, newStepError(stepName, err)
		} else {
			step.state = StepStateCompleted
			return result, nil
		}
	}

	step.task = asynctask.AfterBoth(bCtx, parentStepT.task, parentStepS.task, instrumentedFunc)

	j.AddStep(step, precedingSteps...)

	return step, nil
}
