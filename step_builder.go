package asyncjob

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/go-asynctask"
)

func AddStep[ST any](bCtx context.Context, j JobDefinitionMeta, stepName string, stepFunc asynctask.AsyncFunc[ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	step := newStepDefinition[ST](stepName, stepTypeTask, optionDecorators...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingDefSteps []StepDefinitionMeta
	for _, depStepName := range step.DependsOn() {
		if depStep, ok := j.GetStep(depStepName); ok {
			precedingDefSteps = append(precedingDefSteps, depStep)
		} else {
			return nil, fmt.Errorf("step [%s] not found", depStepName)
		}
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingDefSteps) == 0 {
		precedingDefSteps = append(precedingDefSteps, j.RootStep())
		step.executionOptions.DependOn = append(step.executionOptions.DependOn, j.RootStep().GetName())
	}

	step.instanceCreator = func(ctx context.Context, ji JobInstanceMeta) StepInstanceMeta {
		var precedingInstances []StepInstanceMeta
		var precedingTasks []asynctask.Waitable
		for _, depStepName := range step.DependsOn() {
			if depStep, ok := ji.GetStepInstance(depStepName); ok {
				precedingInstances = append(precedingInstances, depStep)
				precedingTasks = append(precedingTasks, depStep.Waitable())
			}
		}

		stepInstance := newStepInstance[ST](step)
		instrumentedFunc := func(ctx context.Context) (*ST, error) {
			if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
				/* this only work on ExecuteAfter from input, asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
				   we need to be consistent on how to set state of dependent step.
				step.executionData.StartTime = time.Now()
				step.state = StepStateFailed
				step.executionData.Duration = 0 */
				return nil, newJobError(ErrPrecedentStepFailure, "")
			}

			stepInstance.executionData.StartTime = time.Now()
			stepInstance.state = StepStateRunning

			var result *ST
			var err error
			if step.executionOptions.RetryPolicy != nil {
				stepInstance.executionData.Retried = &RetryReport{}
				result, err = newRetryer(step.executionOptions.RetryPolicy, stepInstance.executionData.Retried, func() (*ST, error) { return stepFunc(ctx) }).Run()
			} else {
				result, err = stepFunc(ctx)
			}

			stepInstance.executionData.Duration = time.Since(stepInstance.executionData.StartTime)

			if err != nil {
				stepInstance.state = StepStateFailed
				return nil, newStepError(stepName, err)
			} else {
				stepInstance.state = StepStateCompleted
				return result, nil
			}
		}
		stepInstance.task = asynctask.Start(ctx, instrumentedFunc)
		ji.AddStepInstance(stepInstance, precedingInstances...)
		return stepInstance
	}

	j.AddStep(step, precedingDefSteps...)
	return step, nil
}

func StepAfter[T, S any](bCtx context.Context, j JobDefinitionMeta, stepName string, parentStep *StepDefinition[T], stepFunc asynctask.ContinueFunc[T, S], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[S], error) {
	// check parentStepT is in this job
	if get, ok := j.GetStep(parentStep.GetName()); !ok || get != parentStep {
		return nil, fmt.Errorf("step [%s] not found in job", parentStep.GetName())
	}

	step := newStepDefinition[S](stepName, stepTypeTask, append(optionDecorators, ExecuteAfter(parentStep))...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.

	var precedingDefSteps []StepDefinitionMeta
	for _, depStepName := range step.DependsOn() {
		if depStep, ok := j.GetStep(depStepName); ok {
			precedingDefSteps = append(precedingDefSteps, depStep)
		} else {
			return nil, fmt.Errorf("step [%s] not found", depStepName)
		}
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingDefSteps) == 0 {
		precedingDefSteps = append(precedingDefSteps, j.RootStep())
	}

	step.instanceCreator = func(ctx context.Context, ji JobInstanceMeta) StepInstanceMeta {
		var precedingInstances []StepInstanceMeta
		var precedingTasks []asynctask.Waitable
		for _, depStepName := range step.DependsOn() {
			if depStep, ok := ji.GetStepInstance(depStepName); ok {
				precedingInstances = append(precedingInstances, depStep)
				precedingTasks = append(precedingTasks, depStep.Waitable())
			}
		}

		parentStepInstanceMeta, _ := ji.GetStepInstance(parentStep.GetName())
		var parentStepInstance *StepInstance[T] = parentStepInstanceMeta.(*StepInstance[T])

		stepInstance := newStepInstance[S](step)
		instrumentedFunc := func(ctx context.Context, t *T) (*S, error) {

			if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
				/* this only work on ExecuteAfter from input, asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
				   we need to be consistent on how to set state of dependent step.
				step.executionData.StartTime = time.Now()
				step.state = StepStateFailed
				step.executionData.Duration = 0 */
				return nil, newJobError(ErrPrecedentStepFailure, "")
			}

			stepInstance.executionData.StartTime = time.Now()
			stepInstance.state = StepStateRunning

			var result *S
			var err error
			if step.executionOptions.RetryPolicy != nil {
				stepInstance.executionData.Retried = &RetryReport{}
				result, err = newRetryer(step.executionOptions.RetryPolicy, stepInstance.executionData.Retried, func() (*S, error) { return stepFunc(ctx, t) }).Run()
			} else {
				result, err = stepFunc(ctx, t)
			}

			stepInstance.executionData.Duration = time.Since(stepInstance.executionData.StartTime)

			if err != nil {
				stepInstance.state = StepStateFailed
				return nil, newStepError(stepName, err)
			} else {
				stepInstance.state = StepStateCompleted
				return result, nil
			}
		}
		stepInstance.task = asynctask.ContinueWith(ctx, parentStepInstance.task, instrumentedFunc)
		ji.AddStepInstance(stepInstance, precedingInstances...)
		return stepInstance
	}

	j.AddStep(step, precedingDefSteps...)
	return step, nil
}

func StepAfterBoth[T, S, R any](bCtx context.Context, j JobDefinitionMeta, stepName string, parentStepT *StepDefinition[T], parentStepS *StepDefinition[S], stepFunc asynctask.AfterBothFunc[T, S, R], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[R], error) {
	// check parentStepT is in this job
	if get, ok := j.GetStep(parentStepT.GetName()); !ok || get != parentStepT {
		return nil, fmt.Errorf("step [%s] not found in job", parentStepT.GetName())
	}
	if get, ok := j.GetStep(parentStepS.GetName()); !ok || get != parentStepS {
		return nil, fmt.Errorf("step [%s] not found in job", parentStepS.GetName())
	}

	step := newStepDefinition[R](stepName, stepTypeTask, append(optionDecorators, ExecuteAfter(parentStepT), ExecuteAfter(parentStepS))...)

	// also consider specified the dependencies from ExecutionOptionPreparer, without consume the result.
	var precedingDefSteps []StepDefinitionMeta
	for _, depStepName := range step.DependsOn() {
		if depStep, ok := j.GetStep(depStepName); ok {
			precedingDefSteps = append(precedingDefSteps, depStep)
		} else {
			return nil, fmt.Errorf("step [%s] not found", depStepName)
		}
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingDefSteps) == 0 {
		precedingDefSteps = append(precedingDefSteps, j.RootStep())
	}

	step.instanceCreator = func(ctx context.Context, ji JobInstanceMeta) StepInstanceMeta {
		var precedingInstances []StepInstanceMeta
		var precedingTasks []asynctask.Waitable
		for _, depStepName := range step.DependsOn() {
			if depStep, ok := ji.GetStepInstance(depStepName); ok {
				precedingInstances = append(precedingInstances, depStep)
				precedingTasks = append(precedingTasks, depStep.Waitable())
			}
		}

		parentStepTInstanceMeta, _ := ji.GetStepInstance(parentStepT.GetName())
		var parentStepTInstance *StepInstance[T] = parentStepTInstanceMeta.(*StepInstance[T])

		parentStepSInstanceMeta, _ := ji.GetStepInstance(parentStepS.GetName())
		var parentStepSInstance *StepInstance[S] = parentStepSInstanceMeta.(*StepInstance[S])

		stepInstance := newStepInstance[R](step)
		instrumentedFunc := func(ctx context.Context, t *T, s *S) (*R, error) {

			if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
				/* this only work on ExecuteAfter from input, asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
				   we need to be consistent on how to set state of dependent step.
				step.executionData.StartTime = time.Now()
				step.state = StepStateFailed
				step.executionData.Duration = 0 */
				return nil, newJobError(ErrPrecedentStepFailure, "")
			}

			stepInstance.executionData.StartTime = time.Now()
			stepInstance.state = StepStateRunning

			var result *R
			var err error
			if step.executionOptions.RetryPolicy != nil {
				stepInstance.executionData.Retried = &RetryReport{}
				result, err = newRetryer(step.executionOptions.RetryPolicy, stepInstance.executionData.Retried, func() (*R, error) { return stepFunc(ctx, t, s) }).Run()
			} else {
				result, err = stepFunc(ctx, t, s)
			}

			stepInstance.executionData.Duration = time.Since(stepInstance.executionData.StartTime)

			if err != nil {
				stepInstance.state = StepStateFailed
				return nil, newStepError(stepName, err)
			} else {
				stepInstance.state = StepStateCompleted
				return result, nil
			}
		}
		stepInstance.task = asynctask.AfterBoth(ctx, parentStepTInstance.task, parentStepSInstance.task, instrumentedFunc)
		ji.AddStepInstance(stepInstance, precedingInstances...)
		return stepInstance
	}

	j.AddStep(step, precedingDefSteps...)
	return step, nil
}
