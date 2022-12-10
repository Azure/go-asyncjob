package asyncjob

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/go-asynctask"
)

// StepFromJobInputMethod construct the step from the method on job input
//   compare to AddStep,
//     the benifit would be less chance of shared state between job instances (i.e. method have a receiver, that receiver is shared between job instances)
//     you can construct the step purely on jobInput (unique per job), thus, the method should be defined on jobInput.
//   if your stepFunc doesn't have receiver, then AddStep is simplier.
func StepFromJobInputMethod[JT, ST any](bCtx context.Context, j *JobDefinition[JT], stepName string, stepFuncCreator func(input *JT) asynctask.AsyncFunc[ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	stepD := newStepDefinition[ST](stepName, stepTypeTask, optionDecorators...)
	precedingDefSteps, err := getDependsOnSteps(stepD, j)
	if err != nil {
		return nil, err
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingDefSteps) == 0 {
		precedingDefSteps = append(precedingDefSteps, j.getRootStep())
		stepD.executionOptions.DependOn = append(stepD.executionOptions.DependOn, j.getRootStep().GetName())
	}

	stepD.instanceCreator = func(ctx context.Context, ji JobInstanceMeta) StepInstanceMeta {
		// TODO: error is ignored here
		precedingInstances, precedingTasks, _ := getDependsOnStepInstances(stepD, ji)

		jiStrongTyped := ji.(*JobInstance[JT])
		stepInstance := newStepInstance[ST](stepD, ji)
		stepInstance.task = asynctask.Start(ctx, instrumentedAddStep(stepInstance, precedingTasks, stepFuncCreator(jiStrongTyped.input)))
		ji.addStepInstance(stepInstance, precedingInstances...)
		return stepInstance
	}

	j.addStep(stepD, precedingDefSteps...)
	return stepD, nil
}

// StepAfterFromJobInputMethod construct the step from the method on job input, with a parent step
//   compare to StepAfter,
//     the benifit would be less chance of shared state between job instances (i.e. method have a receiver, that receiver is shared between job instances)
//     you can construct the step purely on jobInput (unique per job), thus, the method should be defined on jobInput.
//   if your stepFunc doesn't have receiver, then StepAfter is simplier.
func StepAfterFromJobInputMethod[JT, PT, ST any](bCtx context.Context, j *JobDefinition[JT], stepName string, parentStep *StepDefinition[PT], stepAfterFuncCreator func(input *JT) asynctask.ContinueFunc[PT, ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	// check parentStepT is in this job
	if get, ok := j.GetStep(parentStep.GetName()); !ok || get != parentStep {
		return nil, fmt.Errorf("step [%s] not found in job", parentStep.GetName())
	}

	stepD := newStepDefinition[ST](stepName, stepTypeTask, append(optionDecorators, ExecuteAfter(parentStep))...)
	precedingDefSteps, err := getDependsOnSteps(stepD, j)
	if err != nil {
		return nil, err
	}

	stepD.instanceCreator = func(ctx context.Context, ji JobInstanceMeta) StepInstanceMeta {
		// TODO: error is ignored here
		precedingInstances, precedingTasks, _ := getDependsOnStepInstances(stepD, ji)

		jiStrongTyped := ji.(*JobInstance[JT])
		parentStepInstance := getStrongTypedStepInstance(parentStep, ji)
		stepInstance := newStepInstance[ST](stepD, ji)
		stepInstance.task = asynctask.ContinueWith(ctx, parentStepInstance.task, instrumentedStepAfter(stepInstance, precedingTasks, stepAfterFuncCreator(jiStrongTyped.input)))
		ji.addStepInstance(stepInstance, precedingInstances...)
		return stepInstance
	}

	j.addStep(stepD, precedingDefSteps...)
	return stepD, nil
}

// StepAfterBothFromJobInputMethod construct the step from the method on job input, with two parent step
//   compare to StepAfterBoth,
//     the benifit would be less chance of shared state between job instances (i.e. method have a receiver, that receiver is shared between job instances)
//     you can construct the step purely on jobInput (unique per job), thus, the method should be defined on jobInput.
//   if your stepFunc doesn't have receiver, then StepAfterBoth is simplier.
func StepAfterBothFromJobInputMethod[JT, PT1, PT2, ST any](bCtx context.Context, j *JobDefinition[JT], stepName string, parentStep1 *StepDefinition[PT1], parentStep2 *StepDefinition[PT2], stepAfterBothFuncCreator func(input *JT) asynctask.AfterBothFunc[PT1, PT2, ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	// check parentStepT is in this job
	if get, ok := j.GetStep(parentStep1.GetName()); !ok || get != parentStep1 {
		return nil, fmt.Errorf("step [%s] not found in job", parentStep1.GetName())
	}
	if get, ok := j.GetStep(parentStep2.GetName()); !ok || get != parentStep2 {
		return nil, fmt.Errorf("step [%s] not found in job", parentStep2.GetName())
	}

	stepD := newStepDefinition[ST](stepName, stepTypeTask, append(optionDecorators, ExecuteAfter(parentStep1), ExecuteAfter(parentStep2))...)
	precedingDefSteps, err := getDependsOnSteps(stepD, j)
	if err != nil {
		return nil, err
	}

	// if a step have no preceding tasks, link it to our rootJob as preceding task, so it won't start yet.
	if len(precedingDefSteps) == 0 {
		precedingDefSteps = append(precedingDefSteps, j.getRootStep())
		stepD.executionOptions.DependOn = append(stepD.executionOptions.DependOn, j.getRootStep().GetName())
	}

	stepD.instanceCreator = func(ctx context.Context, ji JobInstanceMeta) StepInstanceMeta {
		// TODO: error is ignored here
		precedingInstances, precedingTasks, _ := getDependsOnStepInstances(stepD, ji)

		jiStrongTyped := ji.(*JobInstance[JT])
		parentStepInstance1 := getStrongTypedStepInstance(parentStep1, ji)
		parentStepInstance2 := getStrongTypedStepInstance(parentStep2, ji)
		stepInstance := newStepInstance[ST](stepD, ji)
		stepInstance.task = asynctask.AfterBoth(ctx, parentStepInstance1.task, parentStepInstance2.task, instrumentedStepAfterBoth(stepInstance, precedingTasks, stepAfterBothFuncCreator(jiStrongTyped.input)))
		ji.addStepInstance(stepInstance, precedingInstances...)
		return stepInstance
	}

	j.addStep(stepD, precedingDefSteps...)
	return stepD, nil
}

// AddStep add a step without take input
//   you can still choose to execute after certain step by pass asyncjob.ExecuteAfter in optionDecorators
func AddStep[JT, ST any](bCtx context.Context, j *JobDefinition[JT], stepName string, stepFunc asynctask.AsyncFunc[ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	return StepFromJobInputMethod(bCtx, j, stepName, func(j *JT) asynctask.AsyncFunc[ST] { return stepFunc }, optionDecorators...)
}

// StepAfter add a step after a preceding step, also take input from that preceding step
func StepAfter[JT, PT, ST any](bCtx context.Context, j *JobDefinition[JT], stepName string, parentStep *StepDefinition[PT], stepFunc asynctask.ContinueFunc[PT, ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	return StepAfterFromJobInputMethod(bCtx, j, stepName, parentStep, func(j *JT) asynctask.ContinueFunc[PT, ST] { return stepFunc }, optionDecorators...)
}

// StepAfterBoth add a step after both preceding steps, also take input from both preceding steps
func StepAfterBoth[JT, PT1, PT2, ST any](bCtx context.Context, j *JobDefinition[JT], stepName string, parentStep1 *StepDefinition[PT1], parentStep2 *StepDefinition[PT2], stepFunc asynctask.AfterBothFunc[PT1, PT2, ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	return StepAfterBothFromJobInputMethod(bCtx, j, stepName, parentStep1, parentStep2, func(j *JT) asynctask.AfterBothFunc[PT1, PT2, ST] { return stepFunc }, optionDecorators...)
}

func instrumentedAddStep[T any](stepInstance *StepInstance[T], precedingTasks []asynctask.Waitable, stepFunc func(ctx context.Context) (*T, error)) func(ctx context.Context) (*T, error) {
	return func(ctx context.Context) (*T, error) {
		if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
			/* this only work on ExecuteAfter from input, asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
			   we need to be consistent on how to set state of dependent step.
			step.executionData.StartTime = time.Now()
			step.state = StepStateFailed
			step.executionData.Duration = 0 */
			return nil, newStepError(ErrPrecedentStepFailure, stepInstance, err)
		}

		stepInstance.executionData.StartTime = time.Now()
		stepInstance.state = StepStateRunning
		ctx = stepInstance.EnrichContext(ctx)

		var result *T
		var err error
		if stepInstance.Definition.executionOptions.RetryPolicy != nil {
			stepInstance.executionData.Retried = &RetryReport{}
			result, err = newRetryer(stepInstance.Definition.executionOptions.RetryPolicy, stepInstance.executionData.Retried, func() (*T, error) { return stepFunc(ctx) }).Run()
		} else {
			result, err = stepFunc(ctx)
		}

		stepInstance.executionData.Duration = time.Since(stepInstance.executionData.StartTime)

		if err != nil {
			stepInstance.state = StepStateFailed
			return nil, newStepError(ErrStepFailed, stepInstance, err)
		} else {
			stepInstance.state = StepStateCompleted
			return result, nil
		}
	}
}

func instrumentedStepAfter[T, S any](stepInstance *StepInstance[S], precedingTasks []asynctask.Waitable, stepFunc func(ctx context.Context, t *T) (*S, error)) func(ctx context.Context, t *T) (*S, error) {
	return func(ctx context.Context, t *T) (*S, error) {
		if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
			/* this only work on ExecuteAfter from input, asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
			   we need to be consistent on how to set state of dependent step.
			step.executionData.StartTime = time.Now()
			step.state = StepStateFailed
			step.executionData.Duration = 0 */
			return nil, newStepError(ErrPrecedentStepFailure, stepInstance, err)
		}

		stepInstance.executionData.StartTime = time.Now()
		stepInstance.state = StepStateRunning
		ctx = stepInstance.EnrichContext(ctx)

		var result *S
		var err error
		if stepInstance.Definition.executionOptions.RetryPolicy != nil {
			stepInstance.executionData.Retried = &RetryReport{}
			result, err = newRetryer(stepInstance.Definition.executionOptions.RetryPolicy, stepInstance.executionData.Retried, func() (*S, error) { return stepFunc(ctx, t) }).Run()
		} else {
			result, err = stepFunc(ctx, t)
		}

		stepInstance.executionData.Duration = time.Since(stepInstance.executionData.StartTime)

		if err != nil {
			stepInstance.state = StepStateFailed
			return nil, newStepError(ErrStepFailed, stepInstance, err)
		} else {
			stepInstance.state = StepStateCompleted
			return result, nil
		}
	}
}

func instrumentedStepAfterBoth[T, S, R any](stepInstance *StepInstance[R], precedingTasks []asynctask.Waitable, stepFunc func(ctx context.Context, t *T, s *S) (*R, error)) func(ctx context.Context, t *T, s *S) (*R, error) {
	return func(ctx context.Context, t *T, s *S) (*R, error) {

		if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
			/* this only work on ExecuteAfter from input, asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
			   we need to be consistent on how to set state of dependent step.
			step.executionData.StartTime = time.Now()
			step.state = StepStateFailed
			step.executionData.Duration = 0 */
			return nil, newStepError(ErrPrecedentStepFailure, stepInstance, err)
		}

		stepInstance.executionData.StartTime = time.Now()
		stepInstance.state = StepStateRunning
		ctx = stepInstance.EnrichContext(ctx)

		var result *R
		var err error
		if stepInstance.Definition.executionOptions.RetryPolicy != nil {
			stepInstance.executionData.Retried = &RetryReport{}
			result, err = newRetryer(stepInstance.Definition.executionOptions.RetryPolicy, stepInstance.executionData.Retried, func() (*R, error) { return stepFunc(ctx, t, s) }).Run()
		} else {
			result, err = stepFunc(ctx, t, s)
		}

		stepInstance.executionData.Duration = time.Since(stepInstance.executionData.StartTime)

		if err != nil {
			stepInstance.state = StepStateFailed
			return nil, newStepError(ErrStepFailed, stepInstance, err)
		} else {
			stepInstance.state = StepStateCompleted
			return result, nil
		}
	}
}

func getDependsOnSteps(step StepDefinitionMeta, j JobDefinitionMeta) ([]StepDefinitionMeta, error) {
	var precedingDefSteps []StepDefinitionMeta
	for _, depStepName := range step.DependsOn() {
		if depStep, ok := j.GetStep(depStepName); ok {
			precedingDefSteps = append(precedingDefSteps, depStep)
		} else {
			return nil, fmt.Errorf("step [%s] not found", depStepName)
		}
	}

	return precedingDefSteps, nil
}

func getDependsOnStepInstances(stepD StepDefinitionMeta, ji JobInstanceMeta) ([]StepInstanceMeta, []asynctask.Waitable, error) {
	var precedingInstances []StepInstanceMeta
	var precedingTasks []asynctask.Waitable
	for _, depStepName := range stepD.DependsOn() {
		if depStep, ok := ji.GetStepInstance(depStepName); ok {
			precedingInstances = append(precedingInstances, depStep)
			precedingTasks = append(precedingTasks, depStep.Waitable())
		} else {
			return nil, nil, fmt.Errorf("runtime step [%s] not found", depStepName)
		}
	}

	return precedingInstances, precedingTasks, nil
}

// this is most vulunerable point of this library
//   we have strongTyped steps
//   we can create stronglyTyped stepInstance from stronglyTyped stepDefinition
//   We cannot store strongTyped stepInstance and passing it to next step
//   now we need this typeAssertion, to beable to link steps
//   in theory, we have all the info, we construct the instance, if it panics, we should fix it.
func getStrongTypedStepInstance[T any](stepD *StepDefinition[T], ji JobInstanceMeta) *StepInstance[T] {
	stepInstanceMeta, ok := ji.GetStepInstance(stepD.GetName())
	if !ok {
		panic(fmt.Sprintf("step [%s] not found in jobInstance", stepD.GetName()))
	}

	stepInstance, ok := stepInstanceMeta.(*StepInstance[T])
	if !ok {
		panic(fmt.Sprintf("step [%s] in jobInstance is not expected Type", stepD.GetName()))
	}

	return stepInstance
}
