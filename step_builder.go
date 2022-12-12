package asyncjob

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/Azure/go-asynctask"
)

// AddStep adds a step to the job definition.
func AddStep[JT, ST any](j *JobDefinition[JT], stepName string, stepFuncCreator func(input *JT) asynctask.AsyncFunc[ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	if err := addStepPreCheck(j, stepName); err != nil {
		return nil, err
	}

	stepD := newStepDefinition[ST](stepName, stepTypeTask, optionDecorators...)
	precedingDefSteps, err := getDependsOnSteps(j, stepD.DependsOn())
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
		stepFunc := stepFuncCreator(jiStrongTyped.input)
		stepFuncWithPanicHandling := func(ctx context.Context) (result *ST, err error) {
			// handle panic from user code
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("Panic cought: %v, StackTrace: %s", r, debug.Stack())
				}
			}()

			result, err = stepFunc(ctx)
			return result, err
		}

		stepInstance := newStepInstance[ST](stepD, ji)
		stepInstance.task = asynctask.Start(ctx, instrumentedAddStep(stepInstance, precedingTasks, stepFuncWithPanicHandling))
		ji.addStepInstance(stepInstance, precedingInstances...)
		return stepInstance
	}

	if err := j.addStep(stepD, precedingDefSteps...); err != nil {
		return nil, err
	}
	return stepD, nil
}

// StepAfter add a step after a preceding step, also take input from that preceding step
func StepAfter[JT, PT, ST any](j *JobDefinition[JT], stepName string, parentStep *StepDefinition[PT], stepAfterFuncCreator func(input *JT) asynctask.ContinueFunc[PT, ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	if err := addStepPreCheck(j, stepName); err != nil {
		return nil, err
	}

	stepD := newStepDefinition[ST](stepName, stepTypeTask, append(optionDecorators, ExecuteAfter(parentStep))...)
	precedingDefSteps, err := getDependsOnSteps(j, stepD.DependsOn())
	if err != nil {
		return nil, err
	}

	stepD.instanceCreator = func(ctx context.Context, ji JobInstanceMeta) StepInstanceMeta {
		// TODO: error is ignored here
		precedingInstances, precedingTasks, _ := getDependsOnStepInstances(stepD, ji)

		jiStrongTyped := ji.(*JobInstance[JT])
		stepFunc := stepAfterFuncCreator(jiStrongTyped.input)
		stepFuncWithPanicHandling := func(ctx context.Context, pt *PT) (result *ST, err error) {
			// handle panic from user code
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("Panic cought: %v, StackTrace: %s", r, debug.Stack())
				}
			}()

			result, err = stepFunc(ctx, pt)
			return result, err
		}

		parentStepInstance := getStrongTypedStepInstance(parentStep, ji)
		stepInstance := newStepInstance[ST](stepD, ji)
		// here ContinueWith may not invoke instrumentedStepAfterBoth at all, if parentStep1 or parentStep2 returns error.
		stepInstance.task = asynctask.ContinueWith(ctx, parentStepInstance.task, instrumentedStepAfter(stepInstance, precedingTasks, stepFuncWithPanicHandling))
		ji.addStepInstance(stepInstance, precedingInstances...)
		return stepInstance
	}

	if err := j.addStep(stepD, precedingDefSteps...); err != nil {
		return nil, err
	}
	return stepD, nil
}

// StepAfterBoth add a step after both preceding steps, also take input from both preceding steps
func StepAfterBoth[JT, PT1, PT2, ST any](j *JobDefinition[JT], stepName string, parentStep1 *StepDefinition[PT1], parentStep2 *StepDefinition[PT2], stepAfterBothFuncCreator func(input *JT) asynctask.AfterBothFunc[PT1, PT2, ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	if err := addStepPreCheck(j, stepName); err != nil {
		return nil, err
	}

	// compiler not allow me to compare parentStep1 and parentStep2 directly with different genericType
	if parentStep1.GetName() == parentStep2.GetName() {
		return nil, ErrDuplicateInputParentStep.WithMessage(MsgDuplicateInputParentStep)
	}

	stepD := newStepDefinition[ST](stepName, stepTypeTask, append(optionDecorators, ExecuteAfter(parentStep1), ExecuteAfter(parentStep2))...)
	precedingDefSteps, err := getDependsOnSteps(j, stepD.DependsOn())
	if err != nil {
		return nil, err
	}

	stepD.instanceCreator = func(ctx context.Context, ji JobInstanceMeta) StepInstanceMeta {
		// TODO: error is ignored here
		precedingInstances, precedingTasks, _ := getDependsOnStepInstances(stepD, ji)

		jiStrongTyped := ji.(*JobInstance[JT])
		stepFunc := stepAfterBothFuncCreator(jiStrongTyped.input)
		stepFuncWithPanicHandling := func(ctx context.Context, pt1 *PT1, pt2 *PT2) (result *ST, err error) {
			// handle panic from user code
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("Panic cought: %v, StackTrace: %s", r, debug.Stack())
				}
			}()

			result, err = stepFunc(ctx, pt1, pt2)
			return result, err
		}
		parentStepInstance1 := getStrongTypedStepInstance(parentStep1, ji)
		parentStepInstance2 := getStrongTypedStepInstance(parentStep2, ji)
		stepInstance := newStepInstance[ST](stepD, ji)
		// here AfterBoth may not invoke instrumentedStepAfterBoth at all, if parentStep1 or parentStep2 returns error.
		stepInstance.task = asynctask.AfterBoth(ctx, parentStepInstance1.task, parentStepInstance2.task, instrumentedStepAfterBoth(stepInstance, precedingTasks, stepFuncWithPanicHandling))
		ji.addStepInstance(stepInstance, precedingInstances...)
		return stepInstance
	}

	if err := j.addStep(stepD, precedingDefSteps...); err != nil {
		return nil, err
	}
	return stepD, nil
}

// AddStepWithStaticFunc is same as AddStep, but the stepFunc passed in shouldn't have receiver. (or you get shared state between job instances)
func AddStepWithStaticFunc[JT, ST any](j *JobDefinition[JT], stepName string, stepFunc asynctask.AsyncFunc[ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	return AddStep(j, stepName, func(j *JT) asynctask.AsyncFunc[ST] { return stepFunc }, optionDecorators...)
}

// StepAfterWithStaticFunc is same as StepAfter, but the stepFunc passed in shouldn't have receiver. (or you get shared state between job instances)
func StepAfterWithStaticFunc[JT, PT, ST any](j *JobDefinition[JT], stepName string, parentStep *StepDefinition[PT], stepFunc asynctask.ContinueFunc[PT, ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	return StepAfter(j, stepName, parentStep, func(j *JT) asynctask.ContinueFunc[PT, ST] { return stepFunc }, optionDecorators...)
}

// StepAfterBothWithStaticFunc is same as StepAfterBoth, but the stepFunc passed in shouldn't have receiver. (or you get shared state between job instances)
func StepAfterBothWithStaticFunc[JT, PT1, PT2, ST any](j *JobDefinition[JT], stepName string, parentStep1 *StepDefinition[PT1], parentStep2 *StepDefinition[PT2], stepFunc asynctask.AfterBothFunc[PT1, PT2, ST], optionDecorators ...ExecutionOptionPreparer) (*StepDefinition[ST], error) {
	return StepAfterBoth(j, stepName, parentStep1, parentStep2, func(j *JT) asynctask.AfterBothFunc[PT1, PT2, ST] { return stepFunc }, optionDecorators...)
}

func instrumentedAddStep[T any](stepInstance *StepInstance[T], precedingTasks []asynctask.Waitable, stepFunc func(ctx context.Context) (*T, error)) func(ctx context.Context) (*T, error) {
	return func(ctx context.Context) (*T, error) {
		if err := asynctask.WaitAll(ctx, &asynctask.WaitAllOptions{}, precedingTasks...); err != nil {
			/* this only work on ExecuteAfter (have precedent step, but not taking input from it)
			   asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
			   we need to be consistent on before we do any state change or error handling. */
			return nil, err
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
			/* this only work on ExecuteAfter (have precedent step, but not taking input from it)
			   asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
			   we need to be consistent on before we do any state change or error handling. */
			return nil, err
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
			/* this only work on ExecuteAfter (have precedent step, but not taking input from it)
			   asynctask.ContinueWith and asynctask.AfterBoth won't invoke instrumentedFunc if any of the preceding task failed.
			   we need to be consistent on before we do any state change or error handling. */
			return nil, err
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

func addStepPreCheck(j JobDefinitionMeta, stepName string) error {
	if j.Sealed() {
		return ErrAddStepInSealedJob.WithMessage(fmt.Sprintf(MsgAddStepInSealedJob, stepName))
	}

	if _, ok := j.GetStep(stepName); ok {
		return ErrAddExistingStep.WithMessage(fmt.Sprintf(MsgAddExistingStep, stepName))
	}

	return nil
}

func getDependsOnSteps(j JobDefinitionMeta, dependsOnSteps []string) ([]StepDefinitionMeta, error) {
	var precedingDefSteps []StepDefinitionMeta
	for _, depStepName := range dependsOnSteps {
		if depStep, ok := j.GetStep(depStepName); ok {
			precedingDefSteps = append(precedingDefSteps, depStep)
		} else {
			return nil, ErrRefStepNotInJob.WithMessage(fmt.Sprintf(MsgRefStepNotInJob, depStepName))
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
			return nil, nil, ErrRuntimeStepNotFound.WithMessage(fmt.Sprintf(MsgRuntimeStepNotFound, depStepName))
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
