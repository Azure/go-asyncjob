package asyncjob

import (
	"context"
)

type JobDefinitionWithResult[Tin, Tout any] struct {
	*JobDefinition[Tin]
	resultStep *StepDefinition[Tout]
}

func JobWithResult[Tin, Tout any](jd *JobDefinition[Tin], resultStep *StepDefinition[Tout]) (*JobDefinitionWithResult[Tin, Tout], error) {
	sdGet, ok := jd.GetStep(resultStep.GetName())
	if !ok || sdGet != resultStep {
		return nil, ErrStepNotInJob
	}

	return &JobDefinitionWithResult[Tin, Tout]{
		JobDefinition: jd,
		resultStep:    resultStep,
	}, nil
}

type JobInstanceWithResult[Tin, Tout any] struct {
	*JobInstance[Tin]
	resultStep *StepInstance[Tout]
}

func (jd *JobDefinitionWithResult[Tin, Tout]) Start(ctx context.Context, input *Tin) *JobInstanceWithResult[Tin, Tout] {
	ji := jd.JobDefinition.Start(ctx, input)

	return &JobInstanceWithResult[Tin, Tout]{
		JobInstance: ji,
		resultStep:  getStrongTypedStepInstance(jd.resultStep, ji),
	}
}

// Result returns the result of the job from result step.
//    it doesn't wait for all steps to finish, you can use Result() after Wait() if desired.
func (ji *JobInstanceWithResult[Tin, Tout]) Result(ctx context.Context) (*Tout, error) {
	return ji.resultStep.task.Result(ctx)
}
