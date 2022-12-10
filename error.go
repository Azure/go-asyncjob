package asyncjob

import (
	"errors"
	"fmt"
)

type JobErrorCode string

const (
	ErrPrecedentStepFailure JobErrorCode = "precedent step failed"
	ErrStepFailed           JobErrorCode = "step failed"
	ErrStepNotInJob         JobErrorCode = "trying to reference to a step not registered in job"
)

func (code JobErrorCode) Error() string {
	return string(code)
}

type JobError struct {
	Code         JobErrorCode
	StepError    error
	StepInstance StepInstanceMeta
	Message      string
}

func newStepError(code JobErrorCode, step StepInstanceMeta, stepErr error) *JobError {
	return &JobError{Code: code, StepInstance: step, StepError: stepErr}
}

func (je *JobError) Error() string {
	if je.Code == ErrStepFailed && je.StepError != nil {
		return fmt.Sprintf("step %q failed: %s", je.StepInstance.GetName(), je.StepError.Error())
	}
	return je.Code.Error() + ": " + je.Message
}

func (je *JobError) Unwrap() error {
	return je.StepError
}

// RootCause track precendent chain and return the first step raised this error.
func (je *JobError) RootCause() error {
	// this step failed, return the error
	if je.Code == ErrStepFailed {
		return je
	}

	// precendent step failure, track to the root
	if je.Code == ErrPrecedentStepFailure {
		precedentStepErr := &JobError{}
		if !errors.As(je.StepError, &precedentStepErr) {
			return je.StepError
		}
		return precedentStepErr.RootCause()
	}

	// no idea
	return je
}
