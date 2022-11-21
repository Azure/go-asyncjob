package asyncjob

import (
	"errors"
	"fmt"
)

type JobErrorCode string

const (
	ErrPrecedentStepFailure JobErrorCode = "precedent step failed"
	ErrStepFailed           JobErrorCode = "step failed"
)

func (code JobErrorCode) Error() string {
	return string(code)
}

type JobError struct {
	Code      JobErrorCode
	StepError error
	StepName  string
	Message   string
}

func newStepError(code JobErrorCode, stepName string, stepErr error) *JobError {
	return &JobError{Code: code, StepName: stepName, StepError: stepErr}
}

func (je *JobError) Error() string {
	if je.Code == ErrStepFailed && je.StepError != nil {
		return fmt.Sprintf("step %q failed: %s", je.StepName, je.StepError.Error())
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
