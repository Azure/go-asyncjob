package asyncjob

import (
	"fmt"
)

type JobErrorCode string

const (
	ErrPrecedentStepFailure JobErrorCode = "precedent step failed"
	ErrStepFailed           JobErrorCode = "current step failed"
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

func newJobError(code JobErrorCode, message string) *JobError {
	return &JobError{Code: code, Message: message}
}

func newStepError(stepName string, stepErr error) *JobError {
	return &JobError{Code: ErrStepFailed, StepName: stepName, StepError: stepErr}
}

func (je *JobError) Error() string {
	if je.Code == ErrStepFailed && je.StepError != nil {
		return fmt.Sprintf("step %q failed: %s", je.StepName, je.StepError.Error())
	}
	return je.Code.Error() + ": " + je.Message
}

func (je *JobError) Unwrap() error {
	if je.Code == ErrStepFailed {
		return je.StepError
	}

	return je.Code
}
