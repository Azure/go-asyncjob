package asyncjob

import (
	"errors"
	"fmt"
)

type JobErrorCode string

const (
	ErrPrecedentStepFailed JobErrorCode = "PrecedentStepFailed"
	ErrStepFailed          JobErrorCode = "StepFailed"

	ErrRefStepNotInJob JobErrorCode = "RefStepNotInJob"
	MsgRefStepNotInJob string       = "trying to reference to step %q, but it is not registered in job"

	ErrAddStepInSealedJob JobErrorCode = "AddStepInSealedJob"
	MsgAddStepInSealedJob string       = "trying to add step %q to a sealed job definition"

	ErrAddExistingStep JobErrorCode = "AddExistingStep"
	MsgAddExistingStep string       = "trying to add step %q to job definition, but it already exists"

	ErrDuplicateInputParentStep JobErrorCode = "DuplicateInputParentStep"
	MsgDuplicateInputParentStep string       = "at least 2 input parentSteps are same"

	ErrRuntimeStepNotFound JobErrorCode = "RuntimeStepNotFound"
	MsgRuntimeStepNotFound string       = "runtime step %q not found, must be a bug in asyncjob"
)

func (code JobErrorCode) Error() string {
	return string(code)
}

func (code JobErrorCode) WithMessage(msg string) *MessageError {
	return &MessageError{Code: code, Message: msg}
}

type MessageError struct {
	Code    JobErrorCode
	Message string
}

func (me *MessageError) Error() string {
	return me.Code.Error() + ": " + me.Message
}

func (me *MessageError) Unwrap() error {
	return me.Code
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
	if je.Code == ErrPrecedentStepFailed {
		precedentStepErr := &JobError{}
		if !errors.As(je.StepError, &precedentStepErr) {
			return je.StepError
		}
		return precedentStepErr.RootCause()
	}

	// no idea
	return je
}
