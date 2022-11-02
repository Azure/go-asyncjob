package asyncjob

import (
	"time"
)

type StepExecutionOptions struct {
	Timeout     time.Duration
	ErrorPolicy StepErrorPolicy
	RetryPolicy StepRetryPolicy

	// dependencies that are not input.
	DependOn []string
}

type StepErrorPolicy struct{}

type StepRetryPolicy interface {
	SleepInterval() time.Duration
}

type ExecutionOptionPreparer func(*StepExecutionOptions) *StepExecutionOptions

func ExecuteAfter(step StepMeta) ExecutionOptionPreparer {
	return func(options *StepExecutionOptions) *StepExecutionOptions {
		options.DependOn = append(options.DependOn, step.GetName())
		return options
	}
}

func ExecuteWithRetry(retryPolicy StepRetryPolicy) ExecutionOptionPreparer {
	return func(options *StepExecutionOptions) *StepExecutionOptions {
		options.RetryPolicy = retryPolicy
		return options
	}
}

type linearRetryPolicy struct {
	sleepInterval time.Duration
}

func (lrp *linearRetryPolicy) SleepInterval() time.Duration {
	return lrp.sleepInterval
}
