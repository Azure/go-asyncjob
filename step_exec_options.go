package asyncjob

import (
	"context"
	"time"
)

type StepExecutionOptions struct {
	Timeout       time.Duration
	ErrorPolicy   StepErrorPolicy
	RetryPolicy   RetryPolicy
	ContextPolicy StepContextPolicy

	// dependencies that are not input.
	DependOn []string
}

type StepErrorPolicy struct{}

type RetryPolicy interface {
	ShouldRetry(error) bool
	SleepInterval() time.Duration
}

// StepContextPolicy allows context enrichment before passing to step.
type StepContextPolicy func(context.Context) context.Context

type ExecutionOptionPreparer func(*StepExecutionOptions) *StepExecutionOptions

// Add precedence to a step.
//   without taking input from it(use StepAfter/StepAfterBoth otherwise)
func ExecuteAfterV2(step StepDefinitionMeta) ExecutionOptionPreparer {
	return func(options *StepExecutionOptions) *StepExecutionOptions {
		options.DependOn = append(options.DependOn, step.GetName())
		return options
	}
}

// Add precedence to a step.
//   without taking input from it(use StepAfter/StepAfterBoth otherwise)
func ExecuteAfter(step StepMeta) ExecutionOptionPreparer {
	return func(options *StepExecutionOptions) *StepExecutionOptions {
		options.DependOn = append(options.DependOn, step.GetName())
		return options
	}
}

// Allow retry of a step on error.
func WithRetry(retryPolicy RetryPolicy) ExecutionOptionPreparer {
	return func(options *StepExecutionOptions) *StepExecutionOptions {
		options.RetryPolicy = retryPolicy
		return options
	}
}

// Limit time spend on a step.
func WithTimeout(timeout time.Duration) ExecutionOptionPreparer {
	return func(options *StepExecutionOptions) *StepExecutionOptions {
		options.Timeout = timeout
		return options
	}
}

func WithEnrichedContext(contextPolicy StepContextPolicy) ExecutionOptionPreparer {
	return func(options *StepExecutionOptions) *StepExecutionOptions {
		options.ContextPolicy = contextPolicy
		return options
	}
}
