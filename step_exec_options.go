package asyncjob

import (
	"context"
	"time"
)

type StepExecutionOptions struct {
	ErrorPolicy   StepErrorPolicy
	RetryPolicy   RetryPolicy
	ContextPolicy StepContextPolicy

	// dependencies that are not input.
	DependOn []string
}

type StepErrorPolicy struct{}

type RetryPolicy interface {
	// ShouldRetry returns true if the error should be retried, and the duration to wait before retrying.
	// The int parameter is the retry count
	ShouldRetry(error, uint) (bool, time.Duration)
}

// StepContextPolicy allows context enrichment before passing to step.
//
//	With StepInstanceMeta you can access StepInstance, StepDefinition, JobInstance, JobDefinition.
type StepContextPolicy func(context.Context, StepInstanceMeta) context.Context

type ExecutionOptionPreparer func(*StepExecutionOptions) *StepExecutionOptions

// Add precedence to a step.
//
//	without taking input from it(use StepAfter/StepAfterBoth otherwise)
func ExecuteAfter(step StepDefinitionMeta) ExecutionOptionPreparer {
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

func WithContextEnrichment(contextPolicy StepContextPolicy) ExecutionOptionPreparer {
	return func(options *StepExecutionOptions) *StepExecutionOptions {
		options.ContextPolicy = contextPolicy
		return options
	}
}
