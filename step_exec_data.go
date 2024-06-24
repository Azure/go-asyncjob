package asyncjob

import (
	"time"
)

// StepExecutionData would measure the step execution time and retry report.
type StepExecutionData struct {
	StartTime time.Time
	Duration  time.Duration
	Retried   *RetryReport
}

// RetryReport would record the retry count (could extend to include each retry duration, ...)
type RetryReport struct {
	Count uint
}
