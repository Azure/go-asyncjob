package asyncjob

import (
	"time"
)

type StepExecutionData struct {
	StartTime time.Time
	Duration  time.Duration
	Retried   *RetryReport
}

type RetryReport struct {
	Count int
}
