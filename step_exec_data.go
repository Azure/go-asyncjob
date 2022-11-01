package asyncjob

import (
	"time"
)

type StepExecutionData struct {
	StartTime time.Time
	Duration  time.Duration
}
