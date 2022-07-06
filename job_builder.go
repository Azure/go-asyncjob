package asyncjob

import (
	"context"
)

type JobBuilder interface {
	BuildJob(context.Context) *Job
}
