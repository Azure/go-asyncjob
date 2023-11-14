package asyncjob

import (
	"time"
)

// internal retryer to execute RetryPolicy interface
type retryer[T any] struct {
	retryPolicy RetryPolicy
	retryReport *RetryReport
	function    func() (T, error)
}

func newRetryer[T any](policy RetryPolicy, report *RetryReport, toRetry func() (T, error)) *retryer[T] {
	return &retryer[T]{retryPolicy: policy, retryReport: report, function: toRetry}
}

func (r retryer[T]) Run() (T, error) {
	t, err := r.function()
	for err != nil {
		if shouldRetry, duration := r.retryPolicy.ShouldRetry(err); shouldRetry {
			r.retryReport.Count++
			time.Sleep(duration)
			t, err = r.function()
		} else {
			break
		}
	}

	return t, err
}
