// Package jobqueue defines application-independent jobs and extension points.
package jobqueue

import (
 "context"
 "errors"
 "time"
)

var ErrUnknownHandler = errors.New("unknown job handler")
var ErrDuplicateHandler = errors.New("handler already registered")
var ErrInvalidJob = errors.New("invalid job")

// Job is a serializable unit of work. Payload interpretation belongs to consumers.
type Job struct {
 ID string
 Type string
 Payload []byte
 RunAt time.Time
 MaxAttempts int
}

// Handler is implemented by applications, not the queue engine.
type Handler interface {
 Handle(context.Context, Job) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, Job) error
func (f HandlerFunc) Handle(ctx context.Context, job Job) error { return f(ctx,job) }

// Store defines a persistence boundary. Implementations must document
// atomicity, ordering, leases, and crash recovery guarantees.
type Store interface {
 Enqueue(context.Context, Job) error
 Claim(context.Context, time.Time, int) ([]Job, error)
 Complete(context.Context, string) error
 Fail(context.Context, string, error) error
}

// RetryPolicy decides whether and when a failed attempt should be retried.
type RetryPolicy interface {
 NextDelay(attempt int, err error) (time.Duration, bool)
}

// Observer receives lifecycle events. Implementations must be concurrency-safe.
type Observer interface {
 OnEvent(context.Context, Event)
}

type Event struct {
 JobID string
 JobType string
 Kind string
 Attempt int
 At time.Time
 Error string
}
