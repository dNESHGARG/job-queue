// Package queue provides a bounded in-memory worker pool.
package queue

import (
 "context"
 "errors"
 "fmt"
 "sync"
 "sync/atomic"
 "time"
)

var ErrInvalidConfig = errors.New("invalid worker configuration")
var ErrClosed = errors.New("queue is closed")
type Job struct { ID string; Run func(context.Context) error }
type Result struct { ID string; Attempts int; Err error }
type Config struct { Workers int; Capacity int; MaxAttempts int; RetryDelay time.Duration; MaxRetryDelay time.Duration }

type Stats struct { Accepted uint64; Started uint64; Succeeded uint64; Failed uint64; Retried uint64 }
type Queue struct {
 accepted atomic.Uint64
 started atomic.Uint64
 succeeded atomic.Uint64
 failed atomic.Uint64
 retried atomic.Uint64
 jobs chan Job
 slots chan struct{}
 results chan Result
 cfg Config
 ctx context.Context
 cancel context.CancelFunc
 mu sync.Mutex
 closed bool
 stopping chan struct{}
 workers sync.WaitGroup
 done chan struct{}
}
func New(parent context.Context, cfg Config) (*Queue, error) {
 if parent == nil || cfg.Workers < 1 || cfg.Capacity < 1 || cfg.MaxAttempts < 1 || cfg.RetryDelay < 0 || cfg.MaxRetryDelay < 0 { return nil, ErrInvalidConfig }
 ctx, cancel := context.WithCancel(parent)
 q := &Queue{jobs: make(chan Job, cfg.Capacity), slots: make(chan struct{}, cfg.Capacity), results: make(chan Result, cfg.Capacity), cfg: cfg, ctx: ctx, cancel: cancel, stopping: make(chan struct{}), done: make(chan struct{})}
 for i := 0; i < cfg.Capacity; i++ { q.slots <- struct{}{} }
 q.workers.Add(cfg.Workers)
 for i := 0; i < cfg.Workers; i++ { go q.worker() }
 go func() { q.workers.Wait(); close(q.results); close(q.done); q.cancel() }()
 return q, nil
}
// Submit waits for buffer capacity without holding the shutdown lock.
// The admission check and send are atomic with Close.
func (q *Queue) Submit(ctx context.Context, job Job) error {
 if ctx == nil { return errors.New("nil submit context") }
 if job.Run == nil { return errors.New("job Run must not be nil") }
 select {
 case <-ctx.Done(): return ctx.Err()
 case <-q.ctx.Done(): return q.ctx.Err()
 case <-q.stopping: return ErrClosed
 case <-q.slots:
 }
 q.mu.Lock()
 defer q.mu.Unlock()
 if q.closed { q.slots <- struct{}{}; return ErrClosed }
 if err := ctx.Err(); err != nil { q.slots <- struct{}{}; return err }
 if err := q.ctx.Err(); err != nil { q.slots <- struct{}{}; return err }
 q.jobs <- job
 q.accepted.Add(1)
 return nil
}
func (q *Queue) worker() {
 defer q.workers.Done()
 for {
  select {
  case <-q.ctx.Done(): return
  case job, ok := <-q.jobs:
   if !ok { return }
   q.slots <- struct{}{}
   q.started.Add(1)
   result := q.execute(job)
   if result.Err == nil { q.succeeded.Add(1) } else { q.failed.Add(1) }
   select { case q.results <- result: case <-q.ctx.Done(): return }
  }
 }
}
func (q *Queue) execute(job Job) Result {
 result := Result{ID: job.ID}
 for attempt := 1; attempt <= q.cfg.MaxAttempts; attempt++ {
  if err := q.ctx.Err(); err != nil { result.Err = err; return result }
  result.Attempts = attempt
  result.Err = runSafely(q.ctx, job.Run)
  if result.Err == nil { return result }
  if attempt < q.cfg.MaxAttempts { q.retried.Add(1) }
  if attempt < q.cfg.MaxAttempts && q.cfg.RetryDelay > 0 {
   timer := time.NewTimer(retryDelay(q.cfg.RetryDelay, q.cfg.MaxRetryDelay, attempt))
   select {
   case <-timer.C:
   case <-q.ctx.Done():
    if !timer.Stop() { select { case <-timer.C: default: } }
    result.Err = q.ctx.Err()
    return result
   }
  }
 }
 return result
}
func runSafely(ctx context.Context, run func(context.Context) error) (err error) {
 defer func() { if recovered := recover(); recovered != nil { err = fmt.Errorf("job panic: %v", recovered) } }()
 return run(ctx)
}
func (q *Queue) Results() <-chan Result { return q.results }
// Close prevents new submissions and drains accepted jobs. Consume Results concurrently.
func (q *Queue) Close() {
 q.mu.Lock()
 if !q.closed { q.closed = true; close(q.stopping); close(q.jobs) }
 q.mu.Unlock()
 <-q.done
}
// Cancel stops workers cooperatively; pending jobs may not produce results.
func (q *Queue) Cancel() { q.cancel() }

// retryDelay returns exponential backoff capped at max. A zero max means no
// explicit cap; duration arithmetic still saturates to avoid overflow.
func retryDelay(base, max time.Duration, failedAttempt int) time.Duration {
 if base <= 0 { return 0 }
 const maxDuration = time.Duration(1<<63 - 1)
 capDelay := maxDuration
 if max > 0 && max < capDelay { capDelay = max }
 delay := base
 if delay > capDelay { return capDelay }
 for i := 1; i < failedAttempt; i++ {
  if delay > capDelay/2 { return capDelay }
  delay *= 2
 }
 return delay
}

// Snapshot returns lock-free counters. A snapshot taken while work is active
// is not a transactionally consistent view of all counters.
func (q *Queue) Snapshot() Stats {
 return Stats{Accepted:q.accepted.Load(), Started:q.started.Load(), Succeeded:q.succeeded.Load(), Failed:q.failed.Load(), Retried:q.retried.Load()}
}
