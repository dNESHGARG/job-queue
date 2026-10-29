package queue

import (
 "context"
 "errors"
 "fmt"
 "sync"
 "time"
)

var ErrInvalidConfig = errors.New("invalid worker configuration")
var ErrClosed = errors.New("queue is closed")

type Job struct {
 ID string
 Run func(context.Context) error
}
type Result struct {
 ID string
 Attempts int
 Err error
}
type Config struct {
 Workers int
 Capacity int
 MaxAttempts int
 RetryDelay time.Duration
}
type Queue struct {
 jobs chan Job
 results chan Result
 cfg Config
 ctx context.Context
 cancel context.CancelFunc
 mu sync.RWMutex
 closed bool
 workers sync.WaitGroup
 done chan struct{}
}
func New(parent context.Context, cfg Config) (*Queue, error) {
 if cfg.Workers < 1 || cfg.Capacity < 1 || cfg.MaxAttempts < 1 || cfg.RetryDelay < 0 { return nil, ErrInvalidConfig }
 ctx, cancel := context.WithCancel(parent)
 q := &Queue{jobs: make(chan Job, cfg.Capacity), results: make(chan Result, cfg.Capacity), cfg: cfg, ctx: ctx, cancel: cancel, done: make(chan struct{})}
 q.workers.Add(cfg.Workers)
 for i:=0; i<cfg.Workers; i++ { go q.worker() }
 go func(){ q.workers.Wait(); close(q.results); close(q.done) }()
 return q,nil
}
func (q *Queue) Submit(ctx context.Context, job Job) error {
 if job.Run == nil { return errors.New("job Run must not be nil") }
 q.mu.RLock()
 defer q.mu.RUnlock()
 if q.closed { return ErrClosed }
 select {
 case <-ctx.Done(): return ctx.Err()
 case <-q.ctx.Done(): return q.ctx.Err()
 case q.jobs <- job: return nil
 }
}
func (q *Queue) worker() {
 defer q.workers.Done()
 for {
  select {
  case <-q.ctx.Done(): return
  case job, ok := <-q.jobs:
   if !ok { return }
   result := q.execute(job)
   select { case q.results <- result: case <-q.ctx.Done(): return }
  }
 }
}
func (q *Queue) execute(job Job) Result {
 result := Result{ID:job.ID}
 for i:=1; i<=q.cfg.MaxAttempts; i++ {
  if err:=q.ctx.Err(); err!=nil { result.Err=err; return result }
  result.Attempts=i
  result.Err=runSafely(q.ctx, job.Run)
  if result.Err==nil { return result }
  if i<q.cfg.MaxAttempts && q.cfg.RetryDelay>0 {
   timer:=time.NewTimer(q.cfg.RetryDelay)
   select { case <-timer.C: case <-q.ctx.Done(): if !timer.Stop() { select {case <-timer.C:default:} }; result.Err=q.ctx.Err(); return result }
  }
 }
 return result
}
func runSafely(ctx context.Context, run func(context.Context) error) (err error) {
 defer func(){ if recovered:=recover(); recovered!=nil { err=fmt.Errorf("job panic: %v", recovered) } }()
 return run(ctx)
}
func (q *Queue) Results() <-chan Result { return q.results }
func (q *Queue) Close() {
 q.mu.Lock()
 if !q.closed { q.closed=true; close(q.jobs) }
 q.mu.Unlock()
 <-q.done
}
func (q *Queue) Cancel() { q.cancel() }
