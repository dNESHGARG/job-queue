package jobqueue

import (
 "context"
 "errors"

 "github.com/dNESHGARG/job-queue/internal/queue"
)

// Engine executes registered application handlers using a bounded worker pool.
// It is in-memory only; use a durable backend before relying on restart recovery.
type Engine struct {
 pool *queue.Queue
 registry *Registry
}

// EngineConfig controls execution and retry behavior.
type EngineConfig struct {
 Workers int
 Capacity int
 MaxAttempts int
}

// ExecutionResult describes a completed or failed job.
type ExecutionResult struct {
 ID string
 Attempts int
 Err error
}

func NewEngine(ctx context.Context, registry *Registry, cfg EngineConfig) (*Engine,error) {
 if registry==nil {return nil,errors.New("nil handler registry")}
 pool,err:=queue.New(ctx,queue.Config{Workers:cfg.Workers,Capacity:cfg.Capacity,MaxAttempts:cfg.MaxAttempts})
 if err!=nil {return nil,err}
 return &Engine{pool:pool,registry:registry},nil
}

// Submit enqueues an application-neutral job. The job is copied before admission.
func (e *Engine) Submit(ctx context.Context, job Job) error {
 if job.ID=="" || job.Type=="" {return ErrInvalidJob}
 payload:=append([]byte(nil),job.Payload...)
 job.Payload=payload
 return e.pool.Submit(ctx,queue.Job{ID:job.ID,Run:func(ctx context.Context)error{
  return e.registry.Dispatch(ctx,job)
 }})
}

// Results must be consumed concurrently with execution and shutdown.
// Callers receive a fresh channel with the same bounded backpressure behavior
// as the underlying pool. Only call Results once per Engine.
func (e *Engine) Results() <-chan queue.Result {return e.pool.Results()}

// Close rejects new work and waits for accepted work to finish.
func (e *Engine) Close() {e.pool.Close()}

// Cancel cooperatively cancels in-flight work.
func (e *Engine) Cancel() {e.pool.Cancel()}

// Snapshot exposes current worker pool counters.
func (e *Engine) Snapshot() queue.Stats {return e.pool.Snapshot()}
