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
type ExecutionResult = queue.Result

// Stats is a snapshot of execution counters.
type Stats = queue.Stats

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
// All consumers read from the same result stream; use one coordinated reader.
func (e *Engine) Results() <-chan ExecutionResult {return e.pool.Results()}

// Close rejects new work and waits for accepted work to finish.
func (e *Engine) Close() {e.pool.Close()}

// Cancel cooperatively cancels in-flight work.
func (e *Engine) Cancel() {e.pool.Cancel()}

// Snapshot exposes current worker pool counters.
func (e *Engine) Snapshot() Stats {return e.pool.Snapshot()}
