package jobqueue

import (
 "fmt"
 "sync"
)

// Registry maps job types to consumer-defined handlers.
type Registry struct {
 mu sync.RWMutex
 handlers map[string]Handler
}
func NewRegistry() *Registry { return &Registry{handlers:make(map[string]Handler)} }
func (r *Registry) Register(jobType string, handler Handler) error {
 if jobType == "" || handler == nil { return ErrInvalidJob }
 r.mu.Lock()
 defer r.mu.Unlock()
 if _,exists:=r.handlers[jobType];exists {return fmt.Errorf("%w: %s",ErrDuplicateHandler,jobType)}
 r.handlers[jobType]=handler
 return nil
}
func (r *Registry) Dispatch(ctx context.Context, job Job) error {
 if job.Type == "" {return ErrInvalidJob}
 r.mu.RLock()
 handler,ok:=r.handlers[job.Type]
 r.mu.RUnlock()
 if !ok {return fmt.Errorf("%w: %s",ErrUnknownHandler,job.Type)}
 return handler.Handle(ctx,job)
}
