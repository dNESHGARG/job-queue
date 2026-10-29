# Job Queue

A bounded in-memory Go worker pool with retries, cancellation, and graceful shutdown. No third-party dependencies.

## Run

```sh
go run ./cmd/demo
go test -race ./...
go vet ./...
```

## Design

- `Workers` controls concurrent job execution.
- `Capacity` limits the buffered backlog. A slot is acquired before enqueueing and returned when a worker dequeues the job.
- `Submit(ctx, job)` waits for capacity without holding the shutdown mutex.
- `Close()` stops admissions, unblocks waiting submitters, and drains accepted work.
- `Cancel()` signals cooperative cancellation; queued work can be abandoned.
- `Results()` closes when workers exit. **Consume results concurrently**, or workers can block on the bounded result channel.
- Retries use exponential backoff with optional `MaxRetryDelay` cap. Panics are converted into job errors.

## Shutdown contract

`Close` is idempotent and safe to call concurrently with `Submit`. Once shutdown is signaled, submissions waiting for capacity return `ErrClosed`. A submission already admitted before shutdown may complete. Calling `Close` while result consumers are stopped can still block; this is an explicit API constraint.

## Limitations and roadmap

Jobs and results exist only in memory. There is no durable storage, distributed coordination, or exactly-once execution guarantee. Future work: explicit result draining policy, exponential backoff with jitter, persistent job IDs, metrics, and dead-letter handling.

See [architecture and correctness](docs/design.md) for synchronization invariants and operational limitations.

## Extensibility API (in progress)

The new `pkg/jobqueue` package defines application-neutral `Job`, `Handler`, `Registry`, `Store`, `RetryPolicy`, and `Observer` contracts.

```go
registry := jobqueue.NewRegistry()
_ = registry.Register("document.transform", jobqueue.HandlerFunc(
    func(ctx context.Context, job jobqueue.Job) error {
        // Application-specific work belongs here.
        return nil
    },
))
err := registry.Dispatch(ctx, jobqueue.Job{
    ID: "task-1", Type: "document.transform", Payload: []byte("input"),
})
```

Run `go run ./examples/handlers` for a complete standalone example.

**Integration status:** The public `Engine` now dispatches registered handlers through the bounded in-memory worker pool. The `Store` interface is not yet connected to execution; it does not provide durability, scheduling, or crash recovery by itself.

## Executing registered jobs

```go
registry := jobqueue.NewRegistry()
_ = registry.Register("report.generate", jobqueue.HandlerFunc(
    func(ctx context.Context, job jobqueue.Job) error {
        return generateReport(ctx, job.Payload)
    },
))
engine, err := jobqueue.NewEngine(ctx, registry, jobqueue.EngineConfig{
    Workers: 4, Capacity: 100, MaxAttempts: 3,
})
if err != nil { return err }

// Consume results concurrently, before shutdown.
go func() {
    for result := range engine.Results() {
        log.Printf("job=%s attempts=%d error=%v", result.ID, result.Attempts, result.Err)
    }
}()
err = engine.Submit(ctx, jobqueue.Job{
    ID: "report-123", Type: "report.generate", Payload: []byte("input"),
})
// On application shutdown: engine.Close()
```

The execution API is still evolving. Scheduled execution, persistent claims, and lease recovery are not implemented yet.
