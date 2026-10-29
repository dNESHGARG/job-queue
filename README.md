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

**Important:** These public extension contracts are currently a foundation, not yet integrated with the legacy in-memory worker pool in `internal/queue`. The `Store` interface does not by itself provide durable execution, scheduling, or recovery. Those features require implementations and integration, and will be developed with tests before any production-readiness claim.
