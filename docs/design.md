# Queue architecture and correctness

## Ownership

- `mu` protects admission state (`closed`) and the atomic enqueue/close boundary.
- `slots` is a counting semaphore for **queued**, not running, jobs. Workers release a slot on dequeue.
- `jobs` closes exactly once during `Close`. A sender holds `mu` only while checking admission and sending into a pre-reserved slot, so that send cannot block on capacity.
- `stopping` closes as soon as admission closes; blocked submitters select on it and return `ErrClosed`.
- `results` is closed only after all workers exit.
- `ctx` broadcasts cancellation to workers and retry waits.

## Lifecycle

1. `New` creates worker goroutines and starts a completion waiter.
2. `Submit` waits for a slot without holding `mu`; after acquiring one it rechecks admission under `mu`.
3. A worker dequeues a job, returns the slot, executes it with bounded retries, and emits a result.
4. `Close` rejects future submissions and drains accepted jobs; callers must concurrently read `Results`.
5. `Cancel` aborts cooperatively; not every accepted job receives a result.

## Guarantees and non-guarantees

- At most `Workers` user functions execute simultaneously.
- Queue buffer holds at most `Capacity` jobs, excluding running jobs.
- A job accepted before graceful shutdown is eligible for execution. If its function hangs, shutdown also hangs.
- Fixed-delay retries have been replaced with exponential delay: `RetryDelay * 2^(attempt-1)`, capped by `MaxRetryDelay` when set. Zero `MaxRetryDelay` means saturation at the maximum representable duration.
- No jitter, persistence, delivery acknowledgement, metrics, or exactly-once semantics yet.
- Results backpressure is a deliberate tradeoff: if nobody consumes results, workers can block and `Close` cannot complete.

## Testing strategy

Run `go test -race ./...` and `go vet ./...`. The suite covers retry exhaustion, backoff bounds, panic recovery, worker concurrency, concurrent submit/close, and cancellation. Stress-testing and fault injection should supplement these unit tests before production use.
