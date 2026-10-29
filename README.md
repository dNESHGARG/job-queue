# Job Queue

A Go worker pool with bounded concurrency, retries, cancellation, and graceful shutdown.

## Run

```sh
go run ./cmd/demo
go test -race ./...
go vet ./...
```

Uses the Go standard library. Jobs are held in memory and are not durable. Callers should consume results concurrently. v0.2 will improve shutdown coordination and retry backoff.
