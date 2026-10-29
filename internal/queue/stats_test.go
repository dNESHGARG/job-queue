package queue

import (
 "context"
 "errors"
 "testing"
)

func TestStatsAfterDrain(t *testing.T) {
 q, err := New(context.Background(), Config{Workers:2, Capacity:4, MaxAttempts:2})
 if err != nil { t.Fatal(err) }
 attempts := 0
 if err := q.Submit(context.Background(), Job{ID:"retry", Run:func(context.Context)error{
  attempts++
  if attempts == 1 { return errors.New("retry me") }
  return nil
 }}); err != nil { t.Fatal(err) }
 if err := q.Submit(context.Background(), Job{ID:"fail", Run:func(context.Context)error{
  return errors.New("permanent")
 }}); err != nil { t.Fatal(err) }
 go q.Close()
 for range q.Results() {}
 got := q.Snapshot()
 if got.Accepted != 2 || got.Started != 2 || got.Succeeded != 1 || got.Failed != 1 || got.Retried != 2 {
  t.Fatalf("unexpected stats: %+v",got)
 }
}
