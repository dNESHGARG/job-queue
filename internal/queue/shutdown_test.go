package queue

import (
 "context"
 "errors"
 "sync"
 "testing"
 "time"
)

func TestCloseUnblocksBlockedSubmit(t *testing.T) {
 q, err := New(context.Background(), Config{Workers:1, Capacity:1, MaxAttempts:1})
 if err != nil { t.Fatal(err) }
 started := make(chan struct{})
 release := make(chan struct{})
 if err := q.Submit(context.Background(), Job{ID:"running", Run:func(context.Context)error{close(started); <-release; return nil}}); err != nil {t.Fatal(err)}
 <-started
 if err := q.Submit(context.Background(), Job{ID:"buffered", Run:func(context.Context)error{return nil}}); err != nil {t.Fatal(err)}
 submitted := make(chan error,1)
 go func(){ submitted <- q.Submit(context.Background(), Job{ID:"blocked", Run:func(context.Context)error{return nil}}) }()
 closed := make(chan struct{})
 go func(){ q.Close(); close(closed) }()
 select { case err := <-submitted: if !errors.Is(err,ErrClosed){t.Fatalf("got %v",err)}; case <-time.After(time.Second):t.Fatal("blocked submit did not unblock") }
 close(release)
 for range q.Results() {}
 select {case <-closed:case <-time.After(time.Second):t.Fatal("close did not finish")}
}
func TestConcurrentCloseAndSubmit(t *testing.T) {
 q,_:=New(context.Background(),Config{Workers:4,Capacity:8,MaxAttempts:1})
 var wg sync.WaitGroup
 for i:=0;i<30;i++ { wg.Add(1); go func(){defer wg.Done(); ctx,cancel:=context.WithTimeout(context.Background(),100*time.Millisecond);defer cancel();_ = q.Submit(ctx,Job{Run:func(context.Context)error{return nil}})}() }
 drained:=make(chan struct{})
 go func(){for range q.Results(){};close(drained)}()
 q.Close()
 wg.Wait()
 <-drained
 q.Close()
}
func TestCancelUnblocksWaitingSubmit(t *testing.T) {
 q,_:=New(context.Background(),Config{Workers:1,Capacity:1,MaxAttempts:1})
 started:=make(chan struct{})
 q.Submit(context.Background(),Job{Run:func(ctx context.Context)error{close(started);<-ctx.Done();return ctx.Err()}})
 <-started
 q.Submit(context.Background(),Job{Run:func(context.Context)error{return nil}})
 result:=make(chan error,1)
 go func(){result<-q.Submit(context.Background(),Job{Run:func(context.Context)error{return nil}})}()
 q.Cancel()
 select {case err:=<-result:if !errors.Is(err,context.Canceled){t.Fatalf("got %v",err)};case <-time.After(time.Second):t.Fatal("blocked submit not canceled")}
 q.Close()
}
