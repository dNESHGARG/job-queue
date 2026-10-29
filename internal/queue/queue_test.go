package queue

import (
 "context"
 "errors"
 "sync/atomic"
 "testing"
 "time"
)

func TestRetriesAndResult(t *testing.T) {
 q,err:=New(context.Background(),Config{Workers:2,Capacity:3,MaxAttempts:3})
 if err!=nil {t.Fatal(err)}
 var calls atomic.Int32
 if err:=q.Submit(context.Background(),Job{ID:"retry",Run:func(context.Context)error{if calls.Add(1)<3{return errors.New("transient")};return nil}});err!=nil {t.Fatal(err)}
 got:= <-q.Results()
 if got.ID!="retry" || got.Attempts!=3 || got.Err!=nil {t.Fatalf("unexpected result: %+v",got)}
 q.Close()
 if _,ok:= <-q.Results();ok {t.Fatal("results channel should be closed")}
}
func TestBoundedConcurrency(t *testing.T) {
 q,_:=New(context.Background(),Config{Workers:2,Capacity:10,MaxAttempts:1})
 var running,max atomic.Int32
 release:=make(chan struct{})
 for i:=0;i<8;i++ {
  if err:=q.Submit(context.Background(),Job{Run:func(context.Context)error{
   n:=running.Add(1);for { old:=max.Load();if n<=old || max.CompareAndSwap(old,n){break} };<-release;running.Add(-1);return nil
  }});err!=nil {t.Fatal(err)}
 }
 close(release)
 for i:=0;i<8;i++ { <-q.Results() }
 q.Close()
 if max.Load()>2 || max.Load()<1 {t.Fatalf("max workers %d",max.Load())}
}
func TestSubmitAfterClose(t *testing.T) {
 q,_:=New(context.Background(),Config{Workers:1,Capacity:1,MaxAttempts:1})
 q.Close()
 if err:=q.Submit(context.Background(),Job{Run:func(context.Context)error{return nil}});!errors.Is(err,ErrClosed){t.Fatalf("got %v",err)}
}
func TestCancelStopsRetry(t *testing.T) {
 q,_:=New(context.Background(),Config{Workers:1,Capacity:1,MaxAttempts:100,RetryDelay:time.Hour})
 started:=make(chan struct{})
 q.Submit(context.Background(),Job{Run:func(context.Context)error{close(started);return errors.New("failed")}})
 <-started
 q.Cancel()
 select {case <-q.done:case <-time.After(time.Second):t.Fatal("cancellation did not stop workers")}
}
func TestInvalidConfig(t *testing.T) {
 if _,err:=New(context.Background(),Config{Workers:0,Capacity:1,MaxAttempts:1});!errors.Is(err,ErrInvalidConfig){t.Fatal(err)}
}
func TestPanicBecomesError(t *testing.T) {
 q,_:=New(context.Background(),Config{Workers:1,Capacity:1,MaxAttempts:1})
 q.Submit(context.Background(),Job{Run:func(context.Context)error{panic("boom")}})
 if result:= <-q.Results();result.Err==nil {t.Fatal("panic should be an error")}
 q.Close()
}
