package jobqueue

import (
 "context"
 "errors"
 "sync"
 "sync/atomic"
 "testing"
)

func TestRegistryDispatch(t *testing.T) {
 r:=NewRegistry()
 var calls atomic.Int32
 err:=r.Register("send",HandlerFunc(func(_ context.Context, job Job)error{
  if string(job.Payload)!="hello" {t.Errorf("unexpected payload")}
  calls.Add(1)
  return nil
 }))
 if err!=nil {t.Fatal(err)}
 if err:=r.Dispatch(context.Background(),Job{ID:"1",Type:"send",Payload:[]byte("hello")});err!=nil {t.Fatal(err)}
 if calls.Load()!=1 {t.Fatal("handler not called")}
}
func TestDuplicateAndUnknown(t *testing.T) {
 r:=NewRegistry()
 handler:=HandlerFunc(func(context.Context,Job)error{return nil})
 if err:=r.Register("send",handler);err!=nil {t.Fatal(err)}
 if err:=r.Register("send",handler);!errors.Is(err,ErrDuplicateHandler){t.Fatalf("got %v",err)}
 if err:=r.Dispatch(context.Background(),Job{Type:"missing"});!errors.Is(err,ErrUnknownHandler){t.Fatalf("got %v",err)}
}
func TestConcurrentDispatch(t *testing.T) {
 r:=NewRegistry()
 var count atomic.Int32
 if err:=r.Register("work",HandlerFunc(func(context.Context,Job)error{count.Add(1);return nil}));err!=nil {t.Fatal(err)}
 var wg sync.WaitGroup
 for i:=0;i<100;i++ {wg.Add(1);go func(){defer wg.Done();if err:=r.Dispatch(context.Background(),Job{Type:"work"});err!=nil {t.Error(err)}}()}
 wg.Wait()
 if count.Load()!=100 {t.Fatalf("got %d",count.Load())}
}
