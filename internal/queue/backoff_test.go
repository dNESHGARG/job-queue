package queue

import (
 "context"
 "errors"
 "testing"
 "time"
)

func TestRetryDelayExponentialAndCapped(t *testing.T) {
 base := 10 * time.Millisecond
 cases := []struct{ attempt int; want time.Duration }{
  {1,10*time.Millisecond},{2,20*time.Millisecond},
  {3,40*time.Millisecond},{4,50*time.Millisecond},{100,50*time.Millisecond},
 }
 for _, tc := range cases {
  if got := retryDelay(base,50*time.Millisecond,tc.attempt); got != tc.want {
   t.Errorf("attempt %d: got %v want %v",tc.attempt,got,tc.want)
  }
 }
}
func TestRetryDelaySaturates(t *testing.T) {
 if got:=retryDelay(time.Duration(1<<62),0,3); got!=time.Duration(1<<63-1) {t.Fatalf("got %v",got)}
 if got:=retryDelay(0,0,4);got!=0 {t.Fatalf("got %v",got)}
}
func TestRetryExhaustion(t *testing.T) {
 q,err:=New(context.Background(),Config{Workers:1,Capacity:1,MaxAttempts:3,RetryDelay:time.Millisecond,MaxRetryDelay:2*time.Millisecond})
 if err!=nil {t.Fatal(err)}
 sentinel:=errors.New("permanent failure")
 if err:=q.Submit(context.Background(),Job{ID:"failing",Run:func(context.Context)error{return sentinel}});err!=nil {t.Fatal(err)}
 result:= <-q.Results()
 if result.Attempts!=3 || !errors.Is(result.Err,sentinel) {t.Fatalf("unexpected result: %+v",result)}
 q.Close()
}
