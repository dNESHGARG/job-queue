package jobqueue

import (
 "context"
 "errors"
 "testing"
)

func TestEngineExecutesRegisteredHandler(t *testing.T) {
 registry:=NewRegistry()
 observed:=make(chan string,1)
 if err:=registry.Register("transform",HandlerFunc(func(_ context.Context,j Job)error{
  observed<-string(j.Payload)
  return nil
 }));err!=nil {t.Fatal(err)}
 engine,err:=NewEngine(context.Background(),registry,EngineConfig{Workers:1,Capacity:2,MaxAttempts:1})
 if err!=nil {t.Fatal(err)}
 payload:=[]byte("original")
 if err:=engine.Submit(context.Background(),Job{ID:"1",Type:"transform",Payload:payload});err!=nil {t.Fatal(err)}
 payload[0]='X'
 result:= <-engine.Results()
 if result.ID!="1" || result.Err!=nil {t.Fatalf("unexpected result: %+v",result)}
 if got:= <-observed;got!="original" {t.Fatalf("payload mutated: %q",got)}
 engine.Close()
}
func TestEngineReportsUnknownHandler(t *testing.T) {
 engine,err:=NewEngine(context.Background(),NewRegistry(),EngineConfig{Workers:1,Capacity:1,MaxAttempts:1})
 if err!=nil {t.Fatal(err)}
 if err:=engine.Submit(context.Background(),Job{ID:"missing-1",Type:"missing"});err!=nil {t.Fatal(err)}
 result:= <-engine.Results()
 if !errors.Is(result.Err,ErrUnknownHandler){t.Fatalf("got %v",result.Err)}
 engine.Close()
}
func TestEngineRejectsInvalidJob(t *testing.T) {
 engine,err:=NewEngine(context.Background(),NewRegistry(),EngineConfig{Workers:1,Capacity:1,MaxAttempts:1})
 if err!=nil {t.Fatal(err)}
 if err:=engine.Submit(context.Background(),Job{Type:"valid"});!errors.Is(err,ErrInvalidJob){t.Fatalf("got %v",err)}
 engine.Close()
}
