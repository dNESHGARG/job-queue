package main

import (
 "context"
 "fmt"
 "log"
 "github.com/dNESHGARG/job-queue/pkg/jobqueue"
)

func main() {
 registry:=jobqueue.NewRegistry()
 err:=registry.Register("document.transform",jobqueue.HandlerFunc(func(_ context.Context, job jobqueue.Job)error{
  fmt.Printf("transform request: %s\n",string(job.Payload))
  return nil
 }))
 if err!=nil {log.Fatal(err)}
 if err:=registry.Dispatch(context.Background(),jobqueue.Job{ID:"example-1",Type:"document.transform",Payload:[]byte("sample.txt")});err!=nil {log.Fatal(err)}
}
