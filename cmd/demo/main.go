package main

import (
 "context"
 "fmt"
 "log"
 "time"
 "github.com/dNESHGARG/job-queue/internal/queue"
)

func main() {
 q,err:=queue.New(context.Background(),queue.Config{Workers:3,Capacity:10,MaxAttempts:3,RetryDelay:50*time.Millisecond})
 if err!=nil {log.Fatal(err)}
 for i:=1;i<=5;i++ {
  id:=fmt.Sprintf("job-%d",i)
  if err:=q.Submit(context.Background(),queue.Job{ID:id,Run:func(ctx context.Context)error{
   select {case <-time.After(20*time.Millisecond):return nil;case <-ctx.Done():return ctx.Err()}
  }});err!=nil {log.Fatal(err)}
 }
 go q.Close()
 for result:=range q.Results(){fmt.Printf("%s attempts=%d error=%v\n",result.ID,result.Attempts,result.Err)}
}
