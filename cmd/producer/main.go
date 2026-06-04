package main

import (
	"context"
	"fmt"
	"jobqueue/internal/queue"
	"github.com/redis/go-redis/v9"
)


func main(){
	var ctx=context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
	q :=queue.New(rdb)

	job_count := 30
	for i:=0;i<job_count;i++{
		payload := fmt.Sprintf(`{"n":%d}`, i)
		_,err :=q.Enqueue(ctx,payload)
		if err!=nil{
			panic(err)
		}
	}
}