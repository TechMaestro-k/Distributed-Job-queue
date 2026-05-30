package main

import(
	"fmt"
	"context"
	"jobqueue/internal/queue"
	"github.com/redis/go-redis/v9"
)


func main(){
	var ctx=context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
	q :=queue.New(rdb)

	payloads :=[]string{
		`{"to":"alice"}`,
		`{"to":"bob"}`,
		`{"to":"carol"}`,
	}

	for _,p :=range payloads{
		jobID,err :=q.Enqueue(ctx,p)
		if err!=nil{
			panic(err)
		}
		fmt.Println(jobID)
	}
}