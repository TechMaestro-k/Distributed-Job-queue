package main

import (
	"context"
	"errors"
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

	for i:=0;i<4;i++{
		jobID,err := q.Claim(ctx,30)
		if errors.Is(err,queue.ErrNoJob){
			fmt.Println("no jobs to claim")
			break
		}
		if err!=nil{
			panic(err)
		}
		fmt.Println("claimed",jobID)



		ack,err :=q.Ack(ctx,jobID)
		if err!=nil{
			panic(err)
		}
		fmt.Println("ack:",ack)
	}
}