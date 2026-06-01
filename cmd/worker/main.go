package main

import (
	"context"
	"errors"
	"fmt"
	"jobqueue/internal/queue"
	"time"

	"github.com/redis/go-redis/v9"
)


func main(){
	var ctx=context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
	q :=queue.New(rdb)

	// payloads :=[]string{
	// 	`{"to":"alice"}`,
	// 	`{"to":"bob"}`,
	// 	`{"to":"carol"}`,
	// }

	job_count := 1000
	for i:=0;i<job_count;i++{
		payload := fmt.Sprintf(`{"n":%d}`, i)
		_,err :=q.Enqueue(ctx,payload)
		if err!=nil{
			panic(err)
		}
	}

	// for i:=0;i<4;i++{
	// 	jobID,err := q.Claim(ctx,30)
	// 	if errors.Is(err,queue.ErrNoJob){
	// 		fmt.Println("no jobs to claim")
	// 		break
	// 	}
	// 	if err!=nil{
	// 		panic(err)
	// 	}
	// 	fmt.Println("claimed",jobID)
	// 	ack,err :=q.Ack(ctx,jobID)
	// 	if err!=nil{
	// 		panic(err)
	// 	}
	// 	fmt.Println("ack:",ack)
	// }
	processed := 0
	fmt.Println("worker started")
	for{
		jobID,err := q.Claim(ctx,30)
		if errors.Is(err,queue.ErrNoJob){
			fmt.Println("queue empty, waiting for jobs, Processed",processed)
			break
		}
		if err!=nil{
			fmt.Println("claim error: ",err)
			time.Sleep(1*time.Second)
			continue
		}

		fmt.Println("worker doing the job")
		//time.Sleep(2* time.Second)

		ack,err := q.Ack(ctx,jobID)
		if err!=nil{
			fmt.Println("Ack error: ", err)
			continue
		}
		fmt.Println("done, ack:", ack)
		
		processed++;
		if processed%100 == 0{
			fmt.Println("Jobs completed: ",processed)
		}

	}
}