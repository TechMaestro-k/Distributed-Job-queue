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

	processed := 0
	fmt.Println("worker started")
	for{
		jobID,err := q.Claim(ctx,15)
		if errors.Is(err,queue.ErrNoJob){
			fmt.Println("queue empty, waiting for jobs, Processed",processed)
			time.Sleep(2 * time.Second)
			continue
		}
		if err!=nil{
			fmt.Println("claim error: ",err)
			time.Sleep(1*time.Second)
			continue
		}

		fmt.Println("CLAIMED", jobID, "— working (8s)...")
		time.Sleep(8* time.Second)

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