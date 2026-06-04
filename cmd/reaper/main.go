package main

import(
	"context"
	"fmt"
	"time"
	"jobqueue/internal/queue"
	"github.com/redis/go-redis/v9"
)
func main(){
	var ctx = context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	q := queue.New(rdb)

	fmt.Println("Reaper started")
	for{
		n,err := q.Reap(ctx,100)
		if err!=nil{
			fmt.Println(err)
		}
		if n>0{
			fmt.Println("number of jobs requeued",n)
		}
		time.Sleep(5*time.Second)
	}
}