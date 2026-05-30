package main

import(
	"fmt"
	"context"
	"github.com/redis/go-redis/v9"
)

var ctx=context.Background()

func main(){
	rbd := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
	enqueueScript :=`
		redis.call('HSET',KEYS[2],
        		'payload',ARGV[2],
        		'state','pending',
		        'attempts',0,
		        'max_attempts',ARGV[3],
        		'enqueued_at',ARGV[4]
				)
		redis.call('LPUSH',KEYS[1],ARGV[1])
		return ARGV[1]
	`

	keys := []string{"queue:emails:pending", "job:emails:job-1"}

	result,err := rbd.Eval(ctx,enqueueScript,keys,
		"job-1",
		`{"to":"Bob}`,
		5,
		1717000000,
	).Result()
	if err!=nil{
		panic(err)
	}

	fmt.Println("enqueued-jobs",result)

}