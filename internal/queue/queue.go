package queue

import(
	"context"
	_ "embed"
	"time"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"errors"
)

var ErrNoJob=errors.New("no job available to claim")

//go:embed scripts/enqueue.lua
var enqueueLua string

//go:embed scripts/claim.lua
var claimLua string

//go:embed scripts/ack.lua
var ackLua string

var enqueueScript=redis.NewScript(enqueueLua)
var claimScript=redis.NewScript(claimLua)
var ackScript=redis.NewScript(ackLua)

type Queue struct{
	rdb *redis.Client
}


//constructor--build a Queue containing the given client, and return a pointer to it
func New(rdb *redis.Client) *Queue{
	return &Queue{rdb:rdb}
}

func (q *Queue) Enqueue(ctx context.Context, payload string)(string, error){
	jobID := uuid.NewString()

	keys := []string{
		"queue:emails:pending",
		"job:emails:" + jobID,
	}

	result,err := enqueueScript.Run(ctx ,q.rdb,keys,
					jobID,
					payload,
					5,
					time.Now().Unix(),
	).Result()
	if err!=nil{
		return "",err
	}

	return result.(string),nil
}

func (q *Queue) Claim(ctx context.Context,timeout int)(string,error){
	keys :=[]string{
		"queue:emails:pending",
		"queue:emails:processing",
	}
	jobID,err := claimScript.Run(ctx,q.rdb,keys,
					time.Now().Unix(),
					timeout,
	).Result()
	if err==redis.Nil{
		return "",ErrNoJob
	}
	if err!=nil{
		return "",err
	}

	return jobID.(string),nil
}

func (q *Queue) Ack(ctx context.Context,jobID string)(bool,error){
	keys:= []string{
		"queue:emails:processing",
		"job:emails:"+jobID,
	}

	result,err :=ackScript.Run(ctx,q.rdb,keys,
				jobID,
				time.Now().Unix(),
	).Result()
	
	if err!=nil{
		return false,err
	}
	return result.(int64)==1,nil
}