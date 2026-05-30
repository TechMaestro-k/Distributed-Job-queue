package queue

import(
	"context"
	_ "embed"
	"time"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

//go:embed scripts/enqueue.lua
var enqueueLua string

var enqueueScript=redis.NewScript(enqueueLua)

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