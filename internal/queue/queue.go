package queue

import(
	"context"
	_ "embed"
	"time"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"errors"
)

const(
	pendingKey = "queue:emails:pending"
	processingKey = "queue:emails:processing"
)

//helper function to create jobKey
// claim.lua builds this same "job:emails:" prefix internally,
// since it doesn't know the job ID until it pops it.
func jobKey(id string)(string){
	return "job:emails:" + id
}

var ErrNoJob=errors.New("no job available to claim")

//go:embed scripts/enqueue.lua
var enqueueLua string

//go:embed scripts/claim.lua
var claimLua string

//go:embed scripts/ack.lua
var ackLua string

//go:embed scripts/reap.lua
var reapLua string

var enqueueScript=redis.NewScript(enqueueLua)
var claimScript=redis.NewScript(claimLua)
var ackScript=redis.NewScript(ackLua)
var reapScript=redis.NewScript(reapLua)

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
		pendingKey,
		jobKey(jobID),
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
		pendingKey,
		processingKey,
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
		processingKey,
		jobKey(jobID),
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

func(q *Queue) Reap(ctx context.Context,batch int)(int,error){
	keys :=[]string{
		processingKey,
		pendingKey,
	}

	result,err :=reapScript.Run(ctx,q.rdb,keys,
					time.Now().Unix(),
					batch,
				).Result()
	if err!=nil{
		return 0,err
	}
	return int(result.(int64)),nil
}