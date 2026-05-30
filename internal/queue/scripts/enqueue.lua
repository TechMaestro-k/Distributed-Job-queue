--KEYS[1]=pending list key (queue:emails:pending) address of pending list
--KEYS[2] =job hash key (job:emails:<id>)
--ARGV[1]= job id
--ARGV[2]=payload real work the job is supposed to do
--ARGV[3]=max_attempts
--ARGV[4]=now (unix timestamp)


redis.call('HSET',KEYS[2],
        'payload',ARGV[2],
        'state','pending',
        'attempts',0,
        'max_attempts',ARGV[3],
        'enqueued_at',ARGV[4]
)

redis.call('LPUSH',KEYS[1],ARGV[1])

return ARGV[1]