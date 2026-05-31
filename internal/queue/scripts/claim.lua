--keys[1] = pending list
--keys[2]= processing zset
--argv[1]=timestamp(now)
--argv[2]=timeout

local job_id=redis.call('RPOP',KEYS[1])
if not job_id then
    return nil
end

local expiry =tonumber(ARGV[1])+tonumber(ARGV[2])
redis.call('ZADD',KEYS[2],expiry,job_id)
redis.call('HSET','job:emails:'..job_id,
            'state','processing',
            'claimed_at',ARGV[1]
)

return job_id