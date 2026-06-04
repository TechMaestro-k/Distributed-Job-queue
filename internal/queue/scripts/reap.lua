--keys[1]=processing zset
--keys[2]=pending list
--argv[1]=now
--argv[2]=batch limit

local expired=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'LIMIT',0,ARGV[2])

for _,job_id in ipairs(expired) do
    redis.call('ZREM',KEYS[1],job_id)
    redis.call('LPUSH',KEYS[2],job_id)
    redis.call('HSET','job:emails:'..job_id,'state','pending')
end

return #expired