--keys[1]=processing zset
--keys[2]=jobs hash
--argv[1]=job id
--argv[2]=timestamp

local removed=redis.call("ZREM",KEYS[1],ARGV[1])
if removed==0 then return 0 end

redis.call('HSET',KEYS[2],
            'state','succeeded',
            'finished_at',ARGV[2]
)

return 1