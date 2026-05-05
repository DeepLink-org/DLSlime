-- Generic heartbeat: refresh TTL for any key.
-- KEYS[1] = the Redis key (engine:{id} or agent:{name})
-- ARGV[1] = TTL in seconds

local key = KEYS[1]
local ttl = tonumber(ARGV[1])

if redis.call('EXISTS', key) == 0 then
    return 0
end

redis.call('EXPIRE', key, ttl)
return 1
