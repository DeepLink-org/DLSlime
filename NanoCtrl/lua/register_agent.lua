-- Atomically register a peer agent hash and set its TTL.
--
-- KEYS[1] = scoped agent key, e.g. "{scope}:agent:{name}" or "agent:{name}"
-- ARGV[1] = TTL seconds
-- ARGV[2] = "1" to reject an existing key, "0" to allow upsert
-- ARGV[3..] = alternating hash field/value pairs

local key = KEYS[1]
local ttl = tonumber(ARGV[1])
local reject_existing = ARGV[2] == "1"

if reject_existing and redis.call("EXISTS", key) == 1 then
    return 0
end

for i = 3, #ARGV, 2 do
    redis.call("HSET", key, ARGV[i], ARGV[i + 1])
end

redis.call("EXPIRE", key, ttl)
return 1
