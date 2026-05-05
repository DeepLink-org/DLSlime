local engine_key = KEYS[1]
local revision_key = KEYS[2]
local channel = KEYS[3]
local ttl = tonumber(ARGV[10])

-- 1. Write engine data to hash
redis.call('HSET', engine_key,
    'id', ARGV[1],
    'role', ARGV[2],
    'host', ARGV[3],
    'port', ARGV[4],
    'world_size', ARGV[5],
    'num_blocks', ARGV[6],
    'peer_addrs', ARGV[7],
    'info', ARGV[8],
    'model_path', ARGV[11]
)

-- 2. Set Expiration (Heartbeat mechanism - prevents zombie nodes)
redis.call('EXPIRE', engine_key, ttl)

-- 3. Atomically increment global revision
local new_revision = redis.call('INCR', revision_key)

-- 4. Construct and publish event.
-- NOTE: embed ARGV[9] (payload JSON) as a raw string to avoid cjson
-- re-encoding empty arrays as objects (cjson encodes empty Lua tables as {}).
local event_json = '{"event_type":"ADD","engine_id":' .. cjson.encode(ARGV[1]) ..
    ',"timestamp":' .. tostring(redis.call('TIME')[1]) ..
    ',"revision":' .. tostring(new_revision) ..
    ',"payload":' .. ARGV[9] .. '}'

-- 5. Publish event atomically (within the same transaction)
redis.call('PUBLISH', channel, event_json)

return new_revision
