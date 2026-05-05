local entity_key = KEYS[1]
local revision_key = KEYS[2]
local channel = KEYS[3]
local ttl = tonumber(ARGV[8])

local entity_type = ARGV[1]
local entity_id = ARGV[2]
local kind = ARGV[3]
local endpoint = ARGV[4]
local metadata = ARGV[5]
local resource = ARGV[6]
local info = ARGV[7]

redis.call('HSET', entity_key,
    'entity_type', entity_type,
    'entity_id', entity_id,
    'kind', kind,
    'endpoint', endpoint,
    'metadata', metadata,
    'resource', resource,
    'info', info
)

redis.call('EXPIRE', entity_key, ttl)

local new_revision = redis.call('INCR', revision_key)

local event_json = '{"event_type":"ADD","entity_type":' .. cjson.encode(entity_type) ..
    ',"entity_id":' .. cjson.encode(entity_id) ..
    ',"kind":' .. cjson.encode(kind) ..
    ',"timestamp":' .. tostring(redis.call('TIME')[1]) ..
    ',"revision":' .. tostring(new_revision) ..
    ',"payload":' .. info .. '}'

redis.call('PUBLISH', channel, event_json)

return new_revision
