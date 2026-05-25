local entity_key = KEYS[1]
local revision_key = KEYS[2]
local channel = KEYS[3]
local entity_type = ARGV[1]
local entity_id = ARGV[2]

local exists = redis.call('EXISTS', entity_key)

if exists == 0 then
    return 0
end

redis.call('DEL', entity_key)

local new_revision = redis.call('INCR', revision_key)

local event = {
    event_type = 'REMOVE',
    entity_type = entity_type,
    entity_id = entity_id,
    timestamp = redis.call('TIME')[1],
    revision = new_revision,
    payload = nil
}
local event_json = cjson.encode(event)

redis.call('PUBLISH', channel, event_json)

return new_revision
