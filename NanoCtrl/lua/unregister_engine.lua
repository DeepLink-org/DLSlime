local engine_key = KEYS[1]
local revision_key = KEYS[2]
local channel = KEYS[3]
local engine_id = ARGV[1]

-- 1. Check if engine exists
local exists = redis.call('EXISTS', engine_key)

if exists == 0 then
    return 0
end

-- 2. Delete engine
redis.call('DEL', engine_key)

-- 3. Atomically increment global revision
local new_revision = redis.call('INCR', revision_key)

-- 4. Construct and publish REMOVE event
local event = {
    event_type = 'REMOVE',
    engine_id = engine_id,
    timestamp = redis.call('TIME')[1],  -- Server time
    revision = new_revision,
    payload = nil
}
local event_json = cjson.encode(event)

-- 5. Publish event atomically
redis.call('PUBLISH', channel, event_json)

return new_revision
