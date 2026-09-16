package redis

// Lua scripts. Each runs atomically in Redis, which is what makes the
// reservation and sequence-guard primitives correct under concurrency.
//
// Cluster note: the cell-migration SREM/SADD in applyLocation use KEYS[3]/KEYS[4]
// (declared by the caller). In a single-node deployment (dev) this is trivially
// safe; in cluster mode the caller must ensure the old and new cell keys hash to
// the same slot or accept a cross-slot error (documented in ADR-010).

// applyLocationScript atomically applies a location update:
//
//	1. reject if the driver's last-applied sequence >= the new sequence
//	   (out-of-order / duplicate guard)
//	2. bump the sequence guard
//	3. write the position hash
//	4. migrate H3 cell membership (SREM old, SADD new)
//	5. update the global geo zset
//
// KEYS[1]=seq:{driver}  KEYS[2]=driver:{id}:loc
// KEYS[3]=h3:{oldCell}:drivers  KEYS[4]=h3:{newCell}:drivers
// KEYS[5]=geo:drivers
// ARGV[1]=newSeq  ARGV[2]=lat  ARGV[3]=lng  ARGV[4]=newCell
// ARGV[5]=tsMs  ARGV[6]=locTTL  ARGV[7]=driverID
//
// Returns 1 if applied, 0 if rejected (stale/duplicate).
const applyLocationScript = `
local last = redis.call('GET', KEYS[1])
if last and tonumber(last) >= tonumber(ARGV[1]) then
    return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', 3600)
redis.call('HSET', KEYS[2], 'lat', ARGV[2], 'lng', ARGV[3], 'cell', ARGV[4], 'seq', ARGV[1], 'ts', ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[6])
if KEYS[3] ~= KEYS[4] then
    redis.call('SREM', KEYS[3], ARGV[7])
end
redis.call('SADD', KEYS[4], ARGV[7])
redis.call('GEOADD', KEYS[5], ARGV[3], ARGV[2], ARGV[7])
return 1
`

// reserveScript atomically reserves a driver for a ride.
//
// KEYS[1]=driver:{id}:lease  KEYS[2]=driver:{id}
// ARGV[1]=rideID  ARGV[2]=leaseValue  ARGV[3]=ttlSeconds
//
// Returns:
//
//	 1  reserved (or re-entrant renewal by the same lease)
//	 0  conflict — another lease owns the driver
//	-1  driver not in a reservable state
const reserveScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
    local owner = redis.call('GET', KEYS[1])
    if owner == ARGV[2] then
        redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
        return 1
    end
    return 0
end
local state = redis.call('HGET', KEYS[2], 'state')
if state == 'RESERVED' then
    -- Stale-state recovery: a RESERVED driver with NO lease can only exist
    -- after the lease expired (its holder crashed). Self-heal: the driver is
    -- free again, so proceed with this reservation.
    state = 'AVAILABLE'
end
if state ~= 'AVAILABLE' then
    return -1
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
redis.call('HSET', KEYS[2], 'state', 'RESERVED', 'reserved_ride', ARGV[1])
return 1
`

// releaseScript is a compare-and-delete: only the holder whose lease value
// matches may release. A stale holder (old version) cannot delete a newer
// lease.
//
// KEYS[1]=driver:{id}:lease  KEYS[2]=driver:{id}
// ARGV[1]=expectedLeaseValue  ARGV[2]=targetState
//
// Returns 1 if released, 0 if the lease was not held by the caller.
const releaseScript = `
local owner = redis.call('GET', KEYS[1])
if owner == ARGV[1] then
    redis.call('DEL', KEYS[1])
    redis.call('HSET', KEYS[2], 'state', ARGV[2])
    return 1
end
return 0
`

// tokenBucketScript implements a distributed token-bucket rate limiter.
//
// KEYS[1]=ratelimit:{scope}:{id}
// ARGV[1]=capacity  ARGV[2]=refillPerSec  ARGV[3]=nowMs
// ARGV[4]=requested  ARGV[5]=ttlMs
//
// Returns 1 if the request is allowed, 0 if rate-limited.
const tokenBucketScript = `
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if tokens == nil then
    tokens = capacity
    ts = now
end
local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + (elapsed / 1000.0) * refill)
local allowed = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[5]))
return allowed
`