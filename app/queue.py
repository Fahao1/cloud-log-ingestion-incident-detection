"""One stream, one persistence consumer group. Never trim unprocessed entries.

ensure_group is adapted from log-observability's app/bus.py (MIT).
See THIRD_PARTY.md and licenses/log-observability-MIT.txt.
"""

import redis

from app.config import Settings

# Atomic capacity check, append, and accepted counter. Redis supplies the receipt clock.
ENQUEUE = """
if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[1]) then return {} end
local t = redis.call('TIME')
local accepted = t[1] .. '.' .. string.format('%06d', tonumber(t[2]))
local id = redis.call('XADD', KEYS[1], '*', 'data', ARGV[2], 'accepted_at', accepted)
redis.call('HINCRBY', KEYS[2], 'accepted_events_total', 1)
return {id, accepted}
"""

# Ownership check fences stale workers; DLQ append / acknowledgment / deletion are atomic.
FINISH = """
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #p == 0 or p[1][2] ~= ARGV[3] then return 0 end
if ARGV[4] == 'dead' then
  redis.call('XADD', KEYS[3], '*', 'source_id', ARGV[2], 'data', ARGV[5],
    'reason', ARGV[6], 'attempts', p[1][4], 'failed_at', redis.call('TIME')[1])
  redis.call('HINCRBY', KEYS[2], 'dead_lettered_events_total', 1)
else
  redis.call('HINCRBY', KEYS[2], 'processed_messages_total', 1)
  if ARGV[4] == 'duplicate' then
    redis.call('HINCRBY', KEYS[2], 'duplicate_deliveries_total', 1)
  end
  local t = redis.call('TIME')
  local delay = math.max(0, tonumber(t[1]) + tonumber(t[2])/1000000 - tonumber(ARGV[7]))
  redis.call('HINCRBYFLOAT', KEYS[2], 'processing_delay_seconds_sum', delay)
  redis.call('HINCRBY', KEYS[2], 'processing_delay_seconds_count', 1)
  for _, b in ipairs({0.01, 0.05, 0.1, 0.5, 1, 5, 30, 60}) do
    if delay <= b then redis.call('HINCRBY', KEYS[2], 'delay_le_' .. tostring(b), 1) end
  end
end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
redis.call('HDEL', KEYS[4], ARGV[2])
return 1
"""


class Queue:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.r = redis.Redis.from_url(
            settings.redis_url, decode_responses=True, socket_timeout=3, socket_connect_timeout=2
        )

    def ensure_group(self):
        try:
            self.r.xgroup_create(self.settings.stream, self.settings.group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def publish(self, event):
        s = self.settings
        return self.r.eval(
            ENQUEUE, 2, s.stream, s.metrics_key, s.queue_capacity, event.model_dump_json()
        )

    def finish(self, consumer, message_id, fields, outcome, reason=""):
        s = self.settings
        return self.r.eval(
            FINISH,
            4,
            s.stream,
            s.metrics_key,
            s.dead_stream,
            f"{s.stream}:failures",
            s.group,
            message_id,
            consumer,
            outcome,
            fields.get("data", ""),
            reason,
            fields.get("accepted_at", "0"),
        )
