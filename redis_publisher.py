from __future__ import annotations

import os
import redis
import json
from typing import Any

# Strict configuration: No defaults.
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
SYNC_QUEUE = os.environ['OUTBOUND_SYNC_QUEUE']

# Connection pool singleton
_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
client = redis.Redis(connection_pool=_pool)

def publish_to_sync_queue(target_table: str, row_data: dict[str, Any]) -> bool:
    """Publishes a payload to the sync queue. Raises exception if Redis fails."""
    payload = json.dumps({"target_table": target_table, "data": row_data}, default=str)
    return client.rpush(SYNC_QUEUE, payload) > 0