"""Redis client for deduplication tracking."""

import os
import json
from typing import Optional, Dict, Any
import redis


def get_redis_client() -> Optional[redis.Redis]:
    """
    Get Redis client from REDIS_URL environment variable.

    Returns:
        Redis client if REDIS_URL is set, None otherwise
    """
    redis_url = os.getenv("REDIS_URL")

    if not redis_url:
        print("WARNING: REDIS_URL not set - deduplication disabled")
        return None

    try:
        client = redis.from_url(
            redis_url,
            decode_responses=True,  # Auto-decode to strings
            socket_connect_timeout=5,
            socket_timeout=5
        )
        # Test connection
        client.ping()
        return client
    except Exception as e:
        print(f"ERROR: Failed to connect to Redis: {e}")
        return None


def has_been_called(order_id: str) -> bool:
    """
    Check if an order has already been called for pre-pickup.

    Args:
        order_id: The order ID to check

    Returns:
        True if order was already called, False otherwise
    """
    client = get_redis_client()

    if not client:
        # If Redis is not available, allow the call (fail open)
        return False

    try:
        key = f"prepickup:{order_id}"
        exists = client.exists(key)
        return bool(exists)
    except Exception as e:
        print(f"ERROR: Redis check failed for order {order_id}: {e}")
        # Fail open - allow the call if Redis errors
        return False


def mark_as_called(order_id: str, pickup_time: str, additional_data: Optional[Dict[str, Any]] = None) -> bool:
    """
    Mark an order as called in Redis with 7-day TTL.

    Args:
        order_id: The order ID
        pickup_time: ISO format pickup time
        additional_data: Optional additional data to store

    Returns:
        True if successfully stored, False otherwise
    """
    client = get_redis_client()

    if not client:
        print(f"WARNING: Redis not available - cannot mark order {order_id} as called")
        return False

    try:
        key = f"prepickup:{order_id}"

        # Build data to store
        data = {
            "called_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            "pickup_time": pickup_time
        }

        if additional_data:
            data.update(additional_data)

        # Store with 7-day TTL (604800 seconds)
        client.setex(
            key,
            604800,  # 7 days
            json.dumps(data)
        )

        print(f"Marked order {order_id} as called in Redis (TTL: 7 days)")
        return True

    except Exception as e:
        print(f"ERROR: Failed to mark order {order_id} as called: {e}")
        return False


def get_call_data(order_id: str) -> Optional[Dict[str, Any]]:
    """
    Get stored call data for an order.

    Args:
        order_id: The order ID

    Returns:
        Dict with call data if found, None otherwise
    """
    client = get_redis_client()

    if not client:
        return None

    try:
        key = f"prepickup:{order_id}"
        data = client.get(key)

        if data:
            return json.loads(data)
        return None

    except Exception as e:
        print(f"ERROR: Failed to get call data for order {order_id}: {e}")
        return None
