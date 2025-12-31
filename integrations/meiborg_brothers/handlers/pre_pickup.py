"""Handler for Peach State pre-pickup notifications.

Runs hourly to find orders with pickups in the next 1-2 hours and
sends webhook to trigger pre-pickup calls.
"""

import json
import os
import re
from typing import Dict, Any, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from handlers.utils import send_webhook
from handlers.redis_client import has_been_called, mark_as_called
import requests


TIMEZONE_MAPPING = {
    "EDT": "America/New_York", "EST": "America/New_York",
    "CDT": "America/Chicago", "CST": "America/Chicago",
    "MDT": "America/Denver", "MST": "America/Denver",
    "PDT": "America/Los_Angeles", "PST": "America/Los_Angeles",
    "HDT": "Pacific/Honolulu", "HST": "Pacific/Honolulu",
    "AKDT": "America/Anchorage", "AKST": "America/Anchorage",
}


def fetch_orders_in_window(
    base_url: str,
    auth_token: str,
    token_type: str,
    company_id: str,
    hours_ahead: int = 2
) -> List[Dict[str, Any]]:
    """
    Fetch orders from McLeod with pickup in the next X hours.

    Args:
        base_url: McLeod API base URL
        auth_token: Auth token
        token_type: Auth type (Bearer, etc.)
        company_id: Company ID for header
        hours_ahead: How many hours ahead to query (default: 2)

    Returns:
        List of order objects
    """
    # Calculate time window
    now = datetime.now()
    end_time = now + timedelta(hours=hours_ahead)

    # Format as McLeod expects: YYYYMMDDHHmmss
    start_param = now.strftime('%Y%m%d%H%M%S')
    end_param = end_time.strftime('%Y%m%d%H%M%S')

    # Build query
    endpoint = f"/ws/orders/search?shipper.sched_arrive_early=>={start_param}&shipper.sched_arrive_early=<{end_param}"
    url = f"{base_url}{endpoint}"

    headers = {
        "Accept": "application/json",
        "Authorization": f"{token_type} {auth_token}",
        "X-com.mcleodsoftware.CompanyID": company_id
    }

    print(f"Querying McLeod: {url}")

    try:
        response = requests.get(url, headers=headers, timeout=90)

        if response.status_code == 200:
            data = response.json()

            # Normalize to list
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            else:
                return []
        else:
            print(f"ERROR: McLeod API returned {response.status_code}: {response.text[:200]}")
            return []

    except Exception as e:
        print(f"ERROR: Failed to fetch orders from McLeod: {e}")
        return []


def process_order(order: Dict[str, Any], webhook_url: str) -> Dict[str, Any]:
    """
    Process a single order - check Redis, send webhook if needed.

    Args:
        order: McLeod order object
        webhook_url: Webhook URL to send to

    Returns:
        Result dict with order_id, success, and details
    """
    order_id = order.get("id", "unknown")

    try:
        # Get basic order info
        stops = order.get("stops", [])
        movements = order.get("movement", [])

        if not stops:
            return {"order_id": order_id, "success": False, "reason": "no_stops"}

        if not movements:
            return {"order_id": order_id, "success": False, "reason": "no_movement"}

        first_stop = stops[0]

        # Check if already picked up
        if first_stop.get("actual_arrival"):
            return {"order_id": order_id, "success": False, "reason": "already_picked_up"}

        # Get scheduled pickup time
        sched_arrive_early = first_stop.get("sched_arrive_early")
        if not sched_arrive_early:
            return {"order_id": order_id, "success": False, "reason": "no_pickup_time"}

        # Parse pickup time
        tz_abbr = first_stop.get("__timezone")
        tz_str = TIMEZONE_MAPPING.get(tz_abbr) if tz_abbr else "America/Chicago"

        dt_part = sched_arrive_early.split("+")[0] if "+" in sched_arrive_early else sched_arrive_early.split("-")[0]
        pickup_time = datetime.strptime(dt_part, "%Y%m%d%H%M%S").replace(tzinfo=ZoneInfo(tz_str))

        # Get movement info
        movement = movements[0] if isinstance(movements, list) else movements

        # Check brokerage status - must be COVERED
        brokerage_status = movement.get("brokerage_status")
        if brokerage_status != "COVERED":
            return {"order_id": order_id, "success": False, "reason": f"status_{brokerage_status}"}

        # Get phone numbers
        driver_phone = movement.get("override_drvr_cell")
        dispatch_phone = movement.get("carrier_phone")

        # Must have at least one phone
        if not driver_phone and not dispatch_phone:
            return {"order_id": order_id, "success": False, "reason": "no_phone"}

        # Check Redis - have we already called this order?
        if has_been_called(order_id):
            return {"order_id": order_id, "success": False, "reason": "already_called"}

        # Clean phone numbers - remove all non-digit characters
        driver_phone_clean = re.sub(r'\D', '', driver_phone) if driver_phone else None
        dispatch_phone_clean = re.sub(r'\D', '', dispatch_phone) if dispatch_phone else None

        # Build webhook payload
        payload = {
            "order_id": order_id,
            "movement_id": order.get("curr_movement_id"),
            "driver_phone": driver_phone_clean,
            "dispatch_phone": dispatch_phone_clean,
            "carrier_name": movement.get("carrier_contact"),  # Use carrier_contact for full carrier name
            "carrier_tractor": movement.get("carrier_tractor"),
            "carrier_trailer": movement.get("carrier_trailer"),
            "scheduled_pickup_time": pickup_time.isoformat(),
            "pickup_location": {
                "city": first_stop.get("city_name") or first_stop.get("city"),
                "state": first_stop.get("state"),
                "zip": first_stop.get("zip_code") or first_stop.get("zip"),
                "address": first_stop.get("address")
            },
            "source": "peach_state_pre_pickup",
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        # Send webhook
        webhook_result = send_webhook(webhook_url, payload)

        # If webhook succeeded, mark as called in Redis
        if webhook_result.get("success"):
            mark_as_called(order_id, pickup_time.isoformat())

        return {
            "order_id": order_id,
            "success": webhook_result.get("success", False),
            "webhook_status": webhook_result.get("webhook_status"),
            "reason": "webhook_sent" if webhook_result.get("success") else "webhook_failed"
        }

    except Exception as e:
        print(f"ERROR processing order {order_id}: {e}")
        return {"order_id": order_id, "success": False, "reason": f"error: {str(e)}"}


def pre_pickup_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main handler for Peach State pre-pickup sync.

    Runs hourly to find orders with pickup in next 2 hours and send webhooks.
    """
    try:
        print("=" * 60)
        print("Running Peach State pre-pickup sync")
        print("=" * 60)

        # Get configuration
        base_url = os.getenv("MCLEOD_BASE_URL")
        auth_token = os.getenv("MCLEOD_AUTH_TOKEN")
        token_type = os.getenv("MCLEOD_AUTH_TYPE", "Bearer")
        company_id = os.getenv("MCLEOD_COMPANY_ID")
        webhook_url = os.getenv("PRE_PICKUP_WEBHOOK_URL")

        # Validate required config
        if not base_url or not auth_token:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "MCLEOD_BASE_URL and MCLEOD_AUTH_TOKEN required"})
            }

        if not company_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "MCLEOD_COMPANY_ID required"})
            }

        if not webhook_url:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "PRE_PICKUP_WEBHOOK_URL required"})
            }

        # Fetch orders from McLeod
        print(f"Fetching orders with pickup in next 2 hours...")
        orders = fetch_orders_in_window(base_url, auth_token, token_type, company_id, hours_ahead=2)

        print(f"Found {len(orders)} orders in time window")

        # Process each order
        results = []
        for order in orders:
            result = process_order(order, webhook_url)
            results.append(result)

        # Summarize results
        total = len(results)
        success = sum(1 for r in results if r.get("success"))
        already_called = sum(1 for r in results if r.get("reason") == "already_called")
        webhook_failed = sum(1 for r in results if r.get("reason") == "webhook_failed")
        filtered = total - success - already_called - webhook_failed

        print()
        print("=" * 60)
        print(f"SUMMARY:")
        print(f"  Total orders found: {total}")
        print(f"  Webhooks sent: {success}")
        print(f"  Already called (skipped): {already_called}")
        print(f"  Webhook failures: {webhook_failed}")
        print(f"  Filtered out: {filtered}")
        print("=" * 60)

        return {
            "statusCode": 200,
            "body": {
                "message": "Pre-pickup sync completed",
                "total_orders": total,
                "webhooks_sent": success,
                "already_called": already_called,
                "webhook_failed": webhook_failed,
                "filtered": filtered,
                "results": results
            }
        }

    except Exception as e:
        print(f"CRITICAL ERROR in pre_pickup_handler: {e}")
        import traceback
        traceback.print_exc()

        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
