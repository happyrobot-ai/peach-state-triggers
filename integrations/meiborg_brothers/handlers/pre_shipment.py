"""Handler for pre-shipment load notifications (~2 hours before pickup)."""

import json
import os
from typing import Dict, Any, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from handlers.utils import fetch_orders, normalize_response_to_list, send_webhook


TIMEZONE_MAPPING = {
    "EDT": "America/New_York", "EST": "America/New_York",
    "CDT": "America/Chicago", "CST": "America/Chicago",
    "MDT": "America/Denver", "MST": "America/Denver",
    "PDT": "America/Los_Angeles", "PST": "America/Los_Angeles",
    "HDT": "Pacific/Honolulu", "HST": "Pacific/Honolulu",
    "AKDT": "America/Anchorage", "AKST": "America/Anchorage",
}


def process_pre_shipment_orders(orders: List[Dict], webhook_url: str, prefix: str = "") -> List[Dict]:
    """Process orders for pre-shipment notifications (~2 hours before pickup)."""
    results = []
    
    for order in orders:
        try:
            order_id = order.get("id", "unknown")
            movement_id = order.get("curr_movement_id")
            stops = order.get("stops", [])
            
            if not stops:
                print(f"{prefix}Order {order_id} - No stops, skipping")
                continue
            
            first_stop = stops[0]
            
            # Check if already picked up
            if first_stop.get("actual_arrival"):
                print(f"{prefix}Order {order_id} - Already picked up, skipping")
                continue
            
            # Get scheduled pickup time
            sched_arrive_early = first_stop.get("sched_arrive_early")
            if not sched_arrive_early:
                print(f"{prefix}Order {order_id} - No scheduled pickup time, skipping")
                continue
            
            # Parse time and calculate hours until pickup
            tz_abbr = first_stop.get("__timezone")
            tz_str = TIMEZONE_MAPPING.get(tz_abbr) if tz_abbr else None
            
            if not tz_str:
                print(f"{prefix}Order {order_id} - Unknown timezone '{tz_abbr}', skipping")
                continue
            
            dt_part = sched_arrive_early.split("+")[0] if "+" in sched_arrive_early else sched_arrive_early.split("-")[0]
            arrive_time = datetime.strptime(dt_part, "%Y%m%d%H%M%S").replace(tzinfo=ZoneInfo(tz_str))
            
            # Get movement details and validate requirements
            movements = order.get("movement", [])
            if not movements:
                print(f"{prefix}Order {order_id} - No movement data, skipping")
                continue
            
            # Find current movement
            current_movement = next((m for m in movements if m.get("id") == movement_id), None)
            if not current_movement:
                print(f"{prefix}Order {order_id} - Current movement not found, skipping")
                continue
            
            # Check brokerage status
            brokerage_status = current_movement.get("brokerage_status")
            if brokerage_status != "BOOKED":
                print(f"{prefix}Order {order_id} - Brokerage status '{brokerage_status}' is not BOOKED, skipping")
                continue
            
            # Get phone numbers
            driver_phone = current_movement.get("override_drvr_cell")
            dispatch_phone = current_movement.get("carrier_phone")
            
            # At least one phone must exist
            if not driver_phone and not dispatch_phone:
                print(f"{prefix}Order {order_id} - Missing both phone numbers (driver: {driver_phone}, dispatch: {dispatch_phone}), skipping")
                continue
            
            # Get additional fields
            carrier_tractor = current_movement.get("carrier_tractor")
            carrier_trailer = current_movement.get("carrier_trailer")
            total_stops = len(stops)
            
            # Calculate both call times
            two_hours_before = arrive_time - timedelta(hours=2)
            thirty_minutes_before = arrive_time - timedelta(minutes=30)
            now_local = datetime.now(ZoneInfo(tz_str))
            seconds_until_2h = int((two_hours_before - now_local).total_seconds())
            seconds_until_30m = int((thirty_minutes_before - now_local).total_seconds())
            
            print(f"{prefix}Order {order_id} - Sending webhooks (2h: {seconds_until_2h}s, 30m: {seconds_until_30m}s)")
            
            # Base payload for both calls
            base_payload = {
                "order_id": order_id,
                "movement_id": movement_id,
                "carrier_phone": driver_phone,
                "dispatch_phone": dispatch_phone,
                "total_stops": total_stops,
                "carrier_tractor": carrier_tractor,
                "carrier_trailer": carrier_trailer,
                "scheduled_pickup_time": arrive_time.isoformat(),
                "source": "meiborg_load_sync_pre_shipment",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
            
            # Send 2-hour webhook
            payload_2h = {
                **base_payload,
                "call_type": "2_hour_before",
                "seconds_until_call": seconds_until_2h,
                "scheduled_call_time": two_hours_before.isoformat()
            }
            result_2h = send_webhook(webhook_url, payload_2h, f"{prefix}[2H]")
            results.append(result_2h)
            
            # Send 30-minute webhook
            payload_30m = {
                **base_payload,
                "call_type": "30_minute_before",
                "seconds_until_call": seconds_until_30m,
                "scheduled_call_time": thirty_minutes_before.isoformat()
            }
            result_30m = send_webhook(webhook_url, payload_30m, f"{prefix}[30M]")
            results.append(result_30m)
            
        except Exception as e:
            print(f"{prefix}Error processing order {order.get('id', 'unknown')}: {e}")
            results.append({"order_id": order.get("id", "unknown"), "error": str(e), "success": False})
    
    return results


def pre_shipment_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handler for pre-shipment load sync (~2 hours before pickup)."""
    try:
        print("Running Meiborg Brothers pre-shipment load sync (2 hours before pickup)")
        
        # Get config (using Railway naming convention)
        base_url = os.getenv("MCLEOD_BASE_URL")
        auth_token = os.getenv("MCLEOD_AUTH_TOKEN")
        token_type = os.getenv("MCLEOD_AUTH_TYPE", "Bearer")
        webhook_url = os.getenv("PRE_SHIPMENT_WEBHOOK_URL")  # HappyRobot pre-shipment workflow hook
        
        if not base_url or not auth_token:
            return {"statusCode": 400, "body": json.dumps({"error": "MCLEOD_BASE_URL and MCLEOD_AUTH_TOKEN required"})}
        
        if not webhook_url:
            return {"statusCode": 400, "body": json.dumps({"error": "PRE_SHIPMENT_WEBHOOK_URL required"})}
        
        # Calculate time window (now to 24 hours from now)
        pacific_tz = ZoneInfo("America/Los_Angeles")
        now = datetime.now(pacific_tz)
        start_time = now
        end_time = now + timedelta(hours=24)
        
        # Format time parameters
        def format_time_param(dt, current_date):
            if dt.date() == current_date:
                return f"t {dt.strftime('%H%M')}"
            else:
                return f"t1 {dt.strftime('%H%M')}"
        
        start_param = format_time_param(start_time, now.date())
        end_param = format_time_param(end_time, now.date())
        
        print(f"Searching for orders scheduled {start_param} to {end_param}")
        
        # Process production
        results = []
        orders = []
        production_error = None
        
        if webhook_url and webhook_url != "N/A":
            try:
                resp = fetch_orders(base_url, auth_token, token_type, start_param, end_param)
                if resp.status_code == 200:
                    orders = normalize_response_to_list(resp.json())
                    print(f"Found {len(orders)} orders")
                    results = process_pre_shipment_orders(orders, webhook_url)
                else:
                    print(f"API request failed with status {resp.status_code}: {resp.text}")
                    production_error = f"API returned status {resp.status_code}"
            except Exception as e:
                print(f"Error fetching production orders: {e}")
                production_error = str(e)
        
        # Process TRN (staging environment)
        trn_results = []
        trn_orders = []
        trn_error = None
        trn_base_url = os.getenv("TRN_MCLEOD_BASE_URL")
        trn_webhook_url = os.getenv("TRN_PRE_SHIPMENT_WEBHOOK_URL")
        
        if trn_base_url and trn_webhook_url:
            try:
                trn_resp = fetch_orders(trn_base_url, os.getenv("TRN_MCLEOD_AUTH_TOKEN"), os.getenv("TRN_MCLEOD_AUTH_TYPE", "Bearer"), start_param, end_param, "TRN - ")
                if trn_resp.status_code == 200:
                    trn_orders = normalize_response_to_list(trn_resp.json())
                    print(f"TRN - Found {len(trn_orders)} orders")
                    trn_results = process_pre_shipment_orders(trn_orders, trn_webhook_url, "TRN - ")
                else:
                    print(f"TRN - API request failed with status {trn_resp.status_code}: {trn_resp.text}")
                    trn_error = f"API returned status {trn_resp.status_code}"
            except Exception as e:
                print(f"TRN - Error fetching orders: {e}")
                trn_error = str(e)
        
        response_body = {
            "message": "Success!",
            "orders_found": len(orders),
            "webhooks_sent": len(results),
            "webhook_results": results,
            "trn_orders_found": len(trn_orders),
            "trn_webhooks_sent": len(trn_results),
            "trn_webhook_results": trn_results,
        }
        
        if production_error:
            response_body["production_error"] = production_error
        if trn_error:
            response_body["trn_error"] = trn_error
        
        return {
            "statusCode": 200,
            "body": response_body,
        }
    
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
