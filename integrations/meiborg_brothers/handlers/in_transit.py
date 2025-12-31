"""Handler for in-transit load check-ins (morning and afternoon calls)."""

import json
import os
from typing import Dict, Any, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from handlers.utils import fetch_orders, normalize_response_to_list, send_webhook


def is_in_transit(order: Dict) -> bool:
    """Check if order is in transit (picked up but not delivered)."""
    stops = order.get("stops", [])
    
    # Need at least 2 stops
    if len(stops) < 2:
        return False
    
    first_stop = stops[0]
    last_stop = stops[-1]
    
    # Must have pickup, no delivery
    has_pickup = bool(first_stop.get("actual_arrival"))
    has_delivery = bool(last_stop.get("actual_arrival"))
    
    return has_pickup and not has_delivery


def passes_brokerage_status_filter(order: Dict) -> bool:
    """Check if order has brokerage_status = TRANSIT."""
    movements = order.get("movement", [])
    
    if not movements:
        return False
    
    # Check first movement's brokerage_status
    movement = movements[0] if isinstance(movements, list) else movements
    brokerage_status = movement.get("brokerage_status")
    
    return brokerage_status == "TRANSIT"


def process_in_transit_orders(
    orders: List[Dict],
    webhook_url: str,
    call_type: str,
    call_time: datetime,
    prefix: str = ""
) -> List[Dict]:
    """
    Process orders for in-transit check-ins.
    
    Filters:
    - Must be in transit (picked up, not delivered)
    - Must have brokerage_status = TRANSIT
    """
    results = []
    
    for order in orders:
        try:
            order_id = order.get("id", "unknown")
            
            # Filter 1: In-transit status
            if not is_in_transit(order):
                print(f"{prefix}Order {order_id} - Not in transit, skipping")
                continue
            
            # Filter 2: Brokerage status = TRANSIT
            if not passes_brokerage_status_filter(order):
                brokerage_status = order.get("movement", [{}])[0].get("brokerage_status", "unknown")
                print(f"{prefix}Order {order_id} - Brokerage status '{brokerage_status}' != TRANSIT, skipping")
                continue
            
            print(f"{prefix}Order {order_id} - In transit with TRANSIT status, sending {call_type} call webhook")
            
            # Get order details
            stops = order.get("stops", [])
            first_stop = stops[0]
            last_stop = stops[-1]
            
            # Get movement_id, phone numbers, and equipment info
            movements = order.get("movement", [])
            movement = movements[0] if isinstance(movements, list) else movements
            movement_id = movement.get("id") if movement else None
            driver_phone = movement.get("override_drvr_cell", "") if movement else ""
            dispatch_phone = movement.get("carrier_phone", "") if movement else ""
            carrier_tractor = movement.get("carrier_tractor", "") if movement else ""
            carrier_trailer = movement.get("carrier_trailer", "") if movement else ""
            
            # Filter 3: At least one phone number must exist
            if not driver_phone and not dispatch_phone:
                print(f"{prefix}Order {order_id} - Missing both phone numbers, skipping")
                continue
            
            # Send webhook with minimal payload
            payload = {
                "order_id": order_id,
                "movement_id": movement_id,
                "driver_phone": driver_phone,
                "dispatch_phone": dispatch_phone,
                "carrier_tractor": carrier_tractor,
                "carrier_trailer": carrier_trailer
            }
            
            result = send_webhook(webhook_url, payload, prefix)
            result["order_id"] = order_id
            result["call_type"] = call_type
            results.append(result)
            
        except Exception as e:
            print(f"{prefix}Error processing order {order.get('id', 'unknown')}: {e}")
            results.append({
                "order_id": order.get("id", "unknown"),
                "call_type": call_type,
                "error": str(e),
                "success": False
            })
    
    return results


def in_transit_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler for in-transit check-ins.
    
    Triggered twice daily:
    - Morning: 9:00 AM Central Time
    - Afternoon: 2:00 PM Central Time
    
    Queries last 7 days to catch multi-day transits.
    Every in-transit load receives both morning and afternoon calls each day.
    
    Parameters from event:
    - call_type: "morning" | "afternoon" (from query string or body)
    """
    try:
        # Get call_type parameter
        query_params = event.get("queryStringParameters") or {}
        call_type = query_params.get("call_type", "morning")
        
        print(f"Running Meiborg Brothers in-transit check-in ({call_type} call)")
        
        # Get configuration
        base_url = os.getenv("MCLEOD_BASE_URL")
        auth_token = os.getenv("MCLEOD_AUTH_TOKEN")
        token_type = os.getenv("MCLEOD_AUTH_TYPE", "Bearer")
        webhook_url = os.getenv("IN_TRANSIT_WEBHOOK_URL")
        
        if not base_url or not auth_token or not webhook_url:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "MCLEOD_BASE_URL, MCLEOD_AUTH_TOKEN, IN_TRANSIT_WEBHOOK_URL required"
                })
            }
        
        # Calculate time window (last 7 days to catch multi-day transits)
        central_tz = ZoneInfo("America/Chicago")
        now = datetime.now(central_tz)
        start_time = now - timedelta(days=7)
        
        # Format time parameters for McLeod API
        start_param = start_time.strftime('%Y%m%d %H%M')
        end_param = now.strftime('%Y%m%d %H%M')
        
        print(f"Querying orders from last 7 days: {start_time.strftime('%Y-%m-%d %H:%M')} to {now.strftime('%Y-%m-%d %H:%M')} Central")
        
        # Process production environment
        results = []
        orders_checked = 0
        production_error = None
        
        if webhook_url != "N/A":
            try:
                resp = fetch_orders(base_url, auth_token, token_type, start_param, end_param)
                if resp.status_code == 200:
                    orders = normalize_response_to_list(resp.json())
                    orders_checked = len(orders)
                    print(f"Fetched {orders_checked} orders, filtering for in-transit with TRANSIT status")
                    results = process_in_transit_orders(orders, webhook_url, call_type, now)
                else:
                    print(f"API request failed with status {resp.status_code}: {resp.text}")
                    production_error = f"API returned status {resp.status_code}"
            except Exception as e:
                print(f"Error fetching production orders: {e}")
                production_error = str(e)
        
        # Process TRN (staging environment)
        trn_results = []
        trn_orders_checked = 0
        trn_error = None
        trn_base_url = os.getenv("TRN_MCLEOD_BASE_URL")
        trn_webhook_url = os.getenv("TRN_IN_TRANSIT_WEBHOOK_URL")
        
        if trn_base_url and trn_webhook_url:
            try:
                trn_resp = fetch_orders(
                    trn_base_url,
                    os.getenv("TRN_MCLEOD_AUTH_TOKEN"),
                    os.getenv("TRN_MCLEOD_AUTH_TYPE", "Bearer"),
                    start_param,
                    end_param,
                    "TRN - "
                )
                if trn_resp.status_code == 200:
                    trn_orders = normalize_response_to_list(trn_resp.json())
                    trn_orders_checked = len(trn_orders)
                    print(f"TRN - Fetched {trn_orders_checked} orders, filtering for in-transit with TRANSIT status")
                    trn_results = process_in_transit_orders(trn_orders, trn_webhook_url, call_type, now, "TRN - ")
                else:
                    print(f"TRN - API request failed with status {trn_resp.status_code}: {trn_resp.text}")
                    trn_error = f"API returned status {trn_resp.status_code}"
            except Exception as e:
                print(f"TRN - Error fetching orders: {e}")
                trn_error = str(e)
        
        # Build response
        response_body = {
            "message": "Success!",
            "call_type": call_type,
            "orders_checked": orders_checked,
            "in_transit_found": len([r for r in results if r.get("success")]),
            "webhooks_sent": len(results),
            "webhook_results": results,
            "trn_orders_checked": trn_orders_checked,
            "trn_in_transit_found": len([r for r in trn_results if r.get("success")]),
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
