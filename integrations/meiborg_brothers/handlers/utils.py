"""Shared utilities for load sync handlers."""

import os
import requests
from typing import Dict, Any


def fetch_orders(
    base_url: str,
    auth_token: str,
    token_type: str,
    start_param: str,
    end_param: str,
    prefix: str = "",
) -> requests.Response:
    """Fetch orders from McLeod API with given time parameters."""
    endpoint = f"/ws/orders/search?shipper.sched_arrive_early=>={start_param}&shipper.sched_arrive_early=<{end_param}"

    print(f"{prefix}Endpoint: {endpoint}")

    headers = {"Accept": "application/json"}
    if token_type and token_type.lower() != "none":
        headers["Authorization"] = f"{token_type.title()} {auth_token}"
    else:
        headers["Authorization"] = auth_token

    # Add company ID header if configured
    company_id = os.environ.get("MCLEOD_COMPANY_ID")
    if company_id:
        headers["X-com.mcleodsoftware.CompanyID"] = company_id

    return requests.get(
        f"{base_url}{endpoint}",
        headers=headers,
        timeout=90,
    )


def normalize_response_to_list(response_data: Any) -> list:
    """Normalize API response to a list of orders."""
    if isinstance(response_data, dict):
        if response_data.get("__type") == "orders":
            return [response_data]
        else:
            return [response_data]
    elif isinstance(response_data, list):
        return response_data
    else:
        return []


def send_webhook(webhook_url: str, payload: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Send webhook and return result."""
    try:
        webhook_resp = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        
        result = {
            "order_id": payload.get("order_id"),
            "webhook_status": webhook_resp.status_code,
            "success": webhook_resp.status_code in (200, 201, 202)
        }
        
        if result["success"]:
            print(f"{prefix}Successfully sent webhook for order {payload.get('order_id')}")
            try:
                result["response"] = webhook_resp.json()
            except:
                result["response"] = webhook_resp.text
        else:
            print(f"{prefix}Failed webhook for order {payload.get('order_id')}: {webhook_resp.text}")
            result["error"] = webhook_resp.text
        
        return result
    
    except Exception as e:
        print(f"{prefix}Error sending webhook: {e}")
        return {
            "order_id": payload.get("order_id"),
            "error": str(e),
            "success": False
        }
