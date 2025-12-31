"""FastAPI server for Meiborg Brothers integrations.

Combines:
- Find Load API (on-demand load search)
- Pre-Shipment Handler (cron: ~2 hours before pickup)
- In-Transit Handler (cron: morning/afternoon check-ins)
"""

import os
import json
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from handlers.find_load import lambda_handler as find_load_handler
from handlers.pre_shipment import pre_shipment_handler
from handlers.in_transit import in_transit_handler
from handlers.find_load_utils import map_find_load_payload, safe_get

# Load environment variables
load_dotenv()

app = FastAPI(title="Meiborg Brothers Integrations", version="1.0.0")


@app.get("/find-load")
async def find_load(request: Request, order_id: str = None):
    """
    Find Load API - On-demand load search via McLeod API.
    Transforms and forwards to broker API.
    
    Supports GET with query parameters.
    """
    try:
        # Build body from query parameters
        body = {}
        if order_id:
            body["order_id"] = order_id
        # Add other query params if present
        for key, value in request.query_params.items():
            if key not in body:
                body[key] = value
        
        # Call find_load handler
        result = find_load_handler({"body": body}, None)
        
        # Parse body if it's a JSON string
        body_content = result.get("body")
        if isinstance(body_content, str):
            body_content = json.loads(body_content)
        
        # Handle handler-level errors
        if result.get("statusCode") != 200:
            return JSONResponse(status_code=result.get("statusCode", 500), content=body_content)
        
        # Extract data and proxy from handler response
        data = body_content.get("data")
        proxy = body_content.get("proxy")
        
        # Extract order from data (could be list or dict)
        if isinstance(data, list) and data:
            order = data[0]
        elif isinstance(data, dict):
            order = data
        else:
            order = {}
        
        # Map the raw McLeod response to clean format
        payload = map_find_load_payload(order, format_ts=True)
        
        # Check if load was found - if load_number is missing/empty, load not found
        load_found = payload.get("load_number") and payload.get("load_number") != ""
        if not load_found:
            payload["internal_next_steps"] = "Please ask the user again for the reference number (load number) to search for the load."
        
        # Extract broker_load_id from proxy response
        broker_id = None
        if proxy and isinstance(proxy, dict):
            resp = proxy.get("response")
            if isinstance(resp, dict):
                broker_id = resp.get("id") or (resp.get("data", {}) if isinstance(resp.get("data"), dict) else {}).get("id")
            elif isinstance(resp, list) and resp and isinstance(resp[0], dict):
                broker_id = resp[0].get("id")
        
        if broker_id:
            payload["broker_load_id"] = broker_id
        
        # Remove rate fields (not to be exposed)
        payload.pop("rate", None)
        payload.pop("posted_carrier_rate", None)
        payload.pop("max_buy", None)
        
        return JSONResponse(status_code=200, content=payload)
    
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "statusCode": 500,
                "error": str(e)
            }
        )

@app.get("/find-load-before-negotiation")
async def find_load_before_negotiation(request: Request):
    try:
        if request.method == "POST":
            body = await request.json()
        else:
            body = dict(request.query_params)
            if "record_length" in body:
                body["record_length"] = int(body["record_length"])
            if "record_offset" in body:
                body["record_offset"] = int(body["record_offset"])

        event = {
            "body": json.dumps(body) if isinstance(body, dict) else body,
            "headers": dict(request.headers),
            "httpMethod": request.method,
            "path": str(request.url.path),
        }

        result = find_load_handler(event, None)
        response_body = json.loads(result["body"]) if "body" in result else result

        if result.get("statusCode") != 200:
            return JSONResponse(status_code=result.get("statusCode", 500), content=response_body)

        data = response_body.get("data")
        proxy = response_body.get("proxy") if isinstance(response_body, dict) else None
        if isinstance(data, list) and data:
            order = data[0]
        elif isinstance(data, dict):
            order = data
        else:
            order = {}

        payload = map_find_load_payload(order, format_ts=False)
        # Add rate from movement.override_max_pay_n (or TBD)
        rate = safe_get(order, "movement", 0, "override_max_pay_n", default=None)
        payload["rate"] = rate if rate is not None else "TBD"
        # Attach broker_load_id if proxy returned it
        broker_id = None
        if proxy and isinstance(proxy, dict):
            resp = proxy.get("response")
            if isinstance(resp, dict):
                broker_id = resp.get("id") or (resp.get("data", {}) if isinstance(resp.get("data"), dict) else {}).get("id")
            elif isinstance(resp, list) and resp and isinstance(resp[0], dict):
                broker_id = resp[0].get("id")
        if broker_id:
            payload["broker_load_id"] = broker_id
        # Include broker raw response for debugging when asked
        if proxy and isinstance(proxy, dict) and "response" in proxy:
            try:
                print("[broker] response:", proxy["response"])  # server log
            except Exception:
                pass
            payload["broker_response"] = proxy["response"]
        return JSONResponse(status_code=200, content=payload)
    except Exception as e:
        return JSONResponse(status_code=500, content={"status_code": 500, "message": f"Internal server error: {str(e)}"})

@app.post("/sync-pre-shipment")
@app.get("/sync-pre-shipment")
async def sync_pre_shipment():
    """
    Pre-Shipment Sync - Triggered by Railway cron every 30 minutes.
    Finds loads ~2 hours before pickup and sends to workflow webhook.
    """
    try:
        # Call pre-shipment handler
        result = pre_shipment_handler({}, None)
        
        # Parse body if it's a JSON string (error case)
        body_content = result.get("body", {})
        if isinstance(body_content, str):
            import json
            body_content = json.loads(body_content)
        
        return JSONResponse(
            status_code=result.get("statusCode", 200),
            content=body_content
        )
    
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "statusCode": 500,
                "error": str(e)
            }
        )


@app.post("/sync-in-transit")
@app.get("/sync-in-transit")
async def sync_in_transit():
    """
    In-Transit Sync - Triggered by Railway cron twice daily.
    Tracks loads that are picked up but not yet delivered.
    """
    try:
        # Call in-transit handler
        result = in_transit_handler({}, None)
        
        # Parse body if it's a JSON string (error case)
        body_content = result.get("body", {})
        if isinstance(body_content, str):
            import json
            body_content = json.loads(body_content)
        
        return JSONResponse(
            status_code=result.get("statusCode", 200),
            content=body_content
        )
    
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "statusCode": 500,
                "error": str(e)
            }
        )


@app.get("/health")
async def health():
    """Health check endpoint for monitoring."""
    return {
        "status": "healthy",
        "service": "meiborg_brothers"
    }


@app.get("/")
async def root():
    """Service information and available endpoints."""
    return {
        "service": "Meiborg Brothers Integrations",
        "description": "Combined API and scheduled job integration",
        "endpoints": {
            "GET /find-load": "Find load API (query params: order_id, status, etc.)",
            "POST /sync-pre-shipment": "Pre-shipment notifications (~2h before pickup)",
            "POST /sync-in-transit": "In-transit check-ins (morning/afternoon)",
            "GET /health": "Health check"
        }
    }


# Run the application if executed directly
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
