"""Lambda handler for Meiborg Brothers Find Load integration."""

import os
import json
from typing import Dict, Any, Optional, List, Tuple
import requests
from handlers.models import FindLoadRequest, FindLoadResponse
from handlers.find_load_utils import map_find_load_payload


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for finding loads via McLeod API.
    
    Args:
        event: Lambda event containing request data
        context: Lambda context object
        
    Returns:
        Dictionary with statusCode and body
    """
    try:
        # Parse request body
        if isinstance(event.get("body"), str):
            body = json.loads(event["body"])
        else:
            body = event.get("body", {})
        
        # Validate request
        request = FindLoadRequest(**body)
        
        # Get McLeod API configuration from environment
        mcleod_base_url = os.environ.get("MCLEOD_BASE_URL")
        mcleod_auth_token = os.environ.get("MCLEOD_AUTH_TOKEN")
        mcleod_auth_type = os.environ.get("MCLEOD_AUTH_TYPE", "Bearer").strip()  # Default to Bearer
        mcleod_company_id = os.environ.get("MCLEOD_COMPANY_ID")
        
        if not mcleod_base_url:
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "message": "Missing required MCLEOD_BASE_URL configuration",
                    "status_code": 500
                })
            }
        
        if not mcleod_auth_token:
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "message": "Missing required MCLEOD_AUTH_TOKEN configuration",
                    "status_code": 500
                })
            }
        
        # Build query parameters
        query_params = {}
        
        # Add order ID filter (exact match)
        if request.order_id:
            query_params["id"] = request.order_id
        
        # Add status filter
        if request.status:
            query_params["orders.status"] = request.status
        
        # Add shipper location filter
        if request.shipper_location_id:
            query_params["shipper.location_id"] = request.shipper_location_id
        
        # Add consignee state filter
        if request.consignee_state:
            query_params["consignee.state"] = request.consignee_state
        
        # Add customer filter
        if request.customer_id:
            query_params["customer.id"] = request.customer_id
        
        # Add pagination
        if request.record_length:
            query_params["recordLength"] = request.record_length
        if request.record_offset:
            query_params["recordOffset"] = request.record_offset
        
        # Add sorting
        if request.order_by:
            query_params["orderBy"] = request.order_by
        
        # Add change tracking
        if request.changed_after_date:
            query_params["changedAfterDate"] = request.changed_after_date
        if request.changed_after_type:
            query_params["changedAfterType"] = request.changed_after_type
        
        # Add any additional parameters
        if request.additional_params:
            query_params.update(request.additional_params)
        
        # Prepare headers
        headers = {
            "Accept": "application/json",
        }

        # Add authorization (required)
        # Format: "Bearer <token>" or "Basic <token>" or just the token if type is empty
        if mcleod_auth_type and mcleod_auth_type.lower() != "none":
            headers["Authorization"] = f"{mcleod_auth_type} {mcleod_auth_token}"
        else:
            headers["Authorization"] = mcleod_auth_token

        # Add company ID header if configured
        if mcleod_company_id:
            headers["X-com.mcleodsoftware.CompanyID"] = mcleod_company_id

        # Make request to McLeod API
        url = f"{mcleod_base_url.rstrip('/')}/ws/orders/search"
        response = requests.get(url, params=query_params, headers=headers, timeout=30)
        
        # Handle response
        if response.status_code == 200:
            try:
                data = response.json()

                # Build and forward Load Event payload (proxy)
                proxy_result = _proxy_load_event(data)

                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "status_code": 200,
                        "data": data,
                        "proxy": proxy_result,
                        "message": "Successfully retrieved loads and proxied load event"
                    })
                }
            except json.JSONDecodeError:
                # If JSON parsing fails, return raw response
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "status_code": 200,
                        "data": response.text,
                        "message": "Successfully retrieved loads (raw response)"
                    })
                }
        else:
            return {
                "statusCode": response.status_code,
                "body": json.dumps({
                    "status_code": response.status_code,
                    "message": f"McLeod API error: {response.text}",
                    "data": None
                })
            }
    
    except ValueError as e:
        # Pydantic validation error
        return {
            "statusCode": 400,
            "body": json.dumps({
                "status_code": 400,
                "message": f"Invalid request: {str(e)}",
                "data": None
            })
        }
    except requests.exceptions.RequestException as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status_code": 500,
                "message": f"Request to McLeod API failed: {str(e)}",
                "data": None
            })
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status_code": 500,
                "message": f"Internal server error: {str(e)}",
                "data": None
            })
        }


def _proxy_load_event(mcleod_data: Any) -> Dict[str, Any]:
    """Transform McLeod order(s) into Load Event payload and POST to Load Event API.

    Returns a dict with status and response for observability.
    """
    try:
        # Read Load Event configuration
        broker_url = os.environ.get("BROKER_URL")
        broker_key = os.environ.get("BROKER_KEY")
        org_id = os.environ.get("ORG_ID")
        load_event_timeout = int(os.environ.get("LOAD_EVENT_TIMEOUT", "30"))

        if not broker_url or not broker_key:
            return {
                "enabled": False,
                "reason": "Missing BROKER_URL or BROKER_KEY",
            }

        # Normalize mcleod_data to a list of orders
        orders: List[Dict[str, Any]]
        if isinstance(mcleod_data, list):
            orders = mcleod_data
        elif isinstance(mcleod_data, dict) and mcleod_data.get("__type") == "orders":
            orders = [mcleod_data]
        else:
            # Unknown structure; forward raw as single payload
            orders = [mcleod_data] if mcleod_data else []

        # Transform orders to Load Event payload(s)
        transformed_payload = _transform_orders_to_load_event(orders)

        # Determine if single object or array to send
        payload_to_send: Any
        if len(transformed_payload) == 0:
            return {"enabled": True, "sent": False, "reason": "No orders to proxy"}
        elif len(transformed_payload) == 1:
            payload_to_send = transformed_payload[0]
        else:
            payload_to_send = transformed_payload

        # Attach org_id if provided
        if org_id:
            if isinstance(payload_to_send, list):
                for p in payload_to_send:
                    p.setdefault("org_id", org_id)
            else:
                payload_to_send.setdefault("org_id", org_id)

        # Send to Load Event API
        url = broker_url
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-Key": broker_key,
        }
        resp = requests.post(url, headers=headers, json=payload_to_send, timeout=load_event_timeout)
        result_body: Any
        try:
            result_body = resp.json()
        except Exception:
            result_body = resp.text

        return {
            "enabled": True,
            "sent": True,
            "status": resp.status_code,
            "response": result_body,
            "url": url,
            "count": len(transformed_payload) if isinstance(payload_to_send, list) else 1,
        }
    except Exception as e:
        return {
            "enabled": True,
            "sent": False,
            "error": str(e),
        }


def _transform_orders_to_load_event(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract essential fields from McLeod orders into Load Event payloads."""
    payloads: List[Dict[str, Any]] = []
    for order in orders:
        try:
            custom_load_id = _get_order_id(order)
            status = _derive_status(order)
            equipment_type = _extract_equipment(order)
            miles = _extract_miles(order)
            max_buy = _extract_max_buy(order)
            posted_carrier_rate = _extract_posted_carrier_rate(order)
            weight = _extract_weight(order)
            origin, destination = _extract_origin_destination(order)
            stops = _extract_stops(order)
            pickup_number, po_number = _extract_reference_numbers(order)
            pickup_open, pickup_close, delivery_open, delivery_close = _extract_overall_windows(origin, destination, stops)
            contacts = _extract_contacts(order)
            commodity_type = order.get("commodity")
            number_of_pieces = order.get("pieces")
            is_partial = bool(order.get("ltl")) if order.get("ltl") is not None else None
            is_hazmat = bool(order.get("hazmat")) if order.get("hazmat") is not None else None
            is_team_required = bool(order.get("teams_required")) if order.get("teams_required") is not None else None
            bol_number = order.get("blnum")
            branch = order.get("revenue_code_id")
            customer_id = order.get("customer_id")
            truck_number, trailer_number = _extract_power_units(order)
            sale_notes = _extract_sale_notes(order)

            payload: Dict[str, Any] = {
                "event_type": "load_upsert",
                "custom_load_id": custom_load_id,
                "status": status,
                "type": "owned",
            }

            if equipment_type:
                payload["equipment_type_name"] = equipment_type
            if miles is not None:
                payload["miles"] = miles
            if max_buy is not None:
                payload["max_buy"] = max_buy
            if posted_carrier_rate is not None:
                payload["posted_carrier_rate"] = posted_carrier_rate
            if weight is not None:
                payload["weight"] = weight
            if commodity_type:
                payload["commodity_type"] = commodity_type
            if number_of_pieces is not None:
                payload["number_of_pieces"] = number_of_pieces
            if is_partial is not None:
                payload["is_partial"] = is_partial
            if is_hazmat is not None:
                payload["is_hazmat"] = is_hazmat
                payload["is_hazardous"] = is_hazmat
            if is_team_required is not None:
                payload["is_team_required"] = is_team_required
            if bol_number:
                payload["bol_number"] = bol_number
            if truck_number:
                payload["truck_number"] = truck_number
            if trailer_number:
                payload["trailer_number"] = trailer_number
            if branch:
                payload["branch"] = branch
            if customer_id:
                payload["customer_id"] = customer_id
            if origin:
                payload["origin"] = origin
            if destination:
                payload["destination"] = destination
            if stops:
                payload["stops"] = stops
            if pickup_number:
                payload["pickup_number"] = pickup_number
            if po_number:
                payload["po_number"] = po_number
            if pickup_open:
                payload["pickup_date_open"] = pickup_open
            if pickup_close:
                payload["pickup_date_close"] = pickup_close
            if delivery_open:
                payload["delivery_date_open"] = delivery_open
            if delivery_close:
                payload["delivery_date_close"] = delivery_close
            if contacts:
                payload["contacts"] = contacts
            if sale_notes:
                payload["sale_notes"] = sale_notes

            payloads.append(payload)
        except Exception:
            # Skip problematic order, continue
            continue

    return payloads


def _get_order_id(order: Dict[str, Any]) -> str:
    # Prefer orders.id; fallback to lme_order_id under freightGroup or other fields
    if "id" in order:
        return str(order["id"]).strip()
    fg = order.get("freightGroup") or {}
    if "lme_order_id" in fg:
        return str(fg["lme_order_id"]).strip()
    return ""


def _derive_status(order: Dict[str, Any]) -> str:
    """Map McLeod status fields into the allowed broker status values.

    Allowed values: at_pickup, picked_up, at_delivery, dispatched, delivered,
    en_route, in_transit, completed, available, covered, unavailable
    """
    allowed = {
        "at_pickup",
        "picked_up",
        "at_delivery",
        "dispatched",
        "delivered",
        "en_route",
        "in_transit",
        "completed",
        "available",
        "covered",
        "unavailable",
    }

    def normalize(text: str) -> Optional[str]:
        if not text:
            return None
        t = text.strip().lower()
        # Direct matches
        if t in allowed:
            return t
        # Common descriptors → allowed
        if t.startswith("deliver") or t == "delv":
            return "delivered"
        if t.startswith("dispatch") or t == "disp":
            return "dispatched"
        if t.startswith("avail") or t == "avail":
            return "available"
        if t in ("at pu", "at_pickup", "atpu", "pu"):
            return "at_pickup"
        if t in ("picked up", "picked_up", "pku"):
            return "picked_up"
        if t in ("at dl", "at_delivery", "atdl", "dl"):
            return "at_delivery"
        if t in ("en route", "enroute", "en_rt"):
            return "en_route"
        if t in ("in transit", "in_transit", "intr"):
            return "in_transit"
        if t.startswith("complet") or t == "cmpl":
            return "completed"
        if t.startswith("cover") or t == "covr":
            return "covered"
        if t.startswith("unavail") or t == "unav":
            return "unavailable"
        return None

    # 1) Try movement.brokerage_status (often coded like DELV/AVAIL/DISP)
    mvts = order.get("movement") or []
    if mvts and isinstance(mvts, list):
        brokerage_status = (mvts[0] or {}).get("brokerage_status")
        mapped = normalize(brokerage_status) if isinstance(brokerage_status, str) else None
        if mapped:
            return mapped

    # 2) Try orders.__statusDescr (text like Delivered/Available/In Transit)
    status_descr = order.get("__statusDescr")
    mapped = normalize(status_descr) if isinstance(status_descr, str) else None
    if mapped:
        return mapped

    # 3) Try orders.status code fallback (e.g., 'D' → delivered)
    code = order.get("status")
    if isinstance(code, str):
        code = code.strip().upper()
        if code == "D":
            return "delivered"

    # Default mapping requested:
    # McLeod A, ACTIVE, REVIEW → available; any other McLeod status → covered
    status_code = (order.get("status") or "").strip().upper()
    status_descr = (order.get("__statusDescr") or "").strip().upper()
    if status_code in {"A", "ACTIVE", "REVIEW"} or status_descr in {"A", "ACTIVE", "REVIEW", "AVAILABLE"}:
        return "available"
    return "covered"


def _extract_equipment(order: Dict[str, Any]) -> Optional[str]:
    # Look in stops.referenceNumbers for Equipment Initial description, else fallback
    stops = order.get("stops") or []
    for st in stops:
        for ref in st.get("referenceNumbers", []) or []:
            if (ref.get("__referenceQualDescr") == "Equipment Initial") and ref.get("reference_number"):
                return str(ref["reference_number"]).strip()
    # Fallback: order.__equipmentTypeDescr
    et = order.get("__equipmentTypeDescr")
    return str(et).strip() if et else None


def _extract_miles(order: Dict[str, Any]) -> Optional[int]:
    # Prefer billed miles from order if present, else movement move_distance
    bill = order.get("bill_distance")
    try:
        if bill is not None:
            return int(float(bill))
    except Exception:
        pass
    mvts = order.get("movement") or []
    if mvts and isinstance(mvts, list):
        miles = (mvts[0] or {}).get("move_distance")
        try:
            return int(miles) if miles is not None else None
        except Exception:
            return None
    return None


def _extract_max_buy(order: Dict[str, Any]) -> Optional[float]:
    mvts = order.get("movement") or []
    if mvts and isinstance(mvts, list):
        # Prefer numeric field if present
        for key in ("max_buy", "max_buy_n"):
            if key in (mvts[0] or {}):
                try:
                    return float((mvts[0] or {})[key])
                except Exception:
                    continue
    return None


def _extract_posted_carrier_rate(order: Dict[str, Any]) -> Optional[float]:
    """Heuristic for posted carrier rate: prefer override_max_pay, else target_pay."""
    mvts = order.get("movement") or []
    if mvts and isinstance(mvts, list):
        for key in ("override_max_pay", "target_pay", "target_pay_n"):
            val = (mvts[0] or {}).get(key)
            if val is not None:
                try:
                    return float(val)
                except Exception:
                    continue
    return None


def _extract_weight(order: Dict[str, Any]) -> Optional[float]:
    # Use weight from first stop if present
    stops = order.get("stops") or []
    for st in stops:
        if "weight" in st and st["weight"] is not None:
            try:
                return float(st["weight"])
            except Exception:
                continue
    return None


def _extract_origin_destination(order: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    stops = order.get("stops") or []
    if not isinstance(stops, list) or not stops:
        return None, None

    def to_loc(st: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "city": st.get("city_name") or st.get("city"),
            "state": st.get("state"),
            "zip": st.get("zip_code") or st.get("zip"),
            "country": "US",
            "address": st.get("address"),
        }

    origin_stop = None
    dest_stop = None
    for st in stops:
        st_type = (st.get("stop_type") or "").upper()
        if not origin_stop and st_type in ("PU", "ORIGIN"):
            origin_stop = st
        if st_type in ("SO", "DESTINATION"):
            dest_stop = st

    origin = to_loc(origin_stop) if origin_stop else None
    destination = to_loc(dest_stop) if dest_stop else None
    return origin, destination


def _format_window(st: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    # McLeod format: YYYYMMDDHHMMSS-0600 -> required: YYYY-MM-DDTHH:MM:SS (no tz)
    def convert(val: Optional[str]) -> Optional[str]:
        if not val or not isinstance(val, str):
            return None
        # Trim timezone suffix if present
        core = val.split("-")[0]
        if len(core) < 14:
            return None
        try:
            return f"{core[0:4]}-{core[4:6]}-{core[6:8]}T{core[8:10]}:{core[10:12]}:{core[12:14]}"
        except Exception:
            return None

    return convert(st.get("sched_arrive_early")), convert(st.get("sched_arrive_late"))


def _extract_stops(order: Dict[str, Any]) -> List[Dict[str, Any]]:
    stops = order.get("stops") or []
    if not stops:
        return []
    
    # First pass: identify which SO is the last one (for destination)
    last_so_idx = -1
    for idx in range(len(stops) - 1, -1, -1):
        st_type_raw = str(stops[idx].get("stop_type") or "").upper()
        if st_type_raw in ("SO", "DESTINATION"):
            last_so_idx = idx
            break
    
    result: List[Dict[str, Any]] = []
    origin_found = False
    
    for idx, st in enumerate(stops):
        st_type_raw = str(st.get("stop_type") or "").upper()
        
        # Map stop types - handle multiple PU/SO stops
        if st_type_raw in ("PU", "ORIGIN"):
            # First PU becomes origin, subsequent PUs become pick
            if not origin_found:
                st_type = "origin"
                origin_found = True
            else:
                st_type = "pick"
        elif st_type_raw in ("SO", "DESTINATION"):
            # Last SO becomes destination, earlier SOs become drop
            if idx == last_so_idx:
                st_type = "destination"
            else:
                st_type = "drop"
        elif st_type_raw in ("PICK", "P"):
            st_type = "pick"
        elif st_type_raw in ("DROP", "D"):
            st_type = "drop"
        else:
            # For unknown types, infer based on position:
            # - First stop → origin (if no origin yet)
            # - Last stop → destination (if no destination yet and no SO found)
            # - Middle stops → pick
            if idx == 0 and not origin_found:
                st_type = "origin"
                origin_found = True
            elif idx == len(stops) - 1 and last_so_idx == -1:
                st_type = "destination"
            else:
                # Default unknown types to pick
                st_type = "pick"

        open_ts, close_ts = _format_window(st)
        # Use movement_sequence or order_sequence if available, otherwise use index + 1
        stop_order = st.get("movement_sequence") or st.get("order_sequence")
        if stop_order is None:
            stop_order = idx + 1
        else:
            try:
                stop_order = int(stop_order)
            except (ValueError, TypeError):
                stop_order = idx + 1
        
        stop_obj: Dict[str, Any] = {
            "type": st_type,
            "location": {
                "city": st.get("city_name") or st.get("city"),
                "state": st.get("state"),
                "zip": st.get("zip_code") or st.get("zip"),
                "country": "US",
                "address": st.get("address"),
            },
            "stop_order": stop_order,
        }
        if open_ts:
            stop_obj["stop_timestamp_open"] = open_ts
        if close_ts:
            stop_obj["stop_timestamp_close"] = close_ts
        if st.get("__loadUnloadDescr"):
            stop_obj["loading_type"] = st.get("__loadUnloadDescr")
        if st.get("notes"):
            stop_obj["notes"] = st.get("notes")

        result.append(stop_obj)

    # Ensure origin and destination exist if possible
    if not any(s["type"] == "origin" for s in result) and stops:
        first = stops[0]
        open_ts, close_ts = _format_window(first)
        result.insert(0, {
            "type": "origin",
            "location": {
                "city": first.get("city_name") or first.get("city"),
                "state": first.get("state"),
                "zip": first.get("zip_code") or first.get("zip"),
                "country": "US",
                "address": first.get("address"),
            },
            "stop_order": 1,
            **({"stop_timestamp_open": open_ts} if open_ts else {}),
            **({"stop_timestamp_close": close_ts} if close_ts else {}),
        })

    if not any(s["type"] == "destination" for s in result) and stops:
        last = stops[-1]
        open_ts, close_ts = _format_window(last)
        result.append({
            "type": "destination",
            "location": {
                "city": last.get("city_name") or last.get("city"),
                "state": last.get("state"),
                "zip": last.get("zip_code") or last.get("zip"),
                "country": "US",
                "address": last.get("address"),
            },
            "stop_order": max((s.get("stop_order", 0) for s in result), default=1) + 1,
            **({"stop_timestamp_open": open_ts} if open_ts else {}),
            **({"stop_timestamp_close": close_ts} if close_ts else {}),
        })

    return result


def _extract_reference_numbers(order: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Return (pickup_number, po_number) from referenceNumbers by qualifiers."""
    pickup = None
    po = None
    for st in (order.get("stops") or []):
        for ref in (st.get("referenceNumbers") or []):
            qual = (ref.get("reference_qual") or ref.get("__referenceQualDescr") or "").upper()
            val = ref.get("reference_number")
            if not val:
                continue
            if qual in ("OQ", "ORDER NUMBER") and not pickup:
                pickup = str(val)
            if qual in ("PO", "PURCHASE ORDER NUMBER") and not po:
                po = str(val)
        if pickup and po:
            break
    return pickup, po


def _extract_overall_windows(origin: Optional[Dict[str, Any]], destination: Optional[Dict[str, Any]], stops: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Derive overall pickup/delivery windows from origin/destination stops if present."""
    pickup_open = pickup_close = delivery_open = delivery_close = None
    for st in stops:
        if st.get("type") == "origin":
            pickup_open = pickup_open or st.get("stop_timestamp_open")
            pickup_close = pickup_close or st.get("stop_timestamp_close")
        elif st.get("type") == "destination":
            delivery_open = delivery_open or st.get("stop_timestamp_open")
            delivery_close = delivery_close or st.get("stop_timestamp_close")
    return pickup_open, pickup_close, delivery_open, delivery_close


def _extract_contacts(order: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build contacts list from operationsUser or enteredUser when available."""
    contacts: List[Dict[str, Any]] = []
    for key in ("operationsUser", "enteredUser"):
        usr = order.get(key) or {}
        name = usr.get("name")
        email = usr.get("email_address")
        phone = usr.get("phone")
        if name or email or phone:
            contacts.append({
                "name": name or "",
                "email": email or "",
                "phone": (str(phone).replace("-", "").replace(" ", "") if phone else ""),
                "type": "assigned",
            })
    return contacts


def _extract_power_units(order: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    mvts = order.get("movement") or []
    if mvts and isinstance(mvts, list):
        m = (mvts[0] or {})
        return (m.get("carrier_tractor"), m.get("carrier_trailer"))
    return None, None


def _extract_sale_notes(order: Dict[str, Any]) -> Optional[str]:
    # Use first PU stop notes concatenated (simple join) if available
    stops = order.get("stops") or []
    for st in stops:
        st_type = (st.get("stop_type") or "").upper()
        if st_type in ("PU", "ORIGIN"):
            notes = st.get("stopNotes") or []
            texts = []
            for n in notes:
                c = n.get("comments")
                if c:
                    texts.append(str(c))
            if texts:
                # Limit size to prevent huge payloads
                joined = " \n".join(texts)
                return joined[:2000]
            break
    return None
