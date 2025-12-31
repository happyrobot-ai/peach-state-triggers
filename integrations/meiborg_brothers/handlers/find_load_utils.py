"""Utility functions for find_load handler."""

from typing import Dict, Any, List, Optional


# -----------------------------
# Safe helpers
# -----------------------------

def safe_get(obj: Any, *keys, default=None):
    """Safely traverse nested dict/list structure."""
    cur = obj
    try:
        for k in keys:
            if isinstance(k, int):
                if isinstance(cur, list) and 0 <= k < len(cur):
                    cur = cur[k]
                else:
                    return default
            else:
                if isinstance(cur, dict) and k in cur:
                    cur = cur[k]
                else:
                    return default
        return cur if cur is not None else default
    except Exception:
        return default


def format_timestamp(raw: Optional[str]) -> Optional[str]:
    """Convert McLeod timestamps (YYYYMMDDHHMMSS-0600) into ISO8601 without timezone."""
    if not raw or not isinstance(raw, str):
        return None
    core = raw.split("-")[0]
    if len(core) < 14:
        return None
    try:
        return f"{core[0:4]}-{core[4:6]}-{core[6:8]}T{core[8:10]}:{core[10:12]}:{core[12:14]}"
    except Exception:
        return None


# -----------------------------
# Stop extraction
# -----------------------------

def extract_pickup_and_delivery(order: Dict[str, Any]):
    """Identify pickup and delivery stops using heuristics."""
    stops = safe_get(order, "stops", default=[]) or []
    pickup = None
    delivery = None

    for st in stops:
        st_type = (st.get("stop_type") or "").upper()
        if not pickup and st_type in ("PU", "ORIGIN"):
            pickup = st
        if st_type in ("SO", "DESTINATION"):
            delivery = st

    if not pickup and stops:
        pickup = stops[0]
    if not delivery and stops:
        delivery = stops[-1]

    return pickup, delivery


def map_stop(stop: Dict[str, Any], format_ts: bool = True) -> Dict[str, Any]:
    """Convert a raw stop record into a formatted stop dictionary."""
    ts = format_timestamp if format_ts else (lambda x: x)

    st_type_raw = (stop.get("stop_type") or "").upper()
    if st_type_raw in ("PU", "ORIGIN"):
        stop_type = "pickup"
    elif st_type_raw in ("SO", "DESTINATION"):
        stop_type = "delivery"
    elif st_type_raw in ("PICK", "P"):
        stop_type = "pick"
    elif st_type_raw in ("DROP", "D"):
        stop_type = "drop"
    else:
        stop_type = "other"

    return {
        "type": stop_type,
        "location_name": safe_get(stop, "location_name"),
        "address": safe_get(stop, "address"),
        "city": safe_get(stop, "city_name"),
        "state": safe_get(stop, "state"),
        "zip": safe_get(stop, "zip_code"),
        "phone": safe_get(stop, "phone"),
        "scheduled_early": ts(safe_get(stop, "sched_arrive_early")),
        "scheduled_late": ts(safe_get(stop, "sched_arrive_late")),
        "actual_arrival": ts(safe_get(stop, "actual_arrival")),
        "status": safe_get(stop, "__statusDescr"),
        "load_type": safe_get(stop, "__loadUnloadDescr"),
        "stop_order": (
            safe_get(stop, "order_sequence")
            or safe_get(stop, "movement_sequence")
            or 0
        ),
    }


# -----------------------------
# Main mapping function
# -----------------------------

def map_find_load_payload(mcleod_data: Dict[str, Any], format_ts: bool = True) -> Dict[str, Any]:
    """
    Transform McLeod API response into the compact find_load response format.
    Only one order should be passed in.
    
    Args:
        mcleod_data: Raw McLeod order data
        format_ts: If True, format timestamps to ISO8601. If False, keep raw format.
    """
    order = mcleod_data or {}

    pickup, delivery = extract_pickup_and_delivery(order)

    # Map all stops
    stops_raw = safe_get(order, "stops", default=[]) or []
    stops = [map_stop(st, format_ts=format_ts) for st in stops_raw]

    payload = {
        "load_number": safe_get(order, "id"),
        "status": safe_get(order, "__statusDescr"),
        "equipment_type": safe_get(order, "__equipmentTypeDescr"),

        "weight": safe_get(order, "weight"),
        "weight_unit": safe_get(order, "weight_um"),
        "pieces": safe_get(order, "pieces"),
        "cases": safe_get(pickup or {}, "cases"),
        "pallets": safe_get(order, "pallets_how_many"),
        "commodity": safe_get(order, "commodity"),

        "distance": safe_get(order, "bill_distance"),
        "distance_unit": safe_get(order, "bill_distance_um"),

        "bol_number": safe_get(order, "blnum"),
        "shipment_id": safe_get(order, "shipment_id"),

        "pickup": {
            "location_name": safe_get(pickup or {}, "location_name"),
            "address": safe_get(pickup or {}, "address"),
            "city": safe_get(pickup or {}, "city_name"),
            "state": safe_get(pickup or {}, "state"),
            "zip": safe_get(pickup or {}, "zip_code"),
            "phone": safe_get(pickup or {}, "phone"),
            "scheduled_early": (format_timestamp if format_ts else lambda x: x)(safe_get(pickup or {}, "sched_arrive_early")),
            "scheduled_late": (format_timestamp if format_ts else lambda x: x)(safe_get(pickup or {}, "sched_arrive_late")),
            "actual_arrival": (format_timestamp if format_ts else lambda x: x)(safe_get(pickup or {}, "actual_arrival")),
            "status": safe_get(pickup or {}, "__statusDescr"),
            "load_type": safe_get(pickup or {}, "__loadUnloadDescr"),
        },

        "delivery": {
            "location_name": safe_get(delivery or {}, "location_name"),
            "address": safe_get(delivery or {}, "address"),
            "city": safe_get(delivery or {}, "city_name"),
            "state": safe_get(delivery or {}, "state"),
            "zip": safe_get(delivery or {}, "zip_code"),
            "phone": safe_get(delivery or {}, "phone"),
            "scheduled_early": (format_timestamp if format_ts else lambda x: x)(safe_get(delivery or {}, "sched_arrive_early")),
            "scheduled_late": (format_timestamp if format_ts else lambda x: x)(safe_get(delivery or {}, "sched_arrive_late")),
            "actual_arrival": (format_timestamp if format_ts else lambda x: x)(safe_get(delivery or {}, "actual_arrival")),
            "status": safe_get(delivery or {}, "__statusDescr"),
            "load_type": safe_get(delivery or {}, "__loadUnloadDescr"),
        },

        "stops": stops,

        "customer": {
            "id": safe_get(order, "customer_id"),
            "name": safe_get(order, "customer", "name"),
        },

        "ordered_date": safe_get(order, "ordered_date"),
        "brokerage": safe_get(order, "movement", 0, "brokerage"),
        "notes": safe_get(order, "planning_comment"),

        "internal_next_steps": (
            "1. Pitch the load with details: equipment, pickup, delivery, stops, distance, weight, commodity.\n\n"
            "2. Ask the driver what number they have in mind. If they ask for the posted rate, "
            "tell them there is no posted rate and ask again.\n\n"
            "3. Internal note: schedule is strict. Do not transfer. "
            "Pickup and delivery times cannot be changed."
        ),
    }

    return payload
