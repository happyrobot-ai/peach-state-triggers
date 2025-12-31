"""Pydantic models for Meiborg Brothers Find Load integration."""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class FindLoadRequest(BaseModel):
    """Request model for finding loads."""
    
    # Search parameters
    order_id: Optional[str] = Field(None, description="Order ID to search for (exact match)")
    status: Optional[str] = Field(None, description="Order status filter (e.g., 'P' for in progress, 'D' for delivered)")
    shipper_location_id: Optional[str] = Field(None, description="Shipper location ID filter (supports wildcards like 'WARE*')")
    consignee_state: Optional[str] = Field(None, description="Consignee state filter (e.g., 'AL')")
    customer_id: Optional[str] = Field(None, description="Customer ID filter")
    
    # Pagination
    record_length: Optional[int] = Field(None, description="Number of records to return")
    record_offset: Optional[int] = Field(None, description="Offset for pagination")
    
    # Sorting
    order_by: Optional[str] = Field(None, description="Sort order (e.g., 'orders.id+DESC', 'shipper.sched_arrive_early')")
    
    # Change tracking
    changed_after_date: Optional[str] = Field(None, description="Return records changed after this date (e.g., 't-1' for yesterday)")
    changed_after_type: Optional[str] = Field(None, description="Change type filter: 'Add' or 'Update'")
    
    # Additional search criteria - flexible dict for other query parameters
    additional_params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional query parameters")


class FindLoadResponse(BaseModel):
    """Response model for find load operation."""
    
    status_code: int = Field(..., description="HTTP status code")
    data: Optional[List[Dict[str, Any]]] = Field(None, description="List of orders returned from McLeod API")
    message: Optional[str] = Field(None, description="Response message")

