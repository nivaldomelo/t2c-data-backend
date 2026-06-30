from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "analytics_summary",
    "build_platform_cockpit_export_rows",
    "build_platform_cockpit_queue_items",
    "build_platform_cockpit_queue_page",
    "build_platform_cockpit_recommended_actions",
    "cockpit_summary",
    "list_platform_domain_events",
    "filter_visible_table_ids",
    "is_table_visible",
    "load_dashboard_profiles_from_read_model",
    "legacy_api_surface_summary",
    "load_dashboard_profiles_with_fallback",
    "load_search_records_from_read_model",
    "load_table_visibility_map",
    "mask_audit_event_payload",
    "mask_certification_summary_payload",
    "mask_dashboard_asset_payload",
    "mask_incident_asset_context_payload",
    "mask_privacy_summary_payload",
    "mask_search_result_payload",
    "mask_table_payload",
    "can_view_sensitive_data",
    "mask_payload_by_policy",
    "mask_row_by_classification",
    "mask_sensitive_value",
    "redact_sensitive_metadata",
    "refresh_platform_read_models",
    "refresh_dashboard_asset_read_model",
    "refresh_search_read_model",
    "record_platform_domain_event_from_audit",
    "record_platform_domain_event_from_usage",
    "serialize_platform_domain_event",
    "track_usage_event",
    "user_can_manage_visibility",
    "visibility_for_profiles",
    "visibility_for_search_records",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "analytics_summary": ("t2c_data.features.platform.analytics", "analytics_summary"),
    "track_usage_event": ("t2c_data.features.platform.analytics", "track_usage_event"),
    "build_platform_cockpit_export_rows": ("t2c_data.features.platform.cockpit_ops", "build_platform_cockpit_export_rows"),
    "build_platform_cockpit_queue_items": ("t2c_data.features.platform.cockpit_ops", "build_platform_cockpit_queue_items"),
    "build_platform_cockpit_queue_page": ("t2c_data.features.platform.cockpit_ops", "build_platform_cockpit_queue_page"),
    "build_platform_cockpit_recommended_actions": ("t2c_data.features.platform.cockpit_ops", "build_platform_cockpit_recommended_actions"),
    "list_platform_domain_events": ("t2c_data.features.platform.events", "list_platform_domain_events"),
    "record_platform_domain_event_from_audit": ("t2c_data.features.platform.events", "record_platform_domain_event_from_audit"),
    "record_platform_domain_event_from_usage": ("t2c_data.features.platform.events", "record_platform_domain_event_from_usage"),
    "serialize_platform_domain_event": ("t2c_data.features.platform.events", "serialize_platform_domain_event"),
    "cockpit_summary": ("t2c_data.features.platform.cockpit", "cockpit_summary"),
    "legacy_api_surface_summary": ("t2c_data.features.platform.legacy_api_surface", "legacy_api_surface_summary"),
    "load_dashboard_profiles_from_read_model": ("t2c_data.features.platform.read_models", "load_dashboard_profiles_from_read_model"),
    "load_dashboard_profiles_with_fallback": ("t2c_data.features.platform.read_models", "load_dashboard_profiles_with_fallback"),
    "load_search_records_from_read_model": ("t2c_data.features.platform.read_models", "load_search_records_from_read_model"),
    "refresh_platform_read_models": ("t2c_data.features.platform.read_models", "refresh_platform_read_models"),
    "refresh_dashboard_asset_read_model": ("t2c_data.features.platform.read_models", "refresh_dashboard_asset_read_model"),
    "refresh_search_read_model": ("t2c_data.features.platform.read_models", "refresh_search_read_model"),
    "filter_visible_table_ids": ("t2c_data.features.platform.visibility", "filter_visible_table_ids"),
    "is_table_visible": ("t2c_data.features.platform.visibility", "is_table_visible"),
    "load_table_visibility_map": ("t2c_data.features.platform.visibility", "load_table_visibility_map"),
    "mask_audit_event_payload": ("t2c_data.features.platform.visibility", "mask_audit_event_payload"),
    "mask_certification_summary_payload": ("t2c_data.features.platform.visibility", "mask_certification_summary_payload"),
    "mask_dashboard_asset_payload": ("t2c_data.features.platform.visibility", "mask_dashboard_asset_payload"),
    "mask_incident_asset_context_payload": ("t2c_data.features.platform.visibility", "mask_incident_asset_context_payload"),
    "mask_privacy_summary_payload": ("t2c_data.features.platform.visibility", "mask_privacy_summary_payload"),
    "mask_search_result_payload": ("t2c_data.features.platform.visibility", "mask_search_result_payload"),
    "mask_table_payload": ("t2c_data.features.platform.visibility", "mask_table_payload"),
    "can_view_sensitive_data": ("t2c_data.features.platform.sensitive_data", "can_view_sensitive_data"),
    "mask_payload_by_policy": ("t2c_data.features.platform.sensitive_data", "mask_payload_by_policy"),
    "mask_row_by_classification": ("t2c_data.features.platform.sensitive_data", "mask_row_by_classification"),
    "mask_sensitive_value": ("t2c_data.features.platform.sensitive_data", "mask_sensitive_value"),
    "redact_sensitive_metadata": ("t2c_data.features.platform.sensitive_data", "redact_sensitive_metadata"),
    "user_can_manage_visibility": ("t2c_data.features.platform.visibility", "user_can_manage_visibility"),
    "visibility_for_profiles": ("t2c_data.features.platform.visibility", "visibility_for_profiles"),
    "visibility_for_search_records": ("t2c_data.features.platform.visibility", "visibility_for_search_records"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    module = import_module(module_name)
    return getattr(module, attr_name)
