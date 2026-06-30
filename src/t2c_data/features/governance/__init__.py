"""Governance feature package with lazy compatibility exports."""


def get_governance_campaigns(*args, **kwargs):
    from t2c_data.features.governance.queries import get_governance_campaigns as _impl

    return _impl(*args, **kwargs)


def get_governance_campaign_queue(*args, **kwargs):
    from t2c_data.features.governance.queries import get_governance_campaign_queue as _impl

    return _impl(*args, **kwargs)


def get_governance_critical_changes(*args, **kwargs):
    from t2c_data.features.governance.queries import get_governance_critical_changes as _impl

    return _impl(*args, **kwargs)


def get_governance_review_summary(*args, **kwargs):
    from t2c_data.features.governance.queries import get_governance_review_summary as _impl

    return _impl(*args, **kwargs)


def get_governance_pending_center(*args, **kwargs):
    from t2c_data.features.governance.queries import get_governance_pending_center as _impl

    return _impl(*args, **kwargs)


def get_governance_pending_center_summary(*args, **kwargs):
    from t2c_data.features.governance.queries import get_governance_pending_center_summary as _impl

    return _impl(*args, **kwargs)


def get_governance_pending_center_summary_light(*args, **kwargs):
    from t2c_data.features.governance.queries import get_governance_pending_center_summary_light as _impl

    return _impl(*args, **kwargs)


def get_governance_pending_center_campaigns(*args, **kwargs):
    from t2c_data.features.governance.queries import get_governance_pending_center_campaigns as _impl

    return _impl(*args, **kwargs)


def get_governance_pending_center_queue(*args, **kwargs):
    from t2c_data.features.governance.queries import get_governance_pending_center_queue as _impl

    return _impl(*args, **kwargs)


def get_governance_classification_review(*args, **kwargs):
    from t2c_data.features.governance.classification_review import get_governance_classification_review as _impl

    return _impl(*args, **kwargs)


def promote_governance_classification_review_tables(*args, **kwargs):
    from t2c_data.features.governance.classification_review import promote_governance_classification_review_tables as _impl

    return _impl(*args, **kwargs)


def get_governance_recommendations(*args, **kwargs):
    from t2c_data.features.governance.recommendations import get_governance_recommendations as _impl

    return _impl(*args, **kwargs)


def get_governance_recommendation_context(*args, **kwargs):
    from t2c_data.features.governance.recommendations import get_governance_recommendation_context as _impl

    return _impl(*args, **kwargs)


def get_governance_notification_summary(*args, **kwargs):
    from t2c_data.features.governance.notifications import get_governance_notification_summary as _impl

    return _impl(*args, **kwargs)


def get_governance_playbooks(*args, **kwargs):
    from t2c_data.features.governance.playbooks import get_governance_playbooks as _impl

    return _impl(*args, **kwargs)


def list_asset_slas(*args, **kwargs):
    from t2c_data.features.governance.change_management import list_asset_slas as _impl

    return _impl(*args, **kwargs)


def upsert_asset_sla(*args, **kwargs):
    from t2c_data.features.governance.change_management import upsert_asset_sla as _impl

    return _impl(*args, **kwargs)


def list_metadata_change_requests(*args, **kwargs):
    from t2c_data.features.governance.change_management import list_metadata_change_requests as _impl

    return _impl(*args, **kwargs)


def create_metadata_change_request(*args, **kwargs):
    from t2c_data.features.governance.change_management import create_metadata_change_request as _impl

    return _impl(*args, **kwargs)


def get_metadata_change_request(*args, **kwargs):
    from t2c_data.features.governance.change_management import get_metadata_change_request as _impl

    return _impl(*args, **kwargs)


def transition_metadata_change_request(*args, **kwargs):
    from t2c_data.features.governance.change_management import transition_metadata_change_request as _impl

    return _impl(*args, **kwargs)


def review_metadata_change_request(*args, **kwargs):
    from t2c_data.features.governance.change_management import review_metadata_change_request as _impl

    return _impl(*args, **kwargs)


def approve_metadata_change_request(*args, **kwargs):
    from t2c_data.features.governance.change_management import approve_metadata_change_request as _impl

    return _impl(*args, **kwargs)


def apply_metadata_change_request(*args, **kwargs):
    from t2c_data.features.governance.change_management import apply_metadata_change_request as _impl

    return _impl(*args, **kwargs)


def reject_metadata_change_request(*args, **kwargs):
    from t2c_data.features.governance.change_management import reject_metadata_change_request as _impl

    return _impl(*args, **kwargs)


def build_governance_recommendation_assistant_payload(*args, **kwargs):
    from t2c_data.features.governance.assistant import build_governance_recommendation_assistant_payload as _impl

    return _impl(*args, **kwargs)


def execute_governance_assistant_action(*args, **kwargs):
    from t2c_data.features.governance.assistant import execute_governance_assistant_action as _impl

    return _impl(*args, **kwargs)


def set_governance_recommendation_feedback(*args, **kwargs):
    from t2c_data.features.governance.assistant import set_governance_recommendation_feedback as _impl

    return _impl(*args, **kwargs)


def get_governance_notifications(*args, **kwargs):
    from t2c_data.features.governance.notifications import get_governance_notifications as _impl

    return _impl(*args, **kwargs)


def refresh_governance_notifications(*args, **kwargs):
    from t2c_data.features.governance.notifications import refresh_governance_notifications as _impl

    return _impl(*args, **kwargs)


def refresh_governance_trust_snapshots(*args, **kwargs):
    from t2c_data.features.governance.trust_history import refresh_governance_trust_snapshots as _impl

    return _impl(*args, **kwargs)


def refresh_governance_recommendations(*args, **kwargs):
    from t2c_data.features.governance.recommendations import refresh_governance_recommendations as _impl

    return _impl(*args, **kwargs)


def resolve_governance_recommendations(*args, **kwargs):
    from t2c_data.features.governance.recommendations import resolve_governance_recommendations as _impl

    return _impl(*args, **kwargs)


def apply_governance_policy_recommendations(*args, **kwargs):
    from t2c_data.features.governance.recommendations import apply_governance_policy_recommendations as _impl

    return _impl(*args, **kwargs)


def mark_owner_review(*args, **kwargs):
    from t2c_data.features.governance.queries import mark_owner_review as _impl

    return _impl(*args, **kwargs)


def mark_privacy_review(*args, **kwargs):
    from t2c_data.features.governance.queries import mark_privacy_review as _impl

    return _impl(*args, **kwargs)


def get_governance_settings_snapshot(*args, **kwargs):
    from t2c_data.features.governance.settings import get_governance_settings_snapshot as _impl

    return _impl(*args, **kwargs)


def get_ownership_summary(*args, **kwargs):
    from t2c_data.features.governance.owners_summary import get_ownership_summary as _impl

    return _impl(*args, **kwargs)


def get_ownership_export_rows(*args, **kwargs):
    from t2c_data.features.governance.owners_summary import get_ownership_export_rows as _impl

    return _impl(*args, **kwargs)


def get_ownership_delete_impact(*args, **kwargs):
    from t2c_data.features.governance.owners_summary import get_ownership_delete_impact as _impl

    return _impl(*args, **kwargs)


def get_ownership_reassign_preview(*args, **kwargs):
    from t2c_data.features.governance.owners_summary import get_ownership_reassign_preview as _impl

    return _impl(*args, **kwargs)


def reassign_ownership_assets(*args, **kwargs):
    from t2c_data.features.governance.owners_summary import reassign_ownership_assets as _impl

    return _impl(*args, **kwargs)


def get_or_create_governance_settings(*args, **kwargs):
    from t2c_data.features.governance.settings import get_or_create_governance_settings as _impl

    return _impl(*args, **kwargs)


def get_effective_legacy_api_disabled_modules(*args, **kwargs):
    from t2c_data.features.governance.settings import get_effective_legacy_api_disabled_modules as _impl

    return _impl(*args, **kwargs)


from t2c_data.features.governance.settings import GovernanceSettingsSnapshot

__all__ = [
    "get_governance_campaigns",
    "get_governance_campaign_queue",
    "get_governance_critical_changes",
    "get_governance_pending_center",
    "get_governance_pending_center_summary",
    "get_governance_pending_center_summary_light",
    "get_governance_pending_center_campaigns",
    "get_governance_pending_center_queue",
    "get_governance_classification_review",
    "get_governance_recommendation_context",
    "get_governance_recommendations",
    "get_governance_notification_summary",
    "get_governance_notifications",
    "get_governance_playbooks",
    "build_governance_recommendation_assistant_payload",
    "execute_governance_assistant_action",
    "get_governance_review_summary",
    "get_ownership_summary",
    "get_ownership_reassign_preview",
    "reassign_ownership_assets",
    "GovernanceSettingsSnapshot",
    "get_effective_legacy_api_disabled_modules",
    "get_governance_settings_snapshot",
    "get_or_create_governance_settings",
    "list_asset_slas",
    "upsert_asset_sla",
    "list_metadata_change_requests",
    "create_metadata_change_request",
    "get_metadata_change_request",
    "transition_metadata_change_request",
    "review_metadata_change_request",
    "approve_metadata_change_request",
    "apply_metadata_change_request",
    "reject_metadata_change_request",
    "promote_governance_classification_review_tables",
    "mark_owner_review",
    "mark_privacy_review",
    "refresh_governance_recommendations",
    "refresh_governance_notifications",
    "refresh_governance_trust_snapshots",
    "set_governance_recommendation_feedback",
    "apply_governance_policy_recommendations",
    "resolve_governance_recommendations",
]
