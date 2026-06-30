"""Dashboard feature package with lazy compatibility exports."""


def get_dashboard_summary(*args, **kwargs):
    from t2c_data.features.dashboard.queries import get_dashboard_summary as _impl

    return _impl(*args, **kwargs)


def build_platform_strategic_summary(*args, **kwargs):
    from t2c_data.features.dashboard.strategy_queries import build_platform_strategic_summary as _impl

    return _impl(*args, **kwargs)


def normalize_filters(*args, **kwargs):
    from t2c_data.features.dashboard.executive_queries import normalize_filters as _impl

    return _impl(*args, **kwargs)


def get_dashboard_executive_summary(*args, **kwargs):
    from t2c_data.features.dashboard.executive_queries import get_dashboard_executive_summary as _impl

    return _impl(*args, **kwargs)


def get_dashboard_executive_top_critical(*args, **kwargs):
    from t2c_data.features.dashboard.executive_queries import get_dashboard_executive_top_critical as _impl

    return _impl(*args, **kwargs)


def get_dashboard_executive_certification(*args, **kwargs):
    from t2c_data.features.dashboard.executive_queries import get_dashboard_executive_certification as _impl

    return _impl(*args, **kwargs)


def get_dashboard_executive_governance_gaps(*args, **kwargs):
    from t2c_data.features.dashboard.executive_queries import get_dashboard_executive_governance_gaps as _impl

    return _impl(*args, **kwargs)


def get_dashboard_executive_dq(*args, **kwargs):
    from t2c_data.features.dashboard.executive_queries import get_dashboard_executive_dq as _impl

    return _impl(*args, **kwargs)


def get_dashboard_executive_incidents(*args, **kwargs):
    from t2c_data.features.dashboard.executive_queries import get_dashboard_executive_incidents as _impl

    return _impl(*args, **kwargs)


def get_dashboard_executive_risk(*args, **kwargs):
    from t2c_data.features.dashboard.executive_queries import get_dashboard_executive_risk as _impl

    return _impl(*args, **kwargs)


def get_dashboard_executive_asset_details(*args, **kwargs):
    from t2c_data.features.dashboard.executive_queries import get_dashboard_executive_asset_details as _impl

    return _impl(*args, **kwargs)


def get_dashboard_executive_campaign_queue(*args, **kwargs):
    from t2c_data.features.dashboard.executive_queries import get_dashboard_executive_campaign_queue as _impl

    return _impl(*args, **kwargs)


__all__ = [
    "get_dashboard_summary",
    "build_platform_strategic_summary",
    "normalize_filters",
    "get_dashboard_executive_summary",
    "get_dashboard_executive_top_critical",
    "get_dashboard_executive_certification",
    "get_dashboard_executive_governance_gaps",
    "get_dashboard_executive_dq",
    "get_dashboard_executive_incidents",
    "get_dashboard_executive_risk",
    "get_dashboard_executive_asset_details",
    "get_dashboard_executive_campaign_queue",
]
