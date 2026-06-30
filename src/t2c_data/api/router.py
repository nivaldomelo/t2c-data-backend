from t2c_data.api.admin import router as admin_router
from t2c_data.api.asset_intelligence import router as asset_intelligence_router
from t2c_data.api.assistant import router as assistant_router
from t2c_data.api.activity import router as activity_router
from t2c_data.api.audit import router as audit_router
from t2c_data.api.auth import router as auth_router
from t2c_data.api.catalog import router as catalog_router
from t2c_data.api.certification import router as certification_router
from t2c_data.api.collaboration import router as collaboration_router
from t2c_data.api.data_owners import router as data_owners_router
from t2c_data.api.dashboard import router as dashboard_router
from t2c_data.api.datasource import router as datasource_router
from t2c_data.api.dq import router as dq_router
from t2c_data.api.external import router as external_router
from t2c_data.api.exports import router as exports_router
from t2c_data.api.glossary import router as glossary_router
from t2c_data.api.governance import router as governance_router
from t2c_data.api.home import router as home_router
from t2c_data.api.integrations import router as integrations_router
from t2c_data.api.ingestion import router as ingestion_router
from t2c_data.api.import_export import router as io_router
from t2c_data.api.incidents import router as incidents_router
from t2c_data.api.lineage import router as lineage_router
from t2c_data.api.metrics import router as metrics_router
from t2c_data.api.contracts import router as contracts_router
from t2c_data.api.metabase import router as metabase_router
from t2c_data.api.notifications import router as notifications_router
from t2c_data.api.operations import router as operations_router
from t2c_data.api.me import router as me_router
from t2c_data.api.privacy_access import router as privacy_access_router
from t2c_data.api.platform import router as platform_router
from t2c_data.api.scan import router as scan_router
from t2c_data.api.search import router as search_router
from t2c_data.api.semantic import router as semantic_router
from t2c_data.api.stewardship import router as stewardship_router
from t2c_data.api.system import router as system_router
from t2c_data.api.table_metadata import router as table_metadata_router
from t2c_data.api.tags import router as tags_router
from fastapi import APIRouter

# Router composition follows the architectural boundaries documented in
# `docs/api-contract.md`:
# - catalog read surfaces
# - metadata mutations
# - operations/runtime
# - governance/compliance
# - admin/system

CATALOG_READ_ROUTERS = [
    system_router,
    catalog_router,
    search_router,
    dashboard_router,
    home_router,
    me_router,
    metrics_router,
    contracts_router,
    integrations_router,
    metabase_router,
    operations_router,
    external_router,
    activity_router,
    asset_intelligence_router,
    exports_router,
]

METADATA_MUTATION_ROUTERS = [
    table_metadata_router,
    tags_router,
    glossary_router,
    data_owners_router,
]

OPERATIONS_ROUTERS = [
    datasource_router,
    scan_router,
    ingestion_router,
    platform_router,
    notifications_router,
]

GOVERNANCE_ROUTERS = [
    governance_router,
    stewardship_router,
    certification_router,
    collaboration_router,
    privacy_access_router,
    dq_router,
    incidents_router,
    lineage_router,
    audit_router,
    semantic_router,
    io_router,
    assistant_router,
]

ADMIN_SYSTEM_ROUTERS = [
    admin_router,
    auth_router,
]

def _build_api_v1_router() -> APIRouter:
    router = APIRouter()
    for group in (
        ADMIN_SYSTEM_ROUTERS,
        CATALOG_READ_ROUTERS,
        METADATA_MUTATION_ROUTERS,
        OPERATIONS_ROUTERS,
        GOVERNANCE_ROUTERS,
    ):
        for feature_router in group:
            router.include_router(feature_router)
    return router


api_v1_router = _build_api_v1_router()


@api_v1_router.get("/ping", response_model=dict[str, str], tags=["system"])
def ping() -> dict[str, str]:
    return {"message": "pong"}
