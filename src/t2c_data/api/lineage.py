from fastapi import APIRouter

from t2c_data.api.lineage_routes import (
    assets_router,
    columns_router,
    events_router,
    graph_router,
    import_export_router,
    relations_router,
    sources_router,
    specs_router,
)

router = APIRouter(prefix="/lineage")
router.include_router(import_export_router)
router.include_router(sources_router)
router.include_router(assets_router)
router.include_router(columns_router)
router.include_router(graph_router)
router.include_router(events_router)
router.include_router(relations_router)
router.include_router(specs_router)
