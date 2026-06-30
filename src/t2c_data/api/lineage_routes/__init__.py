from t2c_data.api.lineage_routes.events import router as events_router
from t2c_data.api.lineage_routes.assets import router as assets_router
from t2c_data.api.lineage_routes.columns import router as columns_router
from t2c_data.api.lineage_routes.graph import router as graph_router
from t2c_data.api.lineage_routes.import_export import router as import_export_router
from t2c_data.api.lineage_routes.relations import router as relations_router
from t2c_data.api.lineage_routes.sources import router as sources_router
from t2c_data.api.lineage_routes.specs import router as specs_router

__all__ = [
    "assets_router",
    "columns_router",
    "events_router",
    "graph_router",
    "import_export_router",
    "relations_router",
    "sources_router",
    "specs_router",
]
