from t2c_data.api.admin_routes.access_control import router as access_control_router
from t2c_data.api.admin_routes.governance import router as governance_router
from t2c_data.api.admin_routes.permissions import router as permissions_router
from t2c_data.api.admin_routes.platform_config import router as platform_config_router
from t2c_data.api.admin_routes.roles import router as roles_router
from t2c_data.api.admin_routes.user_audit import router as user_audit_router
from t2c_data.api.admin_routes.users import router as users_router

__all__ = [
    "access_control_router",
    "governance_router",
    "permissions_router",
    "platform_config_router",
    "roles_router",
    "user_audit_router",
    "users_router",
]
