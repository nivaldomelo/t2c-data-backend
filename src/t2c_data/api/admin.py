from __future__ import annotations

from fastapi import APIRouter

from t2c_data.api.admin_routes import (
    access_control_router,
    governance_router,
    permissions_router,
    platform_config_router,
    roles_router,
    user_audit_router,
    users_router,
)

router = APIRouter(prefix="/admin", tags=["admin"])
router.include_router(users_router)
router.include_router(roles_router)
router.include_router(permissions_router)
router.include_router(access_control_router)
router.include_router(governance_router)
router.include_router(platform_config_router)
router.include_router(user_audit_router)
