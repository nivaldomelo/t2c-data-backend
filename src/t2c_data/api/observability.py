"""Probes de observabilidade no nível raiz (sem prefixo /api/v1), sem autenticação.

Padrão Turn2C (infra-standards.md → Probes): `startupProbe`/`livenessProbe` em `/liveness`
(processo vivo, sem tocar dependências) e `readinessProbe` em `/readiness` (checa o banco de
estado, 503 se indisponível). `/health` é alias do liveness para o sintético (Zabbix).
`/metrics` (Prometheus) é exposto em `main.py` via prometheus-fastapi-instrumentator.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db

router = APIRouter(tags=["observability"])


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.get("/liveness", include_in_schema=False)
def liveness() -> dict[str, str]:
    """Processo vivo. Não toca dependências (compartilhado com o startupProbe)."""
    return {"status": "alive", "timestamp": _now()}


@router.get("/health", include_in_schema=False)
def health_alias() -> dict[str, str]:
    """Alias de liveness para o sintético (Zabbix), sem autenticação."""
    return {"status": "ok", "timestamp": _now()}


@router.get("/readiness", include_in_schema=False)
def readiness(response: Response, db: Session = Depends(get_db)) -> dict[str, str]:
    """Pronto para tráfego: valida o banco de estado (SELECT 1). 503 se indisponível.

    Probe leve, distinto do `/api/v1/ready` (diagnóstico detalhado para operação).
    """
    try:
        db.execute(text("SELECT 1"))
    except Exception:  # noqa: BLE001
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not_ready", "timestamp": _now()}
    return {"status": "ready", "timestamp": _now()}
