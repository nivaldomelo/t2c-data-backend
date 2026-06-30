#!/usr/bin/env bash
set -euo pipefail

export PYTHONUNBUFFERED=1
export PGPASSWORD="${PGPASSWORD:-}"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "[entrypoint] ERROR: DATABASE_URL is empty or unset. Load the root .env file or export the database URL before starting the backend." >&2
  echo "[entrypoint] Expected: the central Postgres container must already be running and reachable as postgres_db:5432, then start the app stack with docker compose -f docker-compose.dev.yml up -d --build" >&2
  exit 1
fi

readarray -t PG_WAIT_ARGS < <(python - <<'PY'
import os
from urllib.parse import urlparse

raw_url = os.environ["DATABASE_URL"]
sqlalchemy_url = raw_url.replace("postgresql+psycopg://", "postgresql://", 1)
parsed = urlparse(sqlalchemy_url)

host = parsed.hostname or "postgres_db"
port = parsed.port or 5432
user = parsed.username or "postgres"
dbname = (parsed.path or "/postgres").lstrip("/") or "postgres"
password = parsed.password or ""

print(host)
print(str(port))
print(user)
print(dbname)
print(password)
PY
)

PG_HOST="${PG_WAIT_ARGS[0]}"
PG_PORT="${PG_WAIT_ARGS[1]}"
PG_USER="${PG_WAIT_ARGS[2]}"
PG_DB="${PG_WAIT_ARGS[3]}"
PG_PASS="${PG_WAIT_ARGS[4]}"

if [[ -n "${PG_PASS}" ]]; then
  export PGPASSWORD="${PG_PASS}"
fi

echo "[entrypoint] Waiting for Postgres at ${PG_HOST}:${PG_PORT} (db=${PG_DB}, user=${PG_USER})..."
for i in $(seq 1 60); do
  if pg_isready -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1; then
    echo "[entrypoint] Postgres is ready"
    break
  fi
  if [[ "$i" -eq 60 ]]; then
    echo "[entrypoint] Postgres did not become ready in time (${PG_HOST}:${PG_PORT})" >&2
    exit 1
  fi
  sleep 1
done

echo "[entrypoint] Verifying Postgres accepts queries at ${PG_HOST}:${PG_PORT}"
for i in $(seq 1 60); do
  if psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 -c 'SELECT 1' >/dev/null 2>&1; then
    echo "[entrypoint] Postgres query probe succeeded"
    break
  fi
  if [[ "$i" -eq 60 ]]; then
    echo "[entrypoint] Postgres query probe failed after waiting for readiness (${PG_HOST}:${PG_PORT})" >&2
    exit 1
  fi
  echo "[entrypoint] Postgres query probe not ready yet; retrying..." >&2
  sleep 1
done

echo "[entrypoint] Ensuring schema ${DB_SCHEMA:-t2c_data}"
python - <<'PY'
import os
from sqlalchemy import create_engine, text

db_url = os.environ["DATABASE_URL"]
schema = os.environ.get("DB_SCHEMA", "t2c_data")
engine = create_engine(db_url)
with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
print(f"[entrypoint] Schema ensured: {schema}")
PY

echo "[entrypoint] Validating Alembic graph"
if ! python scripts/validate_alembic_graph.py; then
  echo "[entrypoint] ERROR: Alembic graph validation failed. Check backend/alembic/versions for duplicate revision ids or missing merge migrations before running the backend." >&2
  exit 1
fi

run_alembic_upgrade() {
  local attempts=5
  local attempt=1
  while [[ "${attempt}" -le "${attempts}" ]]; do
    echo "[entrypoint] Running Alembic migrations (attempt ${attempt}/${attempts})"
    if alembic upgrade head; then
      echo "[entrypoint] Alembic migrations completed"
      return 0
    else
      local exit_code=$?
      echo "[entrypoint] Alembic upgrade failed on attempt ${attempt}/${attempts} with exit code ${exit_code}. Retrying in 5 seconds..." >&2
      sleep 5
    fi
    attempt=$((attempt + 1))
  done
  echo "[entrypoint] ERROR: Alembic migrations failed after ${attempts} attempts." >&2
  return 1
}

if ! run_alembic_upgrade; then
  echo "[entrypoint] ERROR: Alembic migrations failed. The API will not start until the database is migrated successfully." >&2
  exit 1
fi

echo "[entrypoint] Starting API (seed runs on FastAPI startup when ENABLE_DB_SEED=true)"
if [[ $# -gt 0 ]]; then
  exec "$@"
fi
exec uvicorn t2c_data.main:app --host 0.0.0.0 --port 8000 --reload
