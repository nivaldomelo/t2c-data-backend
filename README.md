# t2c-data-backend

Backend (API) da plataforma **t2c_data** — catálogo, governança, data quality, certificação,
privacidade, observabilidade, linhagem e operação de dados. FastAPI + SQLAlchemy + Alembic, empacotado
para EKS (Helm in-tree) no padrão Turn2C.

> Repositório gerado a partir da separação do monorepo `t2c_data` (ver
> `docs/separacao-backend-frontend.md` no repo original). Pacote Python em **`src/t2c_data/`**.

## Stack
- Python 3.12, **FastAPI**, **SQLAlchemy 2.0**, **Alembic**
- PostgreSQL (schema `t2c_data`); Spark para DQ/profiling; integrações Metabase, Data Lake (S3), Airflow (read-model)
- Auth JWT (Bearer); RBAC (admin/editor/viewer/stewardship/data_owner)

## Layout
```
src/t2c_data/      app (api/ features/ models/ core/)
alembic/           migrations            scripts/ (workers, seeds, manutenção)
tests/             pytest
.helm/             Helm chart in-tree (Deployment api + workers, Ingress, HPA, PDB, ServiceMonitor, migrate hook)
.github/workflows/ cicd.yaml (build → ECR → helm upgrade)
Dockerfile         imagem de produção (non-root 1001)
infra.yaml         descritor da app (name t2c-data-backend, python 3.12)
```

## Rodar localmente
```bash
# 1) Instalar (layout src)
pip install -e ".[dev]"
# 2) Variáveis (ver .env.example) — DATABASE_URL é obrigatório
export DATABASE_URL='postgresql+psycopg://user:pass@localhost:5432/t2c_data'
export ENV=dev
# 3) Migrations
alembic upgrade head
# 4) API
uvicorn t2c_data.main:app --reload --port 8000
```
Workers (background): `python scripts/run_platform_job_worker.py --source datasource --job-type scan`
e `python scripts/run_metabase_worker.py`.

## Variáveis de ambiente
Ver [.env.example](.env.example). Principais: `DATABASE_URL` (RDS **com `?sslmode=require`** em prd/apc),
`DB_SCHEMA`, `ENV` (`dev|prd|apc`), `JWT_SECRET_KEY`, `DATASOURCE_SECRET_KEY`, `CORS_ALLOW_ORIGINS`
(domínio do frontend; nunca `*` em prod), `DQ_EXECUTION_ENGINE=spark`, `SPARK_*`, `AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_REGION`
(via Secret), `LOG_JSON=true` (cluster), `AIRFLOW_SOURCE_SCHEMA`, `METABASE_*`. **Nunca versionar segredos.**

## Testes
```bash
ruff check src tests && mypy && pytest
```

## Docker
```bash
docker build -f Dockerfile -t t2c-data-backend:local .
# imagem non-root (uid 1001), sem auto-migrate, CMD uvicorn t2c_data.main:app
```

## Health checks (sem auth, nível raiz)
- `GET /liveness` — processo vivo (startup/liveness probe; alias `GET /health`)
- `GET /readiness` — `SELECT 1` no banco (503 se indisponível)
- `GET /metrics` — Prometheus (prometheus-fastapi-instrumentator)
- `GET /api/v1/ready/detailed` — diagnóstico operacional completo (admin)

## Kubernetes / EKS (Helm in-tree)
Chart em `.helm/`. Deploy via pipeline (`.github/workflows/cicd.yaml`):
`build (ruff/mypy/pytest) → docker → ECR ({env}-{sha}) → helm upgrade -i -n {env}-app`.
Migração Alembic roda como **hook `pre-upgrade`** (não nos pods da API). Banco = **RDS gerenciado**;
AWS via **chaves no Secret** (IRSA é direção futura). Ingress ALB em `{appName}.{domain}`.

> DevOps: validar o chart (`helm lint`/`helm template`) e o `cicd.yaml` contra `new-app-template`/`t2c-drift-guard`.

## Integrações externas
PostgreSQL (RDS), S3/Data Lake (credenciais por conexão no app), Spark (DQ), Metabase (sync + linhagem de
consumo), Airflow (read-model de operação — ver `docs/instalacao-produtiva-airflow-openlineage-t2c-data.md`
no repo de documentação), OpenLineage (ingestão push/pull).

## Segurança
- Segredos nunca em texto puro (`ALLOW_PLAINTEXT_SECRETS=false` em prod); via Secret/`secret-values.yaml` runtime.
- CORS restrito por env; auth Bearer; container non-root + `readOnlyRootFilesystem`.
- Frontend é um repositório separado (`t2c-data-frontend`, S3/CloudFront) — consome esta API por URL.
