# t2c-data-backend

Backend (API) da plataforma **t2c_data** — catálogo, governança, data quality, certificação,
privacidade, observabilidade, linhagem e operação de dados. FastAPI + SQLAlchemy + Alembic, empacotado
para EKS (Helm in-tree) no padrão Turn2C.

> Repositório gerado a partir da separação do monorepo `t2c_data` (ver
> `docs/separacao-backend-frontend.md` no repo original). Pacote Python em **`src/t2c_data/`**.

---

## 🚀 Deploy (DevOps) — variáveis de ambiente e migrações

> **Leitura obrigatória antes do primeiro deploy.** Ambientes: **`develop` → dev**, **`main` → prd**
> (este projeto **não** usa `apc`/apice — isso é do projeto Apice). RDS/Metabase/Spark/S3 são distintos por ambiente.

### O que o DevOps precisa provisionar
- **RDS PostgreSQL vazio por ambiente** (dev e prd) + usuário com **permissão de DDL** — as migrações criam o schema `t2c_data` e todas as tabelas. **SSL obrigatório** (`?sslmode=require`).
- (Se usar operação/observabilidade) banco **operacional de controle** (schema `controle`), **bucket S3** (results do Spark / data lake) e **credenciais AWS**.
- Cluster **Spark** (repo `t2c-data-spark`) alcançável pela rede do EKS.

### Variáveis de ambiente
No Helm entram em **ConfigMap** (`values.config`, não-secretas) e **Secret** (`values.secrets`, valores reais via `secret-values.yaml` gerado no deploy a partir dos GitHub Secrets).

> Spark, Metabase e o banco de controle **também** podem ser ajustados em runtime pela UI (**Administração → Configuração da Plataforma**). As env vars abaixo são o **baseline** de boot. **Só** podem vir de env (nunca da UI): `DATABASE_URL`, `JWT_SECRET_KEY`, `DATASOURCE_SECRET_KEY`.

**Obrigatórias / críticas (prd recusa subir sem):**

| Var | Local | Nota |
|---|---|---|
| `ENV` | Config | `dev` ou `prd` (qualquer valor ≠ dev/local/test = produção → validações estritas). |
| `DATABASE_URL` | Secret | Banco do catálogo. **Sempre** `postgresql+psycopg://user:pass@host:5432/db?sslmode=require`. |
| `JWT_SECRET_KEY` | Secret | Forte, não-default (assina tokens). |
| `DATASOURCE_SECRET_KEY` | Secret | Forte, **≠ JWT**, sem "change-me". ⚠️ Criptografa credenciais de fontes **e todo o blob de Configuração da Plataforma**. Perder/rotacionar sem re-encriptar torna esses dados ilegíveis. |
| `CORS_ALLOW_ORIGINS` | Config | Domínio do frontend (CloudFront), vírgula-separado. **Nunca `*`** em prd. |
| `ENABLE_DB_SEED` | Config | **`false`** em prd. |
| `INITIAL_ADMIN_EMAIL` / `INITIAL_ADMIN_PASSWORD` | Secret | Admin inicial criado pela **migração** de RBAC (senha forte). |

**Banco de controle (read-model, schema `controle`):** `OPERATIONAL_DATABASE_URL` (Secret, com `?sslmode=require`) **ou** `OPERATIONAL_DB_HOST/PORT/NAME/USER` (Config) + `OPERATIONAL_DB_PASSWORD` (Secret) + `OPERATIONAL_DB_SCHEMA` (Config, default `controle`).

**Spark:** `DQ_EXECUTION_ENGINE=spark`, `SPARK_MASTER_URL=spark://t2c-data-spark-master.<ns>.svc.cluster.local:7077`, `SPARK_DRIVER_HOST`, `SPARK_DRIVER_BIND_ADDRESS=0.0.0.0`, `SPARK_RESULTS_DIR` (**use `s3a://bucket/prefixo` em prd**). (Config)

**AWS:** `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (Secret) + `AWS_REGION` (Config).

**Metabase (opcional):** `METABASE_ENABLED`, `METABASE_BASE_URL`, `METABASE_AUTH_TYPE`, `METABASE_AUTH_USERNAME` (Config) + `METABASE_AUTH_SECRET` (Secret).

**Observabilidade / e-mail (opcional):** `LOG_JSON=true` (Config); SMTP: `SMTP_HOST/PORT/USERNAME` (Config) + `SMTP_PASSWORD` (Secret).

Schedulers já vêm em `worker` (correto p/ prd) — não use `embedded_dev_only` fora de dev. Lista completa em [.env.example](.env.example).

### Criação/atualização das tabelas (dev e prd)
Alembic via **hook do Helm** — **não** há auto-migração no boot da API.
1. `git push` (`develop`→dev, `main`→prd) → CI/CD builda imagem → ECR.
2. `helm upgrade -i` aplica em ordem: **ConfigMap + Secret** (hook weight `-10`) → **Job `{app}-migrate`** (`pre-upgrade`, weight `-5`) que roda **`alembic upgrade head`**. `backoffLimit: 0` → **falhou a migração, o deploy aborta** (pods novos não sobem).
3. Só então os Deployments (API + workers) sobem com o schema atualizado.

As migrações **criam o schema `t2c_data` e as tabelas**, semeiam **RBAC + admin inicial** (usa `INITIAL_ADMIN_*`); no 1º boot o app grava os **defaults de referência** de Configuração da Plataforma (não-secretos, criptografados) uma única vez. **dev e prd usam a mesma cadeia de migrações**, mudando apenas o RDS e a `DATASOURCE_SECRET_KEY` de cada ambiente.

---

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

## Dev local completo (API + workers + Spark)
Espelha o stack do monorepo (API, `scan-worker`, `metabase-worker`, `spark-master`, `spark-worker`).
Requer o repo **`t2c-data-spark` como irmão** (`../t2c-data-spark`) e um `.env` local (de `.env.example`).
```bash
cp .env.example .env   # preencha DATABASE_URL (?sslmode=require em prod), segredos, etc.
docker compose -f docker-compose.local.yml up --build
# API em :8000 · Spark UI em :8080 · frontend roda à parte (Vite -> VITE_API_URL=http://localhost:8000/api/v1)
```
Os workers de background (mesma imagem, comandos distintos) e o cluster Spark permitem testar
**DQ/profiling/scan** de ponta a ponta. Em produção (EKS) esses processos são Deployments do Helm
(`scan-worker`/`metabase-worker`) e o Spark é o cluster do repo `t2c-data-spark`.

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
