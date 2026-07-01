# Production image — Turn2C standard (non-root uid 1001, no `COPY . .`, readOnlyRootFilesystem-friendly).
# Migrations do NOT run here — they run as a Helm pre-upgrade hook Job.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# build-essential: fallback p/ deps sem wheel; openjdk + postgresql-client: necessários ao engine Spark/DQ.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    postgresql-client \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1001 appuser && useradd --uid 1001 --gid 1001 --create-home appuser

# Dependências primeiro (cache de layer).
COPY requirements.lock.txt pyproject.toml uv.lock ./
RUN pip install --upgrade pip \
    && pip install -r requirements.lock.txt \
    && pip install pyspark==3.5.1

# Artefatos de aplicação — copiados EXPLICITAMENTE (proibido `COPY . .` no padrão Turn2C).
COPY alembic.ini ./alembic.ini
COPY src ./src
COPY alembic ./alembic
COPY scripts ./scripts
# Jobs PySpark (lado driver) + drivers JDBC (--jars): SPARK_JOBS_DIR=/opt/spark/jobs, SPARK_LOCAL_JARS_DIR=/app/jars.
# (Executores no cluster Spark precisam de dq_common via mount/--py-files — ver nota de produção no README.)
COPY jars ./jars
COPY spark-jobs /opt/spark/jobs

RUN pip install . \
    && chown -R appuser:appuser /app /opt/spark/jobs

EXPOSE 8000

# Liveness sem auth, sem prefixo /api/v1 (padrão Turn2C).
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/liveness', timeout=3)"

USER 1001

# API de produção — sem --reload e sem migração (a migração é hook pre-upgrade do Helm).
# Os Deployments de scheduler/workers sobrescrevem o command no chart.
CMD ["uvicorn", "t2c_data.main:app", "--host", "0.0.0.0", "--port", "8000"]
