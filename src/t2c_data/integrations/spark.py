from __future__ import annotations

import logging
import os
import subprocess
import tempfile
import tarfile
from dataclasses import dataclass
from pathlib import Path
from time import monotonic

from t2c_data.core.redaction import format_command_for_log

logger = logging.getLogger(__name__)
DEFAULT_SPARK_REDACTION_REGEX = r"(?i)secret|password|token|access[.]?key|api[.]?key|credential|authorization|jdbc|aws_secret_access_key|aws_session_token"


class SparkSubmitError(RuntimeError):
    pass


@dataclass(frozen=True)
class SparkSubmitConfig:
    submit_bin: str
    master_url: str
    jobs_dir: str
    local_jars_dir: str
    local_jars_cache_dir: str
    packages: str
    packages_enabled: bool
    results_dir: str
    driver_host: str
    driver_bind_address: str
    driver_memory: str
    executor_memory: str
    redaction_regex: str
    timeout_seconds: int
    auth_secret: str | None = None

    def job_path(self, job_filename: str) -> str:
        return f"{self.jobs_dir.rstrip('/')}/{job_filename}"

    def ensure_results_dir(self) -> Path:
        path = Path(self.results_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def temporary_result_file(self, *, job_type: str, job_run_id: int) -> Path:
        tmp = tempfile.NamedTemporaryFile(prefix=f"{job_type}-run-{job_run_id}-", suffix=".json", delete=False)
        path = Path(tmp.name)
        tmp.close()
        return path

    def _extract_archive_jars(self, archive_path: Path, extraction_dir: Path) -> list[Path]:
        extraction_dir.mkdir(parents=True, exist_ok=True)
        extracted_paths: list[Path] = []
        with tarfile.open(archive_path, "r:gz") as archive:
            for member in archive.getmembers():
                if not member.isfile():
                    continue
                if not member.name.lower().endswith(".jar"):
                    continue
                target_path = extraction_dir / Path(member.name).name
                if not target_path.exists():
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        continue
                    target_path.write_bytes(extracted.read())
                extracted_paths.append(target_path)
        return extracted_paths

    def resolve_local_jars(self) -> list[str]:
        source_dir = Path(self.local_jars_dir)
        if not source_dir.exists():
            return []

        cache_dir = Path(self.local_jars_cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)

        jar_paths: set[str] = set()
        for jar_path in sorted(source_dir.rglob("*.jar")):
            if jar_path.is_file():
                jar_paths.add(str(jar_path.resolve()))

        for archive_path in sorted(
            path for path in source_dir.rglob("*") if path.is_file() and path.name.lower().endswith((".tar.gz", ".tgz"))
        ):
            archive_name = archive_path.name[:-7] if archive_path.name.lower().endswith(".tar.gz") else archive_path.stem
            extraction_dir = cache_dir / archive_name
            extracted = self._extract_archive_jars(archive_path, extraction_dir)
            for jar_path in extracted:
                jar_paths.add(str(jar_path.resolve()))

        return sorted(jar_paths)

    def resolve_py_files(self, job_filename: str) -> list[str]:
        """Módulos Python irmãos do job (ex.: dq_common.py) enviados aos executores via --py-files.

        Assim o cluster Spark permanece genérico (não precisa dos jobs do t2c_data na imagem);
        o executor recebe as dependências no classpath Python. Exclui o próprio job (o spark-submit
        já distribui o arquivo principal)."""
        source = Path(self.jobs_dir)
        if not source.exists():
            return []
        primary = job_filename.strip()
        return sorted(
            str(path.resolve())
            for path in source.glob("*.py")
            if path.is_file() and path.name != primary
        )


def get_spark_submit_config() -> SparkSubmitConfig:
    return SparkSubmitConfig(
        submit_bin=os.getenv("SPARK_SUBMIT_BIN", "spark-submit"),
        master_url=os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
        jobs_dir=os.getenv("SPARK_JOBS_DIR", "/opt/spark/jobs"),
        local_jars_dir=os.getenv("SPARK_LOCAL_JARS_DIR", "/app/jars"),
        local_jars_cache_dir=os.getenv("SPARK_LOCAL_JARS_CACHE_DIR", "/tmp/spark-local-jars"),
        packages=os.getenv("SPARK_PACKAGES", "org.postgresql:postgresql:42.7.4"),
        packages_enabled=os.getenv("SPARK_PACKAGES_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"},
        results_dir=os.getenv("SPARK_RESULTS_DIR", "/data/spark-results"),
        driver_host=os.getenv("SPARK_DRIVER_HOST", "backend"),
        driver_bind_address=os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"),
        driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "1g"),
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "1g"),
        redaction_regex=os.getenv("SPARK_REDACTION_REGEX", DEFAULT_SPARK_REDACTION_REGEX),
        timeout_seconds=int(os.getenv("SPARK_SUBMIT_TIMEOUT_SECONDS", "900")),
        auth_secret=(os.getenv("SPARK_AUTH_SECRET") or "").strip() or None,
    )


class SparkSubmitRunner:
    def __init__(self, config: SparkSubmitConfig) -> None:
        self.config = config

    def build_command(self, job_filename: str, args: list[str]) -> list[str]:
        command = [
            self.config.submit_bin,
            "--master",
            self.config.master_url,
            "--conf",
            f"spark.driver.host={self.config.driver_host}",
            "--conf",
            f"spark.driver.bindAddress={self.config.driver_bind_address}",
            "--conf",
            f"spark.redaction.regex={self.config.redaction_regex}",
            *(
                ["--conf", "spark.authenticate=true", "--conf", f"spark.authenticate.secret={self.config.auth_secret}"]
                if self.config.auth_secret
                else []
            ),
            "--driver-memory",
            self.config.driver_memory,
            "--executor-memory",
            self.config.executor_memory,
        ]
        local_jars = self.config.resolve_local_jars()
        if local_jars:
            command.extend(["--jars", ",".join(local_jars)])
        elif self.config.packages_enabled and self.config.packages.strip():
            command.extend(["--packages", self.config.packages])
        # Envia módulos irmãos (dq_common) aos executores — cluster Spark permanece genérico.
        py_files = self.config.resolve_py_files(job_filename)
        if py_files:
            command.extend(["--py-files", ",".join(py_files)])
        command.extend([self.config.job_path(job_filename), *args])
        return command

    def run(self, job_filename: str, args: list[str], *, timeout_seconds: int | None = None) -> subprocess.CompletedProcess[str]:
        effective_timeout = timeout_seconds or self.config.timeout_seconds
        cmd = self.build_command(job_filename, args)
        started_at = monotonic()
        logger.info(
            "spark_submit_start job_filename=%s timeout_seconds=%s master_url=%s command=%s",
            job_filename,
            effective_timeout,
            self.config.master_url,
            format_command_for_log(cmd),
        )
        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=effective_timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            duration_ms = int((monotonic() - started_at) * 1000)
            logger.error(
                "spark_submit_timeout job_filename=%s duration_ms=%s timeout_seconds=%s",
                job_filename,
                duration_ms,
                effective_timeout,
            )
            raise SparkSubmitError(
                f"spark-submit timed out after {effective_timeout}s for job {job_filename}"
            ) from exc
        duration_ms = int((monotonic() - started_at) * 1000)
        logger.info(
            "spark_submit_finish job_filename=%s duration_ms=%s return_code=%s",
            job_filename,
            duration_ms,
            completed.returncode,
        )
        return completed
