"""DQ worker: dedicated long-running process for Data Quality scheduling/execution.

Em produção os schedulers de DQ rodam em modo "worker" (DQ_SCHEDULER_MODE / DQ_PROFILING_SCHEDULER_MODE),
o que faz a API PULAR o loop embutido (embedded_scheduler_allowed só é True em dev). Este processo é
quem de fato executa os ciclos em produção:

  1) run_dq_profiling_scheduler_cycle — despacha profilings agendados que venceram, e
  2) run_dq_scheduler_cycle          — executa/enfileira regras de DQ agendadas.

Cada ciclo enfileira o trabalho via threads daemon (services.dq_spark), então o DRIVER do
spark-submit (client-mode) roda NESTE pod — por isso o Deployment dq-worker é dimensionado para
hospedar N drivers concorrentes (ver .helm/values.yaml). Mantenha o paralelismo de profiling por
schema (schema_concurrency) coerente com a memória do pod e com spark.cores.max do cluster.

Roda como um único processo dedicado (estável em dev e prod, ao contrário do --reload da API).
"""

from __future__ import annotations

import logging
import time

from app.features.data_quality.profiling_scheduler import run_dq_profiling_scheduler_cycle
from app.features.data_quality.scheduler import run_dq_scheduler_cycle

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("dq_worker")

POLL_SECONDS = 30


def main() -> None:
    logger.info("dq worker started (profiling + rules scheduler cycles), poll=%ss", POLL_SECONDS)
    while True:
        try:
            result = run_dq_profiling_scheduler_cycle(trigger="worker")
            queued = result.get("queued")
            if queued:
                logger.info("profiling scheduler queued %s run(s)", queued)
        except Exception:  # noqa: BLE001
            logger.exception("dq profiling scheduler cycle failed")

        try:
            result = run_dq_scheduler_cycle(trigger="worker")
            queued = result.get("queued")
            if queued:
                logger.info("rules scheduler queued %s run(s)", queued)
        except Exception:  # noqa: BLE001
            logger.exception("dq rules scheduler cycle failed")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
