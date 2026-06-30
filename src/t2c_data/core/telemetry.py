from __future__ import annotations

from collections import Counter, deque
from statistics import mean
from threading import Lock
from time import monotonic
from typing import Any


class RuntimeMetrics:
    def __init__(self, *, duration_window_size: int = 512, route_window_size: int = 128) -> None:
        self._lock = Lock()
        self._started_at = monotonic()
        self._in_flight_requests = 0
        self._total_requests = 0
        self._client_error_requests = 0
        self._server_error_requests = 0
        self._request_methods: Counter[str] = Counter()
        self._status_families: Counter[str] = Counter()
        self._durations_ms: deque[float] = deque(maxlen=duration_window_size)
        self._route_totals: Counter[str] = Counter()
        self._route_statuses: Counter[tuple[str, str]] = Counter()
        self._route_durations_ms: dict[str, deque[float]] = {}
        self._route_window_size = route_window_size
        self._rate_limit_hits: Counter[str] = Counter()
        self._job_runs: Counter[str] = Counter()
        self._job_failures: Counter[str] = Counter()
        self._job_statuses: Counter[tuple[str, str]] = Counter()
        self._job_durations_ms: dict[str, deque[float]] = {}
        self._diagnostic_events: Counter[tuple[str, str, str]] = Counter()
        self._internal_alerts: Counter[tuple[str, str, str]] = Counter()
        self._export_events: Counter[tuple[str, str, str]] = Counter()
        self._api_auth_events: Counter[str] = Counter()

    def request_started(self, *, method: str) -> None:
        with self._lock:
            self._in_flight_requests += 1
            self._request_methods[method.upper()] += 1

    def request_finished(
        self,
        *,
        status_code: int | None,
        duration_ms: float,
        method: str | None = None,
        route: str | None = None,
    ) -> None:
        with self._lock:
            if self._in_flight_requests > 0:
                self._in_flight_requests -= 1
            self._total_requests += 1
            if status_code is not None:
                family = f"{int(status_code) // 100}xx"
                self._status_families[family] += 1
                if 400 <= status_code < 500:
                    self._client_error_requests += 1
                elif status_code >= 500:
                    self._server_error_requests += 1
            self._durations_ms.append(float(duration_ms))
            if route:
                normalized_route = str(route)
                self._route_totals[normalized_route] += 1
                if status_code is not None:
                    self._route_statuses[(normalized_route, str(int(status_code)))] += 1
                route_durations = self._route_durations_ms.get(normalized_route)
                if route_durations is None:
                    route_durations = deque(maxlen=self._route_window_size)
                    self._route_durations_ms[normalized_route] = route_durations
                route_durations.append(float(duration_ms))
                if method:
                    self._request_methods[method.upper()] += 0

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            durations = list(self._durations_ms)
            _, p95 = _summarize_durations(durations)
            return {
                "uptime_seconds": round(monotonic() - self._started_at, 2),
                "in_flight_requests": self._in_flight_requests,
                "total_requests": self._total_requests,
                "client_error_requests": self._client_error_requests,
                "server_error_requests": self._server_error_requests,
                "avg_duration_ms": round(mean(durations), 2) if durations else 0.0,
                "p95_duration_ms": p95,
                "methods": dict(self._request_methods),
                "status_families": dict(self._status_families),
            }

    def rate_limit_hit(self, *, route_group: str) -> None:
        with self._lock:
            self._rate_limit_hits[route_group] += 1

    def job_finished(self, *, job: str, duration_ms: float, success: bool, status: str | None = None) -> None:
        with self._lock:
            self._job_runs[job] += 1
            if not success:
                self._job_failures[job] += 1
            if status:
                self._job_statuses[(job, str(status).strip().lower())] += 1
            durations = self._job_durations_ms.get(job)
            if durations is None:
                durations = deque(maxlen=self._route_window_size)
                self._job_durations_ms[job] = durations
            durations.append(float(duration_ms))

    def diagnostic_emitted(self, *, module: str, severity: str, cause: str) -> None:
        with self._lock:
            self._diagnostic_events[(str(module).strip().lower(), str(severity).strip().lower(), str(cause).strip().lower())] += 1

    def internal_alert_generated(self, *, module: str, severity: str, channel: str = "inbox") -> None:
        with self._lock:
            self._internal_alerts[(str(module).strip().lower(), str(severity).strip().lower(), str(channel).strip().lower())] += 1

    def export_event(self, *, module: str, outcome: str, classification: str) -> None:
        with self._lock:
            self._export_events[(str(module).strip().lower(), str(outcome).strip().lower(), str(classification).strip().lower())] += 1

    def api_auth_event(self, *, outcome: str) -> None:
        with self._lock:
            self._api_auth_events[str(outcome).strip().lower()] += 1

    def export_prometheus(self) -> str:
        with self._lock:
            uptime = round(monotonic() - self._started_at, 2)
            total_requests = self._total_requests
            in_flight = self._in_flight_requests
            client_errors = self._client_error_requests
            server_errors = self._server_error_requests
            methods = dict(self._request_methods)
            status_families = dict(self._status_families)
            durations = list(self._durations_ms)
            route_totals = dict(self._route_totals)
            route_statuses = dict(self._route_statuses)
            route_durations = {key: list(value) for key, value in self._route_durations_ms.items()}
            rate_limit_hits = dict(self._rate_limit_hits)
            job_runs = dict(self._job_runs)
            job_failures = dict(self._job_failures)
            job_statuses = dict(self._job_statuses)
            job_durations = {key: list(value) for key, value in self._job_durations_ms.items()}
            diagnostic_events = dict(self._diagnostic_events)
            internal_alerts = dict(self._internal_alerts)
            export_events = dict(self._export_events)
            api_auth_events = dict(self._api_auth_events)

        avg_duration = round(mean(durations), 2) if durations else 0.0
        _, p95_duration = _summarize_durations(durations)
        lines = [
            "# HELP t2c_runtime_uptime_seconds Tempo de atividade do serviço.",
            "# TYPE t2c_runtime_uptime_seconds gauge",
            f"t2c_runtime_uptime_seconds {uptime}",
            "# HELP t2c_runtime_requests_in_flight Requisições em andamento.",
            "# TYPE t2c_runtime_requests_in_flight gauge",
            f"t2c_runtime_requests_in_flight {in_flight}",
            "# HELP t2c_runtime_requests_total Total de requisições processadas.",
            "# TYPE t2c_runtime_requests_total counter",
            f"t2c_runtime_requests_total {total_requests}",
            "# HELP t2c_runtime_requests_client_error_total Total de respostas 4xx.",
            "# TYPE t2c_runtime_requests_client_error_total counter",
            f"t2c_runtime_requests_client_error_total {client_errors}",
            "# HELP t2c_runtime_requests_server_error_total Total de respostas 5xx.",
            "# TYPE t2c_runtime_requests_server_error_total counter",
            f"t2c_runtime_requests_server_error_total {server_errors}",
            "# HELP t2c_runtime_request_duration_ms_avg Média do tempo de resposta (ms).",
            "# TYPE t2c_runtime_request_duration_ms_avg gauge",
            f"t2c_runtime_request_duration_ms_avg {avg_duration}",
            "# HELP t2c_runtime_request_duration_ms_p95 P95 do tempo de resposta (ms).",
            "# TYPE t2c_runtime_request_duration_ms_p95 gauge",
            f"t2c_runtime_request_duration_ms_p95 {p95_duration}",
        ]

        for method, count in methods.items():
            lines.append(
                f't2c_runtime_requests_by_method_total{{method="{_escape_label(method)}"}} {count}'
            )
        for family, count in status_families.items():
            lines.append(
                f't2c_runtime_requests_by_status_family_total{{family="{_escape_label(family)}"}} {count}'
            )
        for route, count in route_totals.items():
            lines.append(
                f't2c_http_route_requests_total{{route="{_escape_label(route)}"}} {count}'
            )
        for (route, status_code), count in route_statuses.items():
            lines.append(
                f't2c_http_route_status_total{{route="{_escape_label(route)}",status="{_escape_label(status_code)}"}} {count}'
            )
        for route, values in route_durations.items():
            avg, p95 = _summarize_durations(values)
            lines.append(
                f't2c_http_route_duration_ms_avg{{route="{_escape_label(route)}"}} {avg}'
            )
            lines.append(
                f't2c_http_route_duration_ms_p95{{route="{_escape_label(route)}"}} {p95}'
            )
        for route_group, count in rate_limit_hits.items():
            lines.append(
                f't2c_rate_limit_hits_total{{route_group="{_escape_label(route_group)}"}} {count}'
            )
        for job, count in job_runs.items():
            failures = int(job_failures.get(job, 0))
            avg, p95 = _summarize_durations(job_durations.get(job, []))
            lines.append(f't2c_jobs_total{{job="{_escape_label(job)}"}} {count}')
            lines.append(f't2c_job_failures_total{{job="{_escape_label(job)}"}} {failures}')
            lines.append(f't2c_job_duration_ms_avg{{job="{_escape_label(job)}"}} {avg}')
            lines.append(f't2c_job_duration_ms_p95{{job="{_escape_label(job)}"}} {p95}')
        for (job, job_status), count in job_statuses.items():
            lines.append(
                f't2c_job_status_total{{job="{_escape_label(job)}",status="{_escape_label(job_status)}"}} {count}'
            )
        for (module, severity, cause), count in diagnostic_events.items():
            lines.append(
                't2c_operational_diagnostics_total'
                f'{{module="{_escape_label(module)}",severity="{_escape_label(severity)}",cause="{_escape_label(cause)}"}} {count}'
            )
        for (module, severity, channel), count in internal_alerts.items():
            lines.append(
                't2c_internal_alerts_total'
                f'{{module="{_escape_label(module)}",severity="{_escape_label(severity)}",channel="{_escape_label(channel)}"}} {count}'
            )
        for (module, outcome, classification), count in export_events.items():
            lines.append(
                't2c_exports_total'
                f'{{module="{_escape_label(module)}",outcome="{_escape_label(outcome)}",classification="{_escape_label(classification)}"}} {count}'
            )
        for outcome, count in api_auth_events.items():
            lines.append(
                f't2c_api_auth_events_total{{outcome="{_escape_label(outcome)}"}} {count}'
            )

        return "\n".join(lines) + "\n"


def _summarize_durations(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    avg = round(mean(values), 2)
    sorted_values = sorted(values)
    index = max(0, min(len(sorted_values) - 1, int(round((len(sorted_values) - 1) * 0.95))))
    p95 = round(sorted_values[index], 2)
    return avg, p95


def _escape_label(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


runtime_metrics = RuntimeMetrics()
