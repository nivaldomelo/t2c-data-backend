from datetime import datetime

from pydantic import BaseModel


class RequestMetricsOut(BaseModel):
    uptime_seconds: float
    in_flight_requests: int
    total_requests: int
    client_error_requests: int
    server_error_requests: int
    avg_duration_ms: float
    p95_duration_ms: float
    methods: dict[str, int]
    status_families: dict[str, int]


class MetricsSummaryOut(BaseModel):
    datasources: int
    schemas: int
    tables: int
    columns: int
    tags: int
    glossary_terms: int
    last_scan_at: datetime | None = None
    requests: RequestMetricsOut
