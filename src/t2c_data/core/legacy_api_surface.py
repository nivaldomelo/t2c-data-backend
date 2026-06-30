LEGACY_API_SURFACE: tuple[dict[str, object], ...] = (
    {
        "module": "auth",
        "legacy_prefixes": ("/api/auth",),
        "canonical_prefixes": ("/api/v1/auth",),
        "note": "Superfície legada removida; use /api/v1/auth.",
    },
    {
        "module": "ready",
        "legacy_prefixes": ("/api/ready",),
        "canonical_prefixes": ("/api/v1/ready",),
        "note": "Superfície legada removida; use /api/v1/ready.",
    },
    {
        "module": "ping",
        "legacy_prefixes": ("/api/ping",),
        "canonical_prefixes": ("/api/v1/ping",),
        "note": "Superfície legada removida; use /api/v1/ping.",
    },
    {
        "module": "datasources",
        "legacy_prefixes": ("/api/datasources",),
        "canonical_prefixes": ("/api/v1/datasources",),
        "note": "Superfície legada removida; use /api/v1/datasources.",
    },
    {
        "module": "scan-runs",
        "legacy_prefixes": ("/api/scan-runs",),
        "canonical_prefixes": ("/api/v1/scan-runs",),
        "note": "Superfície legada removida; use /api/v1/scan-runs.",
    },
    {
        "module": "catalog",
        "legacy_prefixes": ("/api/catalog",),
        "canonical_prefixes": ("/api/v1/catalog",),
        "note": "Leitura de catálogo já possui sucessor canônico completo em /api/v1/catalog.",
    },
    {
        "module": "tables",
        "legacy_prefixes": ("/api/tables",),
        "canonical_prefixes": ("/api/v1/tables",),
        "note": "Mutações manuais de metadados permanecem em /api/v1/tables.",
    },
    {
        "module": "metrics",
        "legacy_prefixes": ("/api/metrics",),
        "canonical_prefixes": ("/api/v1/metrics",),
        "note": "Endpoint de resumo técnico com sucessor direto em /api/v1/metrics.",
    },
    {
        "module": "home",
        "legacy_prefixes": ("/api/home",),
        "canonical_prefixes": ("/api/v1/home",),
        "note": "Resumo inicial da aplicação já deve ser consumido via /api/v1/home.",
    },
    {
        "module": "me",
        "legacy_prefixes": ("/api/me",),
        "canonical_prefixes": ("/api/v1/me",),
        "note": "Sessão autenticada deve convergir totalmente para /api/v1/me.",
    },
)

REMOVED_LEGACY_API_MODULES: tuple[str, ...] = (
    "auth",
    "ready",
    "ping",
    "datasources",
    "scan-runs",
    "catalog",
    "tables",
    "metrics",
    "home",
    "me",
)

LEGACY_API_MANAGED_MODULES: tuple[str, ...] = tuple(str(item["module"]) for item in LEGACY_API_SURFACE)
ACTIVE_LEGACY_API_SURFACE: tuple[dict[str, object], ...] = tuple(
    item for item in LEGACY_API_SURFACE if str(item["module"]) not in REMOVED_LEGACY_API_MODULES
)


def legacy_surface_item_for_module(module: str) -> dict[str, object] | None:
    normalized = module.strip().lower()
    for item in LEGACY_API_SURFACE:
        if str(item["module"]) == normalized:
            return item
    return None


def legacy_surface_route_match(path: str) -> tuple[dict[str, object] | None, str]:
    normalized = path or "/api"
    for item in LEGACY_API_SURFACE:
        legacy_prefixes = tuple(str(prefix) for prefix in item["legacy_prefixes"])
        canonical_prefixes = tuple(str(prefix) for prefix in item["canonical_prefixes"])
        for legacy_prefix, canonical_prefix in zip(legacy_prefixes, canonical_prefixes, strict=False):
            if normalized == legacy_prefix or normalized.startswith(f"{legacy_prefix}/"):
                suffix = normalized[len(legacy_prefix) :]
                return item, f"{canonical_prefix}{suffix}"
    fallback = f"/api/v1{normalized[4:]}" if normalized != "/api" else "/api/v1"
    return None, fallback


__all__ = [
    "LEGACY_API_MANAGED_MODULES",
    "ACTIVE_LEGACY_API_SURFACE",
    "LEGACY_API_SURFACE",
    "REMOVED_LEGACY_API_MODULES",
    "legacy_surface_item_for_module",
    "legacy_surface_route_match",
]
