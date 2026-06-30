from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from math import ceil
from statistics import mean

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.platform.read_models import load_dashboard_profiles_with_fallback
from t2c_data.features.dashboard.support import TableProfile
from t2c_data.features.pagination import normalize_page_params
from t2c_data.features.privacy_access.policy import suggest_possible_personal_data
from t2c_data.models.catalog import ColumnEntity, DataOwner, Database, Schema, TableEntity
from t2c_data.schemas.data_owner import (
    OwnershipAreaDistributionOut,
    OwnershipAssetDistributionOut,
    OwnershipDeleteImpactAssetOut,
    OwnershipDeleteImpactMetricsOut,
    OwnershipDeleteImpactOwnerOut,
    OwnershipDeleteImpactOut,
    OwnershipDistributionOut,
    OwnershipOwnerSummaryOut,
    OwnershipPriorityOut,
    OwnershipRankingItemOut,
    OwnershipRankingsOut,
    OwnershipReassignAssetOut,
    OwnershipReassignImpactOut,
    OwnershipReassignOwnerOut,
    OwnershipReassignPreviewOut,
    OwnershipReassignRequestIn,
    OwnershipReassignResultOut,
    OwnershipSummaryOut,
    OwnershipTotalsOut,
    OwnershipUnownedAssetOut,
)
from t2c_data.services.audit import AuditFieldChange, log_field_changes


_WIDE_ACCESS_SCOPES = {"public", "authenticated", None, ""}
_RISK_ORDER = {"critical": 4, "high": 3, "medium": 2, "low": 1}
_CERTIFICATION_PENDING_STATUSES = {"not_assessed", "not_eligible", "in_review", "rejected", "expired", "revalidation_pending"}


def _is_wide_access(access_scope: str | None) -> bool:
    return access_scope in _WIDE_ACCESS_SCOPES


def _possible_personal_data(table: TableEntity, possible_personal_table_ids: set[int] | None = None) -> bool:
    if table.has_personal_data or table.has_sensitive_personal_data:
        return False
    if possible_personal_table_ids is not None and table.id in possible_personal_table_ids:
        return True
    return suggest_possible_personal_data(list(table.columns or []))


def _privacy_pending(table: TableEntity, *, possible_personal_data: bool) -> bool:
    has_relevant_privacy = table.has_personal_data or table.has_sensitive_personal_data or possible_personal_data
    if not has_relevant_privacy:
        return False
    if table.has_personal_data and not table.legal_basis:
        return True
    if possible_personal_data and not table.privacy_reviewed_at:
        return True
    if table.has_sensitive_personal_data and _is_wide_access(table.access_scope):
        return True
    if table.has_personal_data and _is_wide_access(table.access_scope) and not table.privacy_reviewed_at:
        return True
    return False


def _owner_risk(
    *,
    asset_count: int,
    critical_assets_without_certification: int,
    open_incidents: int,
    critical_incidents: int,
    sensitive_wide_access_assets: int,
    privacy_pending_assets: int,
    dq_unmonitored_assets: int,
    certification_pending_assets: int,
    assets_without_description: int,
    assets_without_sla: int,
) -> tuple[str, str | None, str | None]:
    if asset_count <= 0:
        return "low", "Sem ativos associados", "Associar ativos reais ou revisar se o cadastro ainda é necessário."

    if critical_incidents > 0 or sensitive_wide_access_assets > 0 or critical_assets_without_certification >= 2:
        if critical_incidents > 0:
            return (
                "critical",
                "Ativos críticos com incidentes críticos abertos",
                "Tratar incidentes críticos primeiro e reavaliar certificação e controles dos ativos afetados.",
            )
        if sensitive_wide_access_assets > 0:
            return (
                "critical",
                "Ativos sensíveis com acesso amplo",
                "Restringir acesso e registrar revisão de privacidade dos ativos sensíveis imediatamente.",
            )
        return (
            "critical",
            "Ativos críticos ainda sem certificação",
            "Priorizar documentação, DQ e revisão de certificação dos ativos críticos.",
        )

    majority_threshold = max(2, ceil(asset_count * 0.5))
    certification_threshold = max(2, ceil(asset_count * 0.6))
    if open_incidents > 0 or privacy_pending_assets > 0 or dq_unmonitored_assets >= majority_threshold or certification_pending_assets >= certification_threshold:
        blocker_candidates = [
            ("Ativos com incidentes abertos", open_incidents, "Resolver incidentes abertos antes de ampliar consumo dos ativos deste owner."),
            ("Ativos com privacidade pendente", privacy_pending_assets, "Registrar revisão de privacidade, base legal e acesso dos ativos pendentes."),
            ("Ativos sem Data Quality monitorado", dq_unmonitored_assets, "Configurar regras de qualidade ou profiling nos ativos ainda sem monitoramento."),
            ("Ativos pendentes de certificação", certification_pending_assets, "Revisar prontidão e avançar com certificação dos ativos mais preparados."),
        ]
        blocker, _, action = max(blocker_candidates, key=lambda item: item[1])
        return "high", blocker, action

    if assets_without_description > 0 or assets_without_sla > 0 or certification_pending_assets > 0:
        blocker_candidates = [
            ("Ativos sem documentação mínima", assets_without_description, "Completar descrição dos ativos e revisar cobertura de metadados."),
            ("Ativos sem SLA", assets_without_sla, "Definir SLA ou freshness esperado para os ativos operacionais do owner."),
            ("Ativos pendentes de certificação", certification_pending_assets, "Revisar pendências restantes e enviar ativos prontos para certificação."),
        ]
        blocker, _, action = max(blocker_candidates, key=lambda item: item[1])
        return "medium", blocker, action

    return "low", "Sem bloqueios críticos aparentes", "Manter revisão periódica de ownership, privacidade e documentação."


def _priority_from_owner(owner: OwnershipOwnerSummaryOut) -> OwnershipPriorityOut | None:
    if owner.risk_level not in {"critical", "high", "medium"}:
        return None
    title_map = {
        "critical": "Owner com risco crítico",
        "high": "Owner com pendências operacionais relevantes",
        "medium": "Owner com pendências de governança",
    }
    return OwnershipPriorityOut(
        type="owner",
        severity=owner.risk_level,
        title=title_map.get(owner.risk_level, "Owner prioritário"),
        description=f"{owner.name} possui {owner.asset_count} ativo(s) e o principal bloqueio atual é: {owner.main_blocker or 'revisar ativos associados'}.",
        owner_id=owner.id,
        asset_id=None,
        recommended_action=owner.recommended_action or "Revisar ativos sob responsabilidade.",
    )


def _ranking_items(owners: list[OwnershipOwnerSummaryOut], *, key: str) -> list[OwnershipRankingItemOut]:
    ranked = sorted(
        owners,
        key=lambda item: (int(getattr(item, key) or 0), _RISK_ORDER.get(item.risk_level, 0), item.name.lower()),
        reverse=True,
    )
    results: list[OwnershipRankingItemOut] = []
    for item in ranked[:5]:
        metric_value = int(getattr(item, key) or 0)
        if metric_value <= 0 and key != "asset_count":
            continue
        results.append(
            OwnershipRankingItemOut(
                owner_id=item.id,
                name=item.name,
                area=item.area,
                status=item.status,
                metric_value=metric_value,
                risk_level=item.risk_level,
            )
        )
    return results


def _load_ownership_inputs(
    db: Session,
    *,
    current_user,
) -> tuple[datetime, list[TableProfile], dict[int, TableEntity], list[DataOwner], set[int]]:
    now = datetime.now(timezone.utc)
    profiles, _ = load_dashboard_profiles_with_fallback(db, now, current_user=current_user)
    visible_table_ids = [profile.table_id for profile in profiles]
    tables = (
        db.scalars(
            select(TableEntity)
            .options(
                selectinload(TableEntity.schema).selectinload(Schema.database),
            )
            .where(TableEntity.id.in_(visible_table_ids))
        ).all()
        if visible_table_ids
        else []
    )
    table_by_id = {table.id: table for table in tables}
    owners = db.scalars(select(DataOwner).order_by(DataOwner.name)).all()
    possible_personal_table_ids: set[int] = set()
    if visible_table_ids:
        possible_personal_table_ids = {
            int(row.table_id)
            for row in db.execute(
                select(ColumnEntity.table_id)
                .distinct()
                .select_from(ColumnEntity)
                .join(TableEntity, ColumnEntity.table_id == TableEntity.id)
                .where(
                    ColumnEntity.table_id.in_(visible_table_ids),
                    TableEntity.has_personal_data.is_(False),
                    TableEntity.has_sensitive_personal_data.is_(False),
                )
                .where(
                    or_(
                        ColumnEntity.name.ilike("%cpf%"),
                        ColumnEntity.name.ilike("%cnpj%"),
                        ColumnEntity.name.ilike("%telefone%"),
                        ColumnEntity.name.ilike("%phone%"),
                        ColumnEntity.name.ilike("%email%"),
                        ColumnEntity.name.ilike("%mail%"),
                        ColumnEntity.name.ilike("%nome%"),
                        ColumnEntity.name.ilike("%name%"),
                        ColumnEntity.name.ilike("%endereco%"),
                        ColumnEntity.name.ilike("%endereço%"),
                        ColumnEntity.name.ilike("%address%"),
                        ColumnEntity.name.ilike("%rg%"),
                        ColumnEntity.name.ilike("%data_nascimento%"),
                        ColumnEntity.name.ilike("%nascimento%"),
                        ColumnEntity.name.ilike("%birth%"),
                    )
                )
            ).all()
        }
    return now, profiles, table_by_id, owners, possible_personal_table_ids


def _build_owner_items(
    owners: list[DataOwner],
    profiles: list[TableProfile],
    table_by_id: dict[int, TableEntity],
    possible_personal_table_ids: set[int] | None = None,
) -> list[OwnershipOwnerSummaryOut]:
    owner_profiles: dict[int, list[TableProfile]] = defaultdict(list)
    owner_tables: dict[int, list[TableEntity]] = defaultdict(list)
    for profile in profiles:
        if profile.data_owner_id is not None:
            owner_profiles[int(profile.data_owner_id)].append(profile)
            if profile.table_id in table_by_id:
                owner_tables[int(profile.data_owner_id)].append(table_by_id[profile.table_id])

    owner_items: list[OwnershipOwnerSummaryOut] = []
    for owner in owners:
        scoped_profiles = owner_profiles.get(owner.id, [])
        scoped_tables = owner_tables.get(owner.id, [])
        possible_personal_data_assets = 0
        assets_without_legal_basis = 0
        assets_without_privacy_review = 0
        restricted_assets = 0
        privacy_pending_assets = 0
        sensitive_wide_access_assets = 0
        for table in scoped_tables:
            possible_personal = _possible_personal_data(table, possible_personal_table_ids)
            if possible_personal:
                possible_personal_data_assets += 1
            if table.has_personal_data and not table.legal_basis:
                assets_without_legal_basis += 1
            if not table.privacy_reviewed_at:
                assets_without_privacy_review += 1
            if table.access_scope in {"restricted", "confidential", "personal_data"}:
                restricted_assets += 1
            if table.has_sensitive_personal_data and _is_wide_access(table.access_scope):
                sensitive_wide_access_assets += 1
            if _privacy_pending(table, possible_personal_data=possible_personal):
                privacy_pending_assets += 1

        asset_count = len(scoped_profiles)
        certified_assets = sum(1 for profile in scoped_profiles if profile.certification_status == "certified")
        certification_pending_assets = sum(1 for profile in scoped_profiles if profile.certification_status in _CERTIFICATION_PENDING_STATUSES)
        dq_monitored_assets = sum(1 for profile in scoped_profiles if profile.dq_score is not None or profile.active_dq_rules_count > 0)
        dq_unmonitored_assets = max(asset_count - dq_monitored_assets, 0)
        open_incidents = sum(int(profile.open_incidents or 0) for profile in scoped_profiles)
        critical_incidents = sum(int(profile.critical_open_incidents or 0) for profile in scoped_profiles)
        assets_with_open_incidents = sum(1 for profile in scoped_profiles if int(profile.open_incidents or 0) > 0)
        assets_without_description = sum(1 for profile in scoped_profiles if not profile.description_complete)
        assets_without_tags = sum(1 for profile in scoped_profiles if int(profile.tags_count or 0) <= 0)
        assets_without_terms = sum(1 for profile in scoped_profiles if int(profile.terms_count or 0) <= 0)
        assets_without_sla = sum(1 for profile in scoped_profiles if not profile.sla_defined)
        personal_data_assets = sum(1 for profile in scoped_profiles if profile.has_personal_data)
        sensitive_data_assets = sum(1 for profile in scoped_profiles if profile.has_sensitive_personal_data)
        critical_assets_without_certification = sum(
            1
            for profile in scoped_profiles
            if (profile.certification_criticality or "").lower() in {"critical", "high"} and profile.certification_status != "certified"
        )
        risk_level, main_blocker, recommended_action = _owner_risk(
            asset_count=asset_count,
            critical_assets_without_certification=critical_assets_without_certification,
            open_incidents=assets_with_open_incidents,
            critical_incidents=critical_incidents,
            sensitive_wide_access_assets=sensitive_wide_access_assets,
            privacy_pending_assets=privacy_pending_assets,
            dq_unmonitored_assets=dq_unmonitored_assets,
            certification_pending_assets=certification_pending_assets,
            assets_without_description=assets_without_description,
            assets_without_sla=assets_without_sla,
        )

        owner_items.append(
            OwnershipOwnerSummaryOut(
                id=owner.id,
                name=owner.name,
                email=owner.email,
                area=owner.area,
                status="active" if owner.is_active else "inactive",
                updated_at=owner.updated_at,
                asset_count=asset_count,
                certified_assets=certified_assets,
                certification_pending_assets=certification_pending_assets,
                eligible_assets=sum(1 for profile in scoped_profiles if profile.certification_status == "eligible"),
                not_eligible_assets=sum(1 for profile in scoped_profiles if profile.certification_status in {"not_assessed", "not_eligible"}),
                in_review_assets=sum(1 for profile in scoped_profiles if profile.certification_status == "in_review"),
                rejected_assets=sum(1 for profile in scoped_profiles if profile.certification_status in {"rejected", "expired"}),
                revalidation_pending_assets=sum(1 for profile in scoped_profiles if profile.certification_status == "revalidation_pending"),
                dq_monitored_assets=dq_monitored_assets,
                dq_unmonitored_assets=dq_unmonitored_assets,
                open_incidents=open_incidents,
                critical_incidents=critical_incidents,
                assets_with_open_incidents=assets_with_open_incidents,
                privacy_pending_assets=privacy_pending_assets,
                personal_data_assets=personal_data_assets,
                sensitive_data_assets=sensitive_data_assets,
                restricted_assets=restricted_assets,
                possible_personal_data_assets=possible_personal_data_assets,
                assets_without_legal_basis=assets_without_legal_basis,
                assets_without_privacy_review=assets_without_privacy_review,
                assets_without_description=assets_without_description,
                assets_without_tags=assets_without_tags,
                assets_without_terms=assets_without_terms,
                assets_without_sla=assets_without_sla,
                average_quality_score=round(mean([profile.dq_score for profile in scoped_profiles if profile.dq_score is not None]), 1)
                if any(profile.dq_score is not None for profile in scoped_profiles)
                else None,
                average_governance_score=None,
                average_readiness_score=round(mean([profile.readiness_score for profile in scoped_profiles]), 1) if scoped_profiles else None,
                risk_level=risk_level,
                main_blocker=main_blocker,
                recommended_action=recommended_action,
            )
        )

    return owner_items


def _filter_owner_items(
    owner_items: list[OwnershipOwnerSummaryOut],
    *,
    query: str | None = None,
    status: str | None = None,
    area: str | None = None,
    owner_id: int | None = None,
) -> list[OwnershipOwnerSummaryOut]:
    filtered_owners = owner_items
    if query:
        normalized = query.strip().lower()
        filtered_owners = [
            item for item in filtered_owners if normalized in item.name.lower() or normalized in item.email.lower() or normalized in (item.area or "").lower()
        ]
    if status in {"active", "inactive"}:
        filtered_owners = [item for item in filtered_owners if item.status == status]
    if area:
        filtered_owners = [item for item in filtered_owners if (item.area or "") == area]
    if owner_id is not None:
        filtered_owners = [item for item in filtered_owners if item.id == owner_id]
    return filtered_owners


def _build_unowned_assets(
    profiles: list[TableProfile],
    table_by_id: dict[int, TableEntity],
    possible_personal_table_ids: set[int] | None = None,
    *,
    critical_only: bool = False,
    privacy_risk_only: bool = False,
    certification_pending_only: bool = False,
    schema_name: str | None = None,
    database_name: str | None = None,
    limit: int | None = 10,
) -> list[OwnershipUnownedAssetOut]:
    unowned_assets: list[OwnershipUnownedAssetOut] = []
    for profile in profiles:
        if profile.data_owner_id is not None:
            continue
        table = table_by_id.get(profile.table_id)
        if table is None:
            continue
        possible_personal = _possible_personal_data(table, possible_personal_table_ids)
        privacy_signal = None
        if possible_personal:
            privacy_signal = "possible_personal_data"
        elif table.has_sensitive_personal_data:
            privacy_signal = "sensitive_personal_data"
        elif table.has_personal_data:
            privacy_signal = "personal_data"

        if critical_only and (profile.certification_criticality or "").lower() not in {"critical", "high"}:
            continue
        if privacy_risk_only and privacy_signal is None:
            continue
        if certification_pending_only and profile.certification_status not in _CERTIFICATION_PENDING_STATUSES:
            continue
        if schema_name and profile.schema_name != schema_name:
            continue
        if database_name and profile.database_name != database_name:
            continue

        recommended_action = "Atribuir owner"
        if privacy_signal is not None:
            recommended_action = "Atribuir owner e revisar privacidade"
        elif profile.certification_status in _CERTIFICATION_PENDING_STATUSES:
            recommended_action = "Atribuir owner e revisar certificação"
        unowned_assets.append(
            OwnershipUnownedAssetOut(
                id=profile.table_id,
                name=profile.table_name,
                database_name=profile.database_name,
                schema_name=profile.schema_name,
                connection_name=profile.datasource_name,
                criticality=profile.certification_criticality,
                certification_status=profile.certification_status,
                privacy_signal=privacy_signal,
                open_incidents=int(profile.open_incidents or 0),
                dq_score=round(profile.dq_score, 1) if profile.dq_score is not None else None,
                updated_at=profile.last_updated_at,
                recommended_action=recommended_action,
            )
        )
    unowned_assets.sort(
        key=lambda item: (
            _RISK_ORDER.get("high" if item.privacy_signal else "medium", 0),
            1 if (item.criticality or "").lower() in {"critical", "high"} else 0,
            item.name.lower(),
        ),
        reverse=True,
    )
    if limit is not None:
        return unowned_assets[:limit]
    return unowned_assets


def _build_ownership_totals(
    profiles: list[TableProfile],
    owner_items: list[OwnershipOwnerSummaryOut],
    table_by_id: dict[int, TableEntity],
    possible_personal_table_ids: set[int] | None = None,
) -> OwnershipTotalsOut:
    return OwnershipTotalsOut(
        owners=len(owner_items),
        active_owners=sum(1 for item in owner_items if item.status == "active"),
        inactive_owners=sum(1 for item in owner_items if item.status == "inactive"),
        owners_with_assets=sum(1 for item in owner_items if item.asset_count > 0),
        owners_without_assets=sum(1 for item in owner_items if item.asset_count == 0),
        assets_with_owner=sum(1 for profile in profiles if profile.data_owner_id is not None),
        assets_without_owner=sum(1 for profile in profiles if profile.data_owner_id is None),
        critical_assets_without_owner=sum(
            1 for profile in profiles if profile.data_owner_id is None and (profile.certification_criticality or "").lower() in {"critical", "high"}
        ),
        personal_data_assets_without_owner=sum(
            1
            for profile in profiles
            if profile.data_owner_id is None and (profile.has_personal_data or profile.has_sensitive_personal_data)
        ),
        certification_pending_assets=sum(1 for profile in profiles if profile.certification_status in _CERTIFICATION_PENDING_STATUSES),
        privacy_pending_assets=sum(
            1
            for profile in profiles
            if (table := table_by_id.get(profile.table_id)) is not None
            and _privacy_pending(table, possible_personal_data=_possible_personal_data(table, possible_personal_table_ids))
        ),
        dq_unmonitored_assets=sum(1 for profile in profiles if profile.dq_score is None and profile.active_dq_rules_count <= 0),
        assets_with_open_incidents=sum(1 for profile in profiles if int(profile.open_incidents or 0) > 0),
    )


def _build_ownership_priorities(
    owner_items: list[OwnershipOwnerSummaryOut],
    unowned_assets: list[OwnershipUnownedAssetOut],
) -> list[OwnershipPriorityOut]:
    priorities: list[OwnershipPriorityOut] = []
    for owner in sorted(owner_items, key=lambda item: (_RISK_ORDER.get(item.risk_level, 0), item.asset_count), reverse=True):
        priority = _priority_from_owner(owner)
        if priority is not None:
            priorities.append(priority)
    for asset in unowned_assets[:5]:
        severity = "high" if asset.privacy_signal or (asset.criticality or "").lower() in {"critical", "high"} else "medium"
        priorities.append(
            OwnershipPriorityOut(
                type="asset",
                severity=severity,
                title="Ativo sem owner",
                description=f"{asset.schema_name}.{asset.name} está sem owner e precisa de ownership para avançar governança e operação.",
                owner_id=None,
                asset_id=asset.id,
                recommended_action=asset.recommended_action,
            )
        )
    return sorted(priorities, key=lambda item: _RISK_ORDER.get(item.severity, 0), reverse=True)[:10]


def _build_ownership_distribution(
    profiles: list[TableProfile],
    owner_items: list[OwnershipOwnerSummaryOut],
    table_by_id: dict[int, TableEntity],
    possible_personal_table_ids: set[int] | None = None,
) -> OwnershipDistributionOut:
    area_buckets: dict[str, OwnershipAreaDistributionOut] = {}
    for owner in owner_items:
        bucket_key = owner.area or "Área não informada"
        bucket = area_buckets.get(bucket_key)
        if bucket is None:
            bucket = OwnershipAreaDistributionOut(area=bucket_key, owners=0, active_owners=0, assets=0)
            area_buckets[bucket_key] = bucket
        bucket.owners += 1
        bucket.active_owners += 1 if owner.status == "active" else 0
        bucket.assets += owner.asset_count

    schema_buckets: dict[tuple[str, str], OwnershipAssetDistributionOut] = {}
    database_buckets: dict[str, OwnershipAssetDistributionOut] = {}
    for profile in profiles:
        table = table_by_id.get(profile.table_id)
        possible_personal = _possible_personal_data(table, possible_personal_table_ids) if table is not None else False
        privacy_pending = _privacy_pending(table, possible_personal_data=possible_personal) if table is not None else False

        schema_key = (profile.database_name, profile.schema_name)
        schema_bucket = schema_buckets.get(schema_key)
        if schema_bucket is None:
            schema_bucket = OwnershipAssetDistributionOut(database_name=profile.database_name, schema_name=profile.schema_name)
            schema_buckets[schema_key] = schema_bucket
        schema_bucket.total_assets += 1
        schema_bucket.assets_with_owner += 1 if profile.data_owner_id is not None else 0
        schema_bucket.assets_without_owner += 1 if profile.data_owner_id is None else 0
        schema_bucket.privacy_pending_assets += 1 if privacy_pending else 0
        schema_bucket.certification_pending_assets += 1 if profile.certification_status in _CERTIFICATION_PENDING_STATUSES else 0

        database_bucket = database_buckets.get(profile.database_name)
        if database_bucket is None:
            database_bucket = OwnershipAssetDistributionOut(database_name=profile.database_name, schema_name=None)
            database_buckets[profile.database_name] = database_bucket
        database_bucket.total_assets += 1
        database_bucket.assets_with_owner += 1 if profile.data_owner_id is not None else 0
        database_bucket.assets_without_owner += 1 if profile.data_owner_id is None else 0
        database_bucket.privacy_pending_assets += 1 if privacy_pending else 0
        database_bucket.certification_pending_assets += 1 if profile.certification_status in _CERTIFICATION_PENDING_STATUSES else 0

    return OwnershipDistributionOut(
        by_area=sorted(area_buckets.values(), key=lambda item: (item.assets, item.area.lower()), reverse=True),
        by_schema=sorted(schema_buckets.values(), key=lambda item: (item.assets_without_owner, item.total_assets, item.schema_name or ""), reverse=True)[:10],
        by_database=sorted(database_buckets.values(), key=lambda item: (item.assets_without_owner, item.total_assets, item.database_name or ""), reverse=True),
    )


def _build_ownership_rankings(owner_items: list[OwnershipOwnerSummaryOut]) -> OwnershipRankingsOut:
    return OwnershipRankingsOut(
        most_assets=_ranking_items(owner_items, key="asset_count"),
        most_certification_pending=_ranking_items(owner_items, key="certification_pending_assets"),
        most_privacy_pending=_ranking_items(owner_items, key="privacy_pending_assets"),
        most_incidents=_ranking_items(owner_items, key="assets_with_open_incidents"),
        most_dq_unmonitored=_ranking_items(owner_items, key="dq_unmonitored_assets"),
        inactive_with_assets=[
            OwnershipRankingItemOut(
                owner_id=item.id,
                name=item.name,
                area=item.area,
                status=item.status,
                metric_value=item.asset_count,
                risk_level=item.risk_level,
            )
            for item in sorted(
                [owner for owner in owner_items if owner.status == "inactive" and owner.asset_count > 0],
                key=lambda owner: (owner.asset_count, _RISK_ORDER.get(owner.risk_level, 0)),
                reverse=True,
            )[:5]
        ],
    )


def get_ownership_summary(
    db: Session,
    *,
    current_user,
    query: str | None = None,
    status: str | None = None,
    area: str | None = None,
    owner_id: int | None = None,
    include_unowned: bool = True,
    page: int = 1,
    page_size: int = 100,
    critical_only: bool = False,
    privacy_risk_only: bool = False,
    certification_pending_only: bool = False,
    schema_name: str | None = None,
    database_name: str | None = None,
) -> OwnershipSummaryOut:
    normalized_page, normalized_page_size = normalize_page_params(
        page=page,
        page_size=page_size,
        default_page_size=100,
        max_page_size=100,
    )
    _, profiles, table_by_id, owners, possible_personal_table_ids = _load_ownership_inputs(db, current_user=current_user)
    owner_items = _build_owner_items(owners, profiles, table_by_id, possible_personal_table_ids)
    filtered_owners = _filter_owner_items(owner_items, query=query, status=status, area=area, owner_id=owner_id)
    total_filtered_owners = len(filtered_owners)
    total_pages = max(1, ceil(total_filtered_owners / normalized_page_size)) if normalized_page_size > 0 else 1
    start = max((normalized_page - 1) * normalized_page_size, 0)
    paged_owners = filtered_owners[start : start + normalized_page_size]
    unowned_assets = _build_unowned_assets(
        profiles,
        table_by_id,
        possible_personal_table_ids,
        critical_only=critical_only,
        privacy_risk_only=privacy_risk_only,
        certification_pending_only=certification_pending_only,
        schema_name=schema_name,
        database_name=database_name,
        limit=10 if include_unowned else 0,
    )
    totals = _build_ownership_totals(profiles, owner_items, table_by_id, possible_personal_table_ids)
    priorities = _build_ownership_priorities(owner_items, unowned_assets)
    distribution = _build_ownership_distribution(profiles, owner_items, table_by_id, possible_personal_table_ids)
    rankings = _build_ownership_rankings(owner_items)
    return OwnershipSummaryOut(
        totals=totals,
        owners_total=total_filtered_owners,
        page=normalized_page,
        page_size=normalized_page_size,
        total_pages=total_pages,
        owners=paged_owners,
        unowned_assets=unowned_assets,
        priorities=priorities,
        distribution=distribution,
        rankings=rankings,
    )


def get_ownership_export_rows(
    db: Session,
    *,
    current_user,
    query: str | None = None,
    status: str | None = None,
    area: str | None = None,
    owner_id: int | None = None,
    include_unowned: bool = True,
    risk_level: str | None = None,
    schema_name: str | None = None,
    database_name: str | None = None,
) -> tuple[list[OwnershipOwnerSummaryOut], list[OwnershipUnownedAssetOut]]:
    _, profiles, table_by_id, owners, possible_personal_table_ids = _load_ownership_inputs(db, current_user=current_user)
    owner_items = _build_owner_items(owners, profiles, table_by_id, possible_personal_table_ids)
    filtered_owners = _filter_owner_items(owner_items, query=query, status=status, area=area, owner_id=owner_id)
    unowned_assets = _build_unowned_assets(
        profiles,
        table_by_id,
        possible_personal_table_ids,
        critical_only=risk_level in {"critical", "high"},
        privacy_risk_only=risk_level in {"high", "critical"},
        certification_pending_only=False,
        schema_name=schema_name,
        database_name=database_name,
        limit=None if include_unowned else 0,
    )
    if risk_level in {"low", "medium", "high", "critical"}:
        filtered_owners = [item for item in filtered_owners if item.risk_level == risk_level]
    return filtered_owners, unowned_assets


def get_ownership_delete_impact(
    db: Session,
    *,
    current_user,
    owner_id: int,
) -> tuple[OwnershipDeleteImpactOut, OwnershipOwnerSummaryOut | None, list[OwnershipUnownedAssetOut]]:
    _, profiles, table_by_id, owners, possible_personal_table_ids = _load_ownership_inputs(db, current_user=current_user)
    owner_items = _build_owner_items(owners, profiles, table_by_id, possible_personal_table_ids)
    owner = next((item for item in owner_items if item.id == owner_id), None)
    if owner is None:
        raise ValueError("Data owner not found")

    sample_assets: list[OwnershipDeleteImpactAssetOut] = []
    for profile in sorted(
        [profile for profile in profiles if profile.data_owner_id == owner_id],
        key=lambda item: (
            _RISK_ORDER.get((item.certification_criticality or "medium").lower(), 0),
            int(item.open_incidents or 0),
            item.table_name.lower(),
        ),
        reverse=True,
    )[:5]:
        table = table_by_id.get(profile.table_id)
        if table is None:
            continue
        reason = "Ativo ficará sem responsável."
        if int(profile.open_incidents or 0) > 0:
            reason = "Ativo possui incidente aberto e ficará sem responsável."
        elif profile.certification_status in _CERTIFICATION_PENDING_STATUSES:
            reason = "Ativo pendente de certificação ficará sem responsável."
        elif table.has_sensitive_personal_data or table.has_personal_data:
            reason = "Ativo com dado sensível/pessoal ficará sem responsável."
        sample_assets.append(
            OwnershipDeleteImpactAssetOut(
                id=profile.table_id,
                name=profile.table_name,
                database=profile.database_name,
                schema_name=profile.schema_name,
                risk=(profile.certification_criticality or "medium").lower() if profile.certification_criticality else "medium",
                reason=reason,
            )
        )

    certified_assets = sum(1 for profile in profiles if profile.data_owner_id == owner_id and profile.certification_status == "certified")
    critical_assets = sum(
        1
        for profile in profiles
        if profile.data_owner_id == owner_id and (profile.certification_criticality or "").lower() in {"critical", "high"}
    )
    personal_data_assets = sum(
        1
        for profile in profiles
        if profile.data_owner_id == owner_id and profile.table_id in table_by_id and table_by_id[profile.table_id].has_personal_data
    )
    sensitive_data_assets = sum(
        1
        for profile in profiles
        if profile.data_owner_id == owner_id and profile.table_id in table_by_id and table_by_id[profile.table_id].has_sensitive_personal_data
    )
    restricted_assets = sum(
        1
        for profile in profiles
        if profile.data_owner_id == owner_id
        and profile.table_id in table_by_id
        and table_by_id[profile.table_id].access_scope in {"restricted", "confidential", "personal_data"}
    )
    open_incidents = sum(int(profile.open_incidents or 0) for profile in profiles if profile.data_owner_id == owner_id)
    certification_pending_assets = sum(
        1 for profile in profiles if profile.data_owner_id == owner_id and profile.certification_status in _CERTIFICATION_PENDING_STATUSES
    )
    privacy_pending_assets = sum(
        1
        for profile in profiles
        if profile.data_owner_id == owner_id and profile.table_id in table_by_id and _privacy_pending(
            table_by_id[profile.table_id],
            possible_personal_data=_possible_personal_data(table_by_id[profile.table_id], possible_personal_table_ids),
        )
    )
    dq_unmonitored_assets = sum(
        1 for profile in profiles if profile.data_owner_id == owner_id and profile.dq_score is None and profile.active_dq_rules_count <= 0
    )

    impact = OwnershipDeleteImpactMetricsOut(
        asset_count=owner.asset_count,
        certified_assets=certified_assets,
        critical_assets=critical_assets,
        personal_data_assets=personal_data_assets,
        sensitive_data_assets=sensitive_data_assets,
        restricted_assets=restricted_assets,
        open_incidents=open_incidents,
        certification_pending_assets=certification_pending_assets,
        privacy_pending_assets=privacy_pending_assets,
        dq_unmonitored_assets=dq_unmonitored_assets,
    )
    warning_message = (
        "Este owner possui ativos associados. Remover agora deixará tabelas sem responsável e pode impactar certificação, privacidade e incidentes."
        if owner.asset_count > 0
        else "Este owner não possui ativos associados e pode ser removido com segurança operacional."
    )
    return (
        OwnershipDeleteImpactOut(
            owner=OwnershipDeleteImpactOwnerOut(id=owner.id, name=owner.name, email=owner.email, area=owner.area),
            impact=impact,
            sample_assets=sample_assets,
            can_delete_without_force=owner.asset_count == 0,
            warning_message=warning_message,
        ),
        owner,
        sample_assets,
    )


def _build_reassign_owner_out(owner: DataOwner) -> OwnershipReassignOwnerOut:
    return OwnershipReassignOwnerOut(id=owner.id, name=owner.name, email=owner.email, area=owner.area)


def _build_reassign_asset_out(
    profile: TableProfile,
    table: TableEntity,
    possible_personal_table_ids: set[int] | None = None,
) -> OwnershipReassignAssetOut:
    recommended_action = "Reatribuir owner"
    if int(profile.open_incidents or 0) > 0:
        recommended_action = "Reatribuir owner e tratar incidentes"
    elif profile.certification_status in _CERTIFICATION_PENDING_STATUSES:
        recommended_action = "Reatribuir owner e revisar certificação"
    elif _privacy_pending(table, possible_personal_data=_possible_personal_data(table, possible_personal_table_ids)):
        recommended_action = "Reatribuir owner e revisar privacidade"
    elif profile.dq_score is None and profile.active_dq_rules_count <= 0:
        recommended_action = "Reatribuir owner e ativar monitoramento de DQ"

    return OwnershipReassignAssetOut(
        id=profile.table_id,
        name=profile.table_name,
        database=profile.database_name,
        schema_name=profile.schema_name,
        criticality=profile.certification_criticality,
        certification_status=profile.certification_status,
        privacy_signal=(
            "possible_personal_data"
            if _possible_personal_data(table, possible_personal_table_ids)
            else "sensitive_personal_data"
            if table.has_sensitive_personal_data
            else "personal_data"
            if table.has_personal_data
            else None
        ),
        has_personal_data=bool(table.has_personal_data),
        has_sensitive_personal_data=bool(table.has_sensitive_personal_data),
        dq_monitored=bool(profile.dq_score is not None or profile.active_dq_rules_count > 0),
        privacy_pending=_privacy_pending(table, possible_personal_data=_possible_personal_data(table, possible_personal_table_ids)),
        open_incidents=int(profile.open_incidents or 0),
        recommended_action=recommended_action,
    )


def _load_reassign_assets(
    profiles: list[TableProfile],
    table_by_id: dict[int, TableEntity],
    *,
    source_owner_id: int,
    asset_ids: list[int] | None = None,
) -> list[tuple[TableProfile, TableEntity]]:
    requested_ids = list(dict.fromkeys(int(asset_id) for asset_id in (asset_ids or []) if int(asset_id) > 0))
    entries = []
    for profile in profiles:
        if profile.data_owner_id != source_owner_id:
            continue
        table = table_by_id.get(profile.table_id)
        if table is None:
            continue
        entries.append((profile, table))
    if not requested_ids:
        return entries

    entry_by_id = {profile.table_id: (profile, table) for profile, table in entries}
    missing = [asset_id for asset_id in requested_ids if asset_id not in entry_by_id]
    if missing:
        raise ValueError(f"Selected assets are not owned by the source owner: {missing}")
    return [entry_by_id[asset_id] for asset_id in requested_ids]


def _reassign_metrics(
    entries: list[tuple[TableProfile, TableEntity]],
    possible_personal_table_ids: set[int] | None = None,
) -> OwnershipReassignImpactOut:
    profiles = [profile for profile, _ in entries]
    tables = [table for _, table in entries]
    return OwnershipReassignImpactOut(
        asset_count=len(entries),
        certified_assets=sum(1 for profile in profiles if profile.certification_status == "certified"),
        critical_assets=sum(1 for profile in profiles if (profile.certification_criticality or "").lower() in {"critical", "high"}),
        personal_data_assets=sum(1 for table in tables if table.has_personal_data),
        sensitive_data_assets=sum(1 for table in tables if table.has_sensitive_personal_data),
        open_incidents=sum(int(profile.open_incidents or 0) for profile in profiles),
        certification_pending_assets=sum(1 for profile in profiles if profile.certification_status in _CERTIFICATION_PENDING_STATUSES),
        privacy_pending_assets=sum(
            1 for profile, table in entries if _privacy_pending(table, possible_personal_data=_possible_personal_data(table, possible_personal_table_ids))
        ),
        dq_unmonitored_assets=sum(1 for profile in profiles if profile.dq_score is None and profile.active_dq_rules_count <= 0),
    )


def get_ownership_reassign_preview(
    db: Session,
    *,
    current_user,
    owner_id: int,
    target_owner_id: int | None = None,
    asset_ids: list[int] | None = None,
    page: int = 1,
    page_size: int = 100,
) -> OwnershipReassignPreviewOut:
    normalized_page, normalized_page_size = normalize_page_params(
        page=page,
        page_size=page_size,
        default_page_size=100,
        max_page_size=100,
    )
    source_owner = db.get(DataOwner, owner_id)
    if source_owner is None:
        raise ValueError("Data owner not found")

    target_owner = db.get(DataOwner, target_owner_id) if target_owner_id is not None else None
    if target_owner_id is not None and target_owner is None:
        raise ValueError("Target data owner not found")
    if target_owner_id is not None and target_owner_id == owner_id:
        raise ValueError("Source and target owners must be different")
    if target_owner is not None and not bool(target_owner.is_active):
        raise ValueError("Target data owner must be active")

    _, profiles, table_by_id, _, possible_personal_table_ids = _load_ownership_inputs(db, current_user=current_user)
    entries = _load_reassign_assets(profiles, table_by_id, source_owner_id=source_owner.id, asset_ids=asset_ids)
    impact = _reassign_metrics(entries, possible_personal_table_ids)
    ordered_entries = sorted(
        entries,
        key=lambda item: (
            _RISK_ORDER.get((item[0].certification_criticality or "medium").lower(), 0),
            int(item[0].open_incidents or 0),
            item[0].table_name.lower(),
        ),
        reverse=True,
    )
    start = max((normalized_page - 1) * normalized_page_size, 0)
    page_entries = ordered_entries[start : start + normalized_page_size]
    return OwnershipReassignPreviewOut(
        source_owner=_build_reassign_owner_out(source_owner),
        target_owner=_build_reassign_owner_out(target_owner) if target_owner is not None else None,
        impact=impact,
        assets=[_build_reassign_asset_out(profile, table, possible_personal_table_ids) for profile, table in page_entries],
        page=normalized_page,
        page_size=normalized_page_size,
        total_assets=len(entries),
    )


def reassign_ownership_assets(
    db: Session,
    *,
    current_user,
    owner_id: int,
    payload: OwnershipReassignRequestIn,
    audit_kwargs: dict | None = None,
) -> OwnershipReassignResultOut:
    source_owner = db.get(DataOwner, owner_id)
    if source_owner is None:
        raise ValueError("Data owner not found")

    target_owner = db.get(DataOwner, payload.target_owner_id)
    if target_owner is None:
        raise ValueError("Target data owner not found")
    if target_owner.id == source_owner.id:
        raise ValueError("Source and target owners must be different")
    if not bool(target_owner.is_active):
        raise ValueError("Target data owner must be active")

    if payload.mode == "selected" and not payload.asset_ids:
        raise ValueError("asset_ids are required when mode is selected")

    _, profiles, table_by_id, _, possible_personal_table_ids = _load_ownership_inputs(db, current_user=current_user)
    entries = _load_reassign_assets(
        profiles,
        table_by_id,
        source_owner_id=source_owner.id,
        asset_ids=payload.asset_ids if payload.mode == "selected" else None,
    )
    if not entries:
        raise ValueError("Source owner has no assets to reassign")

    normalized_note = payload.note.strip() if payload.note and payload.note.strip() else None
    audit_base = dict(audit_kwargs or {})
    if current_user is not None and audit_base.get("user_id") is None:
        audit_base["user_id"] = getattr(current_user, "id", None)

    for profile, table in entries:
        before_owner_id = table.data_owner_id
        before_owner = table.owner
        before_owner_email = table.owner_email
        table.data_owner = target_owner
        table.data_owner_id = target_owner.id
        table.owner = target_owner.name
        table.owner_email = target_owner.email
        log_field_changes(
            db,
            action="data_owner.reassign_assets",
            entity_type="table",
            entity_id=table.id,
            changes=[
                AuditFieldChange(field_name="data_owner_id", before=before_owner_id, after=table.data_owner_id),
                AuditFieldChange(field_name="owner", before=before_owner, after=table.owner),
                AuditFieldChange(field_name="owner_email", before=before_owner_email, after=table.owner_email),
            ],
            source_module="data_owners.reassign_assets",
            metadata={
                "message": "Ownership reassigned",
                "source_owner_id": source_owner.id,
                "target_owner_id": target_owner.id,
                "note": normalized_note,
                "mode": payload.mode,
            },
            audit_kwargs=audit_base,
            actor_user_id=getattr(current_user, "id", None),
        )

    db.commit()
    assets = [_build_reassign_asset_out(profile, table, possible_personal_table_ids) for profile, table in entries]
    return OwnershipReassignResultOut(
        reassigned_count=len(entries),
        source_owner_id=source_owner.id,
        target_owner_id=target_owner.id,
        assets=assets,
        note=normalized_note,
    )
