from __future__ import annotations

from typing import Any

from t2c_data.features.platform.events import should_emit_platform_domain_event

_CATEGORY_LABELS = {
    "governance": "Governança",
    "quality": "Qualidade",
    "operation": "Operação",
    "incident": "Incidente",
    "audit": "Auditoria",
    "platform": "Plataforma",
    "tags": "Tags",
    "classification": "Classificação",
    "certification": "Certificação",
}


def _titleize_event_key(event_key: str) -> str:
    tail = event_key.split(".")[-1].replace("_", " ").strip()
    return tail.title() if tail else event_key


_SUPPORTED_PLATFORM_EVENTS: list[dict[str, Any]] = [
    {"event_key": "datasource.create", "category": "operation", "description": "Datasource cadastrada na plataforma.", "entity_types": ["datasource"]},
    {"event_key": "datasource.update", "category": "operation", "description": "Datasource atualizada na plataforma.", "entity_types": ["datasource"]},
    {"event_key": "datasource.delete", "category": "operation", "description": "Datasource removida da plataforma.", "entity_types": ["datasource"]},
    {"event_key": "dq.profiling.run.start", "category": "quality", "description": "Execução de profiling iniciada para uma tabela.", "entity_types": ["table"]},
    {"event_key": "dq.profiling.run.finish", "category": "quality", "description": "Execução de profiling concluída para uma tabela.", "entity_types": ["table"]},
    {"event_key": "dq.profiling.schema_run.start", "category": "quality", "description": "Execução de profiling iniciada para um schema.", "entity_types": ["schema"]},
    {"event_key": "dq.profiling.schema_run.finish", "category": "quality", "description": "Execução de profiling concluída para um schema.", "entity_types": ["schema"]},
    {"event_key": "glossary.term.assign", "category": "governance", "description": "Termo de negócio vinculado a um ativo.", "entity_types": ["table", "column"]},
    {"event_key": "glossary.term.create", "category": "governance", "description": "Termo de negócio criado no glossário.", "entity_types": ["glossary_term"]},
    {"event_key": "glossary.term.delete", "category": "governance", "description": "Termo de negócio removido do glossário.", "entity_types": ["glossary_term"]},
    {"event_key": "glossary.term.unassign", "category": "governance", "description": "Termo de negócio removido de um ativo.", "entity_types": ["table", "column"]},
    {"event_key": "glossary.term.update", "category": "governance", "description": "Termo de negócio atualizado.", "entity_types": ["glossary_term"]},
    {"event_key": "governance.classification_review.batch_promote", "category": "governance", "description": "Itens da revisão de classificação promovidos para tratamento.", "entity_types": ["table"]},
    {"event_key": "governance.recommendation.assistant.execute", "category": "governance", "description": "Ação assistida executada a partir de uma recommendation.", "entity_types": ["governance_recommendation"]},
    {"event_key": "governance.recommendation.batch_policy_apply", "category": "governance", "description": "Policies aplicadas em lote na fila central.", "entity_types": ["governance_recommendation"]},
    {"event_key": "governance.recommendation.batch_resolve", "category": "governance", "description": "Recommendations resolvidas em lote.", "entity_types": ["governance_recommendation"]},
    {"event_key": "governance.recommendation.feedback", "category": "governance", "description": "Feedback registrado para recommendation assistida.", "entity_types": ["governance_recommendation"]},
    {"event_key": "incident.create", "category": "incident", "description": "Incidente aberto na plataforma.", "entity_types": ["incident"]},
    {"event_key": "incident.update", "category": "incident", "description": "Incidente atualizado na plataforma.", "entity_types": ["incident"]},
    {"event_key": "incident.delete", "category": "incident", "description": "Incidente removido na plataforma.", "entity_types": ["incident"]},
    {"event_key": "lineage.asset.create", "category": "operation", "description": "Ativo de linhagem criado.", "entity_types": ["lineage_asset"]},
    {"event_key": "lineage.asset.ensure_from_table", "category": "operation", "description": "Ativo de linhagem garantido a partir de uma tabela.", "entity_types": ["table"]},
    {"event_key": "lineage.asset.update", "category": "operation", "description": "Ativo de linhagem atualizado.", "entity_types": ["lineage_asset"]},
    {"event_key": "lineage.column_edge.deactivate", "category": "operation", "description": "Relação de coluna desativada na linhagem.", "entity_types": ["lineage_column_edge"]},
    {"event_key": "lineage.column_edge.manual_upsert", "category": "operation", "description": "Relação de coluna criada ou atualizada manualmente.", "entity_types": ["lineage_column_edge"]},
    {"event_key": "lineage.column_edge.update", "category": "operation", "description": "Relação de coluna atualizada.", "entity_types": ["lineage_column_edge"]},
    {"event_key": "lineage.relation.create", "category": "operation", "description": "Relação de linhagem criada.", "entity_types": ["lineage_relation"]},
    {"event_key": "lineage.relation.deactivate", "category": "operation", "description": "Relação de linhagem desativada.", "entity_types": ["lineage_relation"]},
    {"event_key": "lineage.relation.update", "category": "operation", "description": "Relação de linhagem atualizada.", "entity_types": ["lineage_relation"]},
    {"event_key": "lineage.source.create", "category": "operation", "description": "Fonte de linhagem cadastrada.", "entity_types": ["lineage_source"]},
    {"event_key": "lineage.source.sync", "category": "operation", "description": "Sincronização de fonte de linhagem executada.", "entity_types": ["lineage_source"]},
    {"event_key": "lineage.source.update", "category": "operation", "description": "Fonte de linhagem atualizada.", "entity_types": ["lineage_source"]},
    {"event_key": "lineage.spec.upsert", "category": "operation", "description": "Especificação de linhagem criada ou atualizada.", "entity_types": ["lineage_spec"]},
    {"event_key": "lineage.table.sync", "category": "operation", "description": "Tabela sincronizada na malha de linhagem.", "entity_types": ["table"]},
    {"event_key": "platform.cockpit.incident.auto_open", "category": "platform", "description": "Incidente operacional aberto automaticamente pelo cockpit.", "entity_types": ["incident"]},
    {"event_key": "platform.cockpit.incident.open", "category": "platform", "description": "Incidente operacional aberto manualmente pelo cockpit.", "entity_types": ["incident"]},
    {"event_key": "platform.cockpit.scan.reprocess", "category": "platform", "description": "Reprocessamento operacional de scan solicitado no cockpit.", "entity_types": ["scan_run"]},
    {"event_key": "platform.automation.rule.create", "category": "platform", "description": "Regra de automação criada.", "entity_types": ["platform_automation_rule"]},
    {"event_key": "platform.automation.rule.update", "category": "platform", "description": "Regra de automação atualizada.", "entity_types": ["platform_automation_rule"]},
    {"event_key": "platform.automation.rule.delete", "category": "platform", "description": "Regra de automação removida.", "entity_types": ["platform_automation_rule"]},
    {"event_key": "platform.automation.action.execute", "category": "platform", "description": "Ação operacional automatizada ou assistida executada.", "entity_types": ["platform_automation_execution"]},
    {"event_key": "platform.automation.action.suggest", "category": "platform", "description": "Ação operacional sugerida pela regra de automação.", "entity_types": ["platform_automation_execution"]},
    {"event_key": "platform.read_models.refresh", "category": "platform", "description": "Read models de busca e dashboard atualizados.", "entity_types": ["platform_read_model"]},
    {"event_key": "platform.usage.ops_cockpit.open_existing_incident", "category": "platform", "description": "Uso registrado para abertura de incidente já existente no cockpit.", "entity_types": ["incident"]},
    {"event_key": "platform.usage.ops_cockpit.open_incident", "category": "platform", "description": "Uso registrado para abertura manual de incidente no cockpit.", "entity_types": ["incident"]},
    {"event_key": "platform.usage.ops_cockpit.open_incident_auto", "category": "platform", "description": "Uso registrado para abertura automática de incidente no cockpit.", "entity_types": ["incident"]},
    {"event_key": "platform.usage.ops_cockpit.reprocess_scan", "category": "platform", "description": "Uso registrado para reprocessar scan operacional.", "entity_types": ["datasource"]},
    {"event_key": "platform.usage.ops_cockpit.rerun_profiling", "category": "platform", "description": "Uso registrado para reenfileirar profiling.", "entity_types": ["table"]},
    {"event_key": "platform.visibility_rule.create", "category": "platform", "description": "Regra de visibilidade criada.", "entity_types": ["asset_visibility_rule"]},
    {"event_key": "platform.visibility_rule.delete", "category": "platform", "description": "Regra de visibilidade removida.", "entity_types": ["asset_visibility_rule"]},
    {"event_key": "stewardship.request.create", "category": "governance", "description": "Solicitação de stewardship criada.", "entity_types": ["stewardship_request"]},
    {"event_key": "table.certification.patch", "category": "certification", "description": "Certificação atualizada para um ativo.", "entity_types": ["table"]},
    {"event_key": "table.owner.review", "category": "governance", "description": "Revisão de owner registrada para um ativo.", "entity_types": ["table"]},
    {"event_key": "table.privacy.patch", "category": "classification", "description": "Privacidade/classificação atualizada para um ativo.", "entity_types": ["table"]},
    {"event_key": "table.privacy.review", "category": "classification", "description": "Revisão de privacidade concluída para um ativo.", "entity_types": ["table"]},
    {"event_key": "tag.assign", "category": "tags", "description": "Tag aplicada a um ativo.", "entity_types": ["table", "column"]},
    {"event_key": "tag.create", "category": "tags", "description": "Tag criada no catálogo.", "entity_types": ["tag"]},
    {"event_key": "tag.delete", "category": "tags", "description": "Tag removida do catálogo.", "entity_types": ["tag"]},
    {"event_key": "tag.import", "category": "tags", "description": "Carga de tags importada para o catálogo.", "entity_types": ["tag"]},
    {"event_key": "tag.intelligence.event.apply", "category": "tags", "description": "Sugestão inteligente de tag aplicada.", "entity_types": ["tag_intelligence_event"]},
    {"event_key": "tag.intelligence.event.apply_batch", "category": "tags", "description": "Sugestões inteligentes de tag aplicadas em lote.", "entity_types": ["tag_intelligence_event"]},
    {"event_key": "tag.intelligence.event.block", "category": "tags", "description": "Sugestão inteligente de tag bloqueada.", "entity_types": ["tag_intelligence_event"]},
    {"event_key": "tag.intelligence.event.block_batch", "category": "tags", "description": "Sugestões inteligentes de tag bloqueadas em lote.", "entity_types": ["tag_intelligence_event"]},
    {"event_key": "tag.intelligence.reprocess", "category": "tags", "description": "Reprocessamento da inteligência de tags executado.", "entity_types": ["tag_intelligence_event"]},
    {"event_key": "tag.unassign", "category": "tags", "description": "Tag removida de um ativo.", "entity_types": ["table", "column"]},
    {"event_key": "tag.update", "category": "tags", "description": "Tag atualizada no catálogo.", "entity_types": ["tag"]},
]


def list_supported_platform_events() -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for item in _SUPPORTED_PLATFORM_EVENTS:
        event_key = str(item["event_key"]).strip().lower()
        if not should_emit_platform_domain_event(event_key, "platform"):
            continue
        category = str(item["category"]).strip().lower()
        items.append(
            {
                "event_key": event_key,
                "display_name": item.get("display_name") or _titleize_event_key(event_key),
                "description": item.get("description") or "Evento suportado pela plataforma.",
                "category": category,
                "category_label": _CATEGORY_LABELS.get(category, category.replace("_", " ").title()),
                "supported": True,
                "active": True,
                "version": "v1",
                "entity_types": list(item.get("entity_types") or []),
                "payload_summary": item.get("payload_summary") or "Payload canônico do evento + contexto do evento.",
                "payload_example_json": {
                    "event": {
                        "event_key": event_key,
                        "category": category,
                        "title": item.get("display_name") or _titleize_event_key(event_key),
                        "summary": item.get("description") or "Evento suportado pela plataforma.",
                        "entity_types": list(item.get("entity_types") or []),
                        "version": "v1",
                    },
                },
            }
        )
    items.sort(key=lambda row: (row["category"], row["event_key"]))
    return {
        "generated_at": None,
        "total": len(items),
        "items": items,
    }


def supported_platform_event_keys() -> set[str]:
    return {item["event_key"] for item in list_supported_platform_events()["items"]}


def supported_platform_event_categories() -> set[str]:
    return {item["category"] for item in list_supported_platform_events()["items"]}


__all__ = [
    "list_supported_platform_events",
    "supported_platform_event_categories",
    "supported_platform_event_keys",
]
