"""add tag automation rules

Revision ID: b2c3d4e5f6a7
Revises: aa12bb34cc56
Create Date: 2026-04-12 19:12:00.000000
"""

from alembic import op
import json
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6a7"
down_revision = "aa12bb34cc56"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tag_automation_rules",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "tag_id",
            sa.Integer(),
            sa.ForeignKey("t2c_data.tags.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("scope", sa.String(length=40), nullable=False, server_default="column"),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="active"),
        sa.Column("action", sa.String(length=30), nullable=False, server_default="apply"),
        sa.Column("category", sa.String(length=60), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("match_fields", sa.JSON(), nullable=True),
        sa.Column("keywords", sa.JSON(), nullable=True),
        sa.Column("aliases", sa.JSON(), nullable=True),
        sa.Column("regex_pattern", sa.Text(), nullable=True),
        sa.Column("min_confidence", sa.Integer(), nullable=False, server_default="90"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_by_user_id",
            sa.Integer(),
            sa.ForeignKey("t2c_data.users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        schema="t2c_data",
    )
    op.create_index("ix_tag_automation_rules_tag_id", "tag_automation_rules", ["tag_id"], schema="t2c_data")
    op.create_index("ix_tag_automation_rules_status", "tag_automation_rules", ["status"], schema="t2c_data")
    op.create_index("ix_tag_automation_rules_scope", "tag_automation_rules", ["scope"], schema="t2c_data")

    tags_table = sa.Table(
        "tags",
        sa.MetaData(),
        sa.Column("slug", sa.String()),
        sa.Column("name", sa.String()),
        sa.Column("status", sa.String()),
        sa.Column("suggested_scope", sa.String()),
        sa.Column("tag_type", sa.String()),
        sa.Column("group_name", sa.String()),
        sa.Column("subgroup_name", sa.String()),
        sa.Column("color", sa.String()),
        schema="t2c_data",
    )
    bind = op.get_bind()
    allowed_slugs = ("nome", "endereco", "email", "telefone", "cpf", "cnpj", "documento", "data-nascimento")
    existing_slugs = {
        row[0]
        for row in bind.execute(
            text(
                """
                SELECT slug
                FROM t2c_data.tags
                WHERE slug IN ('nome','endereco','email','telefone','cpf','cnpj','documento','data-nascimento')
                """
            )
        ).all()
    }
    tag_rows = [
        {
            "slug": "nome",
            "name": "Nome",
            "status": "active",
            "suggested_scope": "column",
            "tag_type": "classificacao_inteligente",
            "group_name": "Classificação inteligente",
            "subgroup_name": "Sensível",
            "color": "#0284c7",
        },
        {
            "slug": "endereco",
            "name": "Endereço",
            "status": "active",
            "suggested_scope": "column",
            "tag_type": "classificacao_inteligente",
            "group_name": "Classificação inteligente",
            "subgroup_name": "Sensível",
            "color": "#14b8a6",
        },
        {
            "slug": "email",
            "name": "Email",
            "status": "active",
            "suggested_scope": "column",
            "tag_type": "classificacao_inteligente",
            "group_name": "Classificação inteligente",
            "subgroup_name": "Sensível",
            "color": "#0ea5e9",
        },
        {
            "slug": "telefone",
            "name": "Telefone",
            "status": "active",
            "suggested_scope": "column",
            "tag_type": "classificacao_inteligente",
            "group_name": "Classificação inteligente",
            "subgroup_name": "Sensível",
            "color": "#22c55e",
        },
        {
            "slug": "cpf",
            "name": "CPF",
            "status": "active",
            "suggested_scope": "column",
            "tag_type": "classificacao_inteligente",
            "group_name": "Classificação inteligente",
            "subgroup_name": "Sensível",
            "color": "#dc2626",
        },
        {
            "slug": "cnpj",
            "name": "CNPJ",
            "status": "active",
            "suggested_scope": "column",
            "tag_type": "classificacao_inteligente",
            "group_name": "Classificação inteligente",
            "subgroup_name": "Sensível",
            "color": "#7c3aed",
        },
        {
            "slug": "documento",
            "name": "Documento",
            "status": "active",
            "suggested_scope": "column",
            "tag_type": "classificacao_inteligente",
            "group_name": "Classificação inteligente",
            "subgroup_name": "Sensível",
            "color": "#2563eb",
        },
        {
            "slug": "data-nascimento",
            "name": "Data de Nascimento",
            "status": "active",
            "suggested_scope": "column",
            "tag_type": "classificacao_inteligente",
            "group_name": "Classificação inteligente",
            "subgroup_name": "Sensível",
            "color": "#a855f7",
        },
    ]
    new_tags = [row for row in tag_rows if row["slug"] not in existing_slugs]
    if new_tags:
        op.bulk_insert(tags_table, new_tags)
    rules = [
        ("nome", "Regra automática: Nome", ["name", "nome", "full_name", "customer_name", "user_name", "nome_completo"], 10),
        ("email", "Regra automática: Email", ["email", "e-mail", "email_address", "user_email", "customer_email", "owner_email"], 10),
        ("telefone", "Regra automática: Telefone", ["telefone", "phone", "phone_number", "celular", "mobile", "whatsapp"], 10),
        ("endereco", "Regra automática: Endereço", ["endereco", "address", "logradouro", "rua", "bairro", "cidade", "cep"], 10),
        ("cpf", "Regra automática: CPF", ["cpf"], 5),
        ("cnpj", "Regra automática: CNPJ", ["cnpj"], 5),
        ("documento", "Regra automática: Documento", ["documento", "doc"], 20),
        ("data-nascimento", "Regra automática: Data de Nascimento", ["data_nascimento", "data nascimento", "birth_date", "dob"], 20),
    ]
    rules_table = sa.Table(
        "tag_automation_rules",
        sa.MetaData(),
        sa.Column("tag_id", sa.Integer()),
        sa.Column("name", sa.String()),
        sa.Column("scope", sa.String()),
        sa.Column("status", sa.String()),
        sa.Column("action", sa.String()),
        sa.Column("category", sa.String()),
        sa.Column("priority", sa.Integer()),
        sa.Column("match_fields", sa.JSON()),
        sa.Column("keywords", sa.JSON()),
        sa.Column("aliases", sa.JSON()),
        sa.Column("regex_pattern", sa.Text()),
        sa.Column("min_confidence", sa.Integer()),
        sa.Column("notes", sa.Text()),
        schema="t2c_data",
    )
    rule_rows = []
    for slug, name, keywords, priority in rules:
        tag_id = bind.execute(text("SELECT id FROM t2c_data.tags WHERE slug = :slug"), {"slug": slug}).scalar()
        if not tag_id:
            continue
        existing = bind.execute(
            text("SELECT id FROM t2c_data.tag_automation_rules WHERE tag_id = :tag_id"),
            {"tag_id": tag_id},
        ).scalar()
        if existing:
            continue
        rule_rows.append(
            {
                "tag_id": tag_id,
                "name": name,
                "scope": "column",
                "status": "active",
                "action": "apply",
                "category": "sensivel",
                "priority": priority,
                "match_fields": ["name", "description", "comment"],
                "keywords": keywords,
                "aliases": [],
                "regex_pattern": None,
                "min_confidence": 90,
                "notes": "Seed sensível controlada",
            }
        )
    if rule_rows:
        op.bulk_insert(rules_table, rule_rows)


def downgrade() -> None:
    op.drop_index("ix_tag_automation_rules_scope", table_name="tag_automation_rules", schema="t2c_data")
    op.drop_index("ix_tag_automation_rules_status", table_name="tag_automation_rules", schema="t2c_data")
    op.drop_index("ix_tag_automation_rules_tag_id", table_name="tag_automation_rules", schema="t2c_data")
    op.drop_table("tag_automation_rules", schema="t2c_data")
