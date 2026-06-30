"""cleanup tag automation rules

Revision ID: b3c4d5e6f7a8
Revises: b2c3d4e5f6a7
Create Date: 2026-04-12 20:10:00.000000
"""

from alembic import op
from sqlalchemy import text


revision = "b3c4d5e6f7a8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
            UPDATE t2c_data.tag_automation_rules
               SET status = 'inactive'
             WHERE tag_id NOT IN (
                SELECT id FROM t2c_data.tags
                 WHERE slug IN ('nome','endereco','email','telefone','cpf','cnpj','documento','data-nascimento')
             )
            """
        )
    )


def downgrade() -> None:
    # Não reativa regras antigas automaticamente.
    pass
