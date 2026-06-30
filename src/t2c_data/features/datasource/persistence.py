from __future__ import annotations

from sqlalchemy import delete
from sqlalchemy.orm import Session

from t2c_data.models.catalog import DataSource
from t2c_data.models.glossary import GlossaryAssignment
from t2c_data.models.tag import TagAssignment, TagAssignmentOverride, TagIntelligenceEvent


def hard_delete_datasource(session: Session, datasource_id: int) -> bool:
    datasource = session.get(DataSource, datasource_id)
    if datasource is None:
        return False

    # Safety cleanup strictly scoped by datasource_id for non-FK polymorphic tables.
    session.execute(delete(TagAssignment).where(TagAssignment.datasource_id == datasource_id))
    session.execute(delete(TagAssignmentOverride).where(TagAssignmentOverride.datasource_id == datasource_id))
    session.execute(delete(TagIntelligenceEvent).where(TagIntelligenceEvent.datasource_id == datasource_id))
    session.execute(delete(GlossaryAssignment).where(GlossaryAssignment.datasource_id == datasource_id))
    # Canonical lineage assets use FK with ON DELETE SET NULL and should survive datasource
    # removal as governance history; legacy lineage cleanup is being sunset and is no longer
    # coupled to datasource hard delete.

    # Main deletion: all direct children with datasource_id FK cascade from here.
    session.execute(delete(DataSource).where(DataSource.id == datasource_id))
    session.commit()
    return True
