from __future__ import annotations

import os
from collections import Counter
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


def _script_directory() -> ScriptDirectory:
    os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
    os.environ.setdefault("ENV", "test")
    backend_dir = Path(__file__).resolve().parents[1]
    config = Config(str(backend_dir / "alembic.ini"))
    config.set_main_option("script_location", str(backend_dir / "alembic"))
    return ScriptDirectory.from_config(config)


def test_alembic_graph_has_single_head_and_unique_revisions() -> None:
    script = _script_directory()
    revisions = [revision.revision for revision in script.walk_revisions()]
    counts = Counter(revisions)
    duplicated = [revision for revision, count in counts.items() if count > 1]

    assert duplicated == []
    assert len(script.get_heads()) == 1
