from __future__ import annotations

import os
import sys
from collections import Counter
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


def main() -> int:
    os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
    os.environ.setdefault("ENV", "test")
    backend_dir = Path(__file__).resolve().parents[1]
    config = Config(str(backend_dir / "alembic.ini"))
    config.set_main_option("script_location", str(backend_dir / "alembic"))
    script = ScriptDirectory.from_config(config)

    revisions = [revision.revision for revision in script.walk_revisions()]
    duplicated = [revision for revision, count in Counter(revisions).items() if count > 1]
    heads = script.get_heads()

    if duplicated:
        print(f"duplicate revision ids detected: {', '.join(sorted(duplicated))}", file=sys.stderr)
        return 1
    if len(heads) != 1:
        print(f"expected exactly one Alembic head, found {len(heads)}: {', '.join(heads)}", file=sys.stderr)
        return 1

    print(f"Alembic graph OK: {len(revisions)} revisions, head={heads[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
