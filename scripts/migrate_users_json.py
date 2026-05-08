"""
One-shot migration: read data/users.json (legacy local-account store) into
the SQLite users table. Idempotent — if a username already exists in the DB
we leave the existing row alone (we don't blow away an SSO-provisioned row
just because someone re-runs migrations).

Run on app startup; safe to run repeatedly.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Locate project root regardless of where this is imported from.
ROOT = Path(__file__).resolve().parent.parent


def migrate(
    users_json_path: Optional[Path] = None,
    db_path: Optional[Path] = None,
) -> dict:
    """
    Returns a dict {migrated, skipped, missing} for visibility. Never raises
    on a missing users.json — that just means there's nothing to migrate.
    """
    import db as _db  # late import so this script works as a module too

    users_file = users_json_path or (ROOT / "data" / "users.json")
    summary = {"migrated": 0, "skipped": 0, "missing": False}

    if not users_file.exists():
        summary["missing"] = True
        return summary

    try:
        users = json.loads(users_file.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("migrate_users_json: cannot read %s (%s)", users_file, exc)
        summary["missing"] = True
        return summary

    for username, udata in users.items():
        if _db.get_user(username, db_path=db_path) is not None:
            summary["skipped"] += 1
            continue

        # Legacy admin row: no email, has hash + salt, is_admin → super_admin.
        _db.upsert_user(
            username=username,
            email=udata.get("email"),
            display_name=udata.get("display_name", username),
            is_super_admin=bool(udata.get("is_admin")),
            pw_hash=udata.get("hash"),
            pw_salt=udata.get("salt"),
            db_path=db_path,
        )
        summary["migrated"] += 1

    return summary


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(ROOT))
    result = migrate()
    print(f"migrate_users_json: {result}")
