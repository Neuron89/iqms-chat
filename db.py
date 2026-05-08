"""
SQLite persistence layer for IQMS Chat.

Stores chat sessions and messages so conversations survive across restarts and
so we can inject prior context into each `claude -p` call. Pure stdlib sqlite3
(no ORM) to match the existing app.py style.
"""

import json
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional


DB_PATH = Path(__file__).parent / "data" / "conversations.db"


SCHEMA = """
CREATE TABLE IF NOT EXISTS chats (
  id TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  title TEXT,
  eplant_id INTEGER,
  created_at TEXT NOT NULL,
  last_modified TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chats_username ON chats(username);
CREATE INDEX IF NOT EXISTS idx_chats_last_modified ON chats(username, last_modified DESC);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
  turn INTEGER NOT NULL,
  role TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
  content TEXT NOT NULL,
  report_url TEXT,
  attachments TEXT,
  timestamp TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id, turn);

-- Phase 2: SSO user store (replaces data/users.json long-term).
CREATE TABLE IF NOT EXISTS users (
  username       TEXT PRIMARY KEY,
  email          TEXT UNIQUE,
  display_name   TEXT NOT NULL,
  active         INTEGER NOT NULL DEFAULT 1,
  is_super_admin INTEGER NOT NULL DEFAULT 0,
  pw_hash        TEXT,
  pw_salt        TEXT,
  created_at     TEXT NOT NULL,
  last_login     TEXT
);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- Phase 2: cached IQMS permission packet per user. One-to-one with users.
CREATE TABLE IF NOT EXISTS iqms_permissions (
  username                TEXT PRIMARY KEY REFERENCES users(username) ON DELETE CASCADE,
  iqms_user_name          TEXT,
  role_names              TEXT NOT NULL,
  module_prefixes         TEXT NOT NULL,
  allowed_table_prefixes  TEXT NOT NULL,
  eplant_ids              TEXT NOT NULL,
  tier                    TEXT NOT NULL CHECK(tier IN ('viewer','operator','admin')),
  synced_at               TEXT NOT NULL,
  raw_packet              TEXT
);
"""


def _connect(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Open a SQLite connection with foreign keys enforced and dict-like rows."""
    path = db_path if db_path is not None else DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(db_path: Optional[Path] = None) -> None:
    """Create tables and indexes if they don't exist. Idempotent."""
    conn = _connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def create_chat(
    username: str,
    eplant_id: Optional[int] = None,
    title: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> str:
    """Create a new chat row and return its UUID."""
    chat_id = str(uuid.uuid4())
    now = _now()
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO chats (id, username, title, eplant_id, created_at, last_modified) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (chat_id, username, title, eplant_id, now, now),
        )
        conn.commit()
    finally:
        conn.close()
    return chat_id


def get_chats(username: str, db_path: Optional[Path] = None) -> list[dict]:
    """Return all chats for a user, most recent first, with message_count."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT c.id, c.username, c.title, c.eplant_id, c.created_at, c.last_modified,
                   COALESCE((SELECT COUNT(*) FROM messages m WHERE m.chat_id = c.id), 0) AS message_count
            FROM chats c
            WHERE c.username = ?
            ORDER BY c.last_modified DESC
            """,
            (username,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_chat(
    chat_id: str,
    username: str,
    db_path: Optional[Path] = None,
) -> Optional[dict]:
    """Return chat dict if owned by username, else None."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT c.id, c.username, c.title, c.eplant_id, c.created_at, c.last_modified,
                   COALESCE((SELECT COUNT(*) FROM messages m WHERE m.chat_id = c.id), 0) AS message_count
            FROM chats c
            WHERE c.id = ? AND c.username = ?
            """,
            (chat_id, username),
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def get_messages(chat_id: str, db_path: Optional[Path] = None) -> list[dict]:
    """Return all messages for a chat, ordered by turn ASC. Decodes attachments JSON."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, chat_id, turn, role, content, report_url, attachments, timestamp "
            "FROM messages WHERE chat_id = ? ORDER BY turn ASC",
            (chat_id,),
        ).fetchall()
        out = []
        for r in rows:
            d = _row_to_dict(r)
            if d.get("attachments"):
                try:
                    d["attachments"] = json.loads(d["attachments"])
                except (json.JSONDecodeError, TypeError):
                    d["attachments"] = None
            else:
                d["attachments"] = None
            out.append(d)
        return out
    finally:
        conn.close()


def add_message(
    chat_id: str,
    role: str,
    content: str,
    report_url: Optional[str] = None,
    attachments: Optional[list] = None,
    db_path: Optional[Path] = None,
) -> int:
    """
    Append a message to a chat. Auto-increments turn, updates the parent chat's
    last_modified, and (if title is null) sets the chat title from the first
    user message (first 60 chars). Returns the new turn number.
    """
    if role not in ("user", "assistant"):
        raise ValueError(f"role must be 'user' or 'assistant', got {role!r}")

    now = _now()
    attachments_json = json.dumps(attachments) if attachments else None

    conn = _connect(db_path)
    try:
        # Compute next turn (max + 1, default 1)
        row = conn.execute(
            "SELECT COALESCE(MAX(turn), 0) AS max_turn FROM messages WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
        next_turn = (row["max_turn"] or 0) + 1

        conn.execute(
            "INSERT INTO messages (chat_id, turn, role, content, report_url, attachments, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (chat_id, next_turn, role, content, report_url, attachments_json, now),
        )

        # Update parent chat's last_modified, and set title from first user message if null
        if role == "user":
            chat_row = conn.execute(
                "SELECT title FROM chats WHERE id = ?", (chat_id,),
            ).fetchone()
            if chat_row and not chat_row["title"]:
                derived = (content or "").strip().splitlines()[0] if content else ""
                derived = derived[:60].strip() or "New chat"
                conn.execute(
                    "UPDATE chats SET title = ?, last_modified = ? WHERE id = ?",
                    (derived, now, chat_id),
                )
            else:
                conn.execute(
                    "UPDATE chats SET last_modified = ? WHERE id = ?",
                    (now, chat_id),
                )
        else:
            conn.execute(
                "UPDATE chats SET last_modified = ? WHERE id = ?",
                (now, chat_id),
            )

        conn.commit()
        return next_turn
    finally:
        conn.close()


def update_chat_title(
    chat_id: str,
    username: str,
    title: str,
    db_path: Optional[Path] = None,
) -> bool:
    """Rename a chat. Enforces ownership. Returns True if a row was updated."""
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "UPDATE chats SET title = ?, last_modified = ? WHERE id = ? AND username = ?",
            (title, _now(), chat_id, username),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def delete_chat(
    chat_id: str,
    username: str,
    db_path: Optional[Path] = None,
) -> bool:
    """Delete a chat (and cascade its messages). Enforces ownership."""
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "DELETE FROM chats WHERE id = ? AND username = ?",
            (chat_id, username),
        )
        # Only sweep messages if the owner check actually deleted the chat. We
        # have ON DELETE CASCADE + PRAGMA foreign_keys = ON so this is mostly
        # belt-and-suspenders for older SQLite builds.
        if cur.rowcount > 0:
            conn.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Phase 2: users + iqms_permissions helpers
# ---------------------------------------------------------------------------

def upsert_user(
    username: str,
    email: Optional[str],
    display_name: str,
    is_super_admin: bool = False,
    pw_hash: Optional[str] = None,
    pw_salt: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> dict:
    """
    Create or update a user row. Idempotent — preserves `active` and `pw_hash`/
    `pw_salt` on existing rows unless explicitly overridden by non-None values.
    Returns the resulting row as a dict.
    """
    now = _now()
    conn = _connect(db_path)
    try:
        existing = conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,),
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO users (username, email, display_name, active, "
                "is_super_admin, pw_hash, pw_salt, created_at, last_login) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?, NULL)",
                (
                    username, email, display_name,
                    1 if is_super_admin else 0,
                    pw_hash, pw_salt, now,
                ),
            )
        else:
            # Only update password fields when explicitly provided. Display
            # name and email always refresh from latest claim.
            new_hash = pw_hash if pw_hash is not None else existing["pw_hash"]
            new_salt = pw_salt if pw_salt is not None else existing["pw_salt"]
            conn.execute(
                "UPDATE users SET email = ?, display_name = ?, "
                "is_super_admin = ?, pw_hash = ?, pw_salt = ? "
                "WHERE username = ?",
                (
                    email, display_name,
                    1 if is_super_admin else 0,
                    new_hash, new_salt, username,
                ),
            )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,),
        ).fetchone()
        return _row_to_dict(row)
    finally:
        conn.close()


def get_user(username: str, db_path: Optional[Path] = None) -> Optional[dict]:
    """Return the user row by username (PK), or None if not found."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,),
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def set_user_active(
    username: str,
    active: bool,
    db_path: Optional[Path] = None,
) -> bool:
    """Toggle the active flag on a user. Returns True if a row was updated."""
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "UPDATE users SET active = ? WHERE username = ?",
            (1 if active else 0, username),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def touch_last_login(username: str, db_path: Optional[Path] = None) -> None:
    """Stamp last_login = now() for the given user. No-op if user missing."""
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE users SET last_login = ? WHERE username = ?",
            (_now(), username),
        )
        conn.commit()
    finally:
        conn.close()


def list_users(db_path: Optional[Path] = None) -> list[dict]:
    """
    Return all users with their IQMS permissions (left-joined). Decoded JSON
    fields (`role_names`, `module_prefixes`, etc.) come back as Python lists.
    """
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT u.username, u.email, u.display_name, u.active,
                   u.is_super_admin, u.created_at, u.last_login,
                   p.iqms_user_name, p.role_names, p.module_prefixes,
                   p.allowed_table_prefixes, p.eplant_ids, p.tier,
                   p.synced_at
            FROM users u
            LEFT JOIN iqms_permissions p ON p.username = u.username
            ORDER BY u.created_at DESC
            """,
        ).fetchall()
        out = []
        for r in rows:
            d = _row_to_dict(r)
            for jf in (
                "role_names", "module_prefixes",
                "allowed_table_prefixes", "eplant_ids",
            ):
                if d.get(jf):
                    try:
                        d[jf] = json.loads(d[jf])
                    except (json.JSONDecodeError, TypeError):
                        d[jf] = []
                else:
                    d[jf] = []
            out.append(d)
        return out
    finally:
        conn.close()


def upsert_iqms_permissions(
    username: str,
    packet: dict,
    db_path: Optional[Path] = None,
) -> dict:
    """
    Persist an IQMS permission packet. `packet` shape (all keys required):
      iqms_user_name (str|None), role_names (list[str]),
      module_prefixes (list[str]), allowed_table_prefixes (list[str]),
      eplant_ids (list[int]), tier ('viewer'|'operator'|'admin'),
      synced_at (ISO str).
    Optional: raw_packet (dict) — full original lookup for forensics.
    """
    role_names = json.dumps(packet.get("role_names") or [])
    module_prefixes = json.dumps(packet.get("module_prefixes") or [])
    allowed_table_prefixes = json.dumps(packet.get("allowed_table_prefixes") or [])
    eplant_ids = json.dumps(packet.get("eplant_ids") or [])
    tier = packet.get("tier") or "viewer"
    if tier not in ("viewer", "operator", "admin"):
        tier = "viewer"
    raw = packet.get("raw_packet")
    raw_json = json.dumps(raw) if raw is not None else json.dumps(packet)
    synced_at = packet.get("synced_at") or _now()
    iqms_user_name = packet.get("iqms_user_name")

    conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO iqms_permissions
              (username, iqms_user_name, role_names, module_prefixes,
               allowed_table_prefixes, eplant_ids, tier, synced_at, raw_packet)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
              iqms_user_name = excluded.iqms_user_name,
              role_names = excluded.role_names,
              module_prefixes = excluded.module_prefixes,
              allowed_table_prefixes = excluded.allowed_table_prefixes,
              eplant_ids = excluded.eplant_ids,
              tier = excluded.tier,
              synced_at = excluded.synced_at,
              raw_packet = excluded.raw_packet
            """,
            (
                username, iqms_user_name, role_names, module_prefixes,
                allowed_table_prefixes, eplant_ids, tier, synced_at, raw_json,
            ),
        )
        conn.commit()
        return get_iqms_permissions(username, db_path=db_path) or {}
    finally:
        conn.close()


def get_iqms_permissions(
    username: str,
    db_path: Optional[Path] = None,
) -> Optional[dict]:
    """Return the cached permission packet for a user, decoded into Python lists."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM iqms_permissions WHERE username = ?", (username,),
        ).fetchone()
        if row is None:
            return None
        d = _row_to_dict(row)
        for jf in (
            "role_names", "module_prefixes",
            "allowed_table_prefixes", "eplant_ids",
        ):
            if d.get(jf):
                try:
                    d[jf] = json.loads(d[jf])
                except (json.JSONDecodeError, TypeError):
                    d[jf] = []
            else:
                d[jf] = []
        if d.get("raw_packet"):
            try:
                d["raw_packet"] = json.loads(d["raw_packet"])
            except (json.JSONDecodeError, TypeError):
                pass
        return d
    finally:
        conn.close()


# Auto-init on import — matches Flask app's "create on first run" pattern.
init_db()
