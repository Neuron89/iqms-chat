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


# Auto-init on import — matches Flask app's "create on first run" pattern.
init_db()
