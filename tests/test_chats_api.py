"""
Tests for the /api/chats REST endpoints.

API contract (from backend agent's plan):
  GET    /api/chats           -> list of {id,title,eplant_id,created_at,last_modified,message_count}
  GET    /api/chats/<id>      -> {chat: {...}, messages: [...]}
  POST   /api/chats           body {title?, eplant_id?} -> {id, title, eplant_id, created_at}
  PATCH  /api/chats/<id>      body {title}              -> {ok: true}
  DELETE /api/chats/<id>                                -> {ok: true}

These tests are skipped (xfail) until /api/chats is wired into app.py.
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path

import pytest


def _api_chats_implemented(app) -> bool:
    """Quick probe: does /api/chats route exist?"""
    rules = [str(r) for r in app.url_map.iter_rules()]
    return any(r.startswith("/api/chats") for r in rules)


@pytest.fixture(autouse=True)
def _skip_if_not_implemented(app):
    if not _api_chats_implemented(app):
        pytest.xfail("awaiting backend agent: /api/chats endpoints not yet implemented")


UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# GET /api/chats
# ---------------------------------------------------------------------------

class TestListChats:

    def test_empty_list_is_empty_array_not_500(self, auth_client):
        r = auth_client.get("/api/chats")
        assert r.status_code == 200
        assert r.get_json() == []

    def test_returns_only_current_users_chats(self, auth_client, db_module, app):
        # Insert chats for alice and bob directly via db.py
        db_path = app.config["TEST_DB_PATH"]
        my_id = db_module.create_chat("alice", eplant_id=1, title="mine", db_path=db_path)
        other_id = db_module.create_chat("bob", eplant_id=1, title="bob's", db_path=db_path)

        r = auth_client.get("/api/chats")
        assert r.status_code == 200
        ids = [c["id"] for c in r.get_json()]
        assert my_id in ids
        assert other_id not in ids

    def test_sorted_by_last_modified_desc(self, auth_client, db_module, app):
        """
        Force a last_modified delta by writing rows directly with explicit
        timestamps (db.py uses second-precision _now(), so a real-time gap
        of <1s would tie).
        """
        db_path = app.config["TEST_DB_PATH"]
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute(
                "INSERT INTO chats (id, username, title, eplant_id, created_at, last_modified) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("aaaa-old", "alice", "older", 1,
                 "2026-05-01T10:00:00", "2026-05-01T10:00:00"),
            )
            conn.execute(
                "INSERT INTO chats (id, username, title, eplant_id, created_at, last_modified) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("bbbb-new", "alice", "newer", 1,
                 "2026-05-08T10:00:00", "2026-05-08T10:00:00"),
            )
            conn.commit()
        finally:
            conn.close()

        r = auth_client.get("/api/chats")
        ids = [c["id"] for c in r.get_json()]
        assert ids.index("bbbb-new") < ids.index("aaaa-old")

    def test_each_entry_has_required_fields(self, auth_client, db_module, app):
        db_path = app.config["TEST_DB_PATH"]
        cid = db_module.create_chat("alice", title="t", eplant_id=2, db_path=db_path)

        r = auth_client.get("/api/chats")
        entries = r.get_json()
        assert len(entries) >= 1
        entry = next(e for e in entries if e["id"] == cid)
        for field in ("id", "title", "eplant_id", "created_at", "last_modified", "message_count"):
            assert field in entry, f"missing field: {field}"

    def test_requires_login(self, client):
        r = client.get("/api/chats", follow_redirects=False)
        assert r.status_code in (302, 401)


# ---------------------------------------------------------------------------
# GET /api/chats/<id>
# ---------------------------------------------------------------------------

class TestGetChatDetail:

    def test_returns_chat_and_messages_in_turn_order(self, auth_client, db_module, app):
        db_path = app.config["TEST_DB_PATH"]
        cid = db_module.create_chat("alice", db_path=db_path)
        db_module.add_message(cid, "user",      "Q1", db_path=db_path)
        db_module.add_message(cid, "assistant", "A1", db_path=db_path)
        db_module.add_message(cid, "user",      "Q2", db_path=db_path)
        db_module.add_message(cid, "assistant", "A2", db_path=db_path)

        r = auth_client.get(f"/api/chats/{cid}")
        assert r.status_code == 200
        body = r.get_json()
        assert "chat" in body and "messages" in body
        msgs = body["messages"]
        assert [m["content"] for m in msgs] == ["Q1", "A1", "Q2", "A2"]
        # Turns must be ascending
        turns = [m["turn"] for m in msgs]
        assert turns == sorted(turns)

    def test_other_users_chat_returns_404(self, auth_client, db_module, app):
        db_path = app.config["TEST_DB_PATH"]
        bobs_chat = db_module.create_chat("bob", title="secret", db_path=db_path)
        r = auth_client.get(f"/api/chats/{bobs_chat}")
        assert r.status_code == 404

    def test_unknown_chat_returns_404(self, auth_client):
        r = auth_client.get(f"/api/chats/{uuid.uuid4()}")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/chats
# ---------------------------------------------------------------------------

class TestCreateChat:

    def test_creates_chat_and_returns_uuid(self, auth_client):
        r = auth_client.post("/api/chats", json={"title": "Hello", "eplant_id": 2})
        assert r.status_code in (200, 201)
        body = r.get_json()
        assert UUID_RE.match(body["id"]), f"expected UUID, got {body['id']!r}"
        assert body.get("title") == "Hello"
        # eplant_id may come back as int or str — accept both
        assert str(body.get("eplant_id")) == "2"
        assert "created_at" in body

    def test_create_with_no_body_uses_defaults(self, auth_client):
        r = auth_client.post("/api/chats", json={})
        assert r.status_code in (200, 201)
        body = r.get_json()
        assert UUID_RE.match(body["id"])

    def test_chat_persists_after_creation(self, auth_client, db_module, app):
        r = auth_client.post("/api/chats", json={"title": "Persisted"})
        cid = r.get_json()["id"]
        db_path = app.config["TEST_DB_PATH"]
        # Should be visible in a follow-up list call
        r2 = auth_client.get("/api/chats")
        ids = [c["id"] for c in r2.get_json()]
        assert cid in ids


# ---------------------------------------------------------------------------
# PATCH /api/chats/<id>
# ---------------------------------------------------------------------------

class TestUpdateChat:

    def test_updates_title(self, auth_client, db_module, app):
        db_path = app.config["TEST_DB_PATH"]
        cid = db_module.create_chat("alice", title="old", db_path=db_path)

        r = auth_client.patch(f"/api/chats/{cid}", json={"title": "new title"})
        assert r.status_code == 200
        assert r.get_json().get("ok") is True

        chat = db_module.get_chat(cid, "alice", db_path=db_path)
        assert chat["title"] == "new title"

    def test_other_users_chat_returns_404(self, auth_client, db_module, app):
        db_path = app.config["TEST_DB_PATH"]
        bobs_chat = db_module.create_chat("bob", title="bob's", db_path=db_path)
        r = auth_client.patch(f"/api/chats/{bobs_chat}", json={"title": "hijacked"})
        assert r.status_code == 404
        # Title NOT changed
        chat = db_module.get_chat(bobs_chat, "bob", db_path=db_path)
        assert chat["title"] == "bob's"

    def test_unknown_chat_returns_404(self, auth_client):
        r = auth_client.patch(f"/api/chats/{uuid.uuid4()}", json={"title": "x"})
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /api/chats/<id>
# ---------------------------------------------------------------------------

class TestDeleteChat:

    def test_deletes_chat_and_messages(self, auth_client, db_module, app):
        db_path = app.config["TEST_DB_PATH"]
        cid = db_module.create_chat("alice", db_path=db_path)
        db_module.add_message(cid, "user", "doomed", db_path=db_path)
        db_module.add_message(cid, "assistant", "ok", db_path=db_path)

        r = auth_client.delete(f"/api/chats/{cid}")
        assert r.status_code == 200
        assert r.get_json().get("ok") is True

        # Chat gone
        assert db_module.get_chat(cid, "alice", db_path=db_path) is None
        # Messages gone (cascade)
        assert db_module.get_messages(cid, db_path=db_path) == []

    def test_other_users_chat_returns_404(self, auth_client, db_module, app):
        db_path = app.config["TEST_DB_PATH"]
        bobs_chat = db_module.create_chat("bob", db_path=db_path)
        db_module.add_message(bobs_chat, "user", "bob's data", db_path=db_path)

        r = auth_client.delete(f"/api/chats/{bobs_chat}")
        assert r.status_code == 404
        # Bob's chat still there
        assert db_module.get_chat(bobs_chat, "bob", db_path=db_path) is not None

    def test_unknown_chat_returns_404(self, auth_client):
        r = auth_client.delete(f"/api/chats/{uuid.uuid4()}")
        assert r.status_code == 404
