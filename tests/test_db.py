"""
Unit tests for db.py — the SQLite persistence layer behind IQMS Chat.

All tests use a temp file DB so we never touch data/conversations.db.
"""

import os
import sys
import time
import tempfile
from pathlib import Path

import pytest

# Make the app directory importable when pytest runs from any CWD.
APP_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(APP_DIR))

import db as db_module  # noqa: E402


@pytest.fixture
def tmp_db(tmp_path):
    """Yield a path to a fresh, initialized DB file. Auto-cleaned by pytest."""
    db_path = tmp_path / "test_conversations.db"
    db_module.init_db(db_path)
    yield db_path


def test_init_db_is_idempotent(tmp_path):
    """init_db should be safe to call repeatedly."""
    db_path = tmp_path / "idem.db"
    db_module.init_db(db_path)
    # Insert a chat to prove the schema exists
    chat_id = db_module.create_chat("alice", eplant_id=2, db_path=db_path)
    assert chat_id

    # Calling init_db again should not blow away data or error
    db_module.init_db(db_path)
    db_module.init_db(db_path)

    chats = db_module.get_chats("alice", db_path=db_path)
    assert len(chats) == 1
    assert chats[0]["id"] == chat_id


def test_create_and_get_chats_roundtrip(tmp_db):
    chat_a = db_module.create_chat("alice", eplant_id=1, title="hello", db_path=tmp_db)
    chat_b = db_module.create_chat("alice", eplant_id=2, db_path=tmp_db)
    chat_c = db_module.create_chat("bob",   eplant_id=1, db_path=tmp_db)

    alice_chats = db_module.get_chats("alice", db_path=tmp_db)
    bob_chats = db_module.get_chats("bob", db_path=tmp_db)

    assert {c["id"] for c in alice_chats} == {chat_a, chat_b}
    assert [c["id"] for c in bob_chats] == [chat_c]

    # Verify shape includes message_count and timestamps
    for c in alice_chats:
        assert "message_count" in c
        assert c["message_count"] == 0
        assert c["created_at"]
        assert c["last_modified"]
        assert c["username"] == "alice"


def test_add_message_increments_turn(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)

    t1 = db_module.add_message(chat_id, "user", "first question", db_path=tmp_db)
    t2 = db_module.add_message(chat_id, "assistant", "first answer", db_path=tmp_db)
    t3 = db_module.add_message(chat_id, "user", "followup", db_path=tmp_db)
    t4 = db_module.add_message(chat_id, "assistant", "second answer",
                               report_url="/reports/foo.md", db_path=tmp_db)

    assert (t1, t2, t3, t4) == (1, 2, 3, 4)

    msgs = db_module.get_messages(chat_id, db_path=tmp_db)
    assert [m["turn"] for m in msgs] == [1, 2, 3, 4]
    assert [m["role"] for m in msgs] == ["user", "assistant", "user", "assistant"]
    assert msgs[3]["report_url"] == "/reports/foo.md"

    # Other chats are independent
    other_chat = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    other_t1 = db_module.add_message(other_chat, "user", "different chat", db_path=tmp_db)
    assert other_t1 == 1


def test_first_user_message_sets_title(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    chat = db_module.get_chat(chat_id, "alice", db_path=tmp_db)
    assert chat["title"] is None

    db_module.add_message(chat_id, "user", "How many work orders ran yesterday?", db_path=tmp_db)
    chat = db_module.get_chat(chat_id, "alice", db_path=tmp_db)
    assert chat["title"] == "How many work orders ran yesterday?"

    # Subsequent messages don't overwrite the title
    db_module.add_message(chat_id, "assistant", "Here's the answer.", db_path=tmp_db)
    db_module.add_message(chat_id, "user", "What about today?", db_path=tmp_db)
    chat = db_module.get_chat(chat_id, "alice", db_path=tmp_db)
    assert chat["title"] == "How many work orders ran yesterday?"


def test_first_user_message_truncates_to_60_chars(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    long_q = "A" * 200
    db_module.add_message(chat_id, "user", long_q, db_path=tmp_db)
    chat = db_module.get_chat(chat_id, "alice", db_path=tmp_db)
    assert chat["title"] == "A" * 60


def test_explicit_title_not_overwritten(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, title="My Custom Title", db_path=tmp_db)
    db_module.add_message(chat_id, "user", "first question", db_path=tmp_db)
    chat = db_module.get_chat(chat_id, "alice", db_path=tmp_db)
    assert chat["title"] == "My Custom Title"


def test_delete_chat_cascades_messages(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    db_module.add_message(chat_id, "user", "q1", db_path=tmp_db)
    db_module.add_message(chat_id, "assistant", "a1", db_path=tmp_db)
    assert len(db_module.get_messages(chat_id, db_path=tmp_db)) == 2

    deleted = db_module.delete_chat(chat_id, "alice", db_path=tmp_db)
    assert deleted is True
    assert db_module.get_chat(chat_id, "alice", db_path=tmp_db) is None
    assert db_module.get_messages(chat_id, db_path=tmp_db) == []


def test_get_chat_enforces_ownership(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    assert db_module.get_chat(chat_id, "alice", db_path=tmp_db) is not None
    assert db_module.get_chat(chat_id, "bob", db_path=tmp_db) is None


def test_delete_chat_enforces_ownership(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    db_module.add_message(chat_id, "user", "q1", db_path=tmp_db)

    # Bob can't delete Alice's chat
    deleted = db_module.delete_chat(chat_id, "bob", db_path=tmp_db)
    assert deleted is False
    assert db_module.get_chat(chat_id, "alice", db_path=tmp_db) is not None
    assert len(db_module.get_messages(chat_id, db_path=tmp_db)) == 1


def test_update_chat_title_enforces_ownership(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)

    assert db_module.update_chat_title(chat_id, "bob", "hijack", db_path=tmp_db) is False
    chat = db_module.get_chat(chat_id, "alice", db_path=tmp_db)
    assert chat["title"] is None

    assert db_module.update_chat_title(chat_id, "alice", "renamed", db_path=tmp_db) is True
    chat = db_module.get_chat(chat_id, "alice", db_path=tmp_db)
    assert chat["title"] == "renamed"


def test_add_message_updates_last_modified(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    chat0 = db_module.get_chat(chat_id, "alice", db_path=tmp_db)
    initial_lm = chat0["last_modified"]

    # Sleep just over a second so the ISO-second-resolution timestamp definitely changes
    time.sleep(1.1)
    db_module.add_message(chat_id, "user", "q1", db_path=tmp_db)
    chat1 = db_module.get_chat(chat_id, "alice", db_path=tmp_db)
    assert chat1["last_modified"] > initial_lm

    time.sleep(1.1)
    db_module.add_message(chat_id, "assistant", "a1", db_path=tmp_db)
    chat2 = db_module.get_chat(chat_id, "alice", db_path=tmp_db)
    assert chat2["last_modified"] > chat1["last_modified"]


def test_message_count_in_get_chats(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    db_module.add_message(chat_id, "user", "q1", db_path=tmp_db)
    db_module.add_message(chat_id, "assistant", "a1", db_path=tmp_db)
    db_module.add_message(chat_id, "user", "q2", db_path=tmp_db)

    chats = db_module.get_chats("alice", db_path=tmp_db)
    assert len(chats) == 1
    assert chats[0]["message_count"] == 3


def test_attachments_roundtrip_as_json(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    atts = [{"name": "foo.png", "type": "image"}, {"name": "bar.csv", "type": "text"}]
    db_module.add_message(chat_id, "user", "look at these", attachments=atts, db_path=tmp_db)

    msgs = db_module.get_messages(chat_id, db_path=tmp_db)
    assert msgs[0]["attachments"] == atts


def test_invalid_role_rejected(tmp_db):
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    with pytest.raises(ValueError):
        db_module.add_message(chat_id, "system", "nope", db_path=tmp_db)


def test_get_chats_ordered_by_last_modified_desc(tmp_db):
    chat_a = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    time.sleep(1.1)
    chat_b = db_module.create_chat("alice", eplant_id=1, db_path=tmp_db)
    time.sleep(1.1)
    # Bump chat_a's last_modified so it's newest now
    db_module.add_message(chat_a, "user", "ping", db_path=tmp_db)

    chats = db_module.get_chats("alice", db_path=tmp_db)
    assert [c["id"] for c in chats] == [chat_a, chat_b]
