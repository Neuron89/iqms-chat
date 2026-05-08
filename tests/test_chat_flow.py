"""
End-to-end /ask integration tests with the claude subprocess mocked.

These exercise the full request -> spawn-claude -> parse-result -> persist
pipeline. Several tests depend on the Backend agent's db.py integration in
app.py and are marked xfail until that lands.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _backend_db_wired(app_module) -> bool:
    """True if /ask appears to use db.py for persistence (rough heuristic)."""
    try:
        import db as _db  # noqa
    except ImportError:
        return False
    src = Path(app_module.__file__).read_text(encoding="utf-8", errors="replace")
    return "import db" in src or "from db " in src or "db.add_message" in src


# ---------------------------------------------------------------------------
# Auth gating
# ---------------------------------------------------------------------------

def test_ask_requires_auth(client):
    r = client.post("/ask", json={"question": "anything"}, follow_redirects=False)
    assert r.status_code in (302, 401)
    if r.status_code == 302:
        assert "/login" in r.headers["Location"]


def test_ask_rejects_empty_question(auth_client, mock_claude_subprocess):
    r = auth_client.post("/ask", json={"question": "   "})
    assert r.status_code == 400
    assert "error" in r.get_json()
    # Subprocess should not have been called
    assert mock_claude_subprocess.calls == []


# ---------------------------------------------------------------------------
# Happy path — basic answer
# ---------------------------------------------------------------------------

def test_ask_returns_mocked_answer(auth_client, mock_claude_subprocess):
    mock_claude_subprocess.set_answer("There are 42 widgets.")
    r = auth_client.post("/ask", json={"question": "How many widgets?"})
    assert r.status_code == 200
    body = r.get_json()
    assert body["answer"] == "There are 42 widgets."
    # Subprocess was actually invoked
    assert len(mock_claude_subprocess.calls) == 1
    # User question was sent on stdin
    assert "How many widgets?" in mock_claude_subprocess.last_input


def test_ask_invokes_claude_with_correct_flags(auth_client, mock_claude_subprocess):
    mock_claude_subprocess.set_answer("ok")
    auth_client.post("/ask", json={"question": "ping"})

    cmd = mock_claude_subprocess.last_cmd
    assert cmd[0] == "claude"
    assert "-p" in cmd
    assert "--output-format" in cmd
    assert cmd[cmd.index("--output-format") + 1] == "json"
    assert "--mcp-config" in cmd
    assert "--system-prompt-file" in cmd
    assert "--permission-mode" in cmd


# ---------------------------------------------------------------------------
# Conversation persistence — these depend on the backend db.py wiring
# ---------------------------------------------------------------------------

def test_ask_creates_new_chat_when_chat_id_missing(
    auth_client, mock_claude_subprocess, app_module, db_module,
):
    """
    Per the API contract: /ask returns chat_id. If db.py is integrated,
    chat + messages should also exist in the DB.
    """
    if db_module is None:
        pytest.xfail("db.py not present yet")
    if not _backend_db_wired(app_module):
        pytest.xfail("awaiting backend agent: /ask not yet using db.py")

    mock_claude_subprocess.set_answer("Hello.")
    r = auth_client.post("/ask", json={"question": "Hi there"})
    assert r.status_code == 200
    body = r.get_json()

    assert "chat_id" in body, "API contract: /ask must return chat_id"
    chat_id = body["chat_id"]

    # Persisted in DB
    db_path = auth_client.application.config["TEST_DB_PATH"]
    chat = db_module.get_chat(chat_id, "alice", db_path=db_path)
    assert chat is not None
    msgs = db_module.get_messages(chat_id, db_path=db_path)
    roles = [m["role"] for m in msgs]
    assert roles == ["user", "assistant"]
    assert "Hi there" in msgs[0]["content"]
    assert "Hello." in msgs[1]["content"]


def test_ask_followup_injects_prior_history(
    auth_client, mock_claude_subprocess, app_module, db_module,
):
    """
    Send Q1, then Q2 with the same chat_id. The prompt sent to claude on Q2
    must reference the Q1 question + answer (history injection contract).
    """
    if db_module is None:
        pytest.xfail("db.py not present yet")
    if not _backend_db_wired(app_module):
        pytest.xfail("awaiting backend agent: /ask not yet using db.py")

    # Turn 1
    mock_claude_subprocess.set_answer("Pluto is a dwarf planet.")
    r1 = auth_client.post("/ask", json={"question": "Is Pluto a planet?"})
    chat_id = r1.get_json().get("chat_id")
    assert chat_id, "expected chat_id back from first /ask"

    # Turn 2 — send WITH chat_id
    mock_claude_subprocess.set_answer("Yes, since 2006.")
    r2 = auth_client.post(
        "/ask",
        json={"question": "When was that decided?", "chat_id": chat_id},
    )
    assert r2.status_code == 200
    assert r2.get_json().get("chat_id") == chat_id

    # The 2nd subprocess call's stdin must include the Q1 question + answer
    # (history injection). The exact format is up to backend, but Q1 content
    # must be substring-recognizable.
    second_input = mock_claude_subprocess.calls[1]["input"]
    assert "Is Pluto a planet?" in second_input
    assert "Pluto is a dwarf planet." in second_input
    assert "When was that decided?" in second_input


# ---------------------------------------------------------------------------
# Report extraction (round-trip via /ask)
# ---------------------------------------------------------------------------

def test_ask_extracts_report_and_strips_markers(
    auth_client, mock_claude_subprocess, app_module,
):
    fake_answer = (
        "Here is your data summary.\n\n"
        "===REPORT_START===\n"
        "production_2026.md\n"
        "===REPORT_CONTENT===\n"
        "# Production 2026\n\n"
        "All looks good.\n"
        "===REPORT_END==="
    )
    mock_claude_subprocess.set_answer(fake_answer)

    r = auth_client.post("/ask", json={"question": "give me a report"})
    assert r.status_code == 200
    body = r.get_json()

    assert "report_url" in body
    assert "===REPORT_START===" not in body["answer"]
    assert "===REPORT_END===" not in body["answer"]
    assert "Here is your data summary." in body["answer"]

    # File got written to alice's reports dir
    expected = app_module.REPORTS_DIR / "alice" / "production_2026.md"
    assert expected.exists()
    assert "# Production 2026" in expected.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Long-history truncation
# ---------------------------------------------------------------------------

def test_long_history_truncated(
    auth_client, mock_claude_subprocess, app_module, db_module,
):
    """
    If prior context > 30K chars, the backend should drop the oldest turns
    rather than blow past Claude's prompt budget.
    """
    if db_module is None:
        pytest.xfail("db.py not present yet")
    if not _backend_db_wired(app_module):
        pytest.xfail("awaiting backend agent: /ask not yet using db.py")

    db_path = auth_client.application.config["TEST_DB_PATH"]

    # Seed a chat owned by alice with a giant fake history.
    chat_id = db_module.create_chat("alice", eplant_id=1, db_path=db_path)
    big_chunk = "A" * 5_000  # 5KB per message
    for i in range(10):  # 50KB total
        db_module.add_message(chat_id, "user", f"Q{i}: {big_chunk}", db_path=db_path)
        db_module.add_message(chat_id, "assistant", f"A{i}: {big_chunk}", db_path=db_path)

    mock_claude_subprocess.set_answer("ok")
    r = auth_client.post(
        "/ask",
        json={"question": "What was Q0?", "chat_id": chat_id},
    )
    assert r.status_code == 200

    payload = mock_claude_subprocess.last_input
    # The very oldest turn ("Q0:") should have been dropped, while the
    # newest one ("Q9:" / "A9:") should still appear.
    assert "Q9:" in payload or "A9:" in payload, \
        "expected newest turn to be present in injected history"
    assert "Q0:" not in payload, \
        "expected oldest turn to be truncated when history > 30KB"


# ---------------------------------------------------------------------------
# Subprocess failure modes
# ---------------------------------------------------------------------------

def test_subprocess_timeout_returns_friendly_error(auth_client, mock_claude_subprocess):
    mock_claude_subprocess.set_raise(
        subprocess.TimeoutExpired(cmd=["claude"], timeout=300)
    )
    r = auth_client.post("/ask", json={"question": "slow query"})
    # Must NOT 500 — should respond with a friendly answer field
    assert r.status_code == 200
    body = r.get_json()
    assert "answer" in body
    assert "too long" in body["answer"].lower() or "timeout" in body["answer"].lower()


def test_subprocess_nonzero_exit_returns_error_message(auth_client, mock_claude_subprocess):
    mock_claude_subprocess.set_response(
        returncode=1,
        stdout="",
        stderr="kaboom: oracle gone",
    )
    r = auth_client.post("/ask", json={"question": "bad"})
    assert r.status_code == 200
    body = r.get_json()
    assert "answer" in body
    assert "error" in body["answer"].lower() or "kaboom" in body["answer"].lower()


def test_subprocess_unparseable_json_falls_back_to_raw_stdout(
    auth_client, mock_claude_subprocess,
):
    # Not JSON — claude printed something else. /ask should fall back to raw
    # stdout instead of crashing.
    mock_claude_subprocess.set_response(returncode=0, stdout="just plain text")
    r = auth_client.post("/ask", json={"question": "hi"})
    assert r.status_code == 200
    assert "just plain text" in r.get_json()["answer"]


# ---------------------------------------------------------------------------
# ePlant context propagation
# ---------------------------------------------------------------------------

def test_eplant_nycoa_appears_in_system_prompt(auth_client, mock_claude_subprocess):
    # Switch session to Nycoa (eplant_id=2)
    with auth_client.session_transaction() as sess:
        sess["eplant_id"] = "2"

    mock_claude_subprocess.set_answer("ok")
    auth_client.post("/ask", json={"question": "list things"})

    sp = mock_claude_subprocess.system_prompt_text()
    # System prompt must reference the active ePlant
    assert "Nycoa" in sp
    assert "EPLANT_ID = 2" in sp


def test_eplant_shawsheen_appears_in_system_prompt(auth_client, mock_claude_subprocess):
    with auth_client.session_transaction() as sess:
        sess["eplant_id"] = "1"

    mock_claude_subprocess.set_answer("ok")
    auth_client.post("/ask", json={"question": "list things"})

    sp = mock_claude_subprocess.system_prompt_text()
    assert "Shawsheen" in sp
    assert "EPLANT_ID = 1" in sp


def test_dataparc_section_only_for_nycoa(auth_client, mock_claude_subprocess):
    # Shawsheen (1) — no dataPARC
    with auth_client.session_transaction() as sess:
        sess["eplant_id"] = "1"
    mock_claude_subprocess.set_answer("ok")
    auth_client.post("/ask", json={"question": "x"})
    sp1 = mock_claude_subprocess.system_prompt_text()
    assert "dataparc" not in sp1.lower() or "dataPARC tools" not in sp1

    # Nycoa (2) — dataPARC available
    with auth_client.session_transaction() as sess:
        sess["eplant_id"] = "2"
    mock_claude_subprocess.set_answer("ok")
    auth_client.post("/ask", json={"question": "x"})
    sp2 = mock_claude_subprocess.system_prompt_text()
    assert "dataPARC" in sp2 or "dataparc" in sp2.lower()
