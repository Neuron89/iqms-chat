"""
Shared pytest fixtures for IQMS Chat test suite.

Tests are hermetic: every test gets a temp users.json, temp reports/ dir,
temp uploads/ dir, and a temp SQLite DB (if/when db.py is integrated into
app.py). The `claude -p` subprocess is always mocked — tests never actually
spawn a real Claude process.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import uuid
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest


# Make sure the project root is on sys.path so we can `import app` and
# `import db` no matter where pytest is invoked from.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _hash_pw(password: str, salt: str) -> str:
    return sha256(f"{salt}:{password}".encode()).hexdigest()


def _make_user(password: str, *, display_name: str = "", is_admin: bool = False) -> dict:
    salt = os.urandom(8).hex()
    return {
        "hash": _hash_pw(password, salt),
        "salt": salt,
        "display_name": display_name or "Test User",
        "is_admin": is_admin,
        "created": "2026-05-08T00:00:00",
    }


@pytest.fixture
def test_users() -> dict:
    """A canonical set of test users used across the suite."""
    return {
        "alice":   _make_user("alicepw",   display_name="Alice",   is_admin=False),
        "bob":     _make_user("bobpw",     display_name="Bob",     is_admin=False),
        "admin":   _make_user("adminpw",   display_name="Admin",   is_admin=True),
    }


# ---------------------------------------------------------------------------
# Core `app` fixture — temp dirs, monkeypatched module globals
# ---------------------------------------------------------------------------

@pytest.fixture
def app(tmp_path, test_users, monkeypatch):
    """
    Import the Flask app fresh, then redirect all on-disk state to tmp_path:
      * data/users.json
      * reports/
      * uploads/
      * data/conversations.db (if db.py is wired in)

    Yields the Flask app instance. Cleans up automatically.
    """
    # Make sure secret key is deterministic for the test run
    monkeypatch.setenv("SECRET_KEY", "test-secret-do-not-use-in-prod")

    # Build temp filesystem layout
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    uploads_dir = tmp_path / "uploads"
    uploads_dir.mkdir()
    users_file = data_dir / "users.json"
    users_file.write_text(json.dumps(test_users, indent=2))

    # Import the app module after env is set up. We always import fresh — clear
    # any cached module so secret_key/env are picked up.
    for mod in ("app", "db"):
        sys.modules.pop(mod, None)

    import app as app_module  # type: ignore

    # Redirect module-level paths
    monkeypatch.setattr(app_module, "DATA_DIR", data_dir, raising=True)
    monkeypatch.setattr(app_module, "USERS_FILE", users_file, raising=True)
    monkeypatch.setattr(app_module, "REPORTS_DIR", reports_dir, raising=True)
    monkeypatch.setattr(app_module, "UPLOADS_DIR", uploads_dir, raising=True)

    # Empty in-memory conversation cache between tests
    if hasattr(app_module, "conversations"):
        app_module.conversations.clear()

    flask_app = app_module.app
    flask_app.config.update(
        TESTING=True,
        SECRET_KEY="test-secret-do-not-use-in-prod",
        SERVER_NAME=None,
        WTF_CSRF_ENABLED=False,
    )

    # If db.py exists and is integrated, give it a temp DB path too. The
    # backend agent's contract: db.py exposes a module-level DB_PATH.
    try:
        import db as db_module  # type: ignore
        test_db_path = data_dir / "conversations.db"
        monkeypatch.setattr(db_module, "DB_PATH", test_db_path, raising=False)
        # Re-init so schema lives in the temp DB
        if hasattr(db_module, "init_db"):
            db_module.init_db(test_db_path)
        # Stash for tests that need it
        flask_app.config["TEST_DB_PATH"] = test_db_path
    except ImportError:
        pass

    flask_app.config["TEST_USERS"] = test_users
    flask_app.config["TEST_TMP"] = tmp_path

    yield flask_app


@pytest.fixture
def client(app):
    """Anonymous Flask test client (no session)."""
    return app.test_client()


@pytest.fixture
def auth_client(app, test_users):
    """
    Test client already logged in as 'alice' (non-admin). Sets the same
    session keys that the real /login route sets.
    """
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["username"] = "alice"
        sess["display_name"] = "Alice"
        sess["is_admin"] = False
        sess["chat_id"] = str(uuid.uuid4())
        sess["eplant_id"] = "1"
    return c


@pytest.fixture
def admin_client(app, test_users):
    """Test client already logged in as the admin user."""
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["username"] = "admin"
        sess["display_name"] = "Admin"
        sess["is_admin"] = True
        sess["chat_id"] = str(uuid.uuid4())
        sess["eplant_id"] = "1"
    return c


# ---------------------------------------------------------------------------
# Mocked claude subprocess
# ---------------------------------------------------------------------------

class FakeCompletedProcess:
    """Stand-in for subprocess.CompletedProcess."""

    def __init__(self, *, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class SubprocessRecorder:
    """
    Replacement for subprocess.run. Records every call, returns a configurable
    response. Tests can change behavior mid-flight via .set_response(...) or
    .set_raise(...).
    """

    def __init__(self, default_answer: str = "Hello from fake claude."):
        self.calls: list[dict] = []
        self._answer = default_answer
        self._returncode = 0
        self._stderr = ""
        self._raise: Exception | None = None

    # ---- configuration -----------------------------------------------------

    def set_answer(self, text: str) -> None:
        self._answer = text
        self._raise = None

    def set_response(self, *, returncode: int = 0, stdout: str | None = None,
                     stderr: str = "") -> None:
        """Set raw stdout/returncode/stderr (skip the auto-JSON wrapping)."""
        self._returncode = returncode
        self._stderr = stderr
        self._raise = None
        if stdout is not None:
            self._raw_stdout = stdout
        else:
            self._raw_stdout = None

    def set_raise(self, exc: Exception) -> None:
        self._raise = exc

    # ---- the call itself ---------------------------------------------------

    def __call__(self, cmd, *, input=None, capture_output=True, text=True,
                 timeout=None, cwd=None, **kwargs):
        # Snapshot the system-prompt-file BEFORE app.py deletes it.
        sp_text = ""
        cmd_list = list(cmd)
        if "--system-prompt-file" in cmd_list:
            idx = cmd_list.index("--system-prompt-file")
            try:
                sp_text = Path(cmd_list[idx + 1]).read_text(
                    encoding="utf-8", errors="replace",
                )
            except Exception:
                sp_text = ""

        self.calls.append({
            "cmd": cmd_list,
            "input": input,
            "timeout": timeout,
            "cwd": cwd,
            "system_prompt": sp_text,
        })
        if self._raise is not None:
            exc = self._raise
            # Don't re-raise on subsequent calls unless explicitly re-set
            self._raise = None
            raise exc

        if getattr(self, "_raw_stdout", None) is not None:
            stdout = self._raw_stdout
            self._raw_stdout = None
        else:
            stdout = json.dumps({
                "result": self._answer,
                "total_cost_usd": 0.001,
                "duration_ms": 12,
            })
        return FakeCompletedProcess(
            returncode=self._returncode,
            stdout=stdout,
            stderr=self._stderr,
        )

    # ---- inspection helpers ------------------------------------------------

    @property
    def last_call(self) -> dict | None:
        return self.calls[-1] if self.calls else None

    @property
    def last_input(self) -> str:
        return (self.last_call or {}).get("input") or ""

    @property
    def last_cmd(self) -> list:
        return (self.last_call or {}).get("cmd") or []

    def system_prompt_text(self) -> str:
        """Return the --system-prompt-file contents captured at call time."""
        return (self.last_call or {}).get("system_prompt", "") or ""


@pytest.fixture
def mock_claude_subprocess(monkeypatch):
    """
    Replace subprocess.run inside app.py with a recorder. Yields the recorder
    so tests can configure the canned response and inspect calls.

    Usage:
        def test_x(auth_client, mock_claude_subprocess):
            mock_claude_subprocess.set_answer("42 widgets")
            r = auth_client.post("/ask", json={"question": "how many?"})
            assert "42" in r.get_json()["answer"]
            assert "how many" in mock_claude_subprocess.last_input
    """
    import app as app_module  # type: ignore
    recorder = SubprocessRecorder()
    # Patch the reference held inside app.py — that's what /ask actually calls.
    monkeypatch.setattr(app_module.subprocess, "run", recorder)
    # Belt-and-suspenders: also patch the global module
    monkeypatch.setattr(subprocess, "run", recorder)
    return recorder


# ---------------------------------------------------------------------------
# Convenience fixture — expose the imported app module for direct calls
# ---------------------------------------------------------------------------

@pytest.fixture
def app_module(app):
    """Returns the imported `app` module (not the Flask app instance)."""
    import app as m  # type: ignore
    return m


@pytest.fixture
def db_module(app):
    """Returns the imported `db` module, or None if not present."""
    try:
        import db as m  # type: ignore
        return m
    except ImportError:
        return None
