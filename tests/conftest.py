"""
Shared pytest fixtures for IQMS Chat test suite.

Tests are hermetic: every test gets a temp users.json, temp reports/ dir,
temp uploads/ dir, and a temp SQLite DB (if/when db.py is integrated into
app.py). The `claude -p` subprocess is always mocked — tests never actually
spawn a real Claude process.
"""

from __future__ import annotations

import copy
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Optional

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
    # Deterministic SSO signing secret for tests. Helpers below reuse this same
    # value when minting portal JWTs so /sso accepts them.
    monkeypatch.setenv(
        "PORTAL_SSO_SECRET",
        "test-portal-sso-secret-please-do-not-ship",
    )

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
    """
    Test client already logged in as a super-admin.

    Phase 1 carried `is_admin` only; Phase 2 introduces `is_super_admin` plus a
    cached IQMS permission packet on the session. We populate BOTH so tests
    written against either phase work without conditional branching. Tests that
    care about the exact session shape can override these keys.
    """
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["username"] = "admin"
        sess["display_name"] = "Admin"
        sess["is_admin"] = True
        sess["is_super_admin"] = True
        sess["chat_id"] = str(uuid.uuid4())
        sess["eplant_id"] = "1"
        sess["permission_packet"] = {
            "iqms_user_name": "ADMIN",
            "email": "admin@nycoa.com",
            "role_names": ["IQALL", "IQALL_REPORTS", "IQCHNG_PASSW_RW", "SUPER_ADMIN"],
            "module_prefixes": ["*"],
            "allowed_table_prefixes": ["%"],
            "eplant_ids": [1, 2, 3],
            "tier": "admin",
            "synced_at": "2026-05-08T00:00:00",
        }
    return c


# ---------------------------------------------------------------------------
# Phase 2 — SSO / IQMS permissions fixtures
# ---------------------------------------------------------------------------

# Canonical packet samples — keep in sync with docs/iqms-permissions-research.md
# and the spec at the top of test_permissions.py.

SUPER_ADMIN_PACKET = {
    "iqms_user_name": "HNESTER",
    "email": "hnester@nycoa.com",
    "role_names": ["IQALL", "IQALL_REPORTS", "IQCHNG_PASSW_RW", "SUPER_ADMIN"],
    "module_prefixes": ["*"],
    "allowed_table_prefixes": ["%"],
    "eplant_ids": [1, 2, 3],
    "tier": "admin",
    "synced_at": "2026-05-08T12:00:00",
}

OPERATOR_PACKET = {
    "iqms_user_name": "KFITZPATRICK",
    "email": "kfitzpatrick@shawsheencc.com",
    "role_names": [
        "IQSO_RW", "IQAR_RO", "IQCUST_RO", "IQINVENTORY_RO",
        "IQORDERS_RW", "IQAP_RO",
    ],
    "module_prefixes": ["SO", "AR", "CUST", "INVENTORY", "ORDERS", "AP"],
    "allowed_table_prefixes": [
        "SO%", "AR%", "CUST%", "INV%", "ITEM%", "S_%", "EPLANT",
    ],
    "eplant_ids": [2],
    "tier": "operator",
    "synced_at": "2026-05-08T12:00:00",
}

VIEWER_PACKET = {
    "iqms_user_name": None,
    "email": "newhire@nycoa.com",
    "role_names": [],
    "module_prefixes": [],
    "allowed_table_prefixes": [],
    "eplant_ids": [],
    "tier": "viewer",
    "synced_at": "2026-05-08T12:00:00",
}


@pytest.fixture
def canned_permission_packets() -> dict:
    """
    Canonical fixtures for the three tiers we care about. Tests that need a
    quick-lookup table by email should consume this fixture.

    Returns a fresh `deepcopy` so individual tests can mutate without bleeding
    state into siblings.
    """
    return {
        "hnester@nycoa.com": copy.deepcopy(SUPER_ADMIN_PACKET),
        "kfitzpatrick@shawsheencc.com": copy.deepcopy(OPERATOR_PACKET),
        "newhire@nycoa.com": copy.deepcopy(VIEWER_PACKET),
    }


class IQMSLookupRecorder:
    """
    Stand-in for `iqms_lookup.fetch_permissions_for_email`. Holds a per-email
    response map and records every call so tests can assert what was looked up.

    Defaults: any email not in the map returns `None` (mirrors the production
    "no IQMS row" path where Agent A's code returns None / falls back to a
    blank viewer packet upstream).

    Tests can mutate `.responses` mid-flight, set `.raise_on_call` to simulate
    Oracle outages, or call `.set(email, packet)` to override a single email.
    """

    def __init__(self, responses: Optional[dict] = None):
        self.responses: dict = dict(responses or {})
        self.calls: list[str] = []
        self.raise_on_call: Optional[Exception] = None

    def set(self, email: str, packet: Optional[dict]) -> None:
        self.responses[email.lower()] = packet

    def __call__(self, email: str) -> Optional[dict]:
        if email is None:
            self.calls.append("")
        else:
            self.calls.append(email)
        if self.raise_on_call is not None:
            exc = self.raise_on_call
            self.raise_on_call = None
            raise exc
        # Case-insensitive email match (matches the Oracle query's LOWER(EMAIL))
        key = (email or "").lower()
        if key in self.responses:
            return copy.deepcopy(self.responses[key])
        # Also try the original case
        if email in self.responses:
            return copy.deepcopy(self.responses[email])
        return None

    @property
    def call_count(self) -> int:
        return len(self.calls)

    @property
    def last_email(self) -> Optional[str]:
        return self.calls[-1] if self.calls else None


@pytest.fixture
def mock_iqms_lookup(monkeypatch, canned_permission_packets):
    """
    Replace `iqms_lookup.fetch_permissions_for_email` with a recorder.

    The recorder is pre-populated with the three canonical packets
    (super_admin, operator, viewer) keyed by email. Tests can override any
    response via `recorder.set(email, packet)` or by directly mutating
    `recorder.responses`.

    If `iqms_lookup` does not yet exist in the project (e.g. Phase 2 backend
    not landed), the fixture still installs a stub module so the import path
    is satisfied. Tests that depend on the real module behavior should mark
    themselves xfail until the module ships.

    Yields the recorder.
    """
    recorder = IQMSLookupRecorder(canned_permission_packets)
    try:
        import iqms_lookup as iqms_lookup_module  # type: ignore
    except ImportError:
        # Synthesize a minimal stub so monkeypatch + import-from-tests works.
        # Real implementation lands with Agent A; tests that exercise it will
        # `pytest.importorskip` or xfail accordingly.
        import types
        iqms_lookup_module = types.ModuleType("iqms_lookup")
        iqms_lookup_module.fetch_permissions_for_email = recorder  # type: ignore[attr-defined]
        sys.modules["iqms_lookup"] = iqms_lookup_module

    monkeypatch.setattr(
        iqms_lookup_module, "fetch_permissions_for_email", recorder, raising=False,
    )
    # Belt-and-suspenders: also patch the symbol on `app` if it has been
    # imported there (Agent A's /sso handler will look it up via the app
    # module).
    try:
        import app as app_module  # type: ignore
        if hasattr(app_module, "fetch_permissions_for_email"):
            monkeypatch.setattr(
                app_module, "fetch_permissions_for_email", recorder, raising=False,
            )
        if hasattr(app_module, "iqms_lookup"):
            monkeypatch.setattr(
                app_module.iqms_lookup, "fetch_permissions_for_email",
                recorder, raising=False,
            )
    except ImportError:
        pass

    return recorder


# Re-export the canonical SSO secret so test files can mint matching JWTs.
PORTAL_SSO_SECRET_TEST = "test-portal-sso-secret-please-do-not-ship"


def _mint_portal_jwt(
    email: str,
    *,
    full_name: str = "Test User",
    portal_role: str = "employee",
    secret: str = PORTAL_SSO_SECRET_TEST,
    issuer: str = "nycoa-portal",
    audience: str = "iqms_chat",
    expires_in_seconds: int = 300,
    extra_claims: Optional[dict] = None,
) -> str:
    """
    Mint a portal-style JWT signed with the test secret. Default claim shape
    matches the production portal: { sub, email, full_name, role, iss, aud,
    iat, exp }. Tests pass overrides via kwargs (e.g. `audience='other'` or
    `expires_in_seconds=-60` for an expired token).
    """
    import jwt as _jwt  # local import so tests work without it at collection time
    now = datetime.now(tz=timezone.utc)
    claims = {
        "sub": email,
        "email": email,
        "full_name": full_name,
        "role": portal_role,
        "iss": issuer,
        "aud": audience,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=expires_in_seconds)).timestamp()),
    }
    if extra_claims:
        claims.update(extra_claims)
    token = _jwt.encode(claims, secret, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("ascii")
    return token


@pytest.fixture
def sso_jwt_for(app) -> Callable[..., str]:
    """
    Helper-fixture: returns a function that mints a portal-issued JWT signed
    with the test-only secret.

    Usage:
        token = sso_jwt_for("hnester@nycoa.com", full_name="Hayden N.")
        token = sso_jwt_for("ghost@nycoa.com", expires_in_seconds=-60)  # expired
    """
    def _factory(email: str, **kwargs) -> str:
        return _mint_portal_jwt(email, **kwargs)
    return _factory


@pytest.fixture
def sso_login(app, sso_jwt_for):
    """
    Convenience fixture: hits /sso?ptoken=<jwt> with the given email and
    returns (client, response). The client retains the resulting session
    cookie so subsequent calls behave as a logged-in user.

    Usage:
        c, r = sso_login("hnester@nycoa.com")
        assert r.status_code in (302, 200)
        # c is now logged in
        c.get("/")
    """
    def _do_login(
        email: str,
        *,
        client=None,
        next_path: str = "/",
        follow_redirects: bool = False,
        **jwt_kwargs,
    ):
        c = client or app.test_client()
        token = sso_jwt_for(email, **jwt_kwargs)
        r = c.get(
            f"/sso?ptoken={token}&next={next_path}",
            follow_redirects=follow_redirects,
        )
        return c, r
    return _do_login


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
        # Snapshot the per-request mcp-config (Phase 2 Layer B writes a fresh
        # one per /ask and deletes it on the way out, so we have to grab it
        # in-flight or tests can't inspect it).
        mcp_text = ""
        mcp_json: dict | None = None
        if "--mcp-config" in cmd_list:
            idx = cmd_list.index("--mcp-config")
            try:
                mcp_text = Path(cmd_list[idx + 1]).read_text(
                    encoding="utf-8", errors="replace",
                )
                try:
                    mcp_json = json.loads(mcp_text)
                except json.JSONDecodeError:
                    mcp_json = None
            except Exception:
                mcp_text = ""

        self.calls.append({
            "cmd": cmd_list,
            "input": input,
            "timeout": timeout,
            "cwd": cwd,
            "system_prompt": sp_text,
            "mcp_config_text": mcp_text,
            "mcp_config_json": mcp_json,
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

    def mcp_config(self) -> dict | None:
        """
        Return the parsed --mcp-config JSON captured at call time, or None if
        the config wasn't readable (e.g. already-deleted per-request config).
        """
        return (self.last_call or {}).get("mcp_config_json")

    def mcp_config_text(self) -> str:
        """Return the raw --mcp-config file contents captured at call time."""
        return (self.last_call or {}).get("mcp_config_text", "") or ""


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
