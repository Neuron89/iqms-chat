"""
Phase 2 — Layer A (prompt-injected access control) + Layer B (per-request
mcp_config) enforcement.

Layer A: when /ask spawns `claude -p`, the user prompt (or system prompt)
must include an "Access control" block listing allowed module/table prefixes
for normal users — and *omit* it entirely for super-admins. We assert this
by snapshotting the captured prompt from `mock_claude_subprocess`.

Layer B: the per-request mcp_config_*.json that `app.py` writes must set
`env.IQMS_ALLOWED_TABLE_PREFIXES` to a comma-separated allowlist (or omit /
set `%` for super-admins). We assert by reading the config file off the
final `--mcp-config` flag value.

Many of these tests rely on Agent A's `/ask` rewrite that injects access
control. Where the current Phase 1 implementation hasn't landed that piece,
the test is `xfail(strict=False)` so it auto-flips when Agent A's code arrives.
"""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Heuristics — detect which Phase 2 pieces are in place
# ---------------------------------------------------------------------------

def _layer_a_landed() -> bool:
    """
    True if /ask injects a per-user Access-control block into the prompt.
    We look for the literal "=== Access control ===" header that the
    `_build_access_control_block` helper emits.
    """
    try:
        import app as app_module  # type: ignore
    except ImportError:
        return False
    src = Path(app_module.__file__).read_text(encoding="utf-8", errors="replace")
    return any(
        marker in src
        for marker in (
            "=== Access control ===",
            "_build_access_control_block",
            "_format_access_control",
            "build_access_control",
        )
    )


def _layer_b_landed() -> bool:
    """
    True if /ask writes a per-request mcp_config with
    IQMS_ALLOWED_TABLE_PREFIXES injected from the session packet.
    """
    try:
        import app as app_module  # type: ignore
    except ImportError:
        return False
    src = Path(app_module.__file__).read_text(encoding="utf-8", errors="replace")
    if "IQMS_ALLOWED_TABLE_PREFIXES" not in src:
        return False
    return any(
        sig in src
        for sig in (
            "_build_mcp_config",
            "MCP_TMP_DIR",
            "mcp_config_per_request",
        )
    )


# ---------------------------------------------------------------------------
# Fixtures — clients seeded with a permission_packet on the session
# ---------------------------------------------------------------------------

# Inline copies of the canned packets — duplicated from conftest's module-level
# constants because importing them at test-collection time would force the
# `app` fixture chain.
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
    "role_names": ["IQSO_RW", "IQAR_RO", "IQCUST_RO", "IQINVENTORY_RO"],
    "module_prefixes": ["SO", "AR", "CUST", "INVENTORY"],
    "allowed_table_prefixes": ["SO%", "AR%", "INV%", "ITEM%", "S_%", "EPLANT"],
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
def super_admin_client(app):
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["username"] = "hnester@nycoa.com"
        sess["display_name"] = "Hayden Nester"
        sess["is_admin"] = True
        sess["is_super_admin"] = True
        sess["chat_id"] = str(uuid.uuid4())
        sess["eplant_id"] = "2"
        sess["eplant_access"] = [1, 2, 3]
        sess["permission_packet"] = SUPER_ADMIN_PACKET
    return c


@pytest.fixture
def operator_client(app):
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["username"] = "kfitzpatrick@shawsheencc.com"
        sess["display_name"] = "Kelly Fitzpatrick"
        sess["is_admin"] = False
        sess["is_super_admin"] = False
        sess["chat_id"] = str(uuid.uuid4())
        sess["eplant_id"] = "2"
        sess["eplant_access"] = [2]
        sess["permission_packet"] = OPERATOR_PACKET
    return c


@pytest.fixture
def viewer_client(app):
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["username"] = "newhire@nycoa.com"
        sess["display_name"] = "New Hire"
        sess["is_admin"] = False
        sess["is_super_admin"] = False
        sess["chat_id"] = str(uuid.uuid4())
        sess["eplant_id"] = "2"
        sess["eplant_access"] = [1, 2, 3]
        sess["permission_packet"] = VIEWER_PACKET
    return c


# ---------------------------------------------------------------------------
# Helpers — pull the captured prompt + mcp_config off mock_claude_subprocess
# ---------------------------------------------------------------------------

def _capture_full_prompt(recorder) -> str:
    """Concatenate the system-prompt-file contents + stdin-input prompt."""
    sp = recorder.system_prompt_text()
    inp = recorder.last_input or ""
    return f"{sp}\n---STDIN---\n{inp}"


def _captured_mcp_config(recorder) -> dict | None:
    """
    Return the mcp_config JSON the app passed to claude. Prefers the in-flight
    snapshot the SubprocessRecorder grabs (per-request configs are deleted by
    /ask once the subprocess returns), falling back to a path read.
    """
    cfg = recorder.mcp_config()
    if cfg is not None:
        return cfg
    cmd = recorder.last_cmd
    if "--mcp-config" not in cmd:
        return None
    path = cmd[cmd.index("--mcp-config") + 1]
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        return None


def _iqms_env_for_request(mcp_config: dict | None) -> dict:
    """Pull the iqms-oracle MCP server env block (or {} if missing)."""
    if not mcp_config:
        return {}
    servers = mcp_config.get("mcpServers") or {}
    iqms = servers.get("iqms-oracle") or {}
    return iqms.get("env") or {}


# ---------------------------------------------------------------------------
# Layer A — prompt-injected access control block
# ---------------------------------------------------------------------------

class TestLayerA_AccessControlPromptBlock:

    @pytest.mark.xfail(
        condition=not _layer_a_landed(),
        reason="Layer A access-control prompt injection not yet implemented",
        strict=False,
    )
    def test_super_admin_prompt_omits_access_control(
        self, super_admin_client, mock_claude_subprocess,
    ):
        """Super-admins should not get the access-control gate text."""
        super_admin_client.post("/ask", json={"question": "select * from arinvt"})
        prompt = _capture_full_prompt(mock_claude_subprocess)
        # No access-control block (case-insensitive)
        assert "access control" not in prompt.lower(), (
            "super-admin prompt should not include access-control gating"
        )

    @pytest.mark.xfail(
        condition=not _layer_a_landed(),
        reason="Layer A access-control prompt injection not yet implemented",
        strict=False,
    )
    def test_operator_prompt_includes_access_control_with_modules(
        self, operator_client, mock_claude_subprocess,
    ):
        """Operators get an access-control block listing their module prefixes."""
        operator_client.post("/ask", json={"question": "show me sales orders"})
        prompt = _capture_full_prompt(mock_claude_subprocess)
        lc = prompt.lower()
        assert "access control" in lc, "operator should get access-control block"
        # At least the operator's module prefixes appear in the prompt
        for mod in ("SO", "AR", "INVENTORY"):
            assert mod in prompt, (
                f"expected operator module {mod} in access-control listing"
            )

    @pytest.mark.xfail(
        condition=not _layer_a_landed(),
        reason="Layer A access-control prompt injection not yet implemented",
        strict=False,
    )
    def test_viewer_prompt_states_zero_db_access(
        self, viewer_client, mock_claude_subprocess,
    ):
        """No-IQMS users get a block instructing Claude to refuse all DB queries."""
        viewer_client.post("/ask", json={"question": "what's in inventory?"})
        prompt = _capture_full_prompt(mock_claude_subprocess)
        lc = prompt.lower()
        assert "access control" in lc
        # The prompt should make clear that no DB queries are allowed.
        # We accept any of: 'no allowed', 'refuse', 'zero', 'cannot query'.
        forbidden_signals = ("no allowed", "refuse", "zero allowed",
                             "cannot query", "denied", "no database")
        assert any(sig in lc for sig in forbidden_signals), (
            f"viewer prompt should signal zero DB access; got:\n{prompt[:1000]}"
        )


# ---------------------------------------------------------------------------
# Layer B — per-request mcp_config IQMS_ALLOWED_TABLE_PREFIXES
# ---------------------------------------------------------------------------

class TestLayerB_McpAllowlistEnvVar:

    @pytest.mark.xfail(
        condition=not _layer_b_landed(),
        reason="Layer B IQMS_ALLOWED_TABLE_PREFIXES not yet implemented",
        strict=False,
    )
    def test_super_admin_allowlist_omitted_or_wildcard(
        self, super_admin_client, mock_claude_subprocess,
    ):
        super_admin_client.post("/ask", json={"question": "ping"})
        env = _iqms_env_for_request(
            _captured_mcp_config(mock_claude_subprocess),
        )
        # Either omitted entirely, or set to wildcard.
        val = env.get("IQMS_ALLOWED_TABLE_PREFIXES")
        assert val is None or val.strip() in ("%", "*", ""), (
            f"super-admin should not be filtered; got IQMS_ALLOWED_TABLE_PREFIXES={val!r}"
        )

    @pytest.mark.xfail(
        condition=not _layer_b_landed(),
        reason="Layer B IQMS_ALLOWED_TABLE_PREFIXES not yet implemented",
        strict=False,
    )
    def test_operator_allowlist_lists_module_prefixes(
        self, operator_client, mock_claude_subprocess,
    ):
        operator_client.post("/ask", json={"question": "ping"})
        env = _iqms_env_for_request(
            _captured_mcp_config(mock_claude_subprocess),
        )
        val = env.get("IQMS_ALLOWED_TABLE_PREFIXES")
        assert val, "operator must have IQMS_ALLOWED_TABLE_PREFIXES set"
        parts = [p.strip() for p in val.split(",") if p.strip()]
        # At least one of the operator's module prefixes is in the allowlist
        assert any(p.startswith("SO") for p in parts), (
            f"expected SO% prefix in operator allowlist; got {parts}"
        )
        assert any(p.startswith("INV") for p in parts), (
            f"expected INV% prefix in operator allowlist; got {parts}"
        )

    @pytest.mark.xfail(
        condition=not _layer_b_landed(),
        reason="Layer B IQMS_ALLOWED_TABLE_PREFIXES not yet implemented",
        strict=False,
    )
    def test_viewer_allowlist_empty_or_minimal(
        self, viewer_client, mock_claude_subprocess,
    ):
        viewer_client.post("/ask", json={"question": "ping"})
        env = _iqms_env_for_request(
            _captured_mcp_config(mock_claude_subprocess),
        )
        val = env.get("IQMS_ALLOWED_TABLE_PREFIXES", "")
        # Either empty string, or only the always-allow security/eplant prefixes.
        if val.strip():
            parts = [p.strip().upper() for p in val.split(",") if p.strip()]
            allowed_minimal = {"S_%", "EPLANT", "EPLANT%", "DUAL"}
            extras = [p for p in parts if p not in allowed_minimal]
            assert not extras, (
                f"viewer should only have always-allow prefixes; got extras "
                f"{extras} (full: {parts})"
            )


# ---------------------------------------------------------------------------
# Module-prefix derivation — direct unit tests (parallel to test_iqms_lookup,
# kept here for permission-perspective readability)
# ---------------------------------------------------------------------------

class TestModuleDerivationAtRouteLevel:
    """
    These exercise the public derivation contract: given a list of role names,
    we get back the right module prefixes. The function lives in iqms_lookup,
    but Phase 2 contract says iqms-chat callers should consume the same logic.
    """

    def test_iqgl_rw_yields_gl(self):
        from iqms_lookup import _derive_module_prefixes
        assert _derive_module_prefixes(["IQGL_RW"]) == ["GL"]

    def test_iqall_yields_wildcard_module(self):
        from iqms_lookup import _derive_module_prefixes
        mods = _derive_module_prefixes(["IQALL"])
        assert "ALL" in mods

    def test_nycoa_smartpage_strips_suffix(self):
        from iqms_lookup import _derive_module_prefixes
        mods = _derive_module_prefixes(["NYCOA_SMARTPAGE_BI_RW"])
        assert mods == ["NYCOA_SMARTPAGE_BI"]

    def test_mixed_role_list_derivation(self):
        from iqms_lookup import _derive_module_prefixes
        roles = ["IQGL_RW", "IQAP_RO", "IQALL", "IQAP_RO"]  # dup
        mods = _derive_module_prefixes(roles)
        # Order-preserving dedupe — first occurrence wins
        assert mods == ["GL", "AP", "ALL"]


# ---------------------------------------------------------------------------
# Smoke check — the existing /ask flow doesn't regress for permission-bearing
# users (no exceptions when a packet is on the session).
# ---------------------------------------------------------------------------

class TestPermissionPacketDoesNotBreakAsk:
    """Phase 1 didn't know about packets — Phase 2 must coexist gracefully."""

    def test_super_admin_ask_returns_200(
        self, super_admin_client, mock_claude_subprocess,
    ):
        mock_claude_subprocess.set_answer("ok")
        r = super_admin_client.post("/ask", json={"question": "hello"})
        assert r.status_code == 200
        assert r.get_json()["answer"] == "ok"

    def test_operator_ask_returns_200(
        self, operator_client, mock_claude_subprocess,
    ):
        mock_claude_subprocess.set_answer("ok")
        r = operator_client.post("/ask", json={"question": "hello"})
        assert r.status_code == 200
        assert r.get_json()["answer"] == "ok"

    def test_viewer_ask_returns_200(
        self, viewer_client, mock_claude_subprocess,
    ):
        mock_claude_subprocess.set_answer("ok")
        r = viewer_client.post("/ask", json={"question": "hello"})
        assert r.status_code == 200
        assert r.get_json()["answer"] == "ok"
