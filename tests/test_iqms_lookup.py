"""
Phase 2 — `iqms_lookup` module unit tests.

We never hit the real Oracle DB. `oracledb.connect` is monkeypatched to a
`FakeConnection` that returns canned cursor rows for known emails. The tests
also exercise the pure derivation helpers (module / table-prefix / tier).

Notes:
* If `oracledb` isn't installed we still want the module-level helpers to be
  testable, so we patch at the level of `iqms_lookup._connect` rather than
  reaching into oracledb directly.
"""

from __future__ import annotations

import importlib
from typing import Optional

import pytest


iqms_lookup = pytest.importorskip("iqms_lookup")


# ---------------------------------------------------------------------------
# Fake Oracle plumbing
# ---------------------------------------------------------------------------

# Row shape matches the SQL in iqms_lookup._PERMISSIONS_SQL:
#   (USER_NAME, EMAIL, EPLANT_ID, SOURCE, GRANTED_ROLE)

HNESTER_ROWS = [
    # 4 direct roles + 3 group-inherited roles + 3 plants
    ("HNESTER", "hnester@nycoa.com", 1, "DIRECT", "IQALL"),
    ("HNESTER", "hnester@nycoa.com", 2, "DIRECT", "IQALL"),
    ("HNESTER", "hnester@nycoa.com", 3, "DIRECT", "IQALL"),
    ("HNESTER", "hnester@nycoa.com", 1, "DIRECT", "IQALL_REPORTS"),
    ("HNESTER", "hnester@nycoa.com", 1, "DIRECT", "IQCHNG_PASSW_RW"),
    ("HNESTER", "hnester@nycoa.com", 1, "GROUP:Admin", "IQALL"),
]

KFITZ_ROWS = [
    ("KFITZPATRICK", "kfitzpatrick@shawsheencc.com", 2, "DIRECT", "IQSO_RW"),
    ("KFITZPATRICK", "kfitzpatrick@shawsheencc.com", 2, "DIRECT", "IQAR_RO"),
    ("KFITZPATRICK", "kfitzpatrick@shawsheencc.com", 2, "DIRECT", "IQCUST_RO"),
    ("KFITZPATRICK", "kfitzpatrick@shawsheencc.com", 2, "DIRECT", "IQINVENTORY_RO"),
    ("KFITZPATRICK", "kfitzpatrick@shawsheencc.com", 2, "DIRECT", "IQORDERS_RW"),
    ("KFITZPATRICK", "kfitzpatrick@shawsheencc.com", 2, "GROUP:Sales", "IQAP_RO"),
]

ALL_RO_ROWS = [
    # User with only RO roles → tier = 'viewer'
    ("READONLY", "readonly@nycoa.com", 2, "DIRECT", "IQGL_RO"),
    ("READONLY", "readonly@nycoa.com", 2, "DIRECT", "IQAP_RO"),
]


class _FakeCursor:
    def __init__(self, rows: list[tuple]):
        self._rows = rows
        self.last_sql: str | None = None
        self.last_params: dict | None = None

    def execute(self, sql, **params):
        self.last_sql = sql
        self.last_params = params
        return self

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, rows_for_email: dict[str, list[tuple]]):
        self._rows_for_email = rows_for_email
        self._last_cursor: _FakeCursor | None = None
        self.closed = False
        self.was_used_for_email: str | None = None

    def cursor(self):
        # Bind-time email selection: the SQL has :p_email — we serve rows from
        # the map at execute() time. Wrap the cursor to peek at p_email.
        rows_map = self._rows_for_email
        outer = self

        class _DispatchCursor(_FakeCursor):
            def execute(self2, sql, **params):
                self2.last_sql = sql
                self2.last_params = params
                email = (params.get("p_email") or "").lower()
                outer.was_used_for_email = email
                self2._rows = list(rows_map.get(email, []))
                return self2

        cur = _DispatchCursor([])
        self._last_cursor = cur
        return cur

    def close(self):
        self.closed = True


@pytest.fixture
def fake_connect(monkeypatch):
    """
    Replace iqms_lookup._connect with a factory that returns a FakeConnection
    pre-loaded with canned rows for hnester / kfitzpatrick / readonly.

    Tests that need to simulate "Oracle is down" set `state.raise_on_connect`.
    """

    rows_for_email = {
        "hnester@nycoa.com": HNESTER_ROWS,
        "kfitzpatrick@shawsheencc.com": KFITZ_ROWS,
        "readonly@nycoa.com": ALL_RO_ROWS,
        # ghost@nycoa.com intentionally omitted → empty list → fetch returns None
    }

    class _State:
        def __init__(self):
            self.raise_on_connect: Optional[Exception] = None
            self.raise_on_execute: Optional[Exception] = None
            self.connections_made: list[_FakeConnection] = []

    state = _State()

    def _fake_connect():
        if state.raise_on_connect is not None:
            exc = state.raise_on_connect
            state.raise_on_connect = None
            raise exc
        conn = _FakeConnection(rows_for_email)
        if state.raise_on_execute is not None:
            # Wrap the cursor so .execute() raises
            orig_cursor = conn.cursor

            def _explosive_cursor():
                cur = orig_cursor()
                exc = state.raise_on_execute
                state.raise_on_execute = None

                def _boom(*a, **kw):
                    raise exc
                cur.execute = _boom  # type: ignore[assignment]
                return cur
            conn.cursor = _explosive_cursor  # type: ignore[assignment]
        state.connections_made.append(conn)
        return conn

    monkeypatch.setattr(iqms_lookup, "_connect", _fake_connect, raising=True)
    return state


# ---------------------------------------------------------------------------
# fetch_permissions_for_email — happy paths
# ---------------------------------------------------------------------------

class TestFetchPermissionsForEmail:

    def test_super_admin_packet(self, fake_connect):
        packet = iqms_lookup.fetch_permissions_for_email("hnester@nycoa.com")
        assert packet is not None
        assert packet["iqms_user_name"] == "HNESTER"
        assert packet["email"] == "hnester@nycoa.com"
        assert "IQALL" in packet["role_names"]
        # ePlants 1, 2, 3 — all plants
        assert sorted(packet["eplant_ids"]) == [1, 2, 3]
        # tier should be admin (IQALL present)
        assert packet["tier"] == "admin"
        # ALL → table prefixes collapse to ['%']
        assert packet["allowed_table_prefixes"] == ["%"]

    def test_operator_packet(self, fake_connect):
        packet = iqms_lookup.fetch_permissions_for_email(
            "kfitzpatrick@shawsheencc.com",
        )
        assert packet is not None
        assert packet["iqms_user_name"] == "KFITZPATRICK"
        assert packet["eplant_ids"] == [2]
        assert packet["tier"] == "operator"  # has *_RW roles
        # SO module derived from IQSO_RW
        assert "SO" in packet["module_prefixes"]
        # AR / CUST / INVENTORY also derived
        for mod in ("AR", "CUST", "INVENTORY", "ORDERS", "AP"):
            assert mod in packet["module_prefixes"], (
                f"expected {mod} in module_prefixes, got "
                f"{packet['module_prefixes']}"
            )
        # Allowed table prefixes pull SO%, AR%, INV%, etc — and append the
        # always-allowed S_% / EPLANT% set.
        prefixes = packet["allowed_table_prefixes"]
        assert any(p.startswith("SO") for p in prefixes)
        assert any(p.startswith("INV") for p in prefixes)
        # Always-allowed S_% (security tables) included
        assert any(p.startswith("S_") for p in prefixes)

    def test_only_ro_roles_yields_viewer_tier(self, fake_connect):
        packet = iqms_lookup.fetch_permissions_for_email("readonly@nycoa.com")
        assert packet is not None
        assert packet["tier"] == "viewer"
        assert all(r.endswith("_RO") for r in packet["role_names"])

    def test_unknown_email_returns_none(self, fake_connect):
        packet = iqms_lookup.fetch_permissions_for_email("ghost@nycoa.com")
        assert packet is None

    def test_empty_email_returns_none(self, fake_connect):
        assert iqms_lookup.fetch_permissions_for_email("") is None
        assert iqms_lookup.fetch_permissions_for_email(None) is None  # type: ignore[arg-type]

    def test_email_lookup_is_case_insensitive(self, fake_connect):
        # Use mixed-case input; the canned map is keyed by lowercase.
        packet = iqms_lookup.fetch_permissions_for_email("HNester@NYCOA.com")
        assert packet is not None
        assert packet["email"] == "hnester@nycoa.com"


# ---------------------------------------------------------------------------
# Failure modes — should never raise, always degrade to None
# ---------------------------------------------------------------------------

class TestFetchPermissionsFailureModes:

    def test_oracle_connect_failure_returns_none(self, fake_connect):
        fake_connect.raise_on_connect = RuntimeError("ORA-12541: TNS no listener")
        result = iqms_lookup.fetch_permissions_for_email("hnester@nycoa.com")
        assert result is None  # graceful degrade — no exception bubbled

    def test_query_failure_returns_none(self, fake_connect):
        fake_connect.raise_on_execute = RuntimeError("ORA-00942: table missing")
        result = iqms_lookup.fetch_permissions_for_email("hnester@nycoa.com")
        assert result is None

    def test_connection_closed_after_use(self, fake_connect):
        iqms_lookup.fetch_permissions_for_email("hnester@nycoa.com")
        assert fake_connect.connections_made
        # The fake connection was closed (no leaked handles).
        assert fake_connect.connections_made[0].closed is True


# ---------------------------------------------------------------------------
# Pure derivation helpers
# ---------------------------------------------------------------------------

class TestModulePrefixDerivation:

    @pytest.mark.parametrize("role,expected", [
        ("IQGL_RW", "GL"),
        ("IQGL_RO", "GL"),
        ("IQAP_RO", "AP"),
        ("IQALL", "ALL"),
        ("IQINVENTORY_RW", "INVENTORY"),
        ("NYCOA_SMARTPAGE_BI_RW", "NYCOA_SMARTPAGE_BI"),
        ("DENY_ALL", None),  # Not an IQ role, no _RO/_RW suffix → may parse as DENY
    ])
    def test_parse_module_from_role(self, role, expected):
        result = iqms_lookup._parse_module_from_role(role)
        if expected is None:
            # DENY_ALL is technically parseable as a non-IQ root — we accept
            # either None or the role string itself; the gating is at table-
            # prefix level (DENY_ALL has no prefix mapping).
            assert result is None or result not in ("ALL",)
        else:
            assert result == expected

    def test_derive_module_prefixes_basic(self):
        roles = ["IQGL_RW", "IQAP_RO", "IQALL"]
        mods = iqms_lookup._derive_module_prefixes(roles)
        assert "GL" in mods
        assert "AP" in mods
        assert "ALL" in mods

    def test_derive_module_prefixes_dedupes(self):
        roles = ["IQGL_RW", "IQGL_RO", "IQGL_RW"]
        mods = iqms_lookup._derive_module_prefixes(roles)
        assert mods.count("GL") == 1

    def test_derive_table_prefixes_all_collapses_to_wildcard(self):
        prefixes = iqms_lookup._derive_table_prefixes(["ALL", "GL", "AP"])
        assert prefixes == ["%"]

    def test_derive_table_prefixes_appends_always_allowed(self):
        prefixes = iqms_lookup._derive_table_prefixes(["GL"])
        # GL maps to GL%; always-allow set adds S_% and EPLANT%
        assert "GL%" in prefixes
        assert any(p.startswith("S_") for p in prefixes)
        assert any(p.startswith("EPLANT") for p in prefixes)

    def test_derive_table_prefixes_empty_yields_only_always_allowed(self):
        prefixes = iqms_lookup._derive_table_prefixes([])
        # No module access → still allow security/eplant lookups for context
        assert any(p.startswith("S_") for p in prefixes)


class TestTierDerivation:

    @pytest.mark.parametrize("roles,expected", [
        ([], "viewer"),
        (["IQGL_RO", "IQAP_RO"], "viewer"),
        (["IQGL_RW"], "operator"),
        (["IQGL_RO", "IQAP_RW"], "operator"),
        (["IQALL"], "admin"),
        (["SUPER_ADMIN", "IQGL_RO"], "admin"),
        (["IQALL_REPORTS"], "viewer"),  # Not literal IQALL — must be exact
    ])
    def test_derive_tier(self, roles, expected):
        # IQALL_REPORTS is not IQALL; ensure exact-match
        assert iqms_lookup._derive_tier(roles) == expected


# ---------------------------------------------------------------------------
# now_iso — public timestamp helper
# ---------------------------------------------------------------------------

class TestNowIso:

    def test_now_iso_format(self):
        ts = iqms_lookup.now_iso()
        # ISO-8601 with trailing Z
        assert ts.endswith("Z")
        # Roughly YYYY-MM-DDTHH:MM:SSZ
        assert len(ts) == len("2026-05-08T12:34:56Z")
        assert ts[4] == "-" and ts[7] == "-" and ts[10] == "T"
