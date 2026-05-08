"""
Phase 2 — Admin user-management API endpoints.

The new admin endpoints are:

  * GET   /admin/users              — list all users (with cached IQMS packets)
  * POST  /admin/users              — invite / pre-create a portal user
  * GET   /admin/users/<u>          — full detail (incl. raw_packet)
  * POST  /admin/users/<u>/active   — toggle active flag
  * POST  /admin/users/<u>/refresh  — re-run iqms_lookup, refresh packet

These currently aren't wired in app.py (Agent A is implementing them in
parallel). The tests are written to the contract; they xfail with
strict=False until the endpoints land, then auto-flip to PASS.

All endpoints require admin (super-admin / IQALL). Non-admins → 403.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Heuristic — detect whether the new admin endpoints are wired
# ---------------------------------------------------------------------------

def _admin_users_api_landed() -> bool:
    """True if app.py declares routes under /admin/users."""
    try:
        import app as app_module  # type: ignore
    except ImportError:
        return False
    src = Path(app_module.__file__).read_text(encoding="utf-8", errors="replace")
    return any(
        marker in src
        for marker in (
            '"/admin/users"',
            "'/admin/users'",
            '/admin/users/<',
        )
    )


def _admin_users_create_landed() -> bool:
    """True if POST /admin/users (invite) is wired."""
    try:
        import app as app_module  # type: ignore
    except ImportError:
        return False
    src = Path(app_module.__file__).read_text(encoding="utf-8", errors="replace")
    return (
        '@app.route("/admin/users", methods=["POST"])' in src
        or "@app.route('/admin/users', methods=['POST'])" in src
        or '"/admin/users", methods=["GET", "POST"]' in src
    )


# Module-level guard — if Phase 2 admin endpoints aren't wired yet, all tests
# in this module are marked xfail. Currently they ARE wired in main; this
# pytestmark stays as a safety net for partial-revert / rollback scenarios.
pytestmark = pytest.mark.xfail(
    condition=not _admin_users_api_landed(),
    reason="Phase 2 /admin/users endpoints not yet implemented",
    strict=False,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_user(db_module, db_path, *, username, email, display_name,
               is_super_admin=False, active=True, packet=None):
    """Insert a user (and optional permission packet) directly via db helpers."""
    db_module.upsert_user(
        username=username,
        email=email,
        display_name=display_name,
        is_super_admin=is_super_admin,
        db_path=db_path,
    )
    if not active:
        db_module.set_user_active(username, False, db_path=db_path)
    if packet is not None:
        db_module.upsert_iqms_permissions(username, packet, db_path=db_path)


@pytest.fixture
def seeded_users(app, db_module):
    """Pre-seed two users in the test DB. Returns (admin_username, normal_username)."""
    db_path = app.config["TEST_DB_PATH"]
    _seed_user(
        db_module, db_path,
        username="admin@nycoa.com",
        email="admin@nycoa.com",
        display_name="Admin User",
        is_super_admin=True,
        packet={
            "iqms_user_name": "ADMIN",
            "email": "admin@nycoa.com",
            "role_names": ["IQALL"],
            "module_prefixes": ["*"],
            "allowed_table_prefixes": ["%"],
            "eplant_ids": [1, 2, 3],
            "tier": "admin",
            "synced_at": "2026-05-08T12:00:00Z",
        },
    )
    _seed_user(
        db_module, db_path,
        username="kfitzpatrick@shawsheencc.com",
        email="kfitzpatrick@shawsheencc.com",
        display_name="Kelly Fitzpatrick",
        is_super_admin=False,
        packet={
            "iqms_user_name": "KFITZPATRICK",
            "email": "kfitzpatrick@shawsheencc.com",
            "role_names": ["IQSO_RW"],
            "module_prefixes": ["SO"],
            "allowed_table_prefixes": ["SO%", "S_%"],
            "eplant_ids": [2],
            "tier": "operator",
            "synced_at": "2026-05-08T12:00:00Z",
        },
    )
    return "admin@nycoa.com", "kfitzpatrick@shawsheencc.com"


@pytest.fixture
def super_admin_session_client(app, seeded_users):
    """Test client logged in as a real super-admin (mirrors a /sso login)."""
    admin_username, _ = seeded_users
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["username"] = admin_username
        sess["display_name"] = "Admin User"
        sess["is_admin"] = True
        sess["is_super_admin"] = True
        sess["chat_id"] = str(uuid.uuid4())
        sess["eplant_id"] = "1"
        sess["eplant_access"] = [1, 2, 3]
        sess["permission_packet"] = {
            "iqms_user_name": "ADMIN",
            "email": admin_username,
            "role_names": ["IQALL"],
            "module_prefixes": ["*"],
            "allowed_table_prefixes": ["%"],
            "eplant_ids": [1, 2, 3],
            "tier": "admin",
            "synced_at": "2026-05-08T12:00:00Z",
        }
    return c


# ---------------------------------------------------------------------------
# GET /admin/users
# ---------------------------------------------------------------------------

class TestListUsers:

    def test_admin_can_list_users(self, super_admin_session_client, seeded_users):
        # Pass Accept: application/json so /admin/users returns the JSON shape
        # rather than the HTML admin page.
        r = super_admin_session_client.get(
            "/admin/users",
            headers={"Accept": "application/json"},
        )
        assert r.status_code == 200
        body = r.get_json()
        # Implementation returns a flat list. Accept either flat or envelope.
        if isinstance(body, dict):
            users = body.get("users") or body.get("data") or []
        else:
            users = body
        assert isinstance(users, list)
        usernames = {u.get("username") for u in users}
        admin_user, normal_user = seeded_users
        assert admin_user in usernames
        assert normal_user in usernames

    def test_list_users_includes_required_fields(
        self, super_admin_session_client, seeded_users,
    ):
        r = super_admin_session_client.get(
            "/admin/users?format=json",
        )
        assert r.status_code == 200
        body = r.get_json()
        users = body.get("users") if isinstance(body, dict) else body
        normal_user = seeded_users[1]
        target = next((u for u in users if u.get("username") == normal_user), None)
        assert target is not None
        for field in ("username", "email", "display_name", "active", "tier"):
            assert field in target, f"missing field {field!r} in user record: {target}"

    def test_non_admin_blocked(self, auth_client):
        # auth_client is alice (non-admin) from conftest
        r = auth_client.get(
            "/admin/users",
            headers={"Accept": "application/json"},
            follow_redirects=False,
        )
        assert r.status_code in (302, 403), (
            f"non-admin should be blocked, got {r.status_code}"
        )

    def test_anonymous_blocked(self, client):
        r = client.get("/admin/users", follow_redirects=False)
        assert r.status_code in (302, 401, 403)
        if r.status_code == 302:
            assert "/login" in r.headers["Location"]


# ---------------------------------------------------------------------------
# POST /admin/users — pre-create / invite
#
# Per the deliverables spec, Phase 2 includes a POST endpoint to invite a
# portal user before they SSO. This is NOT yet implemented in the current
# /admin/users wiring (only GET); each test xfails individually so it
# auto-flips when the endpoint lands.
# ---------------------------------------------------------------------------

class TestCreateUser:

    @pytest.mark.xfail(
        condition=not _admin_users_create_landed(),
        reason="POST /admin/users invite endpoint not yet implemented",
        strict=False,
    )
    def test_admin_can_create_user(
        self, super_admin_session_client, app, db_module,
    ):
        r = super_admin_session_client.post(
            "/admin/users",
            json={
                "email": "newhire@nycoa.com",
                "display_name": "New Hire",
            },
        )
        assert r.status_code in (200, 201)
        users = db_module.list_users(db_path=app.config["TEST_DB_PATH"])
        emails = {(u.get("email") or "").lower() for u in users}
        assert "newhire@nycoa.com" in emails

    @pytest.mark.xfail(
        condition=not _admin_users_create_landed(),
        reason="POST /admin/users invite endpoint not yet implemented",
        strict=False,
    )
    def test_create_requires_email(self, super_admin_session_client):
        r = super_admin_session_client.post(
            "/admin/users", json={"display_name": "Anon"},
        )
        assert r.status_code == 400

    @pytest.mark.xfail(
        condition=not _admin_users_create_landed(),
        reason="POST /admin/users invite endpoint not yet implemented",
        strict=False,
    )
    def test_non_admin_cannot_create(self, auth_client):
        r = auth_client.post(
            "/admin/users",
            json={"email": "x@y.com", "display_name": "x"},
            follow_redirects=False,
        )
        assert r.status_code in (302, 403)


# ---------------------------------------------------------------------------
# GET /admin/users/<u>
# ---------------------------------------------------------------------------

class TestGetUser:

    def test_admin_can_get_user_detail(
        self, super_admin_session_client, seeded_users,
    ):
        _, normal_user = seeded_users
        r = super_admin_session_client.get(f"/admin/users/{normal_user}")
        assert r.status_code == 200
        body = r.get_json()
        # Implementation returns {"user": {...}, "permissions": {...}} —
        # accept either that shape or a flat dict carrying the same fields.
        user_obj = body.get("user") if "user" in body else body
        assert user_obj.get("username") == normal_user, (
            f"detail response missing 'username'; got body={body}"
        )
        packet_like = (
            body.get("permissions")
            or body.get("raw_packet")
            or body.get("permission_packet")
            or {}
        )
        assert packet_like, f"detail response missing permissions/raw_packet: {body}"
        # The cached packet should carry the operator role set we seeded
        roles = packet_like.get("role_names") or []
        assert "IQSO_RW" in roles

    def test_get_unknown_user_returns_404(self, super_admin_session_client):
        r = super_admin_session_client.get(
            "/admin/users/does-not-exist@nowhere.com",
        )
        assert r.status_code == 404

    def test_non_admin_blocked(self, auth_client, seeded_users):
        _, normal_user = seeded_users
        r = auth_client.get(
            f"/admin/users/{normal_user}", follow_redirects=False,
        )
        assert r.status_code in (302, 403)


# ---------------------------------------------------------------------------
# POST /admin/users/<u>/active
# ---------------------------------------------------------------------------

class TestSetUserActive:

    def test_admin_can_deactivate_user(
        self, super_admin_session_client, seeded_users, app, db_module,
    ):
        _, normal_user = seeded_users
        r = super_admin_session_client.post(
            f"/admin/users/{normal_user}/active",
            json={"active": False},
        )
        assert r.status_code == 200

        row = db_module.get_user(
            normal_user, db_path=app.config["TEST_DB_PATH"],
        )
        assert row is not None
        assert row.get("active") in (0, False)

    def test_admin_can_reactivate_user(
        self, super_admin_session_client, seeded_users, app, db_module,
    ):
        _, normal_user = seeded_users
        # Deactivate, then reactivate
        super_admin_session_client.post(
            f"/admin/users/{normal_user}/active", json={"active": False},
        )
        r = super_admin_session_client.post(
            f"/admin/users/{normal_user}/active", json={"active": True},
        )
        assert r.status_code == 200
        row = db_module.get_user(
            normal_user, db_path=app.config["TEST_DB_PATH"],
        )
        assert row.get("active") in (1, True)

    def test_deactivated_user_blocked_from_sso(
        self, super_admin_session_client, seeded_users, sso_login,
        mock_iqms_lookup,
    ):
        """End-to-end: deactivate via API, confirm SSO is then blocked."""
        _, normal_user = seeded_users
        super_admin_session_client.post(
            f"/admin/users/{normal_user}/active", json={"active": False},
        )
        c, r = sso_login(normal_user)
        assert r.status_code in (302, 401, 403)
        with c.session_transaction() as sess:
            assert "username" not in sess

    def test_unknown_user_returns_404(self, super_admin_session_client):
        r = super_admin_session_client.post(
            "/admin/users/does-not-exist@nowhere.com/active",
            json={"active": False},
        )
        assert r.status_code == 404

    def test_non_admin_blocked(self, auth_client, seeded_users):
        _, normal_user = seeded_users
        r = auth_client.post(
            f"/admin/users/{normal_user}/active",
            json={"active": False},
            follow_redirects=False,
        )
        assert r.status_code in (302, 403)


# ---------------------------------------------------------------------------
# POST /admin/users/<u>/refresh
# ---------------------------------------------------------------------------

class TestRefreshUser:

    def test_admin_can_refresh_packet(
        self, super_admin_session_client, seeded_users,
        app, db_module, mock_iqms_lookup,
    ):
        _, normal_user = seeded_users
        # Capture the seeded synced_at
        before = db_module.get_iqms_permissions(
            normal_user, db_path=app.config["TEST_DB_PATH"],
        )
        before_synced = (before or {}).get("synced_at")

        # Pre-seed a fresh packet with a strictly-greater synced_at
        new_packet = {
            "iqms_user_name": "KFITZPATRICK",
            "email": normal_user,
            "role_names": ["IQSO_RW", "IQAR_RO"],  # one role added
            "module_prefixes": ["SO", "AR"],
            "allowed_table_prefixes": ["SO%", "AR%", "S_%"],
            "eplant_ids": [2],
            "tier": "operator",
            "synced_at": "2026-12-31T23:59:59Z",
        }
        mock_iqms_lookup.set(normal_user, new_packet)

        r = super_admin_session_client.post(
            f"/admin/users/{normal_user}/refresh",
        )
        assert r.status_code == 200

        # The lookup mock was called with this email
        assert normal_user in mock_iqms_lookup.calls

        after = db_module.get_iqms_permissions(
            normal_user, db_path=app.config["TEST_DB_PATH"],
        )
        assert after is not None
        if before_synced:
            assert after.get("synced_at") >= before_synced
        # The role list should reflect the refresh
        assert "IQAR_RO" in (after.get("role_names") or [])

    def test_refresh_unknown_user_returns_404(self, super_admin_session_client):
        r = super_admin_session_client.post(
            "/admin/users/does-not-exist@nowhere.com/refresh",
        )
        assert r.status_code == 404

    def test_non_admin_blocked(self, auth_client, seeded_users):
        _, normal_user = seeded_users
        r = auth_client.post(
            f"/admin/users/{normal_user}/refresh", follow_redirects=False,
        )
        assert r.status_code in (302, 403)
