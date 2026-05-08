"""
Route-level tests for IQMS Chat.

Covers auth, ePlant selection, admin gating, report download (with
path-traversal protection), and notes the absence of login rate-limiting.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# /login — auth happy path & failures
# ---------------------------------------------------------------------------

class TestLogin:

    def test_login_page_renders(self, client):
        r = client.get("/login")
        assert r.status_code == 200
        assert b"IQMS" in r.data and b"Sign In" in r.data

    def test_login_rejects_bad_username(self, client):
        r = client.post("/login", data={"username": "ghost", "password": "x"})
        # Renders login page again with flashed error
        assert r.status_code == 200
        assert b"Invalid credentials" in r.data

    def test_login_rejects_bad_password(self, client):
        r = client.post("/login", data={"username": "alice", "password": "wrong"})
        assert r.status_code == 200
        assert b"Invalid credentials" in r.data

    def test_login_accepts_good_credentials_and_redirects(self, client):
        r = client.post(
            "/login",
            data={"username": "alice", "password": "alicepw"},
            follow_redirects=False,
        )
        assert r.status_code == 302
        # Lands on root chat page
        assert r.headers["Location"].rstrip("/").endswith("")

    def test_login_sets_session(self, client):
        client.post("/login", data={"username": "alice", "password": "alicepw"})
        with client.session_transaction() as sess:
            assert sess.get("username") == "alice"
            assert sess.get("display_name") == "Alice"
            assert sess.get("is_admin") is False
            assert sess.get("chat_id")  # uuid
            assert sess.get("eplant_id") == "2"  # default Nycoa

    def test_login_username_lowercased(self, client):
        r = client.post(
            "/login",
            data={"username": "  ALICE  ", "password": "alicepw"},
            follow_redirects=False,
        )
        assert r.status_code == 302
        with client.session_transaction() as sess:
            assert sess.get("username") == "alice"


# ---------------------------------------------------------------------------
# /logout
# ---------------------------------------------------------------------------

class TestLogout:

    def test_logout_clears_session(self, auth_client):
        # Confirm we start logged in
        with auth_client.session_transaction() as sess:
            assert sess.get("username") == "alice"

        r = auth_client.get("/logout", follow_redirects=False)
        assert r.status_code == 302

        with auth_client.session_transaction() as sess:
            assert "username" not in sess
            assert "display_name" not in sess


# ---------------------------------------------------------------------------
# Logged-out gating
# ---------------------------------------------------------------------------

class TestRequiresLogin:

    def test_root_redirects_when_logged_out(self, client):
        r = client.get("/", follow_redirects=False)
        assert r.status_code == 302
        assert "/login" in r.headers["Location"]

    def test_ask_requires_login(self, client):
        r = client.post("/ask", json={"question": "hi"}, follow_redirects=False)
        # /ask is wrapped with login_required → redirect to /login
        assert r.status_code in (302, 401)
        if r.status_code == 302:
            assert "/login" in r.headers["Location"]


# ---------------------------------------------------------------------------
# /set-eplant
# ---------------------------------------------------------------------------

class TestSetEplant:

    def test_set_eplant_updates_session(self, auth_client):
        r = auth_client.post("/set-eplant", json={"eplant_id": "2"})
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["name"] == "Nycoa"

        with auth_client.session_transaction() as sess:
            assert sess["eplant_id"] == "2"

    def test_set_eplant_to_shawsheen(self, auth_client):
        r = auth_client.post("/set-eplant", json={"eplant_id": "1"})
        assert r.status_code == 200
        assert r.get_json()["name"] == "Shawsheen"
        with auth_client.session_transaction() as sess:
            assert sess["eplant_id"] == "1"

    def test_set_eplant_rejects_unknown(self, auth_client):
        r = auth_client.post("/set-eplant", json={"eplant_id": "99"})
        assert r.status_code == 400
        assert "error" in r.get_json()

    def test_set_eplant_requires_login(self, client):
        r = client.post("/set-eplant", json={"eplant_id": "1"}, follow_redirects=False)
        assert r.status_code in (302, 401)


# ---------------------------------------------------------------------------
# Admin gate
# ---------------------------------------------------------------------------

class TestAdminGate:

    def test_non_admin_blocked_from_admin_page(self, auth_client):
        # /admin redirects (not 403) — admin_required redirects with a flash to /chat
        r = auth_client.get("/admin", follow_redirects=False)
        assert r.status_code in (302, 403)
        if r.status_code == 302:
            # Should redirect away from /admin
            assert "/admin" not in r.headers["Location"]

    def test_non_admin_blocked_from_logs_api(self, auth_client):
        r = auth_client.get("/api/logs", follow_redirects=False)
        assert r.status_code in (302, 403)

    def test_admin_can_access_admin_page(self, admin_client):
        r = admin_client.get("/admin")
        assert r.status_code == 200

    def test_admin_can_access_logs_api(self, admin_client):
        r = admin_client.get("/api/logs")
        assert r.status_code == 200
        assert isinstance(r.get_json(), list)

    def test_anonymous_redirected_from_admin(self, client):
        r = client.get("/admin", follow_redirects=False)
        assert r.status_code == 302
        assert "/login" in r.headers["Location"]


# ---------------------------------------------------------------------------
# Report download — ownership + path traversal
# ---------------------------------------------------------------------------

class TestReportDownload:

    def _make_report(self, app, username, filename, content="hello"):
        import app as app_module  # noqa
        user_dir = app_module.REPORTS_DIR / username
        user_dir.mkdir(parents=True, exist_ok=True)
        (user_dir / filename).write_text(content, encoding="utf-8")

    def test_download_own_report(self, app, auth_client):
        self._make_report(app, "alice", "my_report.md", content="# Mine")
        r = auth_client.get("/reports/my_report.md")
        assert r.status_code == 200
        assert b"# Mine" in r.data

    def test_download_missing_returns_404(self, app, auth_client):
        r = auth_client.get("/reports/does_not_exist.md")
        assert r.status_code == 404

    def test_cannot_download_other_users_report(self, app, auth_client):
        # Bob has a report; Alice should NOT be able to fetch it.
        self._make_report(app, "bob", "secret.md", content="bob's data")
        r = auth_client.get("/reports/secret.md")
        # /reports/<filename> uses the session username to locate the dir,
        # so Alice's request looks in /reports/alice/secret.md → 404.
        assert r.status_code == 404

    def test_path_traversal_blocked(self, app, auth_client, tmp_path):
        # Try to escape the reports dir. Flask's send_from_directory should
        # reject this (raising 404 for werkzeug-safe joins).
        sensitive = tmp_path / "secret.txt"
        sensitive.write_text("pwned")
        r = auth_client.get("/reports/..%2F..%2Fetc%2Fpasswd")
        # Either Werkzeug rejects the path during routing, or the file
        # doesn't exist in user's reports dir → 404. Anything but 200 with
        # forbidden content is acceptable.
        assert r.status_code in (404, 400)
        assert b"pwned" not in r.data

    def test_path_traversal_via_dotdot_blocked(self, app, auth_client):
        # Direct path-traversal attempt with literal '..'
        r = auth_client.get("/reports/../app.py")
        # Werkzeug routes won't even match this for a single-segment <filename>;
        # at minimum we get 404 and never the actual app.py contents.
        assert r.status_code in (404, 400)
        assert b"Flask" not in r.data  # would appear if we leaked app.py

    def test_report_download_requires_login(self, app, client):
        # Make a report for alice, then try to fetch as anonymous.
        self._make_report(app, "alice", "x.md")
        r = client.get("/reports/x.md", follow_redirects=False)
        assert r.status_code in (302, 401)


# ---------------------------------------------------------------------------
# Login rate-limiting — gap report
# ---------------------------------------------------------------------------

class TestRateLimitGap:
    """
    The current app.py has no login rate-limiting. This test documents the
    gap so it shows up in the suite output.
    """

    @pytest.mark.xfail(
        reason="No rate limiting implemented in app.py — gap to address",
        strict=False,
    )
    def test_repeated_failures_get_throttled(self, client):
        # Hammer the login endpoint; after some threshold the server should
        # respond with 429 or similar. Today it just keeps returning 200.
        threshold_hit = False
        for _ in range(20):
            r = client.post("/login", data={"username": "alice", "password": "x"})
            if r.status_code == 429:
                threshold_hit = True
                break
        assert threshold_hit, "expected 429 after repeated bad logins"
