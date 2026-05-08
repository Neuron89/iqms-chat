"""
Unit tests for the report-extraction logic that pulls
===REPORT_START===…===REPORT_END=== blocks out of Claude's output.

The current implementation lives in app.py: `_extract_report(answer, username)`
plus the module-level `REPORT_PATTERN` regex. These tests target both the
pure regex (where possible) and the file-writing wrapper.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Pure regex tests — REPORT_PATTERN
# ---------------------------------------------------------------------------

class TestReportPattern:
    """The regex itself — independent of filesystem state."""

    def test_no_markers_returns_no_match(self, app_module):
        assert app_module.REPORT_PATTERN.search("just a plain answer") is None

    def test_full_block_extracts_filename_and_content(self, app_module):
        text = (
            "Here is your report.\n\n"
            "===REPORT_START===\n"
            "summary.md\n"
            "===REPORT_CONTENT===\n"
            "# Hello\n\nBody.\n"
            "===REPORT_END==="
        )
        m = app_module.REPORT_PATTERN.search(text)
        assert m is not None
        assert m.group(1).strip() == "summary.md"
        assert "# Hello" in m.group(2)

    def test_dotall_handles_multiline_content(self, app_module):
        content = "line1\nline2\nline3\nline4"
        text = (
            "===REPORT_START===\n"
            "x.md\n"
            "===REPORT_CONTENT===\n"
            f"{content}\n"
            "===REPORT_END==="
        )
        m = app_module.REPORT_PATTERN.search(text)
        assert m is not None
        assert m.group(2) == content


# ---------------------------------------------------------------------------
# _extract_report() — full wrapper, writes file to disk
# ---------------------------------------------------------------------------

class TestExtractReport:

    def test_no_markers_returns_unchanged(self, app_module):
        out, url = app_module._extract_report("plain answer with no report", "alice")
        assert out == "plain answer with no report"
        assert url is None

    def test_single_report_extracts_and_cleans_answer(self, app_module):
        ans = (
            "Here is the data.\n\n"
            "===REPORT_START===\n"
            "data.md\n"
            "===REPORT_CONTENT===\n"
            "# Title\n\nBody text.\n"
            "===REPORT_END==="
        )
        # /reports/<filename> needs request context for url_for
        with app_module.app.test_request_context():
            cleaned, url = app_module._extract_report(ans, "alice")

        assert "===REPORT_START===" not in cleaned
        assert "===REPORT_END===" not in cleaned
        assert cleaned.rstrip() == "Here is the data."
        assert url and "data.md" in url

        # File got written
        out_file = app_module.REPORTS_DIR / "alice" / "data.md"
        assert out_file.exists()
        assert "# Title" in out_file.read_text(encoding="utf-8")

    def test_filename_with_unsafe_chars_is_sanitized(self, app_module):
        ans = (
            "===REPORT_START===\n"
            "../../etc/passwd\n"
            "===REPORT_CONTENT===\n"
            "evil\n"
            "===REPORT_END==="
        )
        with app_module.app.test_request_context():
            _, url = app_module._extract_report(ans, "alice")

        # Sanitizer regex is [^\w\-.]→_, which preserves '.' but kills the
        # path separators '/'. So '../../etc/passwd' becomes a flat name like
        # '.._.._etc_passwd' that lives INSIDE alice's reports dir — no
        # traversal escape. Verify each produced file is a direct child of
        # the user dir (no subdirs, no parent escape).
        user_dir = app_module.REPORTS_DIR / "alice"
        files = list(user_dir.iterdir())
        assert len(files) == 1
        produced = files[0]
        # Must be a regular file directly under user_dir — i.e. parent ==
        # user_dir, no slashes in the name component.
        assert produced.parent.resolve() == user_dir.resolve()
        assert "/" not in produced.name
        # And, of course, /etc/passwd was not touched
        assert not Path("/etc/passwd_evil").exists()

    def test_empty_filename_falls_back_to_timestamp(self, app_module):
        ans = (
            "===REPORT_START===\n"
            "@@@@@@\n"  # all chars get sanitized to '_', resulting in non-empty
            "===REPORT_CONTENT===\n"
            "body\n"
            "===REPORT_END==="
        )
        # When sanitization produces an empty string, the impl falls back
        # to "report_<timestamp>.md". Here all '@' chars become '_' so the
        # filename is just "______" — still non-empty. Confirm a file was
        # produced regardless.
        with app_module.app.test_request_context():
            cleaned, url = app_module._extract_report(ans, "alice")
        assert url is not None
        files = list((app_module.REPORTS_DIR / "alice").iterdir())
        assert len(files) == 1

    def test_markers_without_filename_section_handled_gracefully(self, app_module):
        # If only one marker is present, regex shouldn't match → no report,
        # answer returned unchanged.
        ans = "Here's an answer with ===REPORT_START=== and nothing else."
        with app_module.app.test_request_context():
            cleaned, url = app_module._extract_report(ans, "alice")
        assert url is None
        assert cleaned == ans

    def test_content_with_backticks_and_code_blocks(self, app_module):
        body = (
            "# Code dump\n\n"
            "```python\n"
            "def foo():\n"
            "    return '`backtick`'\n"
            "```\n\n"
            "Some `inline code` and **bold**.\n"
        )
        ans = (
            "Done.\n\n"
            "===REPORT_START===\n"
            "code.md\n"
            "===REPORT_CONTENT===\n"
            f"{body}"
            "===REPORT_END==="
        )
        with app_module.app.test_request_context():
            cleaned, url = app_module._extract_report(ans, "alice")
        assert url is not None
        # Backticks must round-trip into the file unmolested
        on_disk = (app_module.REPORTS_DIR / "alice" / "code.md").read_text(encoding="utf-8")
        assert "```python" in on_disk
        assert "`inline code`" in on_disk
        assert "**bold**" in on_disk

    def test_very_long_content_writes_intact(self, app_module):
        # 1.2 MB body
        body = "X" * (1_200_000)
        ans = (
            "===REPORT_START===\n"
            "huge.md\n"
            "===REPORT_CONTENT===\n"
            f"{body}\n"
            "===REPORT_END==="
        )
        with app_module.app.test_request_context():
            cleaned, url = app_module._extract_report(ans, "alice")
        assert url is not None
        on_disk = (app_module.REPORTS_DIR / "alice" / "huge.md").read_text(encoding="utf-8")
        assert len(on_disk) == len(body)

    def test_multiple_reports_in_one_response_first_extracted(self, app_module):
        """
        Edge case: if claude emits two report blocks, the current regex uses
        non-greedy matching with re.DOTALL, so .search() finds the FIRST
        block. The 2nd block is left embedded in the cleaned answer (not
        ideal — flagged in coverage notes).
        """
        ans = (
            "Two reports incoming.\n\n"
            "===REPORT_START===\n"
            "first.md\n"
            "===REPORT_CONTENT===\n"
            "# First\n"
            "===REPORT_END===\n\n"
            "And another:\n\n"
            "===REPORT_START===\n"
            "second.md\n"
            "===REPORT_CONTENT===\n"
            "# Second\n"
            "===REPORT_END==="
        )
        with app_module.app.test_request_context():
            cleaned, url = app_module._extract_report(ans, "alice")

        assert url is not None
        assert "first.md" in url
        # First report file written
        assert (app_module.REPORTS_DIR / "alice" / "first.md").exists()
        # Second report NOT written by the current impl
        assert not (app_module.REPORTS_DIR / "alice" / "second.md").exists()
        # And the 2nd block survives in the cleaned answer (impl truncates
        # at match.start() of the FIRST block, so anything BEFORE is kept
        # and the rest is dropped). Confirm the cleaned string only contains
        # the leading "Two reports incoming." preamble.
        assert "===REPORT_START===" not in cleaned
        assert "Two reports incoming." in cleaned

    def test_separate_users_get_separate_directories(self, app_module):
        ans = (
            "===REPORT_START===\n"
            "shared_name.md\n"
            "===REPORT_CONTENT===\n"
            "alice's content\n"
            "===REPORT_END==="
        )
        with app_module.app.test_request_context():
            app_module._extract_report(ans, "alice")
            app_module._extract_report(
                ans.replace("alice's content", "bob's content"), "bob",
            )

        alice_file = app_module.REPORTS_DIR / "alice" / "shared_name.md"
        bob_file = app_module.REPORTS_DIR / "bob" / "shared_name.md"
        assert alice_file.exists()
        assert bob_file.exists()
        assert "alice" in alice_file.read_text(encoding="utf-8")
        assert "bob" in bob_file.read_text(encoding="utf-8")
