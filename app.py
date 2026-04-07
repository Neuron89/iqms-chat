"""
IQMS Chat — Web interface for querying IQMS ERP via Claude + MCP.
Users log in, ask questions in plain English, get answers from Claude
which queries the IQMS Oracle database through the MCP server.
"""

import csv
import io
import json
import os
import re
import subprocess
import tempfile
import uuid
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, jsonify, send_from_directory,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(32))
app.config["MAX_CONTENT_LENGTH"] = 20 * 1024 * 1024  # 20 MB

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
USERS_FILE = DATA_DIR / "users.json"
REPORTS_DIR = Path(__file__).parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)
UPLOADS_DIR = Path(__file__).parent / "uploads"
UPLOADS_DIR.mkdir(exist_ok=True)

MAX_UPLOAD_SIZE = 20 * 1024 * 1024  # 20 MB
ALLOWED_EXTENSIONS = {
    # Images (passed as file path for Claude to read visually)
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp",
    # Documents (text extracted and inlined)
    ".docx", ".xlsx", ".xls", ".csv",
    ".txt", ".md", ".json", ".xml", ".yaml", ".yml",
    ".pdf",
}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"}

# MCP config — IQMS Oracle is always available, dataPARC only for Nycoa
IQMS_MCP = {
    "command": "node",
    "args": ["/home/hnester/iqms-plugin/plugins/iqms-team-tools/mcp-servers/iqms-oracle/index.js"],
    "env": {
        "IQMS_DB_USER": "mcp_readonly",
        "IQMS_DB_PASSWORD": "Palmvilla119",
        "IQMS_DB_CONNECT": "IQORA",
    },
}
DATAPARC_MCP = {
    "command": "node",
    "args": ["/home/hnester/dataparc-mcp/dist/index.js"],
    "env": {
        "SQL_SERVER": "10.10.1.248",
        "SQL_PORT": "1433",
        "SQL_DATABASE": "ctc_config",
        "SQL_USER": "hnester",
        "SQL_PASSWORD": "Palmvilla119",
        "SQL_DOMAIN": "scada",
        "SQL_TRUST_CERT": "true",
        "DEFAULT_PLANT": "NYCOA",
    },
}

# Write both config files (with and without dataPARC)
MCP_CONFIG_IQMS = DATA_DIR / "mcp_config_iqms.json"
MCP_CONFIG_IQMS.write_text(json.dumps({"mcpServers": {"iqms-oracle": IQMS_MCP}}, indent=2))

MCP_CONFIG_ALL = DATA_DIR / "mcp_config_all.json"
MCP_CONFIG_ALL.write_text(json.dumps({"mcpServers": {"iqms-oracle": IQMS_MCP, "dataparc": DATAPARC_MCP}}, indent=2))

CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "opus")

# ePlant definitions
EPLANTS = {
    "1": {"name": "Shawsheen", "company": "Shawsheen Rubber Co Inc"},
    "2": {"name": "Nycoa", "company": "Nylon Corporation of America"},
    "3": {"name": "Bradford", "company": "Bradford Industries"},
}

SYSTEM_PROMPT_TEMPLATE = """You are an IQMS ERP database assistant. Users ask questions about manufacturing data,
production records, work orders, quality, inventory, and other ERP data stored in the IQMS Oracle database.

Use the iqms-oracle MCP tools to query the database and answer questions. Available IQMS tools:
- query: Run read-only SQL against the IQMS Oracle database
- list-tables: List tables matching a pattern
- describe-table: Show column details for a table
- sample-data: Preview rows from a table
- search-columns: Find columns by name across tables
- table-relationships: Show foreign key relationships
- table-indexes: Show indexes on a table
- row-count: Count rows in a table
{dataparc_section}
CRITICAL — ePlant Filtering:
The user is currently working with ePlant: {eplant_name} (EPLANT_ID = {eplant_id}, {eplant_company}).
You MUST add "WHERE EPLANT_ID = {eplant_id}" (or "AND EPLANT_ID = {eplant_id}") to EVERY query that
touches a table containing an EPLANT_ID column. Most major tables have this column (ARINVT, WORKORDER,
ORDERS, PDAYPROD, DAYPROD, ARCUSTO, etc. — 571+ tables). Always check if the table has EPLANT_ID and
filter accordingly. Never return data from other ePlants.

Important guidelines:
- All queries are read-only (SELECT only)
- The schema owner is IQMS — prefix tables as IQMS.TABLE_NAME
- PDAYPROD contains archived daily production data (the real historical data)
- DAYPROD is just the unarchived buffer (current/recent entries only)
- Give clear, concise answers. Format numbers and tables nicely using markdown.
- If you're unsure about a table or column, use the discovery tools first.
- Only show the final answer to the user. Do not describe your internal steps or tool calls.

Report Generation:
When the user asks for a report, generate the data and format it as a clean, well-structured
markdown document. At the very end of your response, output the report content between these
exact markers on their own lines:

===REPORT_START===
(report filename, e.g. production_summary_2025.md)
===REPORT_CONTENT===
(full report content in markdown)
===REPORT_END===

Then tell the user their report is ready for download. The system will automatically detect
these markers and create a downloadable file. Use descriptive filenames with the ePlant name
and date. For CSV data, use .csv extension instead of .md."""

DATAPARC_PROMPT_SECTION = """
Additionally, you have access to dataPARC SCADA/historian tools for real-time and historical
process data. dataPARC is only used at the Nycoa plant. Available dataPARC tools:
- searchTags: Search for dataPARC tags by name pattern (e.g. %RX1%, %Temperature%)
- readCurrentValues: Get the current/latest value of one or more tags
- readRawTags: Read raw historical tag data for a time range
- readInterpolatedTags: Read interpolated (evenly-spaced) historical tag data
- readAtTimeTags: Read tag values at specific timestamps

When the user asks about process data, sensor readings, temperatures, pressures, line speeds,
or anything SCADA/historian related, use the dataPARC tools. You can combine IQMS and dataPARC
data in a single answer when relevant (e.g. correlating production records with process parameters).
"""

# Store conversation histories in memory (keyed by session chat_id)
conversations: dict[str, list] = {}

# ---------------------------------------------------------------------------
# User store
# ---------------------------------------------------------------------------

def _load_users() -> dict:
    if USERS_FILE.exists():
        return json.loads(USERS_FILE.read_text())
    return {}


def _save_users(users: dict):
    USERS_FILE.write_text(json.dumps(users, indent=2))


def _hash_pw(password: str, salt: str = "") -> str:
    return sha256(f"{salt}:{password}".encode()).hexdigest()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "username" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "username" not in session:
            return redirect(url_for("login"))
        users = _load_users()
        user = users.get(session["username"], {})
        if not user.get("is_admin"):
            flash("Admin access required.", "error")
            return redirect(url_for("chat"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        users = _load_users()
        if username not in users:
            flash("Invalid credentials.", "error")
            return render_template("login.html")

        if users[username]["hash"] != _hash_pw(password, users[username]["salt"]):
            flash("Invalid credentials.", "error")
            return render_template("login.html")

        session["username"] = username
        session["display_name"] = users[username].get("display_name", username)
        session["is_admin"] = users[username].get("is_admin", False)
        session["chat_id"] = str(uuid.uuid4())
        session["eplant_id"] = "2"  # Default to Nycoa
        return redirect(url_for("chat"))

    return render_template("login.html")


@app.route("/logout")
def logout():
    chat_id = session.get("chat_id")
    if chat_id and chat_id in conversations:
        del conversations[chat_id]
    session.clear()
    return redirect(url_for("login"))


# ---------------------------------------------------------------------------
# ePlant selection
# ---------------------------------------------------------------------------

@app.route("/set-eplant", methods=["POST"])
@login_required
def set_eplant():
    data = request.get_json()
    eplant_id = data.get("eplant_id", "1")
    if eplant_id in EPLANTS:
        session["eplant_id"] = eplant_id
        return jsonify({"ok": True, "name": EPLANTS[eplant_id]["name"]})
    return jsonify({"error": "Invalid ePlant"}), 400


# ---------------------------------------------------------------------------
# File upload & processing
# ---------------------------------------------------------------------------

def _extract_text_from_docx(filepath: Path) -> str:
    """Extract text content from a .docx file."""
    from docx import Document
    doc = Document(str(filepath))
    lines = []
    for para in doc.paragraphs:
        lines.append(para.text)
    # Also extract tables
    for table in doc.tables:
        for row in table.rows:
            cells = [cell.text.strip() for cell in row.cells]
            lines.append(" | ".join(cells))
    return "\n".join(lines)


def _extract_text_from_xlsx(filepath: Path) -> str:
    """Extract content from an .xlsx file as CSV-like text."""
    from openpyxl import load_workbook
    wb = load_workbook(str(filepath), read_only=True, data_only=True)
    output = []
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        output.append(f"=== Sheet: {sheet_name} ===")
        buf = io.StringIO()
        writer = csv.writer(buf)
        for row in ws.iter_rows(values_only=True):
            writer.writerow([str(c) if c is not None else "" for c in row])
        output.append(buf.getvalue())
    wb.close()
    return "\n".join(output)


def _process_upload(filepath: Path) -> dict:
    """Process an uploaded file. Returns dict with 'type' and content info."""
    ext = filepath.suffix.lower()

    if ext in IMAGE_EXTENSIONS:
        # Images: Claude CLI can read them via the Read tool
        return {"type": "image", "path": str(filepath), "name": filepath.name}

    if ext == ".pdf":
        # PDFs: Claude CLI can read them via the Read tool
        return {"type": "pdf", "path": str(filepath), "name": filepath.name}

    if ext == ".docx":
        text = _extract_text_from_docx(filepath)
        return {"type": "text", "content": text[:100000], "name": filepath.name}

    if ext in (".xlsx", ".xls"):
        text = _extract_text_from_xlsx(filepath)
        return {"type": "text", "content": text[:100000], "name": filepath.name}

    # Plain text / csv / json / etc — read directly
    try:
        text = filepath.read_text(encoding="utf-8", errors="replace")
        return {"type": "text", "content": text[:100000], "name": filepath.name}
    except Exception:
        return {"type": "error", "name": filepath.name}


@app.route("/upload", methods=["POST"])
@login_required
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "No file selected"}), 400

    ext = Path(f.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return jsonify({"error": f"File type '{ext}' not supported"}), 400

    # Save to user-specific uploads directory
    username = session["username"]
    user_dir = UPLOADS_DIR / username
    user_dir.mkdir(exist_ok=True)

    # Unique filename to avoid collisions
    safe_name = re.sub(r'[^\w\-.]', '_', f.filename)
    unique_name = f"{uuid.uuid4().hex[:8]}_{safe_name}"
    filepath = user_dir / unique_name
    f.save(str(filepath))

    # Check file size
    if filepath.stat().st_size > MAX_UPLOAD_SIZE:
        filepath.unlink()
        return jsonify({"error": "File too large (max 20 MB)"}), 400

    # Process the file
    result = _process_upload(filepath)
    result["upload_id"] = unique_name
    result["original_name"] = f.filename

    return jsonify(result)


# ---------------------------------------------------------------------------
# Chat
# ---------------------------------------------------------------------------

@app.route("/")
@login_required
def chat():
    chat_id = session.get("chat_id", "")
    messages = conversations.get(chat_id, [])
    eplant_id = session.get("eplant_id", "1")
    return render_template("chat.html",
                           username=session["username"],
                           display_name=session.get("display_name", session["username"]),
                           messages=messages,
                           eplants=EPLANTS,
                           current_eplant=eplant_id)


@app.route("/new-chat", methods=["POST"])
@login_required
def new_chat():
    old_id = session.get("chat_id")
    if old_id and old_id in conversations:
        del conversations[old_id]
    session["chat_id"] = str(uuid.uuid4())
    return redirect(url_for("chat"))


# Report marker pattern
REPORT_PATTERN = re.compile(
    r'===REPORT_START===\s*\n(.+?)\n===REPORT_CONTENT===\s*\n(.*?)\n===REPORT_END===',
    re.DOTALL,
)


def _extract_report(answer: str, username: str) -> tuple[str, str | None]:
    """Extract report from answer if present. Returns (cleaned_answer, report_url or None)."""
    match = REPORT_PATTERN.search(answer)
    if not match:
        return answer, None

    filename = match.group(1).strip()
    content = match.group(2).strip()

    # Sanitize filename
    filename = re.sub(r'[^\w\-.]', '_', filename)
    if not filename:
        filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    # Save to user-specific reports directory
    user_dir = REPORTS_DIR / username
    user_dir.mkdir(exist_ok=True)
    report_path = user_dir / filename
    report_path.write_text(content, encoding="utf-8")

    # Remove the report markers from the displayed answer
    clean_answer = answer[:match.start()].rstrip()
    report_url = url_for("download_report", filename=filename)

    return clean_answer, report_url


@app.route("/ask", methods=["POST"])
@login_required
def ask():
    data = request.get_json()
    question = data.get("question", "").strip()
    attachments = data.get("attachments", [])
    if not question:
        return jsonify({"error": "Empty question"}), 400

    chat_id = session.get("chat_id", str(uuid.uuid4()))
    if chat_id not in conversations:
        conversations[chat_id] = []

    username = session["username"]

    # Build display content for user message (show attachment names)
    display_content = question
    if attachments:
        file_names = [a.get("original_name", a.get("name", "file")) for a in attachments]
        display_content = question + "\n\n📎 " + ", ".join(file_names)

    # Add user message
    conversations[chat_id].append({
        "role": "user",
        "content": display_content,
        "timestamp": datetime.now().strftime("%H:%M"),
    })

    # Build the full prompt with file contents
    full_prompt = question

    # Collect image/pdf file paths for Claude to read
    file_paths_for_claude = []

    for att in attachments:
        att_type = att.get("type")
        name = att.get("original_name", att.get("name", "file"))

        if att_type == "text":
            # Inline text content
            content = att.get("content", "")
            full_prompt += f"\n\n--- Attached file: {name} ---\n{content}\n--- End of {name} ---"

        elif att_type in ("image", "pdf"):
            # Claude CLI will read these via the Read tool
            fpath = att.get("path", "")
            if fpath and Path(fpath).exists():
                file_paths_for_claude.append(fpath)
                full_prompt += f"\n\n[Attached {att_type}: {name} — saved at {fpath}. Use the Read tool to view it.]"

    # Build ePlant-specific system prompt
    eplant_id = session.get("eplant_id", "1")
    eplant = EPLANTS.get(eplant_id, EPLANTS["1"])

    # dataPARC only available for Nycoa (eplant 2)
    is_nycoa = eplant_id == "2"
    dataparc_section = DATAPARC_PROMPT_SECTION if is_nycoa else ""
    mcp_config_file = str(MCP_CONFIG_ALL) if is_nycoa else str(MCP_CONFIG_IQMS)

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        eplant_id=eplant_id,
        eplant_name=eplant["name"],
        eplant_company=eplant["company"],
        dataparc_section=dataparc_section,
    )

    # Build the claude command
    cmd = [
        "claude",
        "-p",
        "--model", CLAUDE_MODEL,
        "--output-format", "json",
        "--mcp-config", mcp_config_file,
        "--permission-mode", "bypassPermissions",
        "--system-prompt", system_prompt,
        "--no-session-persistence",
    ]

    # Add file paths as allowed directories so Claude can read them
    for fpath in file_paths_for_claude:
        cmd.extend(["--add-dir", str(Path(fpath).parent)])

    cmd.append(full_prompt)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=180,
            cwd="/home/hnester",
        )

        if result.returncode != 0:
            error_msg = result.stderr.strip() or "Claude returned an error."
            answer = f"Sorry, I encountered an error: {error_msg}"
        else:
            try:
                output = json.loads(result.stdout)
                answer = output.get("result", result.stdout.strip())
            except json.JSONDecodeError:
                answer = result.stdout.strip()

    except subprocess.TimeoutExpired:
        answer = "Sorry, the query took too long. Try a more specific question."
    except Exception as e:
        answer = f"Sorry, something went wrong: {str(e)}"

    # Check for report in the answer
    answer, report_url = _extract_report(answer, username)

    # Add assistant message
    msg = {
        "role": "assistant",
        "content": answer,
        "timestamp": datetime.now().strftime("%H:%M"),
    }
    if report_url:
        msg["report_url"] = report_url
    conversations[chat_id].append(msg)

    resp = {"answer": answer}
    if report_url:
        resp["report_url"] = report_url
    return jsonify(resp)


# ---------------------------------------------------------------------------
# Report downloads
# ---------------------------------------------------------------------------

@app.route("/reports/<filename>")
@login_required
def download_report(filename):
    username = session["username"]
    user_dir = REPORTS_DIR / username
    if not (user_dir / filename).exists():
        return "Report not found.", 404
    return send_from_directory(user_dir, filename, as_attachment=True)


# ---------------------------------------------------------------------------
# Admin — User Management
# ---------------------------------------------------------------------------

@app.route("/admin")
@admin_required
def admin():
    users = _load_users()
    return render_template("admin.html",
                           username=session["username"],
                           display_name=session.get("display_name", session["username"]),
                           users=users)


@app.route("/admin/add-user", methods=["POST"])
@admin_required
def add_user():
    username = request.form.get("username", "").strip().lower()
    display_name = request.form.get("display_name", "").strip()
    password = request.form.get("password", "")
    is_admin = request.form.get("is_admin") == "on"

    if not username or not password:
        flash("Username and password are required.", "error")
        return redirect(url_for("admin"))

    if len(password) < 4:
        flash("Password must be at least 4 characters.", "error")
        return redirect(url_for("admin"))

    users = _load_users()
    if username in users:
        flash(f"User '{username}' already exists.", "error")
        return redirect(url_for("admin"))

    salt = os.urandom(16).hex()
    users[username] = {
        "hash": _hash_pw(password, salt),
        "salt": salt,
        "display_name": display_name or username,
        "is_admin": is_admin,
        "created": datetime.now().isoformat(),
    }
    _save_users(users)
    flash(f"User '{display_name or username}' created.", "success")
    return redirect(url_for("admin"))


@app.route("/admin/delete-user", methods=["POST"])
@admin_required
def delete_user():
    username = request.form.get("username", "")
    if username == session["username"]:
        flash("You can't delete yourself.", "error")
        return redirect(url_for("admin"))

    users = _load_users()
    if username in users:
        name = users[username].get("display_name", username)
        del users[username]
        _save_users(users)
        flash(f"User '{name}' deleted.", "success")
    return redirect(url_for("admin"))


@app.route("/admin/reset-password", methods=["POST"])
@admin_required
def reset_password():
    username = request.form.get("username", "")
    new_password = request.form.get("new_password", "")

    if len(new_password) < 4:
        flash("Password must be at least 4 characters.", "error")
        return redirect(url_for("admin"))

    users = _load_users()
    if username not in users:
        flash("User not found.", "error")
        return redirect(url_for("admin"))

    salt = os.urandom(16).hex()
    users[username]["hash"] = _hash_pw(new_password, salt)
    users[username]["salt"] = salt
    _save_users(users)
    flash(f"Password reset for '{users[username].get('display_name', username)}'.", "success")
    return redirect(url_for("admin"))


# ---------------------------------------------------------------------------
# Bootstrap admin if no users exist
# ---------------------------------------------------------------------------
def _ensure_admin():
    users = _load_users()
    if not users:
        salt = os.urandom(16).hex()
        users["admin"] = {
            "hash": _hash_pw("admin", salt),
            "salt": salt,
            "display_name": "Administrator",
            "is_admin": True,
            "created": datetime.now().isoformat(),
        }
        _save_users(users)
        print(">>> Default admin account created (username: admin, password: admin)")
        print(">>> Change the password immediately via the admin panel!")


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    _ensure_admin()
    app.run(host="0.0.0.0", port=5055, debug=False)
