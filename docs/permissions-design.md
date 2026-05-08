# IQMS Chat — SSO Identity & Role-Based Permissions Design

Status: design only (no code changed)
Author: Permissions Architect agent
Last updated: 2026-05-08

## Goals

1. Replace the current "every SSO visitor logs in as `admin`" hack (`app.py` `/sso` route, ~line 463–513) with real per-user identity sourced from the NYCOA Portal.
2. Introduce role-based access control over which IQMS Oracle data domains a user can query.
3. Layer enforcement so jailbreaks of the model alone don't leak HR/payroll/finance data.

---

## 1. SSO flow (target state)

### 1a. Sequence (browser <-> portal <-> iqms-chat)

```
Browser              Portal (:4070)                       iqms-chat (:5055)
   |                    |                                       |
   | click IQMS Chat    |                                       |
   |-------tile-------->|                                       |
   |                    | GET /api/sso/iqms_chat?next=/         |
   |                    | (authenticate middleware: portal JWT) |
   |                    | lookupEmployee(email) -> directory    |
   |                    | check directory.access['iqms_chat']   |
   |                    | jwt.sign({email, full_name,           |
   |                    |   portal_role}, PORTAL_SSO_SECRET,    |
   |                    |   {iss:'nycoa-portal', aud:'iqms_chat',|
   |                    |    expiresIn: 300})                   |
   |                    | -> redirect_url ?ptoken=<jwt>&next=/  |
   |<--JSON redirect----|                                       |
   |--GET /sso?ptoken=<jwt>&next=/--------------------------->  |
   |                                                            | jwt.decode(ptoken,
   |                                                            |   PORTAL_SSO_SECRET,
   |                                                            |   iss='nycoa-portal',
   |                                                            |   aud='iqms_chat')
   |                                                            | upsert user row, set session
   |<------302 to /---------------------------------------------|
```

### 1b. JWT contract (portal already implements this)

Source of truth: `/home/hnester/portal/server/src/routes/sso.ts`.

- Algorithm: HS256, shared secret env `PORTAL_SSO_SECRET` (already wired on both ends — see iqms-chat `.env`).
- Issuer: `nycoa-portal`
- Audience: `iqms_chat` (per-module pin — a token minted for MOC will not validate here)
- TTL: 300 s (one-shot login token; the iqms-chat Flask session takes over after exchange)
- Payload claims: `{ email, full_name, portal_role }`. `portal_role` is one of `employee | manager | hr | admin` (from `/home/hnester/portal/packages/shared/src/constants.ts`). Standard JWT fields `iat`, `exp`, `iss`, `aud` are included by `jsonwebtoken`.
- No `sub` claim is currently set. We will treat `email` (lowercased) as the canonical user key. If we later want a stable opaque identifier we should ask the portal team to add `sub: directory.id` — out of scope here.

### 1c. iqms-chat verification + user mapping (replaces today's behavior)

Today (`app.py` `/sso`): JWT is verified correctly, then **everyone is logged in as the local `admin` account** (`session["username"] = "admin"`, line 503). That is the line we are removing.

Target behavior on a successful JWT decode:

1. Lowercase `claims['email']`. Reject if missing.
2. `db.upsert_user_from_sso(email, full_name, portal_role)`:
   - If a row exists, update `display_name`, `last_login`, set `active=1` (re-activate previously deactivated users only after admin review — see open question below — for v1 we just bump `last_login`, not `active`).
   - If no row, **auto-provision** a new user with default role `viewer` and `eplant_access = [1,2,3]` (all three plants — restricting plant access is a separate axis the admin can tune later). `active=1`.
3. If the row is `active=0`, redirect back to `/login` with a flash "Account disabled — contact admin."
4. Write Flask session: `username` = email, `display_name`, `role`, `is_admin = (role == 'admin')`, `eplant_access`, `chat_id`, `eplant_id` defaulted to first allowed plant.
5. Log `Portal SSO sign-in for <email> -> role=<role>`.

Recommended first-login policy: **auto-provision as `viewer` (active immediately)**. The chat is gated upstream by Portal directory `access['iqms_chat']`, so anyone reaching iqms-chat is already cleared by IT. `viewer` only sees production/inventory data — there is no leakage risk in giving them that automatically. Alternative ("admin must approve before any access") creates support tickets and helpdesk friction without a corresponding security gain. See open question Q1 for confirmation.

### 1d. Logout / token expiry

- iqms-chat session lifetime: keep the current Flask session default (browser session). No change.
- Portal token (`ptoken`) is single-use in practice — only the `/sso` route consumes it. After exchange, all auth is the Flask session cookie. If a user idles past the Flask session and hits a protected route, they are redirected to `/login`, where they should re-enter via the portal tile (no local password for SSO users).
- Logout (`/logout`) clears the Flask session as today; no portal-side logout call.

---

## 2. User record (post-SSO)

Move from `data/users.json` to a new `users` table in the existing SQLite DB (`data/conversations.db`, managed by `db.py`).

### 2a. Schema

```sql
CREATE TABLE IF NOT EXISTS users (
  username       TEXT PRIMARY KEY,         -- canonical = lowercased email for SSO users; "admin" for legacy
  email          TEXT UNIQUE,              -- nullable for legacy local accounts
  display_name   TEXT NOT NULL,
  role           TEXT NOT NULL CHECK(role IN ('viewer','operator','manager','executive','admin')),
  eplant_access  TEXT NOT NULL DEFAULT '[1,2,3]',  -- JSON array of allowed EPLANT_IDs
  active         INTEGER NOT NULL DEFAULT 1,
  portal_role    TEXT,                     -- mirror of JWT portal_role at last login
  pw_hash        TEXT,                     -- only populated for legacy local accounts
  pw_salt        TEXT,
  created_at     TEXT NOT NULL,
  last_login     TEXT
);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
```

### 2b. Coexistence with existing local `admin`

- Migration script (one-off, idempotent): read `data/users.json`, insert into `users` with `role='admin'`, `eplant_access='[1,2,3]'`, copy `hash`/`salt` into `pw_hash`/`pw_salt`. Keep `users.json` on disk as a backup until v1 has been live for a week.
- Login form (`/login`) keeps working for accounts with `pw_hash IS NOT NULL`. SSO-provisioned users have a null hash and **cannot** log in via the form (good — they have no password).
- This means Hayden's existing `admin` login keeps working through the rollout.

### 2c. Mapping portal_role -> initial iqms-chat role

Only used at first auto-provision; admins can override in the user list afterward.

| portal_role | initial iqms-chat role |
|---|---|
| `employee` | `viewer` |
| `manager`  | `viewer` (NOT `manager` — the iqms-chat `manager` role grants finance read; many portal "managers" are line leads who don't need that. Promote manually.) |
| `hr`       | `viewer` (HR users opt into payroll role only after admin assignment) |
| `admin`    | `admin` (portal admins flow straight through) |

---

## 3. Role definitions

Five roles. All roles can chat; the differences are in (a) which IQMS modules/tables they can ask about, and (b) admin capabilities.

Tables called out below come from `~/.claude/agent-memory/erp-database/module-reference.md` and confirmed cross-references in `foreign-keys.md`. Wildcards are SQL-style; matching is case-insensitive on table name.

### 3.1 `viewer` (default)

- **Modules**: Manufacturing, basic Quality, Inventory.
- **Allowed tables** (allowlist mode is too restrictive — we use a denylist):
- **Denied table prefixes / exact names**:
  - Payroll/T&A: `PR_EMP`, `PR_PAYTYPE`, `PR_DEDUCTION`, `PR_TAX`, `DAY_EMP`, `DAY_HRS`, `DAY_LABOR_PROJECT`, any `PR_*`, `DAY_*EMP*`
  - Finance/GL: `GLACCT`, `GLPERIODS`, `FRL_ACCT_*`, `ACCRUED_FREIGHT*`, `ARINVOICE*`
  - AP/Vendor financials: `VENDOR` (full row deny — vendor names leak pricing context), `PO` (PO pricing), `ARCUSTO` (customer terms), `SHIP_TO`, `BILL_TO`
  - Pricing: `AKA`, `AKA_BREAKS`, `STANDARD` (cost columns — the table is needed for BOM lookups; soft-deny via prompt only — see Layer A below)
  - HR/Security: `S_USER_GENERAL`, `S_USERS`, `S_ROLE`, `EMP_*`
  - System Admin: `EDI_*`, `IQALERT*`, anything `S_SYS*`
- **ePlants**: all by default (per-user override allowed).
- **Reports**: yes (markdown reports of allowed data).
- **Admin caps**: none.

### 3.2 `operator`

- Everything `viewer` has, **plus**:
  - Sales orders header/lines: `ORDERS`, `ORD_DETAIL`, `RELEASES`, `SHIPMENT_DTL` (qty, dates — but not pricing columns; soft-restricted via prompt).
  - Extended quality: `CAR_HDR`, `CAR_DTL`, `SPC_*`, `QINVT`, `DHR`, `DHR_DTL`, `APQP*`, `ECO`.
  - Customer master read **without financial fields**: `ARCUSTO` is allowed but the prompt instructs Claude to never SELECT `CREDIT_LIMIT`, `TERMS_*`, `BALANCE_*`, `YTD_*` columns.
- Still denied: payroll, GL, AP, vendor pricing, system admin.

### 3.3 `manager`

- Everything `operator` has, **plus**:
  - Costing summaries: `STANDARD` cost columns, costing analysis from `ARINVT`.
  - GL read for management reporting: `GLACCT`, `GLPERIODS`, `FRL_ACCT_*`.
  - AP summaries: `VENDOR`, `PO`, `ACCRUED_FREIGHT*`, `ARINVOICE*`.
  - Pricing: `AKA`, `AKA_BREAKS`, full `ARCUSTO` (terms, credit).
- Still denied: payroll/T&A (`PR_*`, `DAY_EMP`, `DAY_HRS`, `DAY_LABOR_PROJECT`), HR (`S_USER*`, `EMP_*`), system admin (`IQALERT*`, `EDI_*`).

### 3.4 `executive`

- Everything `manager` has, **plus**: payroll/T&A and HR read (`PR_*`, `DAY_EMP`, `DAY_HRS`, `DAY_LABOR_PROJECT`, `EMP_*`).
- Denied only: system admin (`IQALERT*`, `EDI_*`, `S_USER_GENERAL`, `S_ROLE`).
- Reports: yes.
- Admin caps: none in iqms-chat (read-only of IQMS, not iqms-chat user mgmt).

### 3.5 `admin`

- Full IQMS read (no IQMS denylist).
- iqms-chat user management: list users, change role, change ePlant access, deactivate, reactivate.
- Logs view: full.

### 3.6 Role -> denylist as data

Denylists are static config — store in `app/permissions.py` as a Python dict keyed by role:
```
ROLE_DENYLIST = {
  'viewer':    ['PR_*','DAY_EMP','DAY_HRS','DAY_LABOR_PROJECT','GLACCT','GL*','FRL_ACCT_*','ACCRUED_FREIGHT*','ARINVOICE*','VENDOR','PO','ARCUSTO','SHIP_TO','BILL_TO','AKA','AKA_BREAKS','S_USER*','S_USERS','S_ROLE','EMP_*','EDI_*','IQALERT*','S_SYS*'],
  'operator':  ['PR_*','DAY_EMP','DAY_HRS','DAY_LABOR_PROJECT','GLACCT','GL*','FRL_ACCT_*','ACCRUED_FREIGHT*','ARINVOICE*','VENDOR','PO','AKA','AKA_BREAKS','S_USER*','S_USERS','S_ROLE','EMP_*','EDI_*','IQALERT*','S_SYS*'],
  'manager':   ['PR_*','DAY_EMP','DAY_HRS','DAY_LABOR_PROJECT','S_USER*','S_USERS','S_ROLE','EMP_*','EDI_*','IQALERT*','S_SYS*'],
  'executive': ['EDI_*','IQALERT*','S_USER_GENERAL','S_ROLE'],
  'admin':     [],
}
```
A small unit test enforces "PR_* denied for everyone except executive and admin" so future edits don't regress.

---

## 4. Enforcement strategy

Three possible layers; we recommend **A immediately, B in Phase 3, C parked as future hardening.**

### Layer A — System prompt denylist (recommended for v1)

Inject the user's denylist into the system prompt sent to `claude -p` (`app.py` lines 789–801). Add a new section to `SYSTEM_PROMPT_TEMPLATE`:

```
ACCESS CONTROL — STRICT:
The current user is "<email>" with role "<role>". You MUST NOT query or describe data
from these table prefixes/names: <comma-separated denylist>.
If the user asks a question that would require those tables, refuse with:
"That data isn't available to your role. Contact <admin contact> if you need access."
Do not list the denied tables back to the user; just refuse the specific request.
```

- Pros: trivial to ship, no MCP changes, fully auditable in logs.
- Cons: prompt-injection / jailbreak can in principle be talked past. Acceptable for `viewer`/`operator` since the data behind the denylist (payroll, GL) carries higher-tier business risk; B closes that gap.

### Layer B — MCP-level filtering (recommended for Phase 3)

Modify the iqms-oracle MCP (`/home/hnester/iqms-plugin/plugins/iqms-team-tools/mcp-servers/iqms-oracle/index.js`) to honor an `IQMS_TABLE_DENYLIST` env var (comma-separated patterns):

- `list-tables` filters out matching tables.
- `describe-table`, `sample-data`, `row-count`, `table-indexes`, `table-relationships`, `search-columns` return "table not accessible to your role" for matches.
- `query` parses the SQL and rejects if any matched table name appears as a token (simple regex per pattern; SQL parsing libraries are overkill for SELECT-only). Failure mode is reject — false positives are tolerable.

iqms-chat passes the denylist by **regenerating the MCP config per request** instead of using the two static configs (`MCP_CONFIG_IQMS`, `MCP_CONFIG_ALL`). New helper `build_mcp_config(role, eplant_id)` writes a tmp JSON in `data/tmp/mcp_<chatid>_<turn>.json` that injects the denylist into `IQMS_MCP['env']['IQMS_TABLE_DENYLIST']`. Pass that path to `--mcp-config`. Clean up the tmp file after `subprocess.run` returns.

This is the security floor we want under Layer A's UX.

### Layer C — Oracle VPD or per-role DB users (future)

- Create separate Oracle read-only users (`mcp_viewer`, `mcp_operator`, `mcp_manager`, `mcp_exec`, `mcp_admin`), each with `GRANT SELECT` only on permitted tables. Switch the MCP env `IQMS_DB_USER` per role.
- VPD policies on `IQMS.PR_*` etc. are even tighter but require IQMS DBA buy-in (changes to a vendor schema are sensitive).
- Park this until Layers A+B have been live and we have data on the actual query patterns.

---

## 5. Admin UI changes

Add to `templates/admin.html` (replacing the current users-list block):

- **User list table**: email, display_name, role (dropdown: viewer/operator/manager/executive/admin), ePlant access (multi-select 1/2/3), active toggle, last_login, "Save" button per row.
- **New user**: not needed — SSO auto-provisions. Keep the existing "create local user" form behind an "Advanced" toggle for emergency local accounts.
- **Search/filter**: by email substring + role. Useful as user count grows past ~20.

Backend routes (Flask, `app.py`):
- `GET /admin/users` — JSON list.
- `POST /admin/users/<email>/role` — change role; admin-only.
- `POST /admin/users/<email>/eplants` — change ePlant access; admin-only.
- `POST /admin/users/<email>/active` — toggle active; admin-only.

Logs page (`templates/logs.html`): no schema change required, but add a "User" filter column and ensure every query logs `username=<email>` (already done in `app.py` line 824 — just confirm post-SSO it's the email, not "admin").

---

## 6. Migration plan

| Phase | Scope | Files touched |
|---|---|---|
| **A** | Add `users` table to `db.py`. Migrate `users.json` -> table. Replace `/sso` handler to upsert per-user. Add role/eplant_access to session. Keep local `admin` login working. | `db.py`, `app.py` (/sso, /login, /logout, login_required) |
| **B** | Layer A enforcement: inject denylist into system prompt. Add `permissions.py` with `ROLE_DENYLIST`. Update `SYSTEM_PROMPT_TEMPLATE`. Per-request MCP config rebuild scaffolded but not yet adding role env. | `app.py`, new `permissions.py`, prompt template |
| **C** | Admin UI: user list table, role/eplant editor, active toggle. Routes for admin user mgmt. | `templates/admin.html`, `app.py` admin routes |
| **D** | Layer B enforcement: MCP-side denylist. Per-request MCP config now writes denylist env. Add `IQMS_TABLE_DENYLIST` parsing in `index.js`. | `iqms-plugin/plugins/iqms-team-tools/mcp-servers/iqms-oracle/index.js` (separate repo!), `app.py` MCP config builder |
| **E** | Audit log: dedicated `query_log` table — user, timestamp, prompt, denied? (bool), elapsed, mcp_config_path. View on logs page. | `db.py`, `app.py`, `templates/logs.html` |

Phases A-C ship first as the user-visible feature. D requires a parallel PR to the iqms-plugin repo and a coordinated deploy. E is gravy but cheap.

---

## 7. Open questions for Hayden

1. **Q1 — First-login policy**: Should new SSO users be auto-active at `viewer` role, or sit in a "pending" state until an admin promotes them? (Recommend auto-active viewer; portal directory already gates iqms-chat access upstream.)
2. **Q2 — Manager mapping**: Should `portal_role='manager'` auto-map to iqms-chat `manager` (gets finance/costing) or default to `viewer` and require explicit promotion? (Recommend default to `viewer` — portal "manager" is too broad.)
3. **Q3 — `ARCUSTO` and credit/financials**: For `operator` role, do we soft-restrict (prompt-level) credit/terms columns or fully deny the `ARCUSTO` table? (Recommend soft-restrict — operators legitimately need customer name/contact, just not credit.)
4. **Q4 — `STANDARD` table cost columns**: `STANDARD` is core for BOMs (operator needs it) but holds costs (manager+ only). Same soft-restrict approach via prompt, or split into two roles? (Recommend soft-restrict, log queries that mention cost columns for spot audit.)
5. **Q5 — Disable vs delete**: When a portal user is offboarded, do we keep their iqms-chat history (deactivate row) or scrub it? (Recommend deactivate + retain — chat history may be relevant evidence.)
6. **Q6 — Local `admin` long-term**: Once SSO is live, do we keep the password-based local `admin` as a break-glass account or remove it? (Recommend keep — useful when portal is down.)

---

## 8. Phase 2 implementation breakdown (parallel agents)

Three agents, non-overlapping file scopes. All three can run in parallel after Phase A merges (Phase A is single-agent because it changes the auth contract).

### Agent 1 — "Auth & User Store" (Phase A)
- **Owns**: `db.py` (add `users` table + helpers), `app.py` `/sso`, `/login`, `/logout`, `login_required`, session shape.
- **Deliverables**:
  - `db.upsert_user_from_sso(email, full_name, portal_role) -> dict`
  - `db.get_user(username) -> dict | None`
  - `db.list_users() -> list[dict]`
  - `db.set_user_role(username, role)`, `db.set_user_eplants(username, eplants)`, `db.set_user_active(username, active)`
  - One-shot `scripts/migrate_users_json.py`
  - `/sso` rewritten to upsert per-user; remove the hardcoded admin mapping.
  - Tests in `tests/test_users.py`.

### Agent 2 — "Permissions Enforcement" (Phases B + D scaffolding)
- **Owns**: new `permissions.py`, the system prompt template constant in `app.py`, the MCP config builder.
- **Deliverables**:
  - `permissions.py` exporting `ROLE_DENYLIST`, `denylist_for(role) -> list[str]`, `format_denylist_prompt(role, email) -> str`.
  - Modify `SYSTEM_PROMPT_TEMPLATE` to include `{access_control}` and pass it from the chat handler.
  - Refactor MCP config write from module-load static files to `build_mcp_config(role, is_nycoa) -> Path`; tmp files in `data/tmp/`.
  - Tests in `tests/test_permissions.py`.
- **Depends on**: Agent 1 having added `role` to `session`.

### Agent 3 — "Admin UI" (Phase C)
- **Owns**: `templates/admin.html`, admin-only Flask routes in `app.py` (`/admin/users` and the three POST routes).
- **Deliverables**:
  - User list table with role dropdown, eplant multi-select, active toggle, save button.
  - Search box (client-side filter).
  - Three new POST routes wired to Agent 1's `db` helpers.
  - Tests in `tests/test_admin_users.py` (HTTP-level, using Flask test client).
- **Depends on**: Agent 1's `db` helpers, but does NOT touch `db.py` itself.

File-scope matrix (no overlap):

| File | Agent 1 | Agent 2 | Agent 3 |
|---|---|---|---|
| `db.py` | edit | - | - |
| `app.py` /sso, /login, /logout, login_required, session | edit | - | - |
| `app.py` SYSTEM_PROMPT_TEMPLATE + chat handler prompt assembly + MCP config | - | edit | - |
| `app.py` /admin/users routes | - | - | edit |
| new `permissions.py` | - | create | - |
| `templates/admin.html` | - | - | edit |
| `scripts/migrate_users_json.py` | create | - | - |
| `tests/` | test_users.py | test_permissions.py | test_admin_users.py |

The MCP-side change (Layer B, in the iqms-plugin repo) is its own separate PR/agent post-Phase 2 — we are not touching that repo in this round.
