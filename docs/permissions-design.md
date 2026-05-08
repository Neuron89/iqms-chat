# IQMS Chat — SSO Identity & IQMS-Driven Permissions Design

Status: implemented (Phase 2)
Authors: Permissions Architect agent (v1, design-only) → iqms-chat Phase 2 Engineer (v2, this version)
Last updated: 2026-05-08

> **What changed since v1?** The original design proposed a five-bucket role model
> (viewer / operator / manager / executive / admin) with hand-curated denylists. Live
> research against IQMS (`docs/iqms-permissions-research.md`) showed that's a step
> backwards: IQMS already has 292 canonical roles, and any 5-bucket abstraction
> misclassifies hybrid users like KFITZPATRICK (sales + read-only-less-finance,
> 92 roles across ~40 modules). v2 of this design uses **IQMS roles directly**.

## 0. TL;DR — what got built

- Portal SSO continues to mint a JWT (`/sso?ptoken=…`); we verify it.
- On verify, look up the user's IQMS roles via `iqms_lookup.py` (Oracle SELECT
  on `S_USER_GENERAL ⨝ S_USERS / S_GROUP_ROLES / S_USER_EPLANTS` — see
  `docs/iqms-permissions-research.md` §3 for the query).
- Persist the user (`users` table) and the resolved permission packet
  (`iqms_permissions` table) into SQLite.
- Inject a per-user "access control" block into the user prompt at `/ask` time
  (Layer A — system-prompt enforcement; system prompt itself stays cacheable).
- Emit the user's `allowed_table_prefixes` as `IQMS_ALLOWED_TABLE_PREFIXES`
  (env, comma-separated) plus `allowedTablePrefixes` (config, JSON array)
  in a per-request MCP config — Agent B reads this for Layer B enforcement.
- Hardcoded super-admin override for `hnester@nycoa.com` so Hayden never gets
  locked out by Oracle outages or schema drift.

---

## 1. SSO flow (no change from v1)

The browser/portal/iqms-chat handshake is unchanged from v1 §1a. The portal
mints HS256 JWTs via `/home/hnester/portal/server/src/routes/sso.ts`,
`iss=nycoa-portal`, `aud=iqms_chat`, 5-minute TTL. JWT shape:
`{ email, full_name, portal_role, iat, exp, iss, aud }`.

What changed is what `/sso` does after a successful decode:

```python
email = claims['email'].lower()
is_super = email in SUPER_ADMIN_EMAILS  # {'hnester@nycoa.com'}

packet = iqms_lookup.fetch_permissions_for_email(email)

if is_super:
    packet = _super_admin_packet(email, packet)   # forces module_prefixes=['*']
elif not packet:
    packet = _empty_permission_packet(email)      # no IQMS access — viewer tier

db.upsert_user(email, email, full_name, is_super_admin=is_super)
db.upsert_iqms_permissions(email, packet)

# session cache (refreshed on every login)
session['username'] = email
session['display_name'] = full_name
session['is_admin'] = is_super or 'IQALL' in packet['role_names']
session['eplant_id'] = str(packet['eplant_ids'][0]) if packet['eplant_ids'] else '2'
session['eplant_access'] = packet['eplant_ids']
session['permission_packet'] = packet
```

If `users.active == 0`, redirect to `/login` with a flash. If the user is
new, the row is auto-active (per Hayden's first-login decision — no pending
state).

The legacy local-account login (`/login` form) keeps working for the bootstrap
`admin` account. SSO users have `pw_hash IS NULL` and cannot use the form.

---

## 2. Permission packet — single source of truth at runtime

Every `/ask` reads `session['permission_packet']`, which is a dict with this
shape:

```python
{
  'iqms_user_name': 'HNESTER',                       # str | None
  'email': 'hnester@nycoa.com',
  'role_names': ['IQALL', 'IQALL_REPORTS', ...],     # raw IQMS role list
  'module_prefixes': ['*'] or ['GL', 'AP', ...],     # derived; ['*'] = super-admin
  'allowed_table_prefixes': ['%'] or ['GL%', ...],   # SQL LIKE patterns
  'eplant_ids': [1, 2, 3],
  'tier': 'admin' | 'operator' | 'viewer',           # UI cosmetic only
  'synced_at': '2026-05-08T12:34:56Z',
}
```

Persisted in `iqms_permissions` (PRIMARY KEY username FK→users). The full
`raw_packet` is stored as JSON for forensics — admins can view it on the
detail page (`/admin/users/<u>`).

### 2.1 How the packet gets derived

`iqms_lookup.fetch_permissions_for_email(email)`:

1. Run the SQL query from `iqms-permissions-research.md` §3 → list of
   `(USER_NAME, EMAIL, EPLANT_ID, SOURCE, GRANTED_ROLE)` rows.
2. Dedupe role names (`role_names`).
3. Dedupe ePlant IDs (`eplant_ids`).
4. Derive `module_prefixes` per role: regex `^IQ([A-Z0-9_]+?)(_RO|_RW|$)` →
   capture group 1. NYCOA / NBS / SHAW custom roles preserve their full
   prefix (e.g. `NYCOA_SMARTPAGE_BI_RW` → `NYCOA_SMARTPAGE_BI`).
5. Map module prefixes → table-prefix LIKE patterns via the
   `MODULE_TABLE_PREFIXES` table (in `iqms_lookup.py`). Always allow
   `S_%` and `EPLANT%` so users can resolve their own context.
   `IQALL` → `["%"]` short-circuits (full read).
6. Derive `tier`: `IQALL` → admin; any `*_RW` → operator; only `*_RO` →
   viewer. (Tier is **cosmetic only** — it drives the UI badge and that's
   it. All actual gating uses `module_prefixes` and `allowed_table_prefixes`.)

If Oracle is unreachable or the query times out, `fetch_permissions_for_email`
returns `None`. The caller (`/sso`) decides what to do:

- Super-admin → synthesize a full-access packet anyway (Hayden never locked out).
- Non-super-admin → "no IQMS access" packet (empty arrays, viewer tier).
  The user can still chat about non-IQMS topics; the system prompt will block
  every DB query.

---

## 3. We use IQMS roles directly — no five-bucket abstraction

The original architect proposed five iqms-chat roles: `viewer`, `operator`,
`manager`, `executive`, `admin`, with hand-curated table denylists per role.

**This design is dropped.** The detailed reasoning is in
`docs/iqms-permissions-research.md` §5. The short version:

- IQMS has 292 canonical roles already. Reinventing 5 buckets means
  hand-mapping every IQMS role into a bucket — and roles like
  `IQVENDOR_RMA_RW`, `NYCOA_SMARTPAGE_BI_RW`, `IQALL_REPORTS` don't fit
  cleanly anywhere.
- Hybrid users (e.g. KFITZPATRICK with 92 effective roles) get misclassified
  no matter which bucket you pick.
- The denylist needs to be maintained by someone every time IQMS adds a new
  role. That someone doesn't exist.
- IQMS roles already encode "what tables this user can read" via the role's
  module prefix. We just expose that.

**Tier (admin / operator / viewer)** survives, but only as a UI badge so the
admin user list shows something at-a-glance. Real gating is by raw role
list + derived `allowed_table_prefixes`.

---

## 4. Enforcement — Layer A (this code) + Layer B (Agent B)

### 4.1 Layer A — system-prompt enforcement (this PR)

In `app.py` `/ask`, we inject an "access control" block into the user prompt
(not the system prompt — keeping the system prompt user-agnostic + cacheable
matters because we pay for prompt caching). The block looks like:

```
=== Access control ===
You are answering on behalf of: <name> (<email>)
Tier: <tier>
Allowed IQMS module prefixes: <comma-separated module list>
Allowed table name patterns: <comma-separated SQL LIKE patterns>

You MUST refuse any query that would read from a table not matching one of
the allowed patterns above. Specifically, the following modules are FORBIDDEN
for this user unless explicitly listed above:
  Payroll / Time & Attendance (PR_*, DAY_*EMP*, DAY_HRS, DAY_LABOR_PROJECT);
  General Ledger (GL%, GLACCT, FRL_ACCT_*);
  Accounts Payable / Vendor financials (AP%, VEND%, PO%);
  Accounts Receivable / Customer credit (ARCUSTO, AR%, AKA%);
  HR (EMP_*, S_USER_GENERAL, S_USERS);
  System administration (EDI_*, IQALERT*, S_SYS*)

If the user asks for data you cannot provide, respond with:
"I'm sorry — your account does not have access to {module} data in IQMS.
 Contact your admin to request access."
Do not attempt the query.

The MCP allowlist (Layer B) will also block these queries server-side, so
even if you attempt them they will fail. Refuse cleanly and inform the user.
=== End access control ===
```

The block is **omitted entirely** when `module_prefixes == ['*']` (super-admin
/ IQALL). For users with no IQMS roles at all, the block reads "Tier: no IQMS
access" and instructs Claude to refuse every DB query.

### 4.2 Layer B — MCP allowlist (Agent B's code, this PR's contract)

`/ask` writes a per-request MCP config with the user's `allowed_table_prefixes`.
The iqms-oracle MCP server (in `/home/hnester/iqms-plugin/`) reads this and
enforces it at the tool level — `list-tables` filters, `query` rejects matching
tables, etc.

**Contract for Agent B**:
- `mcpServers["iqms-oracle"].env.IQMS_ALLOWED_TABLE_PREFIXES` — comma-separated
  string of SQL LIKE patterns. Whitespace around commas is tolerated.
- `mcpServers["iqms-oracle"].allowedTablePrefixes` — JSON array form of the
  same. Agent B may read either; if both are present, prefer the config-level
  array.
- When the user is a super-admin (`module_prefixes == ['*']`), neither key is
  emitted. The MCP defaults to no filtering.
- Empty list / missing key = no filtering (default). Empty list semantically
  means "no IQMS access at all" — but Layer A blocks the query before it ever
  reaches the MCP, and Layer B can choose to enforce empty-list as "deny all"
  for defense in depth.

---

## 5. SQLite schema additions (Phase 2)

Both tables are auto-created in `db.py` on first import (idempotent CREATE IF
NOT EXISTS).

```sql
CREATE TABLE users (
  username       TEXT PRIMARY KEY,         -- canonical = lowercased email; "admin" for legacy
  email          TEXT UNIQUE,
  display_name   TEXT NOT NULL,
  active         INTEGER NOT NULL DEFAULT 1,
  is_super_admin INTEGER NOT NULL DEFAULT 0,
  pw_hash        TEXT,                     -- only for legacy local accounts
  pw_salt        TEXT,
  created_at     TEXT NOT NULL,
  last_login     TEXT
);
CREATE INDEX idx_users_email ON users(email);

CREATE TABLE iqms_permissions (
  username                TEXT PRIMARY KEY REFERENCES users(username) ON DELETE CASCADE,
  iqms_user_name          TEXT,
  role_names              TEXT NOT NULL,    -- JSON array
  module_prefixes         TEXT NOT NULL,    -- JSON array
  allowed_table_prefixes  TEXT NOT NULL,    -- JSON array of LIKE patterns
  eplant_ids              TEXT NOT NULL,    -- JSON array
  tier                    TEXT NOT NULL CHECK(tier IN ('viewer','operator','admin')),
  synced_at               TEXT NOT NULL,
  raw_packet              TEXT              -- full JSON for forensics
);
```

Helpers in `db.py`: `upsert_user`, `get_user`, `set_user_active`,
`touch_last_login`, `list_users` (joined view), `upsert_iqms_permissions`,
`get_iqms_permissions`.

A one-shot `scripts/migrate_users_json.py` migrates `data/users.json` into
the `users` table (idempotent — skips usernames that already exist). Runs
automatically at app startup so the legacy admin row lands without manual
intervention.

---

## 6. Admin UI (Phase 2)

The admin page at `/admin` now renders **two cards**:

1. **SSO Users** (primary) — table of every user provisioned via portal SSO,
   with email, display name, tier badge, role count + role names tooltip,
   module prefixes, ePlant access, active status, last login. Buttons:
   - **View** → modal with full role list + raw packet JSON.
   - **Re-sync** → POSTs `/admin/users/<u>/refresh`, re-pulls IQMS, persists.
   - **Disable / Enable** → POSTs `/admin/users/<u>/active` with `{active: bool}`.
   - Client-side filter input over email/name/role-name.
2. **Local accounts (break-glass)** — the original users.json table. Add
   user form is hidden inside an "Advanced" `<details>` collapsible. Reset
   PW / Delete actions still work for the legacy admin.

New API routes:
- `GET /admin/users` — list (HTML or JSON via `?format=json` or `Accept: application/json`).
- `GET /admin/users/<username>` — JSON detail for one user (used by the modal).
- `POST /admin/users/<username>/active` — body `{active: bool}` → `{ok: true}`.
- `POST /admin/users/<username>/refresh` — re-pull and persist; returns the
  updated packet.

All routes are `@admin_required`.

---

## 7. Super-admin override

`SUPER_ADMIN_EMAILS = {"hnester@nycoa.com"}` (in `app.py`).

When `email in SUPER_ADMIN_EMAILS`:
- `is_super_admin = 1` on the `users` row.
- Permission packet is **always** synthesized to:
  `module_prefixes=['*']`, `allowed_table_prefixes=['%']`, `tier='admin'`,
  `eplant_ids=[1,2,3]`. Any IQMS roles found get appended (so the admin UI
  shows the real role list), but the gating fields are hard-coded.
- Layer A access-control block is **omitted** entirely.
- Layer B `IQMS_ALLOWED_TABLE_PREFIXES` is **omitted** (no filtering).

This is deliberate: even if Oracle is down, IQMS account is renamed, or
some future audit query returns 0 rows, Hayden gets full access.

---

## 8. Open / deferred

- **Q1 (first-login policy)**: ANSWERED — auto-active.
- **Q2 (manager mapping)**: OBSOLETE — no `manager` role anymore; we use
  IQMS roles.
- **Q3, Q4 (column-level restrictions on ARCUSTO / STANDARD)**: still open;
  Layer A prompt currently doesn't list specific column restrictions.
  Defer until we see actual queries that need this.
- **Q5 (offboarding)**: ANSWERED — deactivate, retain history.
- **Q6 (local admin long-term)**: ANSWERED — keep as break-glass.
- **Layer C (Oracle VPD / per-role DB users)**: still parked. Revisit after
  Layers A+B have been live for a quarter.
- **`DBA_TAB_PRIVS` audit**: research doc §4 still wants a DBA-run dump of
  exact role→table grants. Once available, the `MODULE_TABLE_PREFIXES`
  starter map in `iqms_lookup.py` should be replaced with the authoritative
  data. Not a Phase 2 blocker.

---

## 9. Files in this PR

- `db.py` — added `users` + `iqms_permissions` tables and helpers.
- `iqms_lookup.py` — new (Oracle query module, module/table-prefix derivation).
- `app.py` — `/sso` rewrite, `/login` reads SQLite first, Layer A access-control
  injection, per-request MCP config writer, new admin user-management routes.
- `templates/admin.html` — SSO Users card + detail modal + filter; legacy
  local accounts moved to a secondary card.
- `scripts/migrate_users_json.py` — one-shot legacy migration.
- `.env` — added `IQMS_DB_USER`, `IQMS_DB_PASSWORD`, `IQMS_DB_DSN`,
  `ORACLE_CLIENT_LIB_DIR` (same creds as the iqms-oracle MCP).
- `docs/permissions-design.md` — this file.
