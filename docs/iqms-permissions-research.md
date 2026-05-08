# IQMS Authorization Model — Research for iqms-chat Phase 2

> Source: live IQMS Oracle DB (10.224.100.118:1521/IQMS) via `mcp_readonly`
> Research date: 2026-05-08
> Purpose: catalog the IQMS authorization model so iqms-chat can mirror each user's
> ERP permissions in the chat UI / system prompt / MCP allowlist.

## TL;DR

**The IQMS authorization model is hybrid: role-based with a group layer on top.**
A user is assigned to one or more **groups** (`S_GROUP`), each group is granted a
bundle of **roles** (`S_GROUP_ROLES`), and roles can also be granted **directly**
on the user row (`S_USERS.ROLE_NAME`). Roles are Oracle-database roles (`IQ_<MODULE>_<LEVEL>`)
that grant SELECT/INSERT/UPDATE/DELETE on the underlying tables, plus a UI-level
overlay (`S_MASTER`/`S_DETAIL`) that controls which screens/buttons render.

## 1. User identity & email

There is **no separate "login table" vs "employee table"**. Two tables matter:

| Table | Rows | What it holds |
|---|---|---|
| `IQMS.S_USERS` | 745 | Multi-row user-to-role/group assignments. PK `ID`, `USER_NAME`. A user appears once per (role or group) assignment — KFITZPATRICK has 19 rows, HNESTER has 5. |
| `IQMS.S_USER_GENERAL` | 203 | One row per actual person. Has `USER_NAME` (PK), **`EMAIL`** (VARCHAR2(1000)), `EPLANT_ID` (home plant), `PR_EMP_ID` (FK to payroll employee), `PHONE_NUMBER`, etc. |

**Email coverage**: 174 of 203 user records have EMAIL populated (86%).
Real format: `aalegria@shawsheencc.com`, `abayne@nycoa.com`. Domains are
`nycoa.com` and `shawsheencc.com`.

**The join key is `S_USER_GENERAL.EMAIL` -> `S_USER_GENERAL.USER_NAME` -> all other tables.**

## 2. Role / group / permission tables

| Table | Rows | Purpose |
|---|---|---|
| `IQMS.S_ROLES` | 292 | Catalog of role names. Pattern `IQ<MODULE>_<LEVEL>` — e.g. `IQGL_RW`, `IQINVENTORY_RO`, `IQAP_INVOICE_RO`, `IQALL`, `DENY_ALL`. Custom roles also exist: `NYCOA_SMARTPAGE_BI_RW`, `SHAW_SMARTPAGE`, `NBS_RECEIVING_RW`. |
| `IQMS.S_GROUP` | 36 | Departmental groups. IDs 12-52. Examples: 12 Admin, 13 Prod Control, 15 Quality Control, 19 Financial, 23 Full Read Only Access, 28 Read Only Access Less Finance, 32 NBS Sales, 49 NBS Executive RO, 52 NBS Quality Manager. |
| `IQMS.S_GROUP_ROLES` | 785 | Group-to-role bundling. `S_GROUP_ID` -> `GRANTED_ROLE_NAME`. This is the "role pack" each group gets. Group 12 (Admin) -> IQALL, IQALL_REPORTS, IQCHNG_PASSW_RW. Group 16 (Shipping/Receiving) -> 13 roles. Groups can also reference *other groups* via `GRANTED_S_GROUP_ID` (nested). |
| `IQMS.S_USERS` | 745 | The actual user assignments. A row carries either `ROLE_NAME` (direct role grant), or `S_GROUP_ID` (group membership), or both. KFITZPATRICK has 17 direct roles + 2 group memberships -> 92 distinct effective roles. |
| `IQMS.S_USER_EPLANTS` | 221 | Plant-scoping. Per-user list of allowed `EPLANT_ID`s. HNESTER has 1,2,3 (all plants); RPOLCE has only 2 (NYCOA). |
| `IQMS.S_ITEM` | 163 | Catalog of UI components (`APP_S_CODE` + `C_NAME` like `ADDDOWNTIME`/`EDOWNREASON`). |
| `IQMS.S_MASTER` | 9,033 | Role-to-screen map: `(APP_S_CODE, ROLE_NAME)`. |
| `IQMS.S_DETAIL` | 175,061 | UI-level granular permissions per (`APP_S_CODE`, `ROLE_NAME`, `C_NAME`) with Y/N flags `C_ENABLED`, `C_INSERT`, `C_DELETE`, `C_RD_WR`, `C_VISIBLE`. This is the field-level layer. |

**Granularity**: four layers stacked.
1. **DB-level** — Oracle role grants SELECT/etc on tables (`IQGL_RW` -> `GLACCT`).
2. **Module-level** — role naming `IQ<MODULE>_<LEVEL>` controls Launcher Bar buttons.
3. **Screen-level** — `S_MASTER` says "this role can see this screen".
4. **Field-level** — `S_DETAIL` per-button/field Y/N flags.

Standard role-name prefixes (top 20 modules): IQTIME&ATTEN (21 roles), IQPROLL (12), IQBOM (7), IQRT (6), IQVENDOR (6), IQPO (5), IQFIN (4), IQPROD (4), IQCASH (4), IQINVENTORY (4), IQCUST (4), IQAP (3), IQGL (3), IQEXPENSE (3), IQORDERS (3), NBS (6 NYCOA-custom).

## 3. The join — email -> effective IQMS permissions

Run this in sqlplus, replacing the bind:

```sql
-- Effective roles for a given email address
SELECT g.USER_NAME, g.EMAIL,
       LISTAGG(DISTINCT se.EPLANT_ID, ',') WITHIN GROUP (ORDER BY se.EPLANT_ID) AS ALLOWED_PLANTS,
       eff.SOURCE, eff.GRANTED_ROLE
FROM IQMS.S_USER_GENERAL g
LEFT JOIN IQMS.S_USER_EPLANTS se ON se.USER_NAME = g.USER_NAME
LEFT JOIN (
  -- (1) directly-granted roles on S_USERS
  SELECT su.USER_NAME, 'DIRECT' AS SOURCE, su.ROLE_NAME AS GRANTED_ROLE
  FROM IQMS.S_USERS su WHERE su.ROLE_NAME IS NOT NULL
  UNION
  -- (2) roles inherited via group membership
  SELECT su.USER_NAME, 'GROUP:' || sg.GROUP_NAME AS SOURCE, sgr.GRANTED_ROLE_NAME
  FROM IQMS.S_USERS su
  JOIN IQMS.S_GROUP sg ON sg.ID = su.S_GROUP_ID
  JOIN IQMS.S_GROUP_ROLES sgr ON sgr.S_GROUP_ID = sg.ID
  WHERE sgr.GRANTED_ROLE_NAME IS NOT NULL
) eff ON eff.USER_NAME = g.USER_NAME
WHERE LOWER(g.EMAIL) = LOWER(:p_email)
GROUP BY g.USER_NAME, g.EMAIL, eff.SOURCE, eff.GRANTED_ROLE
ORDER BY eff.SOURCE, eff.GRANTED_ROLE;
```

Verified live: `hnester@nycoa.com` -> 7 effective roles (4 direct + 3 from
group "Admin": IQALL, IQALL_REPORTS, IQCHNG_PASSW_RW). `kfitzpatrick@shawsheencc.com` -> 92.

For the **flattened table-level read list**, you would normally inner-join roles to
`DBA_TAB_PRIVS` (or to `S_MASTER`/`S_DETAIL` for screen-level). `mcp_readonly` is
**not granted** `DBA_TAB_PRIVS` / `DBA_ROLE_PRIVS` — only `ALL_*` views, which only
show grants the connected role can already see. To get the *true* role -> table map we
need either (a) a one-time DBA-run dump, or (b) ask DBA to grant `SELECT` on
`DBA_TAB_PRIVS` and `DBA_ROLE_PRIVS` to `mcp_readonly`. Without that, we have to rely
on role-name conventions (e.g. `IQGL_*` -> GL tables) and `S_MASTER`/`S_DETAIL`
for screen-level mapping.

## 4. Sensitive-table grant audit

Tables `PR_EMP`, `GLACCT`, `ARCUSTO` all exist in the IQMS schema. **However, with
the `mcp_readonly` privileges, `ALL_TAB_PRIVS` returns zero rows for them** — the
view filters to grants visible through our own role chain. The IQMS docs
(`/home/hnester/iqms-docs/IQMS/System Admin/Security Roles.md`) confirm by convention:

- `PR_EMP` (HR/payroll wages): granted by `IQ_PR_*` roles. Excluded from the
  Crystal Report Writer role explicitly. `IQPROLL_*` and `IQ_HR_*` family.
- `GLACCT` (general ledger): granted by `IQ_GL_*` roles (`IQGL_RW`, `IQGL_RO`,
  `IQFIN_*`). Also excluded from CRW role.
- `ARCUSTO` (customer master with credit terms): granted by `IQ_AR_*`,
  `IQ_SO_*`, `IQ_CUST_*` roles. Visible to most user-facing roles since Sales
  Orders need it; credit-term columns specifically may be field-locked via
  `S_DETAIL`.

**Action item for full audit**: have DBA run, on a privileged session,
`SELECT GRANTEE FROM DBA_TAB_PRIVS WHERE OWNER='IQMS' AND TABLE_NAME IN ('PR_EMP','GLACCT','ARCUSTO') AND PRIVILEGE='SELECT';`
and paste results here.

## 5. Recommendation for Phase 2

**Sync nightly into a local table, refreshed on login if stale.** The role
catalog is small (292 roles, 36 groups, ~200 active users with email — well
under 10K total relations). On user login to iqms-chat, look up `email` in a
local `iqms_user_permissions` table; if last_synced is older than 24h, re-pull
from Oracle using the query in section 3. **The "permission packet" we hand to
the system-prompt builder and MCP allowlist should be a structured object with
three lists**: (a) `eplant_ids` from `S_USER_EPLANTS` (drives the existing
ePlant selector), (b) `role_names` — the flat union of direct + group-inherited
roles, (c) a derived `module_prefixes` list (extract `IQ<MODULE>` prefix from
each role and dedupe) which the MCP allowlist can use as table-prefix gates
(e.g. role `IQGL_RW` -> module `GL` -> allow tables matching `GLACCT`,
`GLBATCH`, `GLYEAR%`). **Drop the architect's 5-role design (viewer/operator/manager/executive/admin) entirely** — IQMS already has the canonical roles; reinventing a 5-bucket
abstraction will misclassify hybrid users like KFITZPATRICK (sales + read-only-less-finance, 92 roles across ~40 modules). Instead, derive a coarse "tier" for chat-UI cosmetics only — e.g. presence of `IQALL` -> Admin, presence of `IQ*_RW` -> Operator, only `IQ*_RO` -> Viewer — but pass the **raw IQMS role list** to the MCP allowlist and system prompt for actual gating.
