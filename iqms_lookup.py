"""
IQMS Oracle permissions lookup.

Given an email address, query IQMS.S_USER_GENERAL → S_USERS / S_GROUP_ROLES /
S_USER_EPLANTS to materialise the user's effective IQMS authorization. The
result ("permission packet") is what /sso caches into iqms_permissions for
the session and what the system prompt + MCP allowlist read from.

The query is the one validated in docs/iqms-permissions-research.md §3.

Connection details come from environment variables (IQMS_DB_USER /
IQMS_DB_PASSWORD / IQMS_DB_DSN) so this module never carries the password.
A failed lookup (Oracle down, user not in IQMS, network timeout) returns
None — the caller decides what to do (the super-admin override and the
"no IQMS access" fallback both live in /sso).
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module → table-prefix mapping. Starter set; extend as we see new IQMS roles.
# Keys are the {MODULE} portion extracted from role names like IQ{MODULE}_RW.
# ---------------------------------------------------------------------------
MODULE_TABLE_PREFIXES: dict[str, list[str]] = {
    "ALL":           ["%"],
    "GL":            ["GL%"],
    "AP":            ["AP%"],
    "AR":            ["AR%", "ARCUSTO"],
    "CUST":          ["AR%", "ARCUSTO"],
    "SO":            ["SO%", "ORDER%", "RELEASES", "ORD_DETAIL"],
    "ORDERS":        ["ORDER%", "SO%", "RELEASES", "ORD_DETAIL"],
    "PROLL":         ["PR%", "EMP_%"],
    "PR":            ["PR%", "EMP_%"],
    "HR":            ["PR%", "EMP_%", "HR_%"],
    "TIMEATTEN":     ["DAY_%", "EMP_%", "PR%"],
    "TIME":          ["DAY_%", "EMP_%"],
    "INVENTORY":     ["INV%", "ITEM%", "ARINVT%"],
    "BOM":           ["BOM%", "STANDARD"],
    "PO":            ["PO%"],
    "VENDOR":        ["VEND%", "PO%"],
    "PROD":          ["PROD%", "DAYPROD%", "PDAYPROD%", "WO%", "WORKORDER%"],
    "RT":            ["RT%"],
    "CASH":          ["CASH%"],
    "EXPENSE":       ["EXP%"],
    "FIN":           ["GL%", "AP%", "AR%", "FIN%"],
    "QC":            ["QINVT%", "CAR%", "DHR%", "SPC%", "APQP%", "ECO%"],
    "QA":            ["QINVT%", "CAR%", "DHR%", "SPC%", "APQP%", "ECO%"],
    "QUALITY":       ["QINVT%", "CAR%", "DHR%", "SPC%", "APQP%", "ECO%"],
    # Customs / NYCOA-specific
    "NYCOA_SMARTPAGE_BI": ["%"],     # SmartPage BI users effectively need broad read
    "VENDOR_RMA":    ["VEND%", "PO%", "RMA%"],
    # IQ-prefixed system / admin roles — usually metadata-only, no extra grants
    "DENYASSIGNPLANT":  [],
    "DENYLOGINCHANGE":  [],
    "CHNG_PASSW":       [],
}

# Tables every authenticated IQMS user can read for self-context
ALWAYS_ALLOWED_PREFIXES: list[str] = ["S_%", "EPLANT%"]


def _parse_module_from_role(role: str) -> Optional[str]:
    """
    Extract the {MODULE} portion of a role like IQ{MODULE}_RW or IQ{MODULE}_RO.
    Returns the module name (e.g. 'GL') or None for non-IQ roles. We map a few
    NYCOA-custom prefixes (NYCOA_*, NBS_*, SHAW_*) to themselves so the table
    mapping can pick them up.
    """
    if not role:
        return None
    role = role.upper()
    if role == "IQALL":
        return "ALL"
    # NYCOA / NBS / SHAW custom roles — preserve the full role-name root
    m = re.match(r"^(NYCOA|NBS|SHAW)_([A-Z0-9_]+?)(_RO|_RW)?$", role)
    if m:
        # Use root + middle (e.g. NYCOA_SMARTPAGE_BI from NYCOA_SMARTPAGE_BI_RW)
        return f"{m.group(1)}_{m.group(2)}"
    # Standard IQ<MODULE>_<LEVEL> pattern
    m = re.match(r"^IQ([A-Z0-9_]+?)(_RO|_RW)?$", role)
    if m:
        return m.group(1)
    return None


def _derive_module_prefixes(role_names: list[str]) -> list[str]:
    """
    Given a list of role names, return the deduped list of module prefixes
    they imply. Returns ['ALL'] if any role is IQALL — caller can short-circuit.
    """
    seen: list[str] = []
    for role in role_names:
        mod = _parse_module_from_role(role)
        if mod and mod not in seen:
            seen.append(mod)
    return seen


def _derive_table_prefixes(module_prefixes: list[str]) -> list[str]:
    """
    Convert a list of module prefixes into a deduped list of SQL LIKE patterns
    that name the tables those modules read. ALL → ['%'] short-circuits.
    Always appends ALWAYS_ALLOWED_PREFIXES so users can resolve their own
    plant/security context.
    """
    if "ALL" in module_prefixes:
        return ["%"]
    out: list[str] = []
    for mod in module_prefixes:
        for pat in MODULE_TABLE_PREFIXES.get(mod, []):
            if pat and pat not in out:
                out.append(pat)
    for pat in ALWAYS_ALLOWED_PREFIXES:
        if pat not in out:
            out.append(pat)
    return out


def _derive_tier(role_names: list[str]) -> str:
    """
    UI-facing coarse tier. IQALL → admin; any *_RW → operator; only *_RO → viewer.
    """
    if not role_names:
        return "viewer"
    upper = [r.upper() for r in role_names]
    if any(r == "IQALL" or r == "SUPER_ADMIN" for r in upper):
        return "admin"
    if any(r.endswith("_RW") for r in upper):
        return "operator"
    return "viewer"


def _now_iso() -> str:
    """UTC ISO-8601 with trailing Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Oracle connection (lazy — the app should boot fine without DB access)
# ---------------------------------------------------------------------------

_oracle_initialized = False


def _ensure_oracle_client() -> None:
    """
    Initialize Oracle Instant Client once. Safe to call repeatedly. If the
    client libs are missing we let the import error bubble up so the caller
    can degrade.
    """
    global _oracle_initialized
    if _oracle_initialized:
        return
    import oracledb  # noqa: F401  — import only when needed
    lib_dir = os.environ.get(
        "ORACLE_CLIENT_LIB_DIR",
        "/usr/lib/oracle/19.28/client64/lib",
    )
    try:
        oracledb.init_oracle_client(lib_dir=lib_dir)
    except oracledb.ProgrammingError:
        # Already initialized in this process — fine.
        pass
    _oracle_initialized = True


def _connect():
    """Open a new Oracle connection using env credentials. Caller must close."""
    import oracledb
    _ensure_oracle_client()
    user = os.environ.get("IQMS_DB_USER", "mcp_readonly")
    password = os.environ.get("IQMS_DB_PASSWORD", "")
    dsn = os.environ.get("IQMS_DB_DSN", "10.224.100.59:1521/IQMS")
    if not password:
        raise RuntimeError(
            "IQMS_DB_PASSWORD not set; cannot connect to IQMS Oracle."
        )
    return oracledb.connect(user=user, password=password, dsn=dsn)


# ---------------------------------------------------------------------------
# The single SQL query that drives everything
# ---------------------------------------------------------------------------

_PERMISSIONS_SQL = """
SELECT g.USER_NAME, g.EMAIL, se.EPLANT_ID, eff.SOURCE, eff.GRANTED_ROLE
FROM IQMS.S_USER_GENERAL g
LEFT JOIN IQMS.S_USER_EPLANTS se ON se.USER_NAME = g.USER_NAME
LEFT JOIN (
  SELECT su.USER_NAME, 'DIRECT' AS SOURCE, su.ROLE_NAME AS GRANTED_ROLE
  FROM IQMS.S_USERS su WHERE su.ROLE_NAME IS NOT NULL
  UNION
  SELECT su.USER_NAME, 'GROUP:' || sg.GROUP_NAME AS SOURCE, sgr.GRANTED_ROLE_NAME
  FROM IQMS.S_USERS su
  JOIN IQMS.S_GROUP sg ON sg.ID = su.S_GROUP_ID
  JOIN IQMS.S_GROUP_ROLES sgr ON sgr.S_GROUP_ID = sg.ID
  WHERE sgr.GRANTED_ROLE_NAME IS NOT NULL
) eff ON eff.USER_NAME = g.USER_NAME
WHERE LOWER(g.EMAIL) = LOWER(:p_email)
"""


def fetch_permissions_for_email(email: str) -> Optional[dict]:
    """
    Look up the IQMS permission packet for a portal email.

    Returns:
        dict with keys {iqms_user_name, email, role_names, module_prefixes,
        allowed_table_prefixes, eplant_ids, tier, synced_at} on success,
        or None when the user is not in IQMS, Oracle is unreachable, or the
        oracledb driver is missing. Never raises — degrades to None and logs.
    """
    if not email:
        return None
    email = email.lower()

    try:
        conn = _connect()
    except Exception as exc:
        logger.warning("IQMS lookup: cannot connect to Oracle (%s)", exc)
        return None

    try:
        cur = conn.cursor()
        try:
            cur.execute(_PERMISSIONS_SQL, p_email=email)
            rows = cur.fetchall()
        finally:
            cur.close()
    except Exception as exc:
        logger.warning("IQMS lookup: query failed for %s (%s)", email, exc)
        try:
            conn.close()
        except Exception:
            pass
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not rows:
        # Email exists in portal but not in IQMS — caller decides fallback.
        logger.info("IQMS lookup: no IQMS user for %s", email)
        return None

    iqms_user_name: Optional[str] = None
    role_names: list[str] = []
    eplant_ids: list[int] = []

    for row in rows:
        # row = (USER_NAME, EMAIL, EPLANT_ID, SOURCE, GRANTED_ROLE)
        u, _e, ep, _src, role = row
        if iqms_user_name is None and u:
            iqms_user_name = u
        if ep is not None:
            try:
                ep_int = int(ep)
            except (TypeError, ValueError):
                ep_int = None
            if ep_int is not None and ep_int not in eplant_ids:
                eplant_ids.append(ep_int)
        if role and role not in role_names:
            role_names.append(role)

    eplant_ids.sort()

    module_prefixes = _derive_module_prefixes(role_names)
    allowed_table_prefixes = _derive_table_prefixes(module_prefixes)
    tier = _derive_tier(role_names)

    packet = {
        "iqms_user_name": iqms_user_name,
        "email": email,
        "role_names": role_names,
        "module_prefixes": module_prefixes,
        "allowed_table_prefixes": allowed_table_prefixes,
        "eplant_ids": eplant_ids or [1, 2, 3],
        "tier": tier,
        "synced_at": _now_iso(),
    }
    return packet


# Public helper for callers that need the timestamp format used by /sso
now_iso = _now_iso
