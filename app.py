import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import requests
from flask import Flask, jsonify, request

APP_VERSION = "1.1.0-crm-bridge-sheets"
SERVICE_NAME = "payeeproof-crm-bridge"
DB_PATH = os.getenv("DB_PATH", "/tmp/payeeproof_crm_bridge.db")
CRM_BRIDGE_SECRET = os.getenv("CRM_BRIDGE_SECRET", "")
CRM_BRIDGE_AUTH_HEADER = os.getenv("CRM_BRIDGE_AUTH_HEADER", "X-PayeeProof-CRM-Secret")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", CRM_BRIDGE_SECRET)
SITE_URL = os.getenv("SITE_URL", "https://payeeproof.com")
MAX_NOTES_LEN = int(os.getenv("MAX_NOTES_LEN", "4000"))

SHEETS_INTAKE_ENABLED = str(os.getenv("SHEETS_INTAKE_ENABLED", "1")).strip().lower() not in {"0", "false", "no", "off"}
SHEETS_INTAKE_URL = os.getenv("SHEETS_INTAKE_URL", "").strip()
SHEETS_INTAKE_SECRET = os.getenv("SHEETS_INTAKE_SECRET", "").strip()
SHEETS_INTAKE_TIMEOUT_SEC = float(os.getenv("SHEETS_INTAKE_TIMEOUT_SEC", "10"))
SHEETS_INTAKE_PRODUCT = os.getenv("SHEETS_INTAKE_PRODUCT", "payeeproof").strip() or "payeeproof"

app = Flask(__name__)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_parent_dir(path_value: str) -> None:
    try:
        Path(path_value).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


ensure_parent_dir(DB_PATH)


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in existing:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def init_db() -> None:
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS crm_leads (
                id TEXT PRIMARY KEY,
                request_id TEXT UNIQUE,
                event TEXT NOT NULL,
                submitted_at TEXT,
                name TEXT,
                company TEXT,
                email TEXT,
                volume TEXT,
                notes TEXT,
                origin TEXT,
                source_ip TEXT,
                user_agent TEXT,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                sheets_status TEXT DEFAULT 'pending',
                sheets_delivery_id TEXT,
                sheets_response_code INTEGER,
                sheets_error TEXT,
                sheets_last_attempt_at TEXT
            )
            """
        )
        ensure_column(conn, "crm_leads", "sheets_status", "TEXT DEFAULT 'pending'")
        ensure_column(conn, "crm_leads", "sheets_delivery_id", "TEXT")
        ensure_column(conn, "crm_leads", "sheets_response_code", "INTEGER")
        ensure_column(conn, "crm_leads", "sheets_error", "TEXT")
        ensure_column(conn, "crm_leads", "sheets_last_attempt_at", "TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_leads_created_at ON crm_leads(created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_leads_email ON crm_leads(email)")


init_db()


def unauthorized(message: str = "Unauthorized"):
    return jsonify({"ok": False, "error": "UNAUTHORIZED", "message": message}), 401


def forbidden(message: str = "Forbidden"):
    return jsonify({"ok": False, "error": "FORBIDDEN", "message": message}), 403


def require_secret(expected: str, header_name: str):
    if not expected:
        return forbidden(f"{header_name} is not configured on the server")
    actual = request.headers.get(header_name, "")
    if not actual:
        return unauthorized(f"Missing {header_name} header")
    if actual != expected:
        return forbidden("Invalid shared secret")
    return None


def trim_text(value: Any, max_len: int = 512) -> str:
    text = str(value or "").strip()
    return text[:max_len]


def normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    req = payload.get("request") or {}
    meta = payload.get("meta") or {}
    links = payload.get("links") or {}
    notes = trim_text(req.get("notes"), MAX_NOTES_LEN)
    return {
        "event": trim_text(payload.get("event") or "pilot_request_created", 100),
        "product": trim_text(payload.get("product") or "payeeproof", 100),
        "request": {
            "request_id": trim_text(req.get("request_id"), 120),
            "submitted_at": trim_text(req.get("submitted_at"), 100),
            "name": trim_text(req.get("name"), 200),
            "company": trim_text(req.get("company"), 200),
            "email": trim_text(req.get("email"), 320),
            "volume": trim_text(req.get("volume"), 200),
            "notes": notes,
        },
        "meta": {
            "origin": trim_text(meta.get("origin"), 300),
            "source_ip": trim_text(meta.get("source_ip"), 100),
            "user_agent": trim_text(meta.get("user_agent"), 500),
        },
        "links": {
            "site": trim_text(links.get("site") or SITE_URL, 300),
            "api_base": trim_text(links.get("api_base"), 300),
        },
    }


def get_lead_by_key(conn: sqlite3.Connection, lead_key: str):
    key = str(lead_key or "").strip()
    if not key:
        return None

    row = conn.execute("SELECT * FROM crm_leads WHERE id = ?", (key,)).fetchone()
    if row:
        return row

    normalized = "".join(ch for ch in key if ch.isalnum() or ch in ("_", "-", ":"))
    if normalized and normalized != key:
        row = conn.execute("SELECT * FROM crm_leads WHERE id = ?", (normalized,)).fetchone()
        if row:
            return row
        key = normalized

    row = conn.execute("SELECT * FROM crm_leads WHERE request_id = ?", (key,)).fetchone()
    if row:
        return row

    if key.startswith("lead_"):
        suffix = key[5:]
        if suffix:
            row = conn.execute(
                "SELECT * FROM crm_leads WHERE id LIKE ? ORDER BY created_at DESC LIMIT 1",
                (f"lead_%{suffix}",),
            ).fetchone()
            if row:
                return row
    return None


def build_sheets_payload(lead_id: str, normalized: Dict[str, Any], created_at: str) -> Dict[str, Any]:
    req = normalized.get("request") or {}
    meta = normalized.get("meta") or {}
    links = normalized.get("links") or {}
    return {
        "shared_secret": SHEETS_INTAKE_SECRET,
        "event": normalized.get("event") or "pilot_request_created",
        "product": SHEETS_INTAKE_PRODUCT,
        "lead": {
            "lead_id": lead_id,
            "request_id": req.get("request_id") or "",
            "created_at": created_at,
            "submitted_at": req.get("submitted_at") or created_at,
            "name": req.get("name") or "",
            "company": req.get("company") or "",
            "email": req.get("email") or "",
            "volume": req.get("volume") or "",
            "notes": req.get("notes") or "",
            "origin": meta.get("origin") or "",
            "source_ip": meta.get("source_ip") or "",
            "user_agent": meta.get("user_agent") or "",
            "site": links.get("site") or SITE_URL,
            "api_base": links.get("api_base") or "",
            "lead_url": f"{SITE_URL.rstrip('/')}/pilot-flow.html" if SITE_URL else "",
        },
    }


def send_to_sheets(lead_id: str, normalized: Dict[str, Any], created_at: str) -> Dict[str, Any]:
    attempted_at = utc_now_iso()
    if not SHEETS_INTAKE_ENABLED:
        return {
            "status": "disabled",
            "delivery_id": None,
            "response_code": None,
            "error": None,
            "last_attempt_at": attempted_at,
        }
    if not SHEETS_INTAKE_URL:
        return {
            "status": "not_configured",
            "delivery_id": None,
            "response_code": None,
            "error": None,
            "last_attempt_at": attempted_at,
        }

    payload = build_sheets_payload(lead_id, normalized, created_at)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "PayeeProof-CRM-Bridge/1.0 (+https://payeeproof.com)",
    }
    try:
        response = requests.post(
            SHEETS_INTAKE_URL,
            headers=headers,
            json=payload,
            timeout=SHEETS_INTAKE_TIMEOUT_SEC,
        )
        response_code = int(response.status_code)
        raw_text = (response.text or "")[:1000]
        delivery_id = None
        data = {}
        try:
            data = response.json() if (response.text or "").strip() else {}
            delivery_id = data.get("row_id") or data.get("delivery_id") or data.get("request_id") or lead_id
        except ValueError:
            pass
        if 200 <= response_code < 300:
            return {
                "status": str(data.get("status") or "sent"),
                "delivery_id": delivery_id or lead_id,
                "response_code": response_code,
                "error": None,
                "last_attempt_at": attempted_at,
            }
        return {
            "status": "failed",
            "delivery_id": delivery_id or lead_id,
            "response_code": response_code,
            "error": f"SHEETS_HTTP_{response_code}: {raw_text}",
            "last_attempt_at": attempted_at,
        }
    except requests.RequestException as exc:
        return {
            "status": "failed",
            "delivery_id": lead_id,
            "response_code": None,
            "error": f"SHEETS_REQUEST_ERROR: {exc}",
            "last_attempt_at": attempted_at,
        }


def persist_sheets_result(conn: sqlite3.Connection, lead_id: str, result: Dict[str, Any]) -> None:
    conn.execute(
        """
        UPDATE crm_leads
        SET sheets_status = ?,
            sheets_delivery_id = ?,
            sheets_response_code = ?,
            sheets_error = ?,
            sheets_last_attempt_at = ?
        WHERE id = ?
        """,
        (
            str(result.get("status") or "unknown"),
            result.get("delivery_id"),
            result.get("response_code"),
            result.get("error"),
            result.get("last_attempt_at") or utc_now_iso(),
            lead_id,
        ),
    )


def decode_payload_json(value: Any) -> Dict[str, Any]:
    try:
        return json.loads(value or "{}")
    except Exception:
        return {}


@app.get("/health")
def health():
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM crm_leads").fetchone()
    return jsonify(
        {
            "ok": True,
            "service": SERVICE_NAME,
            "version": APP_VERSION,
            "time": utc_now_iso(),
            "db_path": DB_PATH,
            "stored_leads": int(row["c"]),
            "auth_header": CRM_BRIDGE_AUTH_HEADER,
            "sheets_intake_enabled": SHEETS_INTAKE_ENABLED,
            "sheets_intake_configured": bool(SHEETS_INTAKE_URL),
        }
    )


@app.post("/crm/intake")
def crm_intake():
    auth_error = require_secret(CRM_BRIDGE_SECRET, CRM_BRIDGE_AUTH_HEADER)
    if auth_error:
        return auth_error

    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict) or not payload:
        return jsonify({"ok": False, "error": "INVALID_JSON"}), 400

    normalized = normalize_payload(payload)
    req = normalized["request"]
    meta = normalized["meta"]
    if not req["request_id"]:
        return jsonify({"ok": False, "error": "REQUEST_ID_REQUIRED"}), 400
    if not req["email"]:
        return jsonify({"ok": False, "error": "EMAIL_REQUIRED"}), 400

    lead_id = f"lead_{uuid.uuid4().hex[:20]}"
    created_at = utc_now_iso()

    with get_db() as conn:
        existing = conn.execute(
            "SELECT id, request_id, created_at, sheets_status, sheets_delivery_id, sheets_response_code FROM crm_leads WHERE request_id = ?",
            (req["request_id"],),
        ).fetchone()
        if existing:
            return jsonify(
                {
                    "ok": True,
                    "status": "duplicate",
                    "lead_id": existing["id"],
                    "request_id": existing["request_id"],
                    "created_at": existing["created_at"],
                    "sheets_notification": str(existing["sheets_status"] or "pending"),
                    "sheets_delivery_id": existing["sheets_delivery_id"],
                    "sheets_response_code": existing["sheets_response_code"],
                }
            ), 200

        conn.execute(
            """
            INSERT INTO crm_leads (
                id, request_id, event, submitted_at, name, company, email, volume, notes,
                origin, source_ip, user_agent, payload_json, created_at,
                sheets_status, sheets_delivery_id, sheets_response_code, sheets_error, sheets_last_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lead_id,
                req["request_id"],
                normalized["event"],
                req["submitted_at"],
                req["name"],
                req["company"],
                req["email"],
                req["volume"],
                req["notes"],
                meta["origin"],
                meta["source_ip"],
                meta["user_agent"],
                json.dumps(normalized, ensure_ascii=False),
                created_at,
                "pending",
                None,
                None,
                None,
                None,
            ),
        )

        sheets_result = send_to_sheets(lead_id, normalized, created_at)
        persist_sheets_result(conn, lead_id, sheets_result)

    return jsonify(
        {
            "ok": True,
            "status": "stored",
            "lead_id": lead_id,
            "request_id": req["request_id"],
            "created_at": created_at,
            "sheets_notification": str(sheets_result.get("status") or "unknown"),
            "sheets_delivery_id": sheets_result.get("delivery_id"),
            "sheets_response_code": sheets_result.get("response_code"),
        }
    ), 200


@app.get("/crm/leads")
def crm_leads():
    auth_error = require_secret(ADMIN_SECRET, "X-Admin-Secret")
    if auth_error:
        return auth_error

    try:
        limit = max(1, min(int(request.args.get("limit", "50")), 200))
    except ValueError:
        limit = 50

    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, request_id, event, submitted_at, name, company, email, volume, notes,
                   origin, created_at, sheets_status, sheets_delivery_id, sheets_response_code,
                   sheets_last_attempt_at
            FROM crm_leads
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    items = [dict(r) for r in rows]
    return jsonify({"ok": True, "count": len(items), "items": items})


@app.get("/crm/leads/<lead_id>")
def crm_lead_detail(lead_id: str):
    auth_error = require_secret(ADMIN_SECRET, "X-Admin-Secret")
    if auth_error:
        return auth_error

    with get_db() as conn:
        row = get_lead_by_key(conn, lead_id)
    if not row:
        return jsonify({"ok": False, "error": "NOT_FOUND"}), 404

    item = dict(row)
    item["payload"] = decode_payload_json(item.pop("payload_json", "{}"))
    return jsonify({"ok": True, "item": item})


@app.post("/crm/leads/<lead_id>/replay-sheets")
def crm_lead_replay_sheets(lead_id: str):
    auth_error = require_secret(ADMIN_SECRET, "X-Admin-Secret")
    if auth_error:
        return auth_error

    with get_db() as conn:
        row = get_lead_by_key(conn, lead_id)
        if not row:
            return jsonify({"ok": False, "error": "NOT_FOUND"}), 404

        payload = decode_payload_json(row["payload_json"])
        result = send_to_sheets(str(row["id"]), payload, str(row["created_at"] or utc_now_iso()))
        persist_sheets_result(conn, str(row["id"]), result)

    return jsonify(
        {
            "ok": True,
            "lead_id": row["id"],
            "request_id": row["request_id"],
            "sheets_notification": str(result.get("status") or "unknown"),
            "sheets_delivery_id": result.get("delivery_id"),
            "sheets_response_code": result.get("response_code"),
            "sheets_error": result.get("error"),
        }
    ), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
