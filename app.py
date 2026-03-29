import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from flask import Flask, jsonify, request

APP_VERSION = "1.0.0-crm-bridge"
SERVICE_NAME = "payeeproof-crm-bridge"
DB_PATH = os.getenv("DB_PATH", "/tmp/payeeproof_crm_bridge.db")
CRM_BRIDGE_SECRET = os.getenv("CRM_BRIDGE_SECRET", "")
CRM_BRIDGE_AUTH_HEADER = os.getenv("CRM_BRIDGE_AUTH_HEADER", "X-PayeeProof-CRM-Secret")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", CRM_BRIDGE_SECRET)
SITE_URL = os.getenv("SITE_URL", "https://payeeproof.com")
MAX_NOTES_LEN = int(os.getenv("MAX_NOTES_LEN", "4000"))

app = Flask(__name__)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_crm_leads_created_at ON crm_leads(created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_crm_leads_email ON crm_leads(email)"
        )


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
            "SELECT id, request_id, created_at FROM crm_leads WHERE request_id = ?",
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
                }
            ), 200

        conn.execute(
            """
            INSERT INTO crm_leads (
                id, request_id, event, submitted_at, name, company, email, volume, notes,
                origin, source_ip, user_agent, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )

    return jsonify(
        {
            "ok": True,
            "status": "stored",
            "lead_id": lead_id,
            "request_id": req["request_id"],
            "created_at": created_at,
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
                   origin, created_at
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
        row = conn.execute(
            "SELECT * FROM crm_leads WHERE id = ?",
            (lead_id,),
        ).fetchone()
    if not row:
        return jsonify({"ok": False, "error": "NOT_FOUND"}), 404

    item = dict(row)
    try:
        item["payload"] = json.loads(item.pop("payload_json", "{}"))
    except Exception:
        item["payload"] = {}
    return jsonify({"ok": True, "item": item})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
