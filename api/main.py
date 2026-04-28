"""Cloud Run API for the JetEx task board and agent queue."""
import base64
import datetime
import hmac
import json
import os
from collections import Counter
from functools import wraps
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify
from flask_cors import CORS
import google.auth
from googleapiclient.discovery import build
from google.cloud import bigquery
from task_store import (
    BadRequest,
    Conflict,
    NotFound,
    PostgresTaskStore,
    StoreError,
    millis_id,
)

app = Flask(__name__)
CORS(app)

SHEET_ID = os.environ.get("SHEET_ID", "170TEmIyh5wEcY_oGbo6CEzLrYQds-tNcLqW_E-L2PE4")
DATABASE_URL = os.environ.get("DATABASE_URL")
BQ_PROJECT = os.environ.get("BQ_PROJECT", "project-051fdc80-6936-4eb8-98d")
MARIAH_TABLE_ID = os.environ.get("MARIAH_TABLE_ID", f"{BQ_PROJECT}.jetex_live.mariah_calls")
MARIAH_CALLS_LIMIT = int(os.environ.get("MARIAH_CALLS_LIMIT", "1000"))
EASTERN_TZ = ZoneInfo("America/New_York")
ALLOWED_EMAIL_DOMAINS = {
    domain.strip().lower()
    for domain in os.environ.get(
        "ALLOWED_EMAIL_DOMAINS",
        "jetexcellence.com,flybellair.com",
    ).split(",")
    if domain.strip()
}
ADMIN_EMAILS = {
    email.strip().lower()
    for email in os.environ.get("ADMIN_EMAILS", "").split(",")
    if email.strip()
}
AUTH_REQUIRED = os.environ.get("AUTH_REQUIRED", "false").lower() in {"1", "true", "yes"}
AGENT_WORKER_TOKEN = os.environ.get("AGENT_WORKER_TOKEN")

_task_store = PostgresTaskStore(DATABASE_URL) if DATABASE_URL else None

_bq_client = None
def get_bq():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=BQ_PROJECT)
    return _bq_client

def get_sheets():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)

def task_store():
    return _task_store

def task_store_enabled():
    return task_store() is not None

def _json_body():
    return request.get_json(silent=True) or {}

def _query_args():
    return {key: value for key, value in request.args.items() if value != ""}

def _decode_auth_payload():
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth.count(".") < 2:
        return {}
    payload = auth.split(".", 2)[1]
    payload += "=" * (-len(payload) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(payload.encode("utf-8")))
    except Exception:
        return {}

def _email_from_headers():
    candidates = [
        request.headers.get("X-User-Email"),
        request.headers.get("X-Forwarded-Email"),
        request.headers.get("X-Goog-Authenticated-User-Email"),
    ]
    payload = _decode_auth_payload()
    candidates.extend([
        payload.get("preferred_username"),
        payload.get("email"),
        payload.get("upn"),
    ])
    for candidate in candidates:
        if not candidate:
            continue
        email = candidate.replace("accounts.google.com:", "").strip().lower()
        if "@" in email:
            return email
    return None

def current_actor():
    worker_token = request.headers.get("X-Agent-Worker-Token")
    if AGENT_WORKER_TOKEN and worker_token and hmac.compare_digest(worker_token, AGENT_WORKER_TOKEN):
        return {"email": "agent-worker", "role": "agent_worker"}

    email = _email_from_headers()
    if not email:
        if AUTH_REQUIRED:
            return {"email": None, "role": None}
        return {"email": "local-dev", "role": "admin"}

    domain = email.rsplit("@", 1)[-1]
    if email in ADMIN_EMAILS:
        return {"email": email, "role": "admin"}
    if domain in ALLOWED_EMAIL_DOMAINS:
        return {"email": email, "role": "member"}
    return {"email": email, "role": None}

def _role_allows(role, required):
    if required == "viewer":
        return role in {"viewer", "member", "admin", "agent_worker"}
    if required == "member":
        return role in {"member", "admin"}
    if required == "admin":
        return role == "admin"
    if required == "agent_worker":
        return role in {"admin", "agent_worker"}
    return False

def require_role(required):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if request.method == "OPTIONS":
                return "", 204
            actor = current_actor()
            if not _role_allows(actor["role"], required):
                status = 401 if actor["email"] is None else 403
                return jsonify({
                    "success": False,
                    "error": "unauthorized",
                    "required_role": required,
                }), status
            return fn(*args, **kwargs)
        return wrapper
    return decorator

def _legacy_task_rows():
    sheets = get_sheets()
    result = sheets.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Tasks!A:T"
    ).execute()
    rows = result.get("values", [])
    if not rows:
        return []
    headers = [cell.strip() for cell in rows[0]]
    fallback_headers = [
        "id",
        "text",
        "category",
        "status",
        "source",
        "session_id",
        "created_at",
        "completed_at",
        "archived_at",
        "repo",
        "acceptance_criteria",
        "agent_type",
        "agent_run_id",
        "branch",
        "pr_url",
        "last_agent_update",
        "review_owner",
    ]
    if "id" not in headers:
        headers = fallback_headers
    tasks = []
    for row in rows[1:]:
        task = {}
        for index, header in enumerate(headers):
            task[header] = row[index] if index < len(row) else ""
        task.setdefault("created_at", task.get("created", ""))
        task.setdefault("completed_at", "")
        task.setdefault("archived_at", "")
        task.setdefault("source", "manual")
        task.setdefault("status", "open")
        tasks.append(task)
    return tasks

def _legacy_list_tasks(args):
    tasks = _legacy_task_rows()
    status_filter = {part.strip() for part in args.get("status", "").split(",") if part.strip()}
    if status_filter:
        tasks = [task for task in tasks if task.get("status") in status_filter]
    if args.get("source"):
        tasks = [task for task in tasks if task.get("source") == args["source"]]
    if args.get("category"):
        tasks = [task for task in tasks if task.get("category") == args["category"]]
    if args.get("repo"):
        tasks = [task for task in tasks if task.get("repo") == args["repo"]]
    if args.get("hide_generated") in {"1", "true", "yes"}:
        tasks = [task for task in tasks if task.get("source") != "claude-code"]
    if args.get("include_archived") not in {"1", "true", "yes"}:
        tasks = [task for task in tasks if task.get("status") != "archived"]
    return tasks

def _mariah_table():
    return get_bq().get_table(MARIAH_TABLE_ID)

def _normalize_dt(value):
    if not value:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed
    return None

def _clean_text(value):
    return value.strip() if isinstance(value, str) else value

def _legacy_lead_generated(row):
    return bool(row.get("raw_subject") or row.get("web_link"))

def _lead_generated(row):
    explicit = row.get("lead_generated")
    if isinstance(explicit, bool):
        return explicit
    email_sent = row.get("email_sent")
    if isinstance(email_sent, bool):
        return email_sent
    return _legacy_lead_generated(row)

def _serialize_mariah_row(row):
    received_at = _normalize_dt(row.get("received_at"))
    return {
        "call_id": row.get("call_id"),
        "received_at": received_at.isoformat() if received_at else None,
        "caller_name": row.get("caller_name"),
        "phone": row.get("phone"),
        "email": row.get("email"),
        "company": row.get("company"),
        "departure": row.get("departure"),
        "destination": row.get("destination"),
        "trip_type": row.get("trip_type"),
        "travel_date": row.get("travel_date"),
        "travel_date_iso": row.get("travel_date_iso"),
        "departure_time": row.get("departure_time"),
        "return_date": row.get("return_date"),
        "return_date_iso": row.get("return_date_iso"),
        "passengers": row.get("passengers"),
        "in_crm": row.get("in_crm"),
        "raw_subject": row.get("raw_subject"),
        "web_link": row.get("web_link"),
        "call_sid": row.get("call_sid"),
        "transcript": row.get("transcript"),
        "email_sent": row.get("email_sent"),
        "email_error": row.get("email_error"),
        "lead_generated": _lead_generated(row),
        "call_outcome": row.get("call_outcome"),
        "hangup_reason": row.get("hangup_reason"),
        "trip_confirmed": row.get("trip_confirmed"),
    }

def _load_mariah_rows(limit=MARIAH_CALLS_LIMIT):
    table = _mariah_table()
    rows = [dict(row.items()) for row in get_bq().list_rows(table, max_results=limit)]
    rows.sort(
        key=lambda row: _normalize_dt(row.get("received_at")) or datetime.datetime.min.replace(
            tzinfo=datetime.timezone.utc
        ),
        reverse=True,
    )
    return rows, table.num_rows

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "task_store": "postgres" if task_store_enabled() else "sheets-fallback",
    })

@app.errorhandler(BadRequest)
def handle_bad_request(error):
    return jsonify({"success": False, "error": str(error)}), 400

@app.errorhandler(NotFound)
def handle_not_found(error):
    return jsonify({"success": False, "error": str(error)}), 404

@app.errorhandler(Conflict)
def handle_conflict(error):
    return jsonify({"success": False, "error": str(error)}), 409

@app.errorhandler(StoreError)
def handle_store_error(error):
    return jsonify({"success": False, "error": str(error)}), 500

@app.route("/api/me", methods=["GET", "OPTIONS"])
@require_role("viewer")
def api_me():
    return jsonify({"success": True, "actor": current_actor()})

@app.route("/api/tasks", methods=["GET", "POST", "OPTIONS"])
def api_tasks():
    if request.method == "OPTIONS":
        return "", 204
    if request.method == "GET":
        actor = current_actor()
        if not _role_allows(actor["role"], "viewer"):
            status = 401 if actor["email"] is None else 403
            return jsonify({"success": False, "error": "unauthorized"}), status
        if task_store_enabled():
            tasks = task_store().list_tasks(_query_args())
        else:
            tasks = _legacy_list_tasks(_query_args())
        return jsonify({"success": True, "tasks": tasks, "count": len(tasks)})

    actor = current_actor()
    if not _role_allows(actor["role"], "member"):
        status = 401 if actor["email"] is None else 403
        return jsonify({"success": False, "error": "unauthorized"}), status
    if not task_store_enabled():
        return _legacy_add_task(_json_body())
    task = task_store().create_task(_json_body())
    return jsonify({"success": True, "task": task}), 201

@app.route("/api/tasks/<task_id>", methods=["GET", "PATCH", "OPTIONS"])
def api_task_detail(task_id):
    if request.method == "OPTIONS":
        return "", 204
    actor = current_actor()
    payload = _json_body() if request.method == "PATCH" else {}
    agent_fields = {
        "repo",
        "acceptance_criteria",
        "agent_type",
        "agent_run_id",
        "branch",
        "pr_url",
        "last_agent_update",
        "review_owner",
    }
    admin_statuses = {
        "archived",
        "ready_for_agent",
        "agent_running",
        "human_review",
        "blocked",
        "merged",
    }
    required = "viewer"
    if request.method == "PATCH":
        requested_status = str(payload.get("status", "")).strip().lower()
        required = "admin" if requested_status in admin_statuses or agent_fields.intersection(payload) else "member"
    if not _role_allows(actor["role"], required):
        status = 401 if actor["email"] is None else 403
        return jsonify({"success": False, "error": "unauthorized"}), status
    if not task_store_enabled():
        return jsonify({"success": False, "error": "Postgres task store is not configured"}), 503
    if request.method == "GET":
        return jsonify({"success": True, "task": task_store().get_task(task_id)})
    return jsonify({"success": True, "task": task_store().update_task(task_id, payload)})

@app.route("/api/tasks/<task_id>/complete", methods=["POST", "OPTIONS"])
@require_role("member")
def api_complete_task(task_id):
    if not task_store_enabled():
        return _legacy_action({"action": "complete", "id": task_id})
    return jsonify({"success": True, "task": task_store().complete_task(task_id)})

@app.route("/api/tasks/<task_id>/archive", methods=["POST", "OPTIONS"])
@require_role("admin")
def api_archive_task(task_id):
    if not task_store_enabled():
        return _legacy_action({"action": "delete", "id": task_id})
    return jsonify({"success": True, "task": task_store().archive_task(task_id)})

@app.route("/api/tasks/<task_id>/send-to-agent", methods=["POST", "OPTIONS"])
@require_role("admin")
def api_send_to_agent(task_id):
    if not task_store_enabled():
        return jsonify({"success": False, "error": "Postgres task store is not configured"}), 503
    task = task_store().send_to_agent(task_id, _json_body())
    return jsonify({"success": True, "task": task})

@app.route("/api/tasks/batch-archive", methods=["POST", "OPTIONS"])
@require_role("admin")
def api_batch_archive():
    if not task_store_enabled():
        return jsonify({"success": False, "error": "Postgres task store is not configured"}), 503
    return jsonify({"success": True, **task_store().batch_archive(_json_body())})

@app.route("/api/agent-runs", methods=["GET", "OPTIONS"])
@require_role("viewer")
def api_agent_runs():
    if not task_store_enabled():
        return jsonify({"success": True, "agent_runs": [], "count": 0})
    runs = task_store().list_agent_runs(_query_args())
    return jsonify({"success": True, "agent_runs": runs, "count": len(runs)})

@app.route("/api/agent-runs/claim", methods=["POST", "OPTIONS"])
@require_role("agent_worker")
def api_claim_agent_task():
    if not task_store_enabled():
        return jsonify({"success": False, "error": "Postgres task store is not configured"}), 503
    data = _json_body()
    task = task_store().claim_ready_task(
        repo=data.get("repo") or "lauren-ship-it-barnhart/jetex-task-board",
        agent_type=data.get("agent_type") or "codex",
        worker_label=data.get("worker_label") or "codex-worker",
    )
    return jsonify({"success": True, "task": task, "claimed": bool(task)})

@app.route("/api/agent-runs/<run_id>", methods=["PATCH", "OPTIONS"])
@require_role("agent_worker")
def api_update_agent_run(run_id):
    if not task_store_enabled():
        return jsonify({"success": False, "error": "Postgres task store is not configured"}), 503
    run = task_store().update_agent_run(run_id, _json_body())
    return jsonify({"success": True, "agent_run": run})

def _legacy_action(data):
    act = (data.get("action", "") or "").strip().lower()
    task_id = str(data.get("id", ""))

    if not act or not task_id:
        return jsonify({"success": False, "error": "missing action or id"}), 400

    sheets = get_sheets()
    result = sheets.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Tasks!A:H"
    ).execute()
    rows = result.get("values", [])
    
    for i, row in enumerate(rows[1:], start=2):
        if len(row) > 0 and str(row[0]) == task_id:
            if act == "complete":
                sheets.spreadsheets().values().update(
                    spreadsheetId=SHEET_ID, range=f"Tasks!D{i}",
                    valueInputOption="RAW", body={"values": [["done"]]}
                ).execute()
                sheets.spreadsheets().values().update(
                    spreadsheetId=SHEET_ID, range=f"Tasks!H{i}",
                    valueInputOption="RAW",
                    body={"values": [[datetime.date.today().isoformat()]]}
                ).execute()
            elif act == "reopen":
                sheets.spreadsheets().values().update(
                    spreadsheetId=SHEET_ID, range=f"Tasks!D{i}",
                    valueInputOption="RAW", body={"values": [["open"]]}
                ).execute()
                sheets.spreadsheets().values().update(
                    spreadsheetId=SHEET_ID, range=f"Tasks!H{i}",
                    valueInputOption="RAW", body={"values": [[""]]}
                ).execute()
            elif act == "urgent":
                current = row[3] if len(row) > 3 else "open"
                new_status = "open" if current == "urgent" else "urgent"
                sheets.spreadsheets().values().update(
                    spreadsheetId=SHEET_ID, range=f"Tasks!D{i}",
                    valueInputOption="RAW", body={"values": [[new_status]]}
                ).execute()
            elif act in {"delete", "archive"}:
                sheets.spreadsheets().values().update(
                    spreadsheetId=SHEET_ID, range=f"Tasks!D{i}",
                    valueInputOption="RAW", body={"values": [["archived"]]}
                ).execute()
            return jsonify({"success": True, "action": act, "id": task_id})

    return jsonify({"success": False, "error": "task not found"}), 404

def _legacy_add_task(data):
    row = [
        data.get("id", millis_id()),
        data.get("text", ""),
        data.get("category", "general"),
        data.get("status", "open"),
        data.get("source", "manual"),
        "",
        datetime.date.today().isoformat(),
        ""
    ]
    sheets = get_sheets()
    sheets.spreadsheets().values().append(
        spreadsheetId=SHEET_ID, range="Tasks!A:H",
        valueInputOption="RAW", insertDataOption="INSERT_ROWS",
        body={"values": [row]}
    ).execute()
    return jsonify({"success": True, "id": row[0]})

@app.route("/action", methods=["POST", "OPTIONS"])
def action():
    if request.method == "OPTIONS":
        return "", 204
    data = _json_body()
    act = (data.get("action", "") or "").strip().lower()
    task_id = str(data.get("id", ""))
    if not act or not task_id:
        return jsonify({"success": False, "error": "missing action or id"}), 400

    required = "admin" if act in {"delete", "archive", "send_to_agent"} else "member"
    actor = current_actor()
    if not _role_allows(actor["role"], required):
        status = 401 if actor["email"] is None else 403
        return jsonify({"success": False, "error": "unauthorized"}), status

    if not task_store_enabled():
        return _legacy_action(data)

    if act == "complete":
        task = task_store().complete_task(task_id)
    elif act == "reopen":
        task = task_store().update_task(task_id, {"status": "open", "completed_at": None, "archived_at": None})
    elif act == "urgent":
        current = task_store().get_task(task_id)
        new_status = "open" if current.get("status") == "urgent" else "urgent"
        task = task_store().update_task(task_id, {"status": new_status})
    elif act in {"delete", "archive"}:
        task = task_store().archive_task(task_id)
    else:
        return jsonify({"success": False, "error": f"unsupported action: {act}"}), 400
    return jsonify({"success": True, "action": act, "id": task_id, "task": task})

@app.route("/add", methods=["POST", "OPTIONS"])
def add_task():
    if request.method == "OPTIONS":
        return "", 204
    actor = current_actor()
    if not _role_allows(actor["role"], "member"):
        status = 401 if actor["email"] is None else 403
        return jsonify({"success": False, "error": "unauthorized"}), status
    data = _json_body()
    if not task_store_enabled():
        return _legacy_add_task(data)
    task = task_store().create_task(data)
    return jsonify({"success": True, "id": task["id"], "task": task})

@app.route("/mariah/calls", methods=["GET", "OPTIONS"])
def mariah_calls():
    """Return all Mariah AI phone agent calls from BigQuery."""
    if request.method == "OPTIONS":
        return "", 204
    rows, total_rows = _load_mariah_rows()
    calls = [_serialize_mariah_row(row) for row in rows]
    return jsonify({"calls": calls, "count": len(calls)})


@app.route("/mariah/stats", methods=["GET", "OPTIONS"])
def mariah_stats():
    """Return aggregated metrics for Mariah calls."""
    if request.method == "OPTIONS":
        return "", 204
    rows, total_rows = _load_mariah_rows(limit=10000)
    total_calls = max(total_rows, len(rows))
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    last_7d_cutoff = now_utc - datetime.timedelta(days=7)
    last_30d_cutoff = now_utc - datetime.timedelta(days=30)

    caller_counter = Counter()
    company_counter = Counter()
    route_counter = Counter()
    hour_counter = Counter()
    trip_type_counter = Counter()
    day_counter = Counter()
    caller_meta = {}

    unique_callers = set()
    unique_companies = set()
    in_crm_count = 0
    new_leads = 0
    last_7d = 0
    last_30d = 0

    for row in rows:
        received_at = _normalize_dt(row.get("received_at"))
        caller_name = _clean_text(row.get("caller_name"))
        company = _clean_text(row.get("company"))
        departure = _clean_text(row.get("departure"))
        destination = _clean_text(row.get("destination"))
        trip_type = _clean_text(row.get("trip_type")) or "Unknown"
        in_crm = row.get("in_crm")

        if caller_name:
            unique_callers.add(caller_name)
            caller_counter[caller_name] += 1
            caller_meta.setdefault(
                caller_name,
                {"company": company, "in_crm": in_crm},
            )

        if company:
            unique_companies.add(company)
            company_counter[company] += 1

        if departure and destination:
            route_counter[f"{departure} → {destination}"] += 1

        trip_type_counter[trip_type] += 1

        if in_crm is True:
            in_crm_count += 1
        if _lead_generated(row):
            new_leads += 1

        if not received_at:
            continue

        if received_at >= last_7d_cutoff:
            last_7d += 1
        if received_at >= last_30d_cutoff:
            last_30d += 1
            day_counter[received_at.date().isoformat()] += 1

        hour_counter[received_at.astimezone(EASTERN_TZ).hour] += 1

    by_day = [
        {"d": day, "calls": calls}
        for day, calls in sorted(day_counter.items())
    ]
    by_hour = [
        {"hour": hour, "calls": calls}
        for hour, calls in sorted(hour_counter.items())
    ]
    top_callers = [
        {
            "name": name,
            "company": caller_meta[name]["company"],
            "calls": calls,
            "in_crm": caller_meta[name]["in_crm"],
        }
        for name, calls in caller_counter.most_common(10)
    ]
    top_companies = [
        {"company": company, "calls": calls}
        for company, calls in company_counter.most_common(10)
    ]
    top_routes = [
        {"route": route, "calls": calls}
        for route, calls in route_counter.most_common(10)
    ]
    trip_types = [
        {"type": trip_type, "calls": calls}
        for trip_type, calls in trip_type_counter.most_common()
    ]

    return jsonify({
        "total_calls": total_calls,
        "unique_callers": len(unique_callers),
        "unique_companies": len(unique_companies),
        "in_crm_count": in_crm_count,
        "new_leads": new_leads,
        "last_7d": last_7d,
        "last_30d": last_30d,
        "by_day": by_day,
        "top_callers": top_callers,
        "top_companies": top_companies,
        "top_routes": top_routes,
        "by_hour": by_hour,
        "trip_types": trip_types,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
