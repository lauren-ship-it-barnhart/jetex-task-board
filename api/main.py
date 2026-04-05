"""Tiny Cloud Run proxy for task board writes to Google Sheets."""
import datetime
import json
import os
from collections import Counter
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify
from flask_cors import CORS
import google.auth
from googleapiclient.discovery import build
from google.cloud import bigquery

app = Flask(__name__)
CORS(app)

SHEET_ID = os.environ.get("SHEET_ID", "170TEmIyh5wEcY_oGbo6CEzLrYQds-tNcLqW_E-L2PE4")
BQ_PROJECT = os.environ.get("BQ_PROJECT", "project-051fdc80-6936-4eb8-98d")
MARIAH_TABLE_ID = os.environ.get("MARIAH_TABLE_ID", f"{BQ_PROJECT}.jetex_live.mariah_calls")
MARIAH_CALLS_LIMIT = int(os.environ.get("MARIAH_CALLS_LIMIT", "1000"))
EASTERN_TZ = ZoneInfo("America/New_York")

_bq_client = None
def get_bq():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=BQ_PROJECT)
    return _bq_client

def get_sheets():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)

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
    return jsonify({"status": "ok"})

@app.route("/action", methods=["POST", "OPTIONS"])
def action():
    if request.method == "OPTIONS":
        return "", 204
    data = request.get_json(force=True)
    act = data.get("action", "")
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
            elif act == "delete":
                # Get sheet ID first
                meta = sheets.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
                sheet_gid = 0
                for s in meta.get("sheets", []):
                    if s["properties"]["title"] == "Tasks":
                        sheet_gid = s["properties"]["sheetId"]
                        break
                sheets.spreadsheets().batchUpdate(
                    spreadsheetId=SHEET_ID,
                    body={"requests": [{"deleteDimension": {
                        "range": {"sheetId": sheet_gid, "dimension": "ROWS",
                                  "startIndex": i - 1, "endIndex": i}
                    }}]}
                ).execute()
            return jsonify({"success": True, "action": act, "id": task_id})
    
    return jsonify({"success": False, "error": "task not found"}), 404

@app.route("/add", methods=["POST", "OPTIONS"])
def add_task():
    if request.method == "OPTIONS":
        return "", 204
    data = request.get_json(force=True)
    row = [
        data.get("id", str(int(datetime.datetime.now().timestamp() * 1000))),
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
    return jsonify({"success": True})

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
