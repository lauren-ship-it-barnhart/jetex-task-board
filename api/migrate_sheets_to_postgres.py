"""One-time Google Sheet to Postgres task migration."""
from __future__ import annotations

import os

import google.auth
from googleapiclient.discovery import build

from task_store import Conflict, PostgresTaskStore


SHEET_ID = os.environ.get("SHEET_ID", "170TEmIyh5wEcY_oGbo6CEzLrYQds-tNcLqW_E-L2PE4")
SHEET_RANGE = os.environ.get("SHEET_RANGE", "Tasks!A:T")
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() in {"1", "true", "yes"}

FALLBACK_HEADERS = [
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


def get_sheets():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    return build("sheets", "v4", credentials=creds)


def sheet_tasks():
    result = get_sheets().spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=SHEET_RANGE,
    ).execute()
    rows = result.get("values", [])
    if not rows:
        return []
    headers = [cell.strip() for cell in rows[0]]
    if "id" not in headers:
        headers = FALLBACK_HEADERS
    tasks = []
    for row in rows[1:]:
        task = {header: row[index] if index < len(row) else "" for index, header in enumerate(headers)}
        if task.get("id") and task.get("text"):
            tasks.append(task)
    return tasks


def main() -> int:
    store = PostgresTaskStore(os.environ["DATABASE_URL"])
    tasks = sheet_tasks()
    created = 0
    updated = 0
    skipped = 0
    for task in tasks:
        if DRY_RUN:
            skipped += 1
            continue
        try:
            store.create_task(task)
            created += 1
        except Conflict:
            task_id = str(task.pop("id"))
            store.update_task(task_id, task)
            updated += 1
    print({
        "sheet_rows": len(tasks),
        "created": created,
        "updated": updated,
        "dry_run_skipped": skipped,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
