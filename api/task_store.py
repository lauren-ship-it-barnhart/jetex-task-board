"""Postgres-backed task and agent queue storage."""
from __future__ import annotations

import datetime as dt
import os
import threading
import time
from typing import Any

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - handled at runtime in Cloud Run.
    psycopg = None
    dict_row = None


TASK_STATUSES = {
    "open",
    "urgent",
    "waiting",
    "someday",
    "done",
    "archived",
    "ready_for_agent",
    "agent_running",
    "human_review",
    "blocked",
    "merged",
}

AGENT_TASK_STATUSES = {
    "ready_for_agent",
    "agent_running",
    "human_review",
    "blocked",
    "merged",
    "archived",
}

TASK_FIELDS = [
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
    "updated_at",
]

PATCHABLE_TASK_FIELDS = {
    "text",
    "category",
    "status",
    "source",
    "session_id",
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
}

AGENT_RUN_FIELDS = [
    "id",
    "task_id",
    "repo",
    "status",
    "agent_type",
    "branch",
    "pr_url",
    "logs_summary",
    "failure_reason",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
]

PATCHABLE_RUN_FIELDS = {
    "status",
    "branch",
    "pr_url",
    "logs_summary",
    "failure_reason",
}

RUN_TO_TASK_STATUS = {
    "queued": "ready_for_agent",
    "ready_for_agent": "ready_for_agent",
    "running": "agent_running",
    "agent_running": "agent_running",
    "human_review": "human_review",
    "blocked": "blocked",
    "merged": "merged",
    "archived": "archived",
}


class StoreError(RuntimeError):
    """Base class for task store errors."""


class NotFound(StoreError):
    """Raised when a requested row does not exist."""


class Conflict(StoreError):
    """Raised when a requested write conflicts with existing data."""


class BadRequest(StoreError):
    """Raised when a request payload is invalid."""


def postgres_configured() -> bool:
    return bool(os.environ.get("DATABASE_URL"))


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def millis_id() -> str:
    return str(int(time.time() * 1000))


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _coerce_datetime(value: Any) -> dt.datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
    if isinstance(value, (int, float)):
        timestamp = value / 1000 if value > 10_000_000_000 else value
        return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc)
    if isinstance(value, str):
        clean = value.strip()
        if not clean:
            return None
        if clean.isdigit():
            return _coerce_datetime(int(clean))
        if clean.endswith("Z"):
            clean = clean[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(clean)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)
    raise BadRequest(f"Invalid datetime value: {value!r}")


def _serialize(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    return value


def serialize_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: _serialize(value) for key, value in row.items()}


def normalize_status(status: str | None, default: str = "open") -> str:
    value = (status or default).strip().lower()
    if value not in TASK_STATUSES:
        raise BadRequest(f"Unsupported task status: {value}")
    return value


def normalize_run_status(status: str | None, default: str = "queued") -> str:
    value = (status or default).strip().lower()
    if value not in RUN_TO_TASK_STATUS:
        raise BadRequest(f"Unsupported agent run status: {value}")
    return value


def _split_filter(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


class PostgresTaskStore:
    def __init__(self, database_url: str):
        if not database_url:
            raise StoreError("DATABASE_URL is not configured")
        self.database_url = database_url
        self._schema_ready = False
        self._schema_lock = threading.Lock()

    def connect(self):
        if psycopg is None:
            raise StoreError("psycopg is not installed")
        conn = psycopg.connect(self.database_url, row_factory=dict_row)
        if not self._schema_ready:
            with self._schema_lock:
                if not self._schema_ready:
                    self._ensure_schema(conn)
                    self._schema_ready = True
        return conn

    def _ensure_schema(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    category TEXT NOT NULL DEFAULT 'general',
                    status TEXT NOT NULL DEFAULT 'open',
                    source TEXT NOT NULL DEFAULT 'manual',
                    session_id TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    archived_at TIMESTAMPTZ,
                    repo TEXT,
                    acceptance_criteria TEXT,
                    agent_type TEXT,
                    agent_run_id TEXT,
                    branch TEXT,
                    pr_url TEXT,
                    last_agent_update TEXT,
                    review_owner TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_runs (
                    id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                    repo TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    agent_type TEXT NOT NULL DEFAULT 'codex',
                    branch TEXT,
                    pr_url TEXT,
                    logs_summary TEXT,
                    failure_reason TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_source ON tasks(source)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_repo ON tasks(repo)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_runs_task_id ON agent_runs(task_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_runs_status ON agent_runs(status)")
        conn.commit()

    def list_tasks(self, args: dict[str, Any]) -> list[dict[str, Any]]:
        clauses, params = self._task_filter_clauses(args)
        sql = f"""
            SELECT {", ".join(TASK_FIELDS)}
            FROM tasks
            {("WHERE " + " AND ".join(clauses)) if clauses else ""}
            ORDER BY
                CASE status
                    WHEN 'urgent' THEN 0
                    WHEN 'open' THEN 1
                    WHEN 'waiting' THEN 2
                    WHEN 'someday' THEN 3
                    WHEN 'ready_for_agent' THEN 4
                    WHEN 'agent_running' THEN 5
                    WHEN 'human_review' THEN 6
                    WHEN 'blocked' THEN 7
                    WHEN 'merged' THEN 8
                    WHEN 'done' THEN 9
                    WHEN 'archived' THEN 10
                    ELSE 11
                END,
                created_at DESC
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [serialize_row(dict(row)) for row in cur.fetchall()]

    def get_task(self, task_id: str) -> dict[str, Any]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {', '.join(TASK_FIELDS)} FROM tasks WHERE id = %s", [task_id])
                row = cur.fetchone()
        if not row:
            raise NotFound("Task not found")
        return serialize_row(dict(row))

    def create_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        task = self._normalize_create_payload(payload)
        fields = list(task.keys())
        placeholders = ", ".join(["%s"] * len(fields))
        sql = f"""
            INSERT INTO tasks ({", ".join(fields)})
            VALUES ({placeholders})
            RETURNING {", ".join(TASK_FIELDS)}
        """
        with self.connect() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, [task[field] for field in fields])
                    row = cur.fetchone()
                conn.commit()
            except psycopg.errors.UniqueViolation as exc:
                conn.rollback()
                raise Conflict("Task id already exists") from exc
        return serialize_row(dict(row))

    def update_task(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        updates = self._normalize_patch_payload(payload)
        if not updates:
            return self.get_task(task_id)
        assignments = [f"{field} = %s" for field in updates.keys()]
        assignments.append("updated_at = NOW()")
        values = list(updates.values()) + [task_id]
        sql = f"""
            UPDATE tasks
            SET {", ".join(assignments)}
            WHERE id = %s
            RETURNING {", ".join(TASK_FIELDS)}
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)
                row = cur.fetchone()
            conn.commit()
        if not row:
            raise NotFound("Task not found")
        return serialize_row(dict(row))

    def complete_task(self, task_id: str) -> dict[str, Any]:
        return self.update_task(task_id, {"status": "done", "completed_at": utc_now()})

    def archive_task(self, task_id: str) -> dict[str, Any]:
        return self.update_task(task_id, {"status": "archived", "archived_at": utc_now()})

    def send_to_agent(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        repo = payload.get("repo") or "lauren-ship-it-barnhart/jetex-task-board"
        agent_type = payload.get("agent_type") or "codex"
        run_id = payload.get("agent_run_id") or f"run-{task_id}-{millis_id()}"
        branch = payload.get("branch") or f"codex/task-{task_id}"
        updates = {
            "status": "ready_for_agent",
            "repo": repo,
            "acceptance_criteria": payload.get("acceptance_criteria"),
            "agent_type": agent_type,
            "agent_run_id": run_id,
            "branch": branch,
            "review_owner": payload.get("review_owner"),
            "last_agent_update": payload.get("last_agent_update") or "Queued for Codex worker",
        }
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE tasks
                    SET status = %(status)s,
                        repo = %(repo)s,
                        acceptance_criteria = %(acceptance_criteria)s,
                        agent_type = %(agent_type)s,
                        agent_run_id = %(agent_run_id)s,
                        branch = %(branch)s,
                        review_owner = %(review_owner)s,
                        last_agent_update = %(last_agent_update)s,
                        updated_at = NOW()
                    WHERE id = %(task_id)s
                    RETURNING {", ".join(TASK_FIELDS)}
                    """,
                    {**updates, "task_id": task_id},
                )
                task = cur.fetchone()
                if not task:
                    raise NotFound("Task not found")
                cur.execute(
                    """
                    INSERT INTO agent_runs (id, task_id, repo, status, agent_type, branch)
                    VALUES (%s, %s, %s, 'queued', %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        repo = EXCLUDED.repo,
                        agent_type = EXCLUDED.agent_type,
                        branch = EXCLUDED.branch,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    [run_id, task_id, repo, agent_type, branch],
                )
            conn.commit()
        return serialize_row(dict(task))

    def claim_ready_task(
        self,
        repo: str = "lauren-ship-it-barnhart/jetex-task-board",
        agent_type: str = "codex",
        worker_label: str = "codex-worker",
    ) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {", ".join(TASK_FIELDS)}
                    FROM tasks
                    WHERE status = 'ready_for_agent'
                      AND COALESCE(NULLIF(repo, ''), %s) = %s
                    ORDER BY created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    [repo, repo],
                )
                task = cur.fetchone()
                if not task:
                    conn.rollback()
                    return None
                task = dict(task)
                run_id = task.get("agent_run_id") or f"run-{task['id']}-{millis_id()}"
                branch = task.get("branch") or f"codex/task-{task['id']}"
                cur.execute(
                    f"""
                    UPDATE tasks
                    SET status = 'agent_running',
                        repo = %s,
                        agent_type = %s,
                        agent_run_id = %s,
                        branch = %s,
                        last_agent_update = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING {", ".join(TASK_FIELDS)}
                    """,
                    [
                        repo,
                        agent_type,
                        run_id,
                        branch,
                        f"Claimed by {worker_label}",
                        task["id"],
                    ],
                )
                claimed = cur.fetchone()
                cur.execute(
                    """
                    INSERT INTO agent_runs (id, task_id, repo, status, agent_type, branch, started_at)
                    VALUES (%s, %s, %s, 'running', %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        status = 'running',
                        repo = EXCLUDED.repo,
                        agent_type = EXCLUDED.agent_type,
                        branch = EXCLUDED.branch,
                        started_at = COALESCE(agent_runs.started_at, NOW()),
                        updated_at = NOW()
                    """,
                    [run_id, task["id"], repo, agent_type, branch],
                )
            conn.commit()
        return serialize_row(dict(claimed))

    def batch_archive(self, filters: dict[str, Any]) -> dict[str, Any]:
        args = dict(filters or {})
        args.setdefault("include_archived", False)
        clauses, params = self._task_filter_clauses(args)
        if not clauses:
            raise BadRequest("At least one filter is required for batch archive")
        dry_run = _as_bool(args.get("dry_run"), False)
        where_sql = " AND ".join(clauses)
        with self.connect() as conn:
            with conn.cursor() as cur:
                if dry_run:
                    cur.execute(f"SELECT id FROM tasks WHERE {where_sql}", params)
                    ids = [row["id"] for row in cur.fetchall()]
                else:
                    cur.execute(
                        f"""
                        UPDATE tasks
                        SET status = 'archived',
                            archived_at = COALESCE(archived_at, NOW()),
                            updated_at = NOW()
                        WHERE {where_sql}
                        RETURNING id
                        """,
                        params,
                    )
                    ids = [row["id"] for row in cur.fetchall()]
            conn.commit()
        return {"count": len(ids), "ids": ids, "dry_run": dry_run}

    def list_agent_runs(self, args: dict[str, Any]) -> list[dict[str, Any]]:
        clauses, params = [], []
        status = _split_filter(args.get("status"))
        if status:
            clauses.append(_in_clause("r.status", status, params))
        repo = args.get("repo")
        if repo:
            clauses.append("r.repo = %s")
            params.append(repo)
        sql = f"""
            SELECT
                r.id, r.task_id, r.repo, r.status, r.agent_type, r.branch,
                r.pr_url, r.logs_summary, r.failure_reason, r.created_at,
                r.updated_at, r.started_at, r.finished_at,
                t.text AS task_text,
                t.category AS task_category,
                t.acceptance_criteria,
                t.review_owner,
                t.last_agent_update
            FROM agent_runs r
            JOIN tasks t ON t.id = r.task_id
            {("WHERE " + " AND ".join(clauses)) if clauses else ""}
            ORDER BY r.updated_at DESC
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [serialize_row(dict(row)) for row in cur.fetchall()]

    def update_agent_run(self, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        updates = {}
        for field in PATCHABLE_RUN_FIELDS:
            if field in payload:
                updates[field] = payload[field]
        if "status" in updates:
            updates["status"] = normalize_run_status(updates["status"])
        if not updates:
            raise BadRequest("No agent run fields to update")

        assignments = [f"{field} = %s" for field in updates.keys()]
        values = list(updates.values())
        status = updates.get("status")
        if status in {"running", "agent_running"}:
            assignments.append("started_at = COALESCE(started_at, NOW())")
        if status in {"human_review", "blocked", "merged", "archived"}:
            assignments.append("finished_at = COALESCE(finished_at, NOW())")
        assignments.append("updated_at = NOW()")
        values.append(run_id)

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE agent_runs
                    SET {", ".join(assignments)}
                    WHERE id = %s
                    RETURNING {", ".join(AGENT_RUN_FIELDS)}
                    """,
                    values,
                )
                run = cur.fetchone()
                if not run:
                    raise NotFound("Agent run not found")
                task_updates = {
                    "last_agent_update": payload.get("logs_summary")
                    or payload.get("failure_reason")
                    or payload.get("last_agent_update"),
                    "branch": payload.get("branch"),
                    "pr_url": payload.get("pr_url"),
                }
                if status:
                    task_updates["status"] = RUN_TO_TASK_STATUS[status]
                task_updates = {key: value for key, value in task_updates.items() if value is not None}
                if task_updates:
                    task_assignments = [f"{field} = %s" for field in task_updates.keys()]
                    task_assignments.append("updated_at = NOW()")
                    cur.execute(
                        f"""
                        UPDATE tasks
                        SET {", ".join(task_assignments)}
                        WHERE id = %s
                        """,
                        list(task_updates.values()) + [run["task_id"]],
                    )
            conn.commit()
        return serialize_row(dict(run))

    def _normalize_create_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        text = (payload.get("text") or "").strip()
        if not text:
            raise BadRequest("Task text is required")
        status = normalize_status(payload.get("status"))
        created = payload.get("created_at", payload.get("created"))
        task = {
            "id": str(payload.get("id") or millis_id()),
            "text": text,
            "category": (payload.get("category") or "general").strip() or "general",
            "status": status,
            "source": (payload.get("source") or "manual").strip() or "manual",
            "session_id": payload.get("session_id") or None,
            "created_at": _coerce_datetime(created) or utc_now(),
            "completed_at": _coerce_datetime(payload.get("completed_at") or payload.get("completedAt")),
            "archived_at": _coerce_datetime(payload.get("archived_at")),
            "repo": payload.get("repo") or None,
            "acceptance_criteria": payload.get("acceptance_criteria") or None,
            "agent_type": payload.get("agent_type") or None,
            "agent_run_id": payload.get("agent_run_id") or None,
            "branch": payload.get("branch") or None,
            "pr_url": payload.get("pr_url") or None,
            "last_agent_update": payload.get("last_agent_update") or None,
            "review_owner": payload.get("review_owner") or None,
        }
        if status == "done" and task["completed_at"] is None:
            task["completed_at"] = utc_now()
        if status == "archived" and task["archived_at"] is None:
            task["archived_at"] = utc_now()
        return task

    def _normalize_patch_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        updates = {}
        for field in PATCHABLE_TASK_FIELDS:
            if field in payload:
                updates[field] = payload[field]
        if "status" in updates:
            updates["status"] = normalize_status(updates["status"])
            if updates["status"] == "done" and "completed_at" not in updates:
                updates["completed_at"] = utc_now()
            if updates["status"] == "archived" and "archived_at" not in updates:
                updates["archived_at"] = utc_now()
            if updates["status"] not in {"done", "archived"}:
                if payload.get("clear_terminal_dates", True):
                    updates.setdefault("completed_at", None)
                    updates.setdefault("archived_at", None)
        for field in ("completed_at", "archived_at"):
            if field in updates:
                updates[field] = _coerce_datetime(updates[field])
        return updates

    def _task_filter_clauses(self, args: dict[str, Any]) -> tuple[list[str], list[Any]]:
        clauses, params = [], []
        status = _split_filter(args.get("status"))
        if status:
            invalid = [value for value in status if value not in TASK_STATUSES]
            if invalid:
                raise BadRequest(f"Unsupported task status filter: {', '.join(invalid)}")
            clauses.append(_in_clause("status", status, params))
        status_ne = args.get("status!=") or args.get("status_ne")
        if status_ne:
            clauses.append("status <> %s")
            params.append(status_ne)
        source = args.get("source")
        if source:
            clauses.append("source = %s")
            params.append(source)
        source_ne = args.get("source!=") or args.get("source_ne")
        if source_ne:
            clauses.append("source <> %s")
            params.append(source_ne)
        category = args.get("category")
        if category:
            clauses.append("category = %s")
            params.append(category)
        repo = args.get("repo")
        if repo:
            clauses.append("repo = %s")
            params.append(repo)
        if _as_bool(args.get("hide_generated"), False):
            clauses.append("source <> %s")
            params.append("claude-code")
        if not _as_bool(args.get("include_archived"), False):
            clauses.append("status <> %s")
            params.append("archived")
        return clauses, params


def _in_clause(field: str, values: list[str], params: list[Any]) -> str:
    placeholders = ", ".join(["%s"] * len(values))
    params.extend(values)
    return f"{field} IN ({placeholders})"
