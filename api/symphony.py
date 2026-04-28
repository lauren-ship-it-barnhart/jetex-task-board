"""Symphony controller tick for the task board agent queue."""
from __future__ import annotations

import json
import os
import time

from task_store import PostgresTaskStore


DEFAULT_REPO = os.environ.get("SYMPHONY_DEFAULT_REPO", "lauren-ship-it-barnhart/jetex-task-board")
STALE_MINUTES = int(os.environ.get("SYMPHONY_STALE_MINUTES", "90"))


def millis_id() -> str:
    return str(int(time.time() * 1000))


def promote_flagged_tasks(store: PostgresTaskStore) -> list[str]:
    """Queue tasks that have been explicitly prepared for an agent.

    A task is considered prepared when it is still in a human-work status but
    already has agent_type, repo, and acceptance_criteria populated. This keeps
    Symphony conservative: normal open tasks stay human-owned until someone
    marks them as agent-ready through the UI or API.
    """
    promoted = []
    with store.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, repo, agent_type, agent_run_id, branch
                FROM tasks
                WHERE status IN ('open', 'urgent', 'waiting')
                  AND COALESCE(agent_type, '') <> ''
                  AND COALESCE(repo, '') <> ''
                  AND COALESCE(acceptance_criteria, '') <> ''
                ORDER BY created_at ASC
                LIMIT 10
                FOR UPDATE SKIP LOCKED
                """
            )
            rows = cur.fetchall()
            for row in rows:
                task_id = row["id"]
                run_id = row["agent_run_id"] or f"run-{task_id}-{millis_id()}"
                branch = row["branch"] or f"codex/task-{task_id}"
                repo = row["repo"] or DEFAULT_REPO
                agent_type = row["agent_type"] or "codex"
                cur.execute(
                    """
                    UPDATE tasks
                    SET status = 'ready_for_agent',
                        repo = %s,
                        agent_type = %s,
                        agent_run_id = %s,
                        branch = %s,
                        last_agent_update = 'Symphony queued explicitly flagged task',
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    [repo, agent_type, run_id, branch, task_id],
                )
                promoted.append(task_id)
        conn.commit()
    return promoted


def ensure_agent_runs(store: PostgresTaskStore) -> list[str]:
    """Make queue state durable for every ready/running/review task."""
    ensured = []
    with store.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, repo, agent_type, agent_run_id, branch
                FROM tasks
                WHERE status IN ('ready_for_agent', 'agent_running', 'human_review', 'blocked', 'merged')
                ORDER BY updated_at DESC
                """
            )
            for task in cur.fetchall():
                task_id = task["id"]
                repo = task["repo"] or DEFAULT_REPO
                agent_type = task["agent_type"] or "codex"
                run_id = task["agent_run_id"] or f"run-{task_id}-{millis_id()}"
                branch = task["branch"] or f"codex/task-{task_id}"
                cur.execute(
                    """
                    UPDATE tasks
                    SET repo = COALESCE(NULLIF(repo, ''), %s),
                        agent_type = COALESCE(NULLIF(agent_type, ''), %s),
                        agent_run_id = COALESCE(NULLIF(agent_run_id, ''), %s),
                        branch = COALESCE(NULLIF(branch, ''), %s),
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    [repo, agent_type, run_id, branch, task_id],
                )
                cur.execute(
                    """
                    INSERT INTO agent_runs (id, task_id, repo, status, agent_type, branch)
                    VALUES (%s, %s, %s, 'queued', %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    [run_id, task_id, repo, agent_type, branch],
                )
                ensured.append(task_id)
        conn.commit()
    return ensured


def block_stale_running_tasks(store: PostgresTaskStore) -> list[str]:
    blocked = []
    with store.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET status = 'blocked',
                    last_agent_update = %s,
                    updated_at = NOW()
                WHERE status = 'agent_running'
                  AND updated_at < NOW() - (%s * INTERVAL '1 minute')
                RETURNING id, agent_run_id
                """,
                [f"Symphony blocked stale run after {STALE_MINUTES} minutes", STALE_MINUTES],
            )
            rows = cur.fetchall()
            for row in rows:
                blocked.append(row["id"])
                if row["agent_run_id"]:
                    cur.execute(
                        """
                        UPDATE agent_runs
                        SET status = 'blocked',
                            failure_reason = %s,
                            logs_summary = %s,
                            finished_at = COALESCE(finished_at, NOW()),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        [
                            f"Stale after {STALE_MINUTES} minutes",
                            f"Symphony blocked stale run after {STALE_MINUTES} minutes",
                            row["agent_run_id"],
                        ],
                    )
        conn.commit()
    return blocked


def main() -> int:
    store = PostgresTaskStore(os.environ["DATABASE_URL"])
    summary = {
        "promoted": promote_flagged_tasks(store),
        "ensured_agent_runs": ensure_agent_runs(store),
        "blocked_stale": block_stale_running_tasks(store),
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
