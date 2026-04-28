"""Cloud Run job entrypoint for the Codex worker pilot."""
from __future__ import annotations

import os
import shlex
import subprocess
import sys

from task_store import PostgresTaskStore


DEFAULT_REPO = "lauren-ship-it-barnhart/jetex-task-board"


def env_required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def run(command: list[str] | str, cwd: str | None = None, extra_env: dict[str, str] | None = None) -> str:
    shell = isinstance(command, str)
    completed = subprocess.run(
        command,
        cwd=cwd,
        shell=shell,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, **(extra_env or {})},
    )
    return (completed.stdout + completed.stderr).strip()


def task_prompt(task: dict) -> str:
    return "\n".join([
        f"Task ID: {task['id']}",
        f"Task: {task.get('text', '')}",
        f"Repo: {task.get('repo') or DEFAULT_REPO}",
        "",
        "Acceptance criteria:",
        task.get("acceptance_criteria") or "Implement the task and preserve existing behavior.",
        "",
        "Rules:",
        "- Create a focused patch for this task only.",
        "- Run the configured checks.",
        "- Do not merge or deploy.",
    ])


def create_draft_pr(repo_dir: str, branch: str, task: dict) -> str:
    title = f"Codex task {task['id']}: {task.get('text', '')[:80]}"
    body = "\n".join([
        "Automated Codex worker draft PR.",
        "",
        f"Task ID: {task['id']}",
        "",
        "Acceptance criteria:",
        task.get("acceptance_criteria") or "",
    ])
    output = run(
        [
            "gh",
            "pr",
            "create",
            "--draft",
            "--title",
            title,
            "--body",
            body,
        ],
        cwd=repo_dir,
    )
    for line in output.splitlines():
        if line.startswith("http://") or line.startswith("https://"):
            return line.strip()
    return output.strip()


def main() -> int:
    store = PostgresTaskStore(env_required("DATABASE_URL"))
    repo = os.environ.get("WORKER_REPO", DEFAULT_REPO)
    repo_dir = env_required("WORKER_REPO_DIR")
    base_ref = os.environ.get("WORKER_BASE_REF", "origin/main")
    codex_command = env_required("CODEX_COMMAND")
    check_command = os.environ.get("CHECK_COMMAND", "")
    worker_label = os.environ.get("WORKER_LABEL", "codex-worker")

    task = store.claim_ready_task(repo=repo, agent_type="codex", worker_label=worker_label)
    if not task:
        print("No ready_for_agent tasks to claim")
        return 0

    run_id = task["agent_run_id"]
    branch = task["branch"] or f"codex/task-{task['id']}"
    prompt = task_prompt(task)

    try:
        run(["git", "fetch", "origin"], cwd=repo_dir)
        run(["git", "checkout", "-B", branch, base_ref], cwd=repo_dir)
        run(codex_command, cwd=repo_dir, extra_env={"CODEX_TASK_PROMPT": prompt})
        if check_command:
            run(check_command, cwd=repo_dir)
        status = run(["git", "status", "--short"], cwd=repo_dir)
        if not status:
            raise RuntimeError("Codex worker produced no file changes")
        run(["git", "add", "-A"], cwd=repo_dir)
        run(["git", "commit", "-m", f"Codex task {task['id']}"], cwd=repo_dir)
        run(["git", "push", "--set-upstream", "origin", branch], cwd=repo_dir)
        pr_url = create_draft_pr(repo_dir, branch, task)
        store.update_agent_run(run_id, {
            "status": "human_review",
            "branch": branch,
            "pr_url": pr_url,
            "logs_summary": "Codex worker completed checks and opened a draft PR.",
        })
        print(f"Opened draft PR: {pr_url}")
        return 0
    except subprocess.CalledProcessError as exc:
        command = exc.cmd if isinstance(exc.cmd, str) else " ".join(shlex.quote(part) for part in exc.cmd)
        detail = (exc.stdout or "") + (exc.stderr or "")
        reason = f"Command failed: {command}\n{detail[-3000:]}"
        store.update_agent_run(run_id, {
            "status": "blocked",
            "failure_reason": reason,
            "logs_summary": reason,
        })
        print(reason, file=sys.stderr)
        return 1
    except Exception as exc:
        reason = f"Worker failed: {exc}"
        store.update_agent_run(run_id, {
            "status": "blocked",
            "failure_reason": reason,
            "logs_summary": reason,
        })
        print(reason, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
