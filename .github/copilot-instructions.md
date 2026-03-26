# Copilot Instructions

## What this repo does
- RepoTrail is a FastAPI app that archives GitHub repository traffic and metadata into SQLite so dashboards can show history beyond GitHub's short retention window.
- It tracks views, clones, referrers, popular paths, issues, pull requests, and contributors for all repositories under a configured owner.

## Architecture hotspots
- `app\main.py`: FastAPI entry point, API routes, scheduler setup, manual refresh logic, and fetch orchestration.
- `app\database.py`: SQLite schema, upserts, and read queries. Keep storage changes centralized here.
- `app\github_client.py`: GitHub API access, pagination, and rate-limit handling.
- `app\config.py`: environment-backed settings validation.
- `app\static\index.html`: single-file dashboard UI. There is no frontend build step.
- `.github\workflows\license-check.yml`: CI check that enforces dependency license rules.

## Commands that exist
```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
bash dev.sh
bash dev.sh --reset
ruff check app
ruff format app
docker compose up --build -d
docker compose down
```
- The Docker image runs `uvicorn app.main:app --host 0.0.0.0 --port 8055`.
- There is currently no automated test suite.

## Environment and runtime notes
- Required in `.env`: `GITHUB_TOKEN` and `GITHUB_OWNER`.
- `GITHUB_TOKEN` needs access to the repositories being tracked; traffic endpoints require push-level access on the target repos.
- Useful optional settings: `API_KEY`, `FETCH_INTERVAL_HOURS`, `PORT`, and `DB_PATH`.
- Runtime data lives in `data\repotrail.db` by default.
- The app schedules background syncs and also exposes manual refresh; avoid introducing overlapping fetch paths outside the existing lock/cooldown flow in `app\main.py`.
- Keep rate-limit-sensitive GitHub logic in `app\github_client.py` or the existing fetch orchestration.

## Conventions for future changes
- Keep API/client code, database logic, and UI changes in their current layers instead of mixing concerns in route handlers.
- Prefer small, surgical changes and preserve the single-file frontend approach unless the repo is intentionally restructured.
- Run Ruff after Python changes. If you add dependencies, make sure they remain compatible with the license-check workflow.
