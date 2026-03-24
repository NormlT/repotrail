import asyncio
import logging
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from app import database as db
from app import github_client as gh
from app.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s -- %(message)s")
logger = logging.getLogger(__name__)

# --- Fetch state ---
_fetch_lock = asyncio.Lock()
_is_fetching = False
_last_refresh_time: float = 0
REFRESH_COOLDOWN = 60  # seconds
GITHUB_RATE_LIMIT_THRESHOLD = 100


async def fetch_all_repos(include_private: bool = True) -> int:
    """Fetch stats for all repos from GitHub. Returns count of repos fetched."""
    global _is_fetching
    async with _fetch_lock:
        if _is_fetching:
            logger.warning("Fetch already in progress, skipping")
            return 0
        _is_fetching = True
        return await _do_fetch(include_private=include_private)


async def _do_fetch(include_private: bool = True) -> int:
    global _is_fetching
    log_id = await db.start_fetch_log()
    count = 0

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30)) as client:
            # Discover repos
            repos = await gh.fetch_repos(client)
            if not include_private:
                repos = [r for r in repos if not r["private"]]
            logger.info("Discovered %d repos (include_private=%s)", len(repos), include_private)

            today = datetime.now(UTC).strftime("%Y-%m-%d")

            for repo_data in repos:
                owner = repo_data["owner"]
                name = repo_data["name"]
                rate_info = gh.get_rate_limit_info()
                remaining = rate_info["rate_limit_remaining"]

                try:
                    # Always upsert repo metadata
                    await db.upsert_repo(repo_data)

                    # Traffic (requires push access)
                    views = await gh.fetch_traffic_views(client, owner, name)
                    await db.upsert_traffic_views(name, views)

                    clones = await gh.fetch_traffic_clones(client, owner, name)
                    await db.upsert_traffic_clones(name, clones)

                    # Skip non-essential if rate limit is low
                    if remaining > GITHUB_RATE_LIMIT_THRESHOLD:
                        referrers = await gh.fetch_referrers(client, owner, name)
                        await db.replace_referrers(name, today, referrers)

                        paths = await gh.fetch_paths(client, owner, name)
                        await db.replace_paths(name, today, paths)

                    # PR counts and contributors
                    open_prs = await gh.fetch_pr_count(client, owner, name, "open")
                    closed_prs = await gh.fetch_pr_count(client, owner, name, "closed")
                    contributors = await gh.fetch_contributor_count(client, owner, name)

                    # Daily snapshot
                    await db.upsert_daily_snapshot(
                        name,
                        today,
                        {
                            "stars": repo_data.get("stars", 0),
                            "forks": repo_data.get("forks", 0),
                            "open_issues": repo_data.get("open_issues", 0),
                            "open_prs": open_prs,
                            "closed_prs": closed_prs,
                            "contributors": contributors,
                        },
                    )

                    count += 1
                    logger.info("Fetched stats for %s (%d/%d)", name, count, len(repos))

                except httpx.HTTPStatusError as e:
                    logger.warning("HTTP error fetching %s: %s", name, e)
                except Exception as e:
                    logger.error("Error fetching %s: %s", name, e)

        await db.complete_fetch_log(log_id, count)
        logger.info("Fetch complete: %d repos", count)

    except Exception as e:
        logger.error("Fetch failed: %s", e)
        await db.complete_fetch_log(log_id, count, status="error", error=str(e))

    finally:
        _is_fetching = False

    return count


def _on_fetch_task_done(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        logger.error("Background fetch failed: %s", exc)


# --- Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    if not settings.github_token:
        logger.warning("GITHUB_TOKEN is not set -- traffic endpoints require authentication")

    await db.init_db()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        fetch_all_repos,
        "interval",
        hours=settings.fetch_interval_hours,
        id="fetch_github",
        max_instances=1,
        misfire_grace_time=300,
    )
    scheduler.start()

    # Initial fetch on startup
    logger.info("Starting initial fetch...")
    try:
        count = await fetch_all_repos()
        logger.info("Initial fetch complete: %d repos", count)
    except Exception as e:
        logger.error("Initial fetch failed: %s", e)

    yield

    scheduler.shutdown()


# --- App ---
app = FastAPI(
    title="RepoTrail",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        settings = get_settings()
        if not settings.api_key:
            return await call_next(request)

        path = request.url.path
        if not path.startswith("/api/"):
            return await call_next(request)

        auth = request.headers.get("Authorization", "")
        if auth == f"Bearer {settings.api_key}":
            return await call_next(request)

        return JSONResponse(status_code=401, content={"error": "Unauthorized"})


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "connect-src 'self'"
        )
        response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
        return response


app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(AuthMiddleware)


@app.exception_handler(sqlite3.Error)
async def sqlite_error_handler(request: Request, exc: sqlite3.Error) -> JSONResponse:
    logger.error("Database error on %s: %s", request.url.path, exc)
    return JSONResponse(status_code=503, content={"error": "Database temporarily unavailable"})


# --- Snapshot helper ---
def _latest_snapshot(snapshots: list[dict] | None) -> dict:
    """Return the last snapshot from a non-empty list, or an empty dict."""
    if snapshots:
        return snapshots[-1]
    return {}


# --- Period helper ---
def _period_to_since(period: str) -> str | None:
    now = datetime.now(UTC)
    match period:
        case "1w":
            return (now - timedelta(weeks=1)).strftime("%Y-%m-%d")
        case "1m":
            return (now - timedelta(days=30)).strftime("%Y-%m-%d")
        case "3m":
            return (now - timedelta(days=90)).strftime("%Y-%m-%d")
        case "1y":
            return (now - timedelta(days=365)).strftime("%Y-%m-%d")
        case "all":
            return None
        case _:
            return (now - timedelta(days=30)).strftime("%Y-%m-%d")


# --- API Routes ---
@app.get("/api/repos")
async def api_repos(include_private: bool = Query(True)) -> dict:
    repos = await db.get_repos(include_private=include_private)
    result = []
    for r in repos:
        snapshots = await db.get_daily_snapshots(r["name"], None)
        latest = _latest_snapshot(snapshots)
        views = await db.get_traffic_views(r["name"], None)
        total_views = sum(v["count"] for v in views)
        result.append(
            {
                "name": r["name"],
                "description": r["description"],
                "private": r["private"],
                "language": r["language"],
                "stars": r["stars"],
                "forks": r["forks"],
                "open_issues": r["open_issues"],
                "open_prs": latest.get("open_prs", 0),
                "closed_prs": latest.get("closed_prs", 0),
                "total_views": total_views,
            }
        )
    return {"repos": result}


@app.get("/api/repos/{repo}/stats", response_model=None)
async def api_repo_stats(repo: str, period: str = Query("1m", pattern=r"^(1w|1m|3m|1y|all)$")) -> dict | JSONResponse:
    repo_data = await db.get_repo(repo)
    if not repo_data:
        return JSONResponse(status_code=404, content={"error": "Repo not found"})

    since = _period_to_since(period)
    views = await db.get_traffic_views(repo, since)
    clones = await db.get_traffic_clones(repo, since)
    snapshots = await db.get_daily_snapshots(repo, since)

    # Get latest snapshot for current counters
    latest_snapshot = _latest_snapshot(snapshots)

    # Fetch commit activity live (not cached) -- skip if rate limit is low
    commits = []
    rate_info = gh.get_rate_limit_info()
    if rate_info["rate_limit_remaining"] > GITHUB_RATE_LIMIT_THRESHOLD:
        try:
            owner = repo_data.get("owner", get_settings().github_owner)
            async with httpx.AsyncClient(timeout=httpx.Timeout(15)) as client:
                all_commits = await gh.fetch_commit_activity(client, owner, repo)
                if since:
                    commits = [c for c in all_commits if c["week"] >= since]
                else:
                    commits = all_commits
        except Exception as e:
            logger.warning("Failed to fetch commit activity for %s: %s", repo, e)

    return {
        "repo": repo,
        "period": period,
        "current": {
            "stars": repo_data.get("stars", 0),
            "forks": repo_data.get("forks", 0),
            "open_issues": repo_data.get("open_issues", 0),
            "open_prs": latest_snapshot.get("open_prs", 0),
            "closed_prs": latest_snapshot.get("closed_prs", 0),
            "contributors": latest_snapshot.get("contributors", 0),
        },
        "views": views,
        "clones": clones,
        "commits": commits,
        "snapshots": [{"date": s["date"], "stars": s["stars"], "forks": s["forks"]} for s in snapshots],
    }


@app.get("/api/repos/{repo}/referrers", response_model=None)
async def api_repo_referrers(repo: str) -> dict | JSONResponse:
    repo_data = await db.get_repo(repo)
    if not repo_data:
        return JSONResponse(status_code=404, content={"error": "Repo not found"})
    referrers = await db.get_latest_referrers(repo)
    return {"repo": repo, "referrers": referrers}


@app.get("/api/repos/{repo}/paths", response_model=None)
async def api_repo_paths(repo: str) -> dict | JSONResponse:
    repo_data = await db.get_repo(repo)
    if not repo_data:
        return JSONResponse(status_code=404, content={"error": "Repo not found"})
    paths = await db.get_latest_paths(repo)
    return {"repo": repo, "paths": paths}


@app.post("/api/refresh")
async def api_refresh(include_private: bool = Query(True)) -> JSONResponse:
    global _last_refresh_time

    if _is_fetching:
        return JSONResponse(
            status_code=429,
            content={"error": "Fetch already in progress", "detail": "Poll /api/status for completion"},
        )

    now = time.time()
    elapsed = now - _last_refresh_time
    if elapsed < REFRESH_COOLDOWN:
        remaining = int(REFRESH_COOLDOWN - elapsed)
        return JSONResponse(
            status_code=429,
            content={"error": "Cooldown active", "detail": f"Try again in {remaining} seconds"},
        )

    _last_refresh_time = now
    task = asyncio.create_task(fetch_all_repos(include_private=include_private))
    task.add_done_callback(_on_fetch_task_done)
    return JSONResponse(status_code=202, content={"message": "Fetch started", "poll": "/api/status"})


@app.get("/api/status")
async def api_status() -> dict:
    last_fetch = await db.get_last_fetch()
    rate_info = gh.get_rate_limit_info()

    now = time.time()
    cooldown = max(0, int(REFRESH_COOLDOWN - (now - _last_refresh_time)))

    return {
        "last_fetch": last_fetch.get("completed_at") if last_fetch else None,
        "last_fetch_status": last_fetch.get("status") if last_fetch else None,
        "repos_fetched": last_fetch.get("repos_fetched", 0) if last_fetch else 0,
        "is_fetching": _is_fetching,
        "rate_limit_remaining": rate_info["rate_limit_remaining"],
        "rate_limit_reset": rate_info["rate_limit_reset"],
        "refresh_cooldown_seconds": cooldown,
    }


# --- Static files ---
STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/{full_path:path}", response_model=None)
async def spa(full_path: str) -> FileResponse | JSONResponse:
    sensitive = (".", "api/", "admin", "wp-", "config", "phpinfo")
    if any(full_path.startswith(s) for s in sensitive):
        return JSONResponse(status_code=404, content={"error": "Not found"})
    return FileResponse(STATIC_DIR / "index.html")
