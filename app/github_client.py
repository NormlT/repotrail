import asyncio
import logging
import re
from datetime import UTC, datetime

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

BASE_URL = "https://api.github.com"

# Module-level rate limit state
_rate_limit_remaining: int = 5000
_rate_limit_reset: str | None = None


def get_rate_limit_info() -> dict:
    return {
        "rate_limit_remaining": _rate_limit_remaining,
        "rate_limit_reset": _rate_limit_reset,
    }


def _headers() -> dict:
    settings = get_settings()
    h = {"Accept": "application/vnd.github.v3+json"}
    if settings.github_token:
        h["Authorization"] = f"Bearer {settings.github_token}"
    return h


def _update_rate_limit(response: httpx.Response) -> None:
    global _rate_limit_remaining, _rate_limit_reset
    remaining = response.headers.get("X-RateLimit-Remaining")
    reset = response.headers.get("X-RateLimit-Reset")
    if remaining is not None:
        _rate_limit_remaining = int(remaining)
    if reset is not None:
        _rate_limit_reset = datetime.fromtimestamp(int(reset), tz=UTC).isoformat()


def _parse_link_count(response: httpx.Response) -> int | None:
    """Extract total count from Link header's last page number."""
    link = response.headers.get("Link", "")
    if 'rel="last"' not in link:
        return None
    for part in link.split(","):
        if 'rel="last"' in part:
            match = re.search(r"[?&]page=(\d+)", part)
            if match:
                return int(match.group(1))
    return None


async def fetch_repos(client: httpx.AsyncClient) -> list[dict]:
    """Fetch all repos for the authenticated user, filtered by GITHUB_OWNER."""
    settings = get_settings()
    owner_filter = settings.github_owner.lower()
    repos = []
    page = 1
    while True:
        resp = await client.get(
            f"{BASE_URL}/user/repos",
            headers=_headers(),
            params={"per_page": 100, "page": page},
        )
        _update_rate_limit(resp)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        for r in data:
            repo_owner = r["owner"]["login"]
            if repo_owner.lower() != owner_filter:
                continue
            repos.append(
                {
                    "owner": repo_owner,
                    "name": r["name"],
                    "description": r.get("description"),
                    "private": r["private"],
                    "language": r.get("language"),
                    "stars": r.get("stargazers_count", 0),
                    "forks": r.get("forks_count", 0),
                    "open_issues": r.get("open_issues_count", 0),
                    "created_at": r.get("created_at"),
                    "updated_at": r.get("updated_at"),
                }
            )
        if len(data) < 100:
            break
        page += 1
    return repos


async def fetch_traffic_views(client: httpx.AsyncClient, owner: str, repo: str) -> list[dict]:
    resp = await client.get(
        f"{BASE_URL}/repos/{owner}/{repo}/traffic/views",
        headers=_headers(),
    )
    _update_rate_limit(resp)
    if resp.status_code == 403:
        logger.warning("No push access for %s traffic/views", repo)
        return []
    resp.raise_for_status()
    data = resp.json()
    return [{"date": v["timestamp"][:10], "count": v["count"], "uniques": v["uniques"]} for v in data.get("views", [])]


async def fetch_traffic_clones(client: httpx.AsyncClient, owner: str, repo: str) -> list[dict]:
    resp = await client.get(
        f"{BASE_URL}/repos/{owner}/{repo}/traffic/clones",
        headers=_headers(),
    )
    _update_rate_limit(resp)
    if resp.status_code == 403:
        logger.warning("No push access for %s traffic/clones", repo)
        return []
    resp.raise_for_status()
    data = resp.json()
    return [{"date": c["timestamp"][:10], "count": c["count"], "uniques": c["uniques"]} for c in data.get("clones", [])]


async def fetch_referrers(client: httpx.AsyncClient, owner: str, repo: str) -> list[dict]:
    resp = await client.get(
        f"{BASE_URL}/repos/{owner}/{repo}/traffic/popular/referrers",
        headers=_headers(),
    )
    _update_rate_limit(resp)
    if resp.status_code == 403:
        return []
    resp.raise_for_status()
    return [{"referrer": r["referrer"], "count": r["count"], "uniques": r["uniques"]} for r in resp.json()]


async def fetch_paths(client: httpx.AsyncClient, owner: str, repo: str) -> list[dict]:
    resp = await client.get(
        f"{BASE_URL}/repos/{owner}/{repo}/traffic/popular/paths",
        headers=_headers(),
    )
    _update_rate_limit(resp)
    if resp.status_code == 403:
        return []
    resp.raise_for_status()
    return [
        {"path": p["path"], "title": p.get("title", ""), "count": p["count"], "uniques": p["uniques"]}
        for p in resp.json()
    ]


async def fetch_pr_count(client: httpx.AsyncClient, owner: str, repo: str, state: str) -> int:
    """Get PR count using per_page=1 + Link header trick."""
    resp = await client.get(
        f"{BASE_URL}/repos/{owner}/{repo}/pulls",
        headers=_headers(),
        params={"state": state, "per_page": 1},
    )
    _update_rate_limit(resp)
    if resp.status_code == 404:
        return 0
    resp.raise_for_status()
    count = _parse_link_count(resp)
    if count is not None:
        return count
    return len(resp.json())


async def fetch_contributor_count(client: httpx.AsyncClient, owner: str, repo: str) -> int:
    resp = await client.get(
        f"{BASE_URL}/repos/{owner}/{repo}/contributors",
        headers=_headers(),
        params={"per_page": 1, "anon": "true"},
    )
    _update_rate_limit(resp)
    if resp.status_code in (403, 404):
        return 0
    resp.raise_for_status()
    count = _parse_link_count(resp)
    if count is not None:
        return count
    return len(resp.json())


async def fetch_commit_activity(client: httpx.AsyncClient, owner: str, repo: str) -> list[dict]:
    """Fetch weekly commit activity. May need retry due to 202 (GitHub computing)."""
    for attempt in range(2):
        resp = await client.get(
            f"{BASE_URL}/repos/{owner}/{repo}/stats/commit_activity",
            headers=_headers(),
        )
        _update_rate_limit(resp)
        if resp.status_code == 202:
            if attempt == 0:
                await asyncio.sleep(2)
                continue
            return []
        if resp.status_code in (403, 404):
            return []
        resp.raise_for_status()
        data = resp.json()
        return [
            {"week": datetime.fromtimestamp(w["week"], tz=UTC).strftime("%Y-%m-%d"), "total": w["total"]} for w in data
        ]
    return []
