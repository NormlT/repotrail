import logging
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite

from app.config import get_settings

logger = logging.getLogger(__name__)


def _connect() -> aiosqlite.Connection:
    settings = get_settings()
    return aiosqlite.connect(settings.db_path, timeout=30)


async def init_db() -> None:
    settings = get_settings()
    data_dir = Path(settings.db_path).parent
    data_dir.mkdir(parents=True, exist_ok=True)

    async with _connect() as db:
        await db.execute("PRAGMA journal_mode=WAL")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS repos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner TEXT NOT NULL DEFAULT '',
                name TEXT UNIQUE NOT NULL,
                description TEXT,
                private BOOLEAN DEFAULT 0,
                language TEXT,
                stars INTEGER DEFAULT 0,
                forks INTEGER DEFAULT 0,
                open_issues INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS traffic_views (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_name TEXT NOT NULL,
                date TEXT NOT NULL,
                count INTEGER DEFAULT 0,
                uniques INTEGER DEFAULT 0,
                UNIQUE(repo_name, date)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS traffic_clones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_name TEXT NOT NULL,
                date TEXT NOT NULL,
                count INTEGER DEFAULT 0,
                uniques INTEGER DEFAULT 0,
                UNIQUE(repo_name, date)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS traffic_referrers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_name TEXT NOT NULL,
                date TEXT NOT NULL,
                referrer TEXT NOT NULL,
                count INTEGER DEFAULT 0,
                uniques INTEGER DEFAULT 0
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS traffic_paths (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_name TEXT NOT NULL,
                date TEXT NOT NULL,
                path TEXT NOT NULL,
                title TEXT,
                count INTEGER DEFAULT 0,
                uniques INTEGER DEFAULT 0
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS daily_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_name TEXT NOT NULL,
                date TEXT NOT NULL,
                stars INTEGER DEFAULT 0,
                forks INTEGER DEFAULT 0,
                open_issues INTEGER DEFAULT 0,
                open_prs INTEGER DEFAULT 0,
                closed_prs INTEGER DEFAULT 0,
                contributors INTEGER DEFAULT 0,
                UNIQUE(repo_name, date)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS fetch_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                repos_fetched INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running',
                error TEXT
            )
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_referrers_repo_date ON traffic_referrers(repo_name, date)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_paths_repo_date ON traffic_paths(repo_name, date)")

        # Migration: add owner column if missing (pre-v2 databases)
        async with db.execute("PRAGMA table_info(repos)") as cur:
            columns = [row[1] for row in await cur.fetchall()]
        if "owner" not in columns:
            await db.execute("ALTER TABLE repos ADD COLUMN owner TEXT NOT NULL DEFAULT ''")

        await db.commit()
    logger.info("Database initialized at %s", settings.db_path)


async def upsert_repo(repo: dict) -> None:
    async with _connect() as db:
        await db.execute(
            """INSERT INTO repos (owner, name, description, private, language, stars, forks, open_issues, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                 owner=excluded.owner, description=excluded.description, private=excluded.private,
                 language=excluded.language, stars=excluded.stars, forks=excluded.forks,
                 open_issues=excluded.open_issues, updated_at=excluded.updated_at""",
            (
                repo.get("owner", ""),
                repo["name"],
                repo.get("description"),
                repo.get("private", False),
                repo.get("language"),
                repo.get("stars", 0),
                repo.get("forks", 0),
                repo.get("open_issues", 0),
                repo.get("created_at"),
                repo.get("updated_at"),
            ),
        )
        await db.commit()


async def upsert_traffic_views(repo_name: str, views: list[dict]) -> None:
    if not views:
        return
    async with _connect() as db:
        await db.executemany(
            """INSERT INTO traffic_views (repo_name, date, count, uniques)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(repo_name, date) DO UPDATE SET count=excluded.count, uniques=excluded.uniques""",
            [(repo_name, v["date"], v["count"], v["uniques"]) for v in views],
        )
        await db.commit()


async def upsert_traffic_clones(repo_name: str, clones: list[dict]) -> None:
    if not clones:
        return
    async with _connect() as db:
        await db.executemany(
            """INSERT INTO traffic_clones (repo_name, date, count, uniques)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(repo_name, date) DO UPDATE SET count=excluded.count, uniques=excluded.uniques""",
            [(repo_name, c["date"], c["count"], c["uniques"]) for c in clones],
        )
        await db.commit()


async def replace_referrers(repo_name: str, date: str, referrers: list[dict]) -> None:
    async with _connect() as db:
        await db.execute("DELETE FROM traffic_referrers WHERE repo_name = ? AND date = ?", (repo_name, date))
        if referrers:
            await db.executemany(
                "INSERT INTO traffic_referrers (repo_name, date, referrer, count, uniques) VALUES (?, ?, ?, ?, ?)",
                [(repo_name, date, r["referrer"], r["count"], r["uniques"]) for r in referrers],
            )
        await db.commit()


async def replace_paths(repo_name: str, date: str, paths: list[dict]) -> None:
    async with _connect() as db:
        await db.execute("DELETE FROM traffic_paths WHERE repo_name = ? AND date = ?", (repo_name, date))
        if paths:
            await db.executemany(
                "INSERT INTO traffic_paths (repo_name, date, path, title, count, uniques) VALUES (?, ?, ?, ?, ?, ?)",
                [(repo_name, date, p["path"], p.get("title", ""), p["count"], p["uniques"]) for p in paths],
            )
        await db.commit()


async def upsert_daily_snapshot(repo_name: str, date: str, snapshot: dict) -> None:
    async with _connect() as db:
        await db.execute(
            """INSERT INTO daily_snapshots (repo_name, date, stars, forks, open_issues, open_prs, closed_prs, contributors)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(repo_name, date) DO UPDATE SET
                 stars=excluded.stars, forks=excluded.forks, open_issues=excluded.open_issues,
                 open_prs=excluded.open_prs, closed_prs=excluded.closed_prs, contributors=excluded.contributors""",
            (
                repo_name,
                date,
                snapshot.get("stars", 0),
                snapshot.get("forks", 0),
                snapshot.get("open_issues", 0),
                snapshot.get("open_prs", 0),
                snapshot.get("closed_prs", 0),
                snapshot.get("contributors", 0),
            ),
        )
        await db.commit()


async def start_fetch_log() -> int:
    async with _connect() as db:
        cursor = await db.execute(
            "INSERT INTO fetch_log (started_at, status) VALUES (?, 'running')",
            (datetime.now(UTC).isoformat(),),
        )
        await db.commit()
        return cursor.lastrowid


async def complete_fetch_log(
    log_id: int, repos_fetched: int, status: str = "success", error: str | None = None
) -> None:
    async with _connect() as db:
        await db.execute(
            "UPDATE fetch_log SET completed_at = ?, repos_fetched = ?, status = ?, error = ? WHERE id = ?",
            (datetime.now(UTC).isoformat(), repos_fetched, status, error, log_id),
        )
        await db.commit()


async def get_last_fetch() -> dict | None:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM fetch_log ORDER BY id DESC LIMIT 1") as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_repos(include_private: bool = True) -> list[dict]:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        if include_private:
            query = "SELECT * FROM repos ORDER BY name"
            params = ()
        else:
            query = "SELECT * FROM repos WHERE private = 0 ORDER BY name"
            params = ()
        async with db.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_repo(name: str) -> dict | None:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM repos WHERE name = ?", (name,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_traffic_views(repo_name: str, since: str | None = None) -> list[dict]:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        if since:
            query = "SELECT date, count, uniques FROM traffic_views WHERE repo_name = ? AND date >= ? ORDER BY date"
            params = (repo_name, since)
        else:
            query = "SELECT date, count, uniques FROM traffic_views WHERE repo_name = ? ORDER BY date"
            params = (repo_name,)
        async with db.execute(query, params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_traffic_clones(repo_name: str, since: str | None = None) -> list[dict]:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        if since:
            query = "SELECT date, count, uniques FROM traffic_clones WHERE repo_name = ? AND date >= ? ORDER BY date"
            params = (repo_name, since)
        else:
            query = "SELECT date, count, uniques FROM traffic_clones WHERE repo_name = ? ORDER BY date"
            params = (repo_name,)
        async with db.execute(query, params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_latest_referrers(repo_name: str) -> list[dict]:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT referrer, count, uniques FROM traffic_referrers
               WHERE repo_name = ? AND date = (SELECT MAX(date) FROM traffic_referrers WHERE repo_name = ?)
               ORDER BY count DESC""",
            (repo_name, repo_name),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_latest_paths(repo_name: str) -> list[dict]:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT path, title, count, uniques FROM traffic_paths
               WHERE repo_name = ? AND date = (SELECT MAX(date) FROM traffic_paths WHERE repo_name = ?)
               ORDER BY count DESC""",
            (repo_name, repo_name),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_daily_snapshots(repo_name: str, since: str | None = None) -> list[dict]:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        if since:
            query = "SELECT * FROM daily_snapshots WHERE repo_name = ? AND date >= ? ORDER BY date"
            params = (repo_name, since)
        else:
            query = "SELECT * FROM daily_snapshots WHERE repo_name = ? ORDER BY date"
            params = (repo_name,)
        async with db.execute(query, params) as cur:
            return [dict(r) for r in await cur.fetchall()]
