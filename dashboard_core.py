"""
generate_dashboard.py
Pulls livestream analytics from PostgreSQL and generates a 4-level
static HTML dashboard:
  index.html                       <- org cards
  {org}/index.html                 <- channel list per org
  {org}/{channel}/index.html       <- stream cards per channel
  {org}/{channel}/{video}.html     <- stream detail + charts

Partial build algorithm:
  - A manifest (dashboard/manifest.json) tracks every stream page.
  - On each run, only stream pages that are NEW or currently LIVE are
    (re)generated. Their parent channel and org pages are then also
    regenerated to reflect updated stream counts / card lists.
  - The index page is always regenerated (trivially cheap).
  - Unchanged stream pages (VOD, already in manifest) are never touched.

Org membership is driven by the ORG_MAP dict below.
"""

import os
import re
import json
import shutil
import sqlite3
import logging
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

_LOCAL_TZ = ZoneInfo("Asia/Jakarta")

def _now_local() -> datetime:
    return datetime.now(_LOCAL_TZ)

try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_AVAILABLE = True
except ImportError:
    # Every function that actually needs psycopg2 (get_conn, get_channel_rows,
    # get_all_streams_bulk, etc.) is only ever called from DB-dependent entry
    # points — generate_live.py, generate_backfill.py, force_regen_pages.py —
    # which install requirements.txt and always have it. generate_blog.py is
    # the one caller that imports this module purely for its HTML helpers
    # (_html_head, _breadcrumb, esc, slugify) and never touches the DB, so it
    # shouldn't need to install psycopg2-binary's compiled wheel just to
    # satisfy an import it never uses. Mirrors the googleapiclient guard right
    # below — if a DB function is called without psycopg2 installed, it fails
    # loudly with a clear NameError at the call site, not a silent no-op.
    _PSYCOPG2_AVAILABLE = False 
  
try:
    from googleapiclient.discovery import build as yt_build
    from googleapiclient.errors import HttpError as _HttpError
    _YT_AVAILABLE = True
except ImportError:
    _YT_AVAILABLE = False

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────
AIVEN_DATABASE_URL = os.environ.get("DATABASE_URL", "") or os.environ.get("AIVEN_DATABASE_URL", "")
OUTPUT_DIR         = Path(os.environ.get("DASHBOARD_OUTPUT_DIR", "dashboard"))
HISTORY_DB_PATH    = os.environ.get(
    "HISTORY_DB_PATH",
    str(Path(__file__).parent.parent / "idvt-indie-history" / "history.db")
)
MANIFEST_PATH      = OUTPUT_DIR / "manifest.json"

# ── org definitions ───────────────────────────────────────────────────────────
ORG_MAP = {
    "indies": {
        "label":   "Independent",
        "color":   "#EB9447",
        "color_light": "#974D0C",
        "desc":    "Indonesian VTubers unaffiliated with any groups or agencies.",
        "channels": [
            ("Shin Derra", 			"talent", "UC01M29MI5-oyfsB3dTCSd_A"),
            ("arikyami",       			"talent", "UC8Xu9VD_PIBm9WPX65cT9fA"),
            ("mei dyanira",       		"talent", "UC7WLSTGAlRjoZLglZDUMJQw"),
            ("Bianca Hantu",      		"talent", "UCNg9MeDGJ5oYzM2zxWvZh9w"),
            ("arcanneJ",       			"talent", "UCw1zeaolGb2zu3VUk8NEfUQ"),
            ("Solace Amerta",       		"talent", "UCIDrjX51xfY1c9r3jiTcBlQ"),
            ("Deidey",   		    	"talent", "UCWrxLFxCjH3_1Ii-SGEBsdQ"),
            ("ONShannon",    		   	"talent", "UCJlcEuTs4LNMPn0xL3czRrA"),
            ("Noemi Hestia",       		"talent", "UCA7tDob1IQiKWXnGktjPKQA"),
            ("Vanta Arissa Ch.",       		"talent", "UCMxeDNGGMqHd1zGK5Fhh-Xg"),
            ("Adelaide Ch.",       		"talent", "UCwgs92GpoFzAEG4QHLgEuRA"),
            ("Jelly si Curut Bodas Ch.",        "talent", "UCkQ7LiqtOgrDb9xs7RdVSag"),
        ],
    },
}

# Build reverse lookup: channel_name → (org_slug, org)
_CH_TO_ORG: dict[str, tuple[str, dict]] = {}
for _slug, _org in ORG_MAP.items():
    for _entry in _org["channels"]:
        _CH_TO_ORG[_entry[0]] = (_slug, _org)

# When ORG_MAP holds exactly one org (the indie build's normal state — every
# solo/unaffiliated talent lives under the single "indies" entry), the
# org-tier page is a pure pass-through: Home -> Org (1 card) -> Channel is
# the same destination as Home -> Channel with an extra click. In that case
# the channel grid is folded directly into write_index() and the org page
# is never generated, so there's no dead-end intermediate page or orphan
# file. If a second org is ever added, this flips back to the normal
# 3-level org/channel/stream layout automatically.
_SINGLE_ORG = len(ORG_MAP) <= 1


# ══════════════════════════════════════════════════════════════════════════════
# MANIFEST
# ══════════════════════════════════════════════════════════════════════════════

def load_manifest() -> dict:
    """
    Returns the manifest dict, keyed by video_id.
    Each entry: {org_slug, ch_slug, ch_name, status, generated_at}
    """
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("Manifest unreadable (%s) — treating as empty.", e)
    return {}


def save_manifest(manifest: dict) -> None:
    """Write manifest atomically via a temp file so a mid-write crash can never
    corrupt the file and cause 'Manifest unreadable' warnings on the next run."""
    try:
        tmp = MANIFEST_PATH.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        tmp.replace(MANIFEST_PATH)   # atomic on POSIX; near-atomic on Windows
    except Exception as e:
        log.warning("Could not save manifest: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_conn():
    if not _PSYCOPG2_AVAILABLE:
        raise RuntimeError(
            "psycopg2 is not installed. Every database-backed function in "
            "this module goes through get_conn(), so this is the one place "
            "that needs to fail loudly and clearly rather than further down "
            "the call stack as a cryptic NameError. Install psycopg2-binary "
            "(e.g. `pip install -r requirements.txt`) before calling any "
            "DB-dependent function — or if you only need the HTML-generation "
            "helpers (as generate_blog.py does), this code path was never "
            "meant to run at all."
        )
    # Connects via PgBouncer (port 6543) which is required on Supabase to
    # avoid exhausting the 60-connection direct Postgres limit (port 5432).
    # PgBouncer runs in transaction-pooling mode so the options= kwarg on
    # psycopg2.connect() is silently dropped — SET commands must be issued
    # explicitly on the connection after it is opened instead.
    conn = psycopg2.connect(AIVEN_DATABASE_URL, sslmode="require")
    with conn.cursor() as cur:
        cur.execute("SET search_path = public")
        cur.execute("SET statement_timeout = 30000")
    conn.commit()
    return conn


def get_channel_rows(conn) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT channel_id, channel_name, table_name, added_at "
            "FROM channels ORDER BY channel_name"
        )
        return cur.fetchall()


_schema_cache: dict[str, dict] = {}  # table_name → {exists, has_view_count}


def _load_schema_cache(conn, tables: list[str]) -> None:
    """
    Bulk-load table existence and view_count column presence for all tables
    in a single query. Results are stored in _schema_cache for the lifetime
    of the process — schema never changes mid-run.
    """
    global _schema_cache
    if not tables:
        return
    placeholders = ",".join(["%s"] * len(tables))
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                t.table_name,
                bool_or(c.column_name = 'view_count') AS has_view_count
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c
                ON  c.table_schema = t.table_schema
                AND c.table_name   = t.table_name
            WHERE t.table_schema = 'public'
              AND t.table_name IN ({placeholders})
            GROUP BY t.table_name
        """, tables)
        for row in cur.fetchall():
            _schema_cache[row["table_name"]] = {
                "exists":         True,
                "has_view_count": bool(row["has_view_count"]),
            }
    # tables not returned by the query simply don't exist
    for t in tables:
        if t not in _schema_cache:
            _schema_cache[t] = {"exists": False, "has_view_count": False}
    log.info("Schema cache loaded for %d tables (%d exist).",
             len(tables), sum(1 for v in _schema_cache.values() if v["exists"]))


def _table_exists(conn, table: str) -> bool:
    if table not in _schema_cache:
        _load_schema_cache(conn, [table])
    return _schema_cache[table]["exists"]


def _has_column(conn, table: str, column: str) -> bool:
    if column != "view_count":
        # only view_count is cached; fall back to direct query for anything else
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name   = %s
                  AND column_name  = %s
            """, (table, column))
            return cur.fetchone() is not None
    if table not in _schema_cache:
        _load_schema_cache(conn, [table])
    return _schema_cache[table]["has_view_count"]


def get_streams_for_channel(conn, table: str) -> list[dict]:
    if not _table_exists(conn, table):
        log.warning("Table '%s' does not exist yet — skipping.", table)
        return []
    view_count_expr = (
        "MAX(view_count) AS view_count"
        if _has_column(conn, table, "view_count")
        else "NULL::BIGINT AS view_count"
    )
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                video_id,
                MAX(video_title)        AS video_title,
                MAX(stream_status)      AS stream_status,
                MIN(collected_at)       AS first_seen,
                MAX(collected_at)       AS last_seen,
                MAX(concurrent_viewers) AS peak_viewers,
                {view_count_expr},
                MAX(like_count)         AS peak_likes,
                MAX(comment_count)      AS peak_comments,
                COUNT(*)                AS data_points
            FROM {table}
            GROUP BY video_id
            ORDER BY first_seen DESC
        """)
        return cur.fetchall()


def get_all_streams_bulk(conn, table_infos: list[tuple[str, str]]) -> dict[str, list[dict]]:
    """
    Fetch summary rows for every channel in one round-trip using UNION ALL.
    *table_infos* is [(channel_name, table_name), ...] for tables that exist.
    Returns {channel_name: [stream_dict, ...]} with streams ordered newest-first.

    Channel names are NOT embedded in the SQL — they are stored in an index
    list and looked up from an integer tag column to avoid encoding issues with
    Unicode characters (e.g. Japanese brackets 【】) that psycopg2's latin-1
    adapter cannot handle.
    """
    if not table_infos:
        return {}

    # Map integer index → channel_name so we never put Unicode into SQL text.
    idx_to_ch: list[str] = []
    parts = []
    for idx, (ch_name, table) in enumerate(table_infos):
        idx_to_ch.append(ch_name)
        view_count_expr = (
            "MAX(view_count) AS view_count"
            if _has_column(conn, table, "view_count")
            else "NULL::BIGINT AS view_count"
        )
        parts.append(f"""
            SELECT
                {idx} AS ch_idx,
                video_id,
                MAX(video_title)        AS video_title,
                MAX(stream_status)      AS stream_status,
                MIN(collected_at)       AS first_seen,
                MAX(collected_at)       AS last_seen,
                MAX(concurrent_viewers) AS peak_viewers,
                {view_count_expr},
                MAX(like_count)         AS peak_likes,
                MAX(comment_count)      AS peak_comments,
                COUNT(*)                AS data_points
            FROM {table}
            GROUP BY video_id
        """)

    union_sql = " UNION ALL ".join(parts) + " ORDER BY ch_idx, first_seen DESC"

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(union_sql)
        rows = cur.fetchall()

    result: dict[str, list[dict]] = {ch: [] for ch, _ in table_infos}
    for row in rows:
        d = dict(row)
        ch = idx_to_ch[d.pop("ch_idx")]
        result[ch].append(d)
    return result


def get_stream_timeseries(conn, table: str, video_id: str) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT collected_at, concurrent_viewers, like_count, comment_count
            FROM {table}
            WHERE video_id = %s
            ORDER BY collected_at
        """, (video_id,))
        return cur.fetchall()


def get_all_rows(conn, table: str, video_id: str) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT * FROM {table}
            WHERE video_id = %s
            ORDER BY collected_at DESC
        """, (video_id,))
        return cur.fetchall()


# ══════════════════════════════════════════════════════════════════════════════
# HISTORY DB HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_history_conn():
    path = HISTORY_DB_PATH
    if not os.path.exists(path):
        log.info("history.db not found at %s — archived streams will not be shown.", path)
        return None
    try:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn
    except Exception as e:
        log.warning("Could not open history.db: %s", e)
        return None


def get_archived_streams_for_channel(hist, channel_name: str,
                                     exclude_video_ids: set) -> list:
    rows = hist.execute("""
        SELECT
            video_id, video_title, stream_status,
            stream_start  AS first_seen,
            stream_end    AS last_seen,
            peak_viewers, avg_viewers, view_count,
            peak_likes, peak_comments, data_points
        FROM streams
        WHERE channel_name = ?
        ORDER BY stream_start DESC
    """, (channel_name,)).fetchall()

    result = []
    for r in rows:
        if r["video_id"] in exclude_video_ids:
            continue
        d = dict(r)
        for key in ("first_seen", "last_seen"):
            val = d.get(key)
            if isinstance(val, str):
                try:
                    d[key] = datetime.fromisoformat(val)
                except ValueError:
                    pass
        d["_source"] = "history"
        result.append(d)
    return result


def get_all_archived_streams(hist, channel_names: list[str]) -> dict[str, list]:
    """
    Bulk-fetch archived streams for all requested channel names in a single
    SQLite query.  Returns {channel_name: [stream_dict, ...]} for every name
    in *channel_names* (missing channels get an empty list).
    """
    if not channel_names:
        return {}
    placeholders = ",".join("?" * len(channel_names))
    rows = hist.execute(f"""
        SELECT
            channel_name,
            video_id, video_title, stream_status,
            stream_start  AS first_seen,
            stream_end    AS last_seen,
            peak_viewers, avg_viewers, view_count,
            peak_likes, peak_comments, data_points
        FROM streams
        WHERE channel_name IN ({placeholders})
        ORDER BY channel_name, stream_start DESC
    """, channel_names).fetchall()

    result: dict[str, list] = {name: [] for name in channel_names}
    for r in rows:
        d = dict(r)
        ch = d.pop("channel_name")
        for key in ("first_seen", "last_seen"):
            val = d.get(key)
            if isinstance(val, str):
                try:
                    d[key] = datetime.fromisoformat(val)
                except ValueError:
                    pass
        d["_source"] = "history"
        result[ch].append(d)
    return result


def get_archived_timeseries(hist, video_id: str) -> list:
    rows = hist.execute("""
        SELECT collected_at, concurrent_viewers, like_count, comment_count
        FROM timeseries
        WHERE video_id = ?
        ORDER BY collected_at
    """, (video_id,)).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("collected_at"), str):
            try:
                d["collected_at"] = datetime.fromisoformat(d["collected_at"])
            except ValueError:
                pass
        result.append(d)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# LOGO / SUBSCRIBER CACHE
# ══════════════════════════════════════════════════════════════════════════════

_CACHE_DIR           = Path(__file__).parent / "cache"
_LOGO_CACHE_FILE     = str(_CACHE_DIR / "channel_logos_cache.json")
_LOGO_FALLBACK_FILE  = str(_CACHE_DIR / "channel_logos_fallback.json")


def _load_fallback() -> tuple[dict[str, str], dict[str, int]]:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if os.path.exists(_LOGO_FALLBACK_FILE):
        try:
            with open(_LOGO_FALLBACK_FILE, encoding="utf-8") as f:
                data = json.load(f)
            logos       = data.get("logos", {})
            subscribers = data.get("subscribers", {})
            saved_at    = data.get("saved_at", "unknown date")
            log.info("Loaded fallback channel data from %s (%d logos, %d subscriber counts).",
                     saved_at, len(logos), len(subscribers))
            return logos, subscribers
        except Exception as e:
            log.warning("Fallback cache unreadable: %s", e)
    return {}, {}


def _save_fallback(logos: dict[str, str], subscribers: dict[str, int]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_LOGO_FALLBACK_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "saved_at":    _now_local().strftime("%Y-%m-%d %H:%M WIB"),
                "logos":       logos,
                "subscribers": subscribers,
            }, f)
        log.info("Fallback channel data updated (%d logos, %d subscriber counts).",
                 len(logos), len(subscribers))
    except Exception as e:
        log.warning("Could not save fallback channel data: %s", e)


def get_channel_data(channel_ids: list[str]) -> tuple[dict[str, str], dict[str, int]]:
    """
    Fetch channel thumbnail URLs and subscriber counts from YouTube API.
    Rotates through all available API keys on 403.
    Falls back to last successful fetch on complete failure.
    Results are cached to disk for the remainder of the local day.

    Cache validity requires BOTH:
      (a) the cache date matches today, AND
      (b) every requested channel_id is already present in the cache.
    If new channel IDs are requested (e.g. newly-added orgs), the cache is
    considered stale and a fresh fetch is performed for all missing IDs.
    The result is then merged back into the cache and saved.
    """
    today = _now_local().strftime("%Y-%m-%d")

    if os.path.exists(_LOGO_CACHE_FILE):
        try:
            with open(_LOGO_CACHE_FILE, encoding="utf-8") as f:
                cache = json.load(f)
            if cache.get("date") == today:
                cached_logos = cache.get("logos", {})
                cached_subs  = cache.get("subscribers", {})
                missing_ids  = [cid for cid in channel_ids if cid not in cached_logos]
                if not missing_ids:
                    log.info("Using cached channel data (%d entries, all present).",
                             len(cached_logos))
                    return cached_logos, cached_subs
                log.info(
                    "Cache is from today but missing %d channel ID(s) — "
                    "fetching missing entries only.",
                    len(missing_ids),
                )
                # Fall through to fetch only the missing IDs, then merge below
                channel_ids = missing_ids
                # Keep existing cached data so we can merge at the end
                _partial_cache = (cached_logos, cached_subs)
            else:
                _partial_cache = None
        except Exception as e:
            log.warning("Logo cache unreadable (%s) — will re-fetch.", e)
            _partial_cache = None
    else:
        _partial_cache = None

    raw_keys = os.environ.get("YOUTUBE_API_KEYS") or os.environ.get("YOUTUBE_API_KEY", "")
    api_keys = [k.strip() for k in raw_keys.split(",") if k.strip()]

    if not _YT_AVAILABLE:
        log.warning("Channel data fetch skipped: google-api-python-client not installed.")
        return _load_fallback()
    if not api_keys:
        log.warning("Channel data fetch skipped: no API keys in environment.")
        return _load_fallback()
    if not channel_ids:
        log.warning("Channel data fetch skipped: channel_ids list is empty.")
        return _load_fallback()

    logos:       dict[str, str] = {}
    subscribers: dict[str, int] = {}
    api_failed   = False

    for i in range(0, len(channel_ids), 50):
        batch      = channel_ids[i:i + 50]
        batch_done = False

        for api_key in api_keys:
            try:
                log.info("channels.list batch %d–%d using key ...%s",
                         i, i + len(batch), api_key[-6:])
                yt   = yt_build("youtube", "v3", developerKey=api_key)
                resp = yt.channels().list(
                    part="snippet,statistics",
                    id=",".join(batch),
                    maxResults=50,
                ).execute()
                items_returned = resp.get("items", [])
                log.info("  → %d item(s) returned (totalResults=%s).",
                         len(items_returned),
                         resp.get("pageInfo", {}).get("totalResults", "?"))
                for item in items_returned:
                    cid    = item["id"]
                    thumbs = item.get("snippet", {}).get("thumbnails", {})
                    url    = (thumbs.get("medium") or thumbs.get("default") or {}).get("url", "")
                    if url:
                        logos[cid] = url
                    else:
                        log.warning("No thumbnail URL found for channel ID: %s", cid)
                    sub_count = item.get("statistics", {}).get("subscriberCount")
                    if sub_count is not None:
                        subscribers[cid] = int(sub_count)
                batch_done = True
                break
            except _HttpError as e:
                if e.resp.status == 403:
                    log.warning("403 on channels.list (key ...%s) — rotating.", api_key[-6:])
                    continue
                log.error("channels.list HTTP error (key ...%s): %s", api_key[-6:], e)
                api_failed = True
                break
            except Exception as e:
                log.error("channels.list unexpected error (key ...%s): %s", api_key[-6:], e)
                api_failed = True
                break

        if not batch_done:
            log.error("All %d key(s) failed for batch %d–%d.", len(api_keys), i, i + len(batch))
            api_failed = True

    if api_failed and not logos and not subscribers:
        log.warning("API fetch produced no data — falling back to last known good channel data.")
        # Still merge with any partial cache we loaded earlier
        if _partial_cache:
            return _partial_cache
        return _load_fallback()

    if api_failed and (logos or subscribers):
        log.warning("API fetch partially failed — merging fresh results with fallback data.")
        fallback_logos, fallback_subs = _load_fallback()
        logos       = {**fallback_logos, **logos}
        subscribers = {**fallback_subs,  **subscribers}

    # Merge with the partial cache (data already present from today's earlier fetch)
    if _partial_cache:
        prev_logos, prev_subs = _partial_cache
        logos       = {**prev_logos, **logos}       # fresh data wins on conflict
        subscribers = {**prev_subs,  **subscribers}

    try:
        with open(_LOGO_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"date": today, "logos": logos, "subscribers": subscribers}, f)
        log.info("Channel data cached — %d logos, %d subscriber count(s) total.",
                 len(logos), len(subscribers))
    except Exception as e:
        log.warning("Could not save channel data cache: %s", e)

    _save_fallback(logos, subscribers)
    return logos, subscribers


# ══════════════════════════════════════════════════════════════════════════════
# UTILITY HELPERS
# ══════════════════════════════════════════════════════════════════════════════

@lru_cache(maxsize=None)
def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def fmt(n) -> str:
    if n is None:
        return "—"
    try:
        return f"{int(n):,}"
    except (ValueError, TypeError):
        return str(n)


def fmt_subs(n) -> str:
    if n is None:
        return "—"
    try:
        n = int(n)
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.1f}K"
        return str(n)
    except (ValueError, TypeError):
        return "—"


# Same K/M-abbreviation logic as fmt_subs, aliased under a name that makes
# sense at peak-viewer call sites — matches the mockup's intent of using
# compact notation (e.g. "18.4K") for peak CCV everywhere: hero KPIs and
# card grids alike, at every level (index/org/channel), both on first
# render and after a range-chip swap. Previously several of these spots
# used fmt() (comma-separated) for the initial render but fmtNum() (K/M) on
# the client after a range swap — same number, two different-looking
# formats depending on whether you'd touched a chip yet.
fmt_compact = fmt_subs


def fmt_dt(dt, time_only: bool = False) -> str:
    """Format a datetime for display in WIB (UTC+7).

    time_only=True returns only HH:MM — used for chart x-axis labels
    so ticks stay readable without the date repeating on every tick.
    """
    if dt is None:
        return "—"
    try:
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            local = dt.astimezone(_LOCAL_TZ)
        else:
            parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            local = parsed.astimezone(_LOCAL_TZ)
        return local.strftime("%H:%M") if time_only else local.strftime("%Y-%m-%d %H:%M WIB")
    except Exception:
        return str(dt)[:16]


def esc(s) -> str:
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))


def _stream_dt(s: dict):
    """Parse a stream's first_seen into a timezone-aware datetime, or None."""
    v = s.get("first_seen")
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(_LOCAL_TZ)
    except Exception:
        return None


def _window_stats(streams: list[dict]) -> dict:
    """
    Buckets a list of streams into three fixed windows — 7d / 30d / all —
    and computes peak viewers + stream count for each. This is the entire
    server-side cost of the range toggle: one pass over data that's already
    in memory (no extra queries), computed once per build. The client just
    swaps between three pre-baked numbers on chip click; there is no
    re-fetch or re-computation happening in the browser.

    Streams with no parseable first_seen still count toward "all" (their
    peak/count aren't excluded just because the date was unparsable) but
    can't be bucketed into 7d/30d since we don't know when they happened.
    """
    now = _now_local()
    cutoff_7d  = now - timedelta(days=7)
    cutoff_30d = now - timedelta(days=30)

    result = {
        "7d":  {"peak": 0, "streams": 0},
        "30d": {"peak": 0, "streams": 0},
        "all": {"peak": 0, "streams": 0},
    }
    for s in streams:
        peak = s.get("peak_viewers") or 0
        result["all"]["streams"] += 1
        if peak > result["all"]["peak"]:
            result["all"]["peak"] = peak

        dt = _stream_dt(s)
        if dt is None:
            continue
        if dt >= cutoff_30d:
            result["30d"]["streams"] += 1
            if peak > result["30d"]["peak"]:
                result["30d"]["peak"] = peak
        if dt >= cutoff_7d:
            result["7d"]["streams"] += 1
            if peak > result["7d"]["peak"]:
                result["7d"]["peak"] = peak
    return result


# ══════════════════════════════════════════════════════════════════════════════
# SHARED CSS + HTML HELPERS
# ══════════════════════════════════════════════════════════════════════════════

_FONTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght'
    '@400;500;700&family=Inter:wght@400;500;600;700&family=IBM+Plex+Mono:wght'
    '@400;500;600&display=swap" rel="stylesheet">'
)

# Google Analytics 4. Loaded on every generated page via _html_head() below,
# and separately pasted into privacy.html/terms.html since those are static
# files copied verbatim by setup_output_dirs() rather than passed through
# _html_head(). Keep the measurement ID in sync across all three call sites
# if it ever changes.
_GA_MEASUREMENT_ID = "G-29RTBLR6HQ"   
_GA_SNIPPET = (
    f'<script async src="https://www.googletagmanager.com/gtag/js?id={_GA_MEASUREMENT_ID}"></script>\n'
    f'<script>\n'
    f'  window.dataLayer = window.dataLayer || [];\n'
    f'  function gtag(){{dataLayer.push(arguments);}}\n'
    f"  gtag('js', new Date());\n"
    f"  gtag('config', '{_GA_MEASUREMENT_ID}');\n"
    f'</script>\n'
)

# _BASE_CSS used to live here as one large inlined stylesheet string.
# It has been split into two static files, both linked in via
# _html_head(): theme.css (tokens/reset/nav shared with the standalone
# privacy.html/terms.html pages) and dashboard.css (everything specific
# to the generated dashboard — cards, KPIs, search overlay, and the few
# rules that intentionally differ from the standalone pages). See those
# two files for the full ruleset previously defined here.



_THEME_JS = """
<script>
(function() {
  var STORAGE_KEY = 'idvt-theme';
  var root  = document.documentElement;
  var btn   = null;
  var thumb = null;

  function getSystemTheme() {
    return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  }
  function getEffectiveTheme() {
    return localStorage.getItem(STORAGE_KEY) || getSystemTheme();
  }
  function applyTheme(theme) {
    root.setAttribute('data-theme', theme);
    if (thumb) {
      theme === 'light' ? thumb.classList.add('is-light') : thumb.classList.remove('is-light');
    }
    // Lets any page-specific script (e.g. the stream page's chart) react to
    // a theme flip without _THEME_JS needing to know charts exist at all.
    window.dispatchEvent(new Event('idvt-theme-change'));
  }
  function toggleTheme() {
    var next = getEffectiveTheme() === 'dark' ? 'light' : 'dark';
    localStorage.setItem(STORAGE_KEY, next);
    applyTheme(next);
  }
  applyTheme(getEffectiveTheme());
  document.addEventListener('DOMContentLoaded', function() {
    btn   = document.getElementById('theme-toggle');
    thumb = document.getElementById('toggle-thumb');
    applyTheme(getEffectiveTheme());
    if (btn) btn.addEventListener('click', toggleTheme);
  });
  window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', function() {
    if (!localStorage.getItem(STORAGE_KEY)) applyTheme(getSystemTheme());
  });
})();
</script>"""

_TOGGLE_HTML = (
    '<div class="theme-toggle">'
    '<button class="toggle-pill" id="theme-toggle" aria-label="Toggle theme" title="Toggle light/dark theme">'
    '<span class="toggle-icon-light">☀</span>'
    '<span class="toggle-thumb" id="toggle-thumb">&#10022;</span>'
    '<span class="toggle-icon-dark">☽</span>'
    '</button>'
    '</div>'
)

# Global search overlay — present on every page (not just the index), backed
# by search-index.json (see write_search_index()). Modal markup is injected
# once per page via _html_foot(); behavior lives in _SEARCH_JS below.
_SEARCH_HTML = (
    '<div class="search-overlay" id="searchOverlay">'
    '<div class="search-modal">'
    '<div class="search-input-row">'
    '&#128269;'
    '<input id="searchInput" placeholder="Search orgs, channels, streams…" autocomplete="off">'
    '<span class="search-esc">ESC</span>'
    '</div>'
    '<div class="search-results" id="searchResults"></div>'
    '<div class="search-footer">'
    '<span>&#8593;&#8595; navigate &#183; &#8629; open</span>'
    '<span id="searchResultCount"></span>'
    '</div>'
    '</div>'
    '</div>'
)

# Tiny, unconditional, every-page fetch of live-count.json (a few bytes) to
# populate the nav's "LIVE n" pill. Deliberately separate from _SEARCH_JS's
# lazy-loaded search-index.json — that file is much bigger and only needed
# once someone actually opens search, whereas this needs to run on every
# single page view, so it gets its own minimal fetch instead of forcing an
# eager load of the full index just to show a count.
_LIVE_PILL_JS = """
<script>
(function () {
  var pill   = document.getElementById('navLivePill');
  var count  = document.getElementById('navLiveCount');
  var strip  = document.getElementById('navPulseStrip');
  if (!pill || !count) return;

  function pulseBars() {
    if (!strip) return;
    strip.innerHTML = '';
    for (var i = 0; i < 12; i++) {
      var b = document.createElement('span');
      b.style.height = (4 + Math.round(Math.random() * 10)) + 'px';
      strip.appendChild(b);
    }
  }
  pulseBars();
  setInterval(pulseBars, 2500);

  var depth = window.SITE_DEPTH || 0;
  fetch('../'.repeat(depth) + 'live-count.json')
    .then(function (r) { return r.json(); })
    .then(function (data) {
      var n = data.count || 0;
      count.textContent = n;
      pill.classList.toggle('has-live', n > 0);
    })
    .catch(function () { /* fine to just leave the pill hidden */ });
})();
</script>"""

_SEARCH_JS = """
<script>
(function () {
  var overlay   = document.getElementById('searchOverlay');
  var input     = document.getElementById('searchInput');
  var results   = document.getElementById('searchResults');
  var countEl   = document.getElementById('searchResultCount');
  var trigger   = document.getElementById('searchTrigger');
  var depth     = window.SITE_DEPTH || 0;
  var indexData = null;
  var indexPromise = null;
  var current   = [];
  var selIndex  = 0;

  var typeIcon  = { org: '\\u25C6', channel: '\\u25CF', stream: '\\u25B6' };
  var typeLabel = { org: 'Organisations', channel: 'Channels', stream: 'Streams' };

  function loadIndex() {
    if (indexPromise) return indexPromise;
    var path = '../'.repeat(depth) + 'search-index.json';
    indexPromise = fetch(path).then(function (r) { return r.json(); })
      .then(function (data) { indexData = data; return data; })
      .catch(function (e) { indexData = []; console.warn('search index unavailable:', e); return []; });
    return indexPromise;
  }

  function score(query, text) {
    query = query.toLowerCase(); text = text.toLowerCase();
    if (!query) return 0;
    if (text.indexOf(query) === 0) return 100;
    if (text.indexOf(query) !== -1) return 60;
    var qi = 0;
    for (var i = 0; i < text.length && qi < query.length; i++) {
      if (text[i] === query[qi]) qi++;
    }
    return qi === query.length ? 25 : -1;
  }

  function highlight(text, query) {
    if (!query) return text;
    var idx = text.toLowerCase().indexOf(query.toLowerCase());
    if (idx === -1) return text;
    return text.slice(0, idx) + '<mark>' + text.slice(idx, idx + query.length) + '</mark>' + text.slice(idx + query.length);
  }

  function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function render(query) {
    if (!indexData) return;
    if (!query) {
      results.innerHTML = '<div class="search-empty">Type to search every organisation, channel, and tracked stream.</div>';
      countEl.textContent = '';
      current = [];
      return;
    }
    var scored = indexData
      .map(function (item) { return Object.assign({}, item, { s: score(query, item.name) }); })
      .filter(function (item) { return item.s >= 0; })
      .sort(function (a, b) { return b.s - a.s; })
      .slice(0, 40);

    current = scored;
    selIndex = 0;
    countEl.textContent = scored.length + ' result' + (scored.length === 1 ? '' : 's');

    if (!scored.length) {
      results.innerHTML = '<div class="search-empty">No matches for "' + esc(query) + '".</div>';
      return;
    }
    var groups = { org: [], channel: [], stream: [] };
    scored.forEach(function (item) { groups[item.type].push(item); });

    var html = '';
    var flat = 0;
    ['org', 'channel', 'stream'].forEach(function (t) {
      if (!groups[t].length) return;
      html += '<div class="result-group-label">' + typeLabel[t] + '</div>';
      groups[t].forEach(function (item) {
        html += '<div class="result-row" data-idx="' + flat + '">' +
          '<div class="result-type-icon">' + typeIcon[t] + '</div>' +
          '<div class="result-main"><div class="result-name">' + highlight(esc(item.name), query) + '</div>' +
          '<div class="result-sub">' + esc(item.sub || '') + '</div></div>' +
          '</div>';
        flat++;
      });
    });
    results.innerHTML = html;
    Array.prototype.forEach.call(results.querySelectorAll('.result-row'), function (row) {
      row.addEventListener('click', function () { navigateTo(parseInt(row.dataset.idx, 10)); });
    });
    updateSel();
  }

  function updateSel() {
    var rows = results.querySelectorAll('.result-row');
    Array.prototype.forEach.call(rows, function (r, i) { r.classList.toggle('sel', i === selIndex); });
    if (rows[selIndex]) rows[selIndex].scrollIntoView({ block: 'nearest' });
  }

  function navigateTo(i) {
    var item = current[i];
    if (!item) return;
    window.location.href = '../'.repeat(depth) + item.path;
  }

  function open() {
    overlay.classList.add('open');
    input.value = '';
    loadIndex().then(function () { render(''); });
    setTimeout(function () { input.focus(); }, 10);
  }
  function close() {
    overlay.classList.remove('open');
  }

  if (trigger) trigger.addEventListener('click', open);
  overlay.addEventListener('click', function (e) { if (e.target === overlay) close(); });
  input.addEventListener('input', function (e) { render(e.target.value); });

  document.addEventListener('keydown', function (e) {
    var isOpen = overlay.classList.contains('open');
    if (!isOpen && e.key === '/' && document.activeElement.tagName !== 'INPUT') {
      e.preventDefault(); open(); return;
    }
    if (!isOpen) return;
    if (e.key === 'Escape') { close(); }
    else if (e.key === 'ArrowDown') { e.preventDefault(); selIndex = Math.min(selIndex + 1, current.length - 1); updateSel(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); selIndex = Math.max(selIndex - 1, 0); updateSel(); }
    else if (e.key === 'Enter') { e.preventDefault(); navigateTo(selIndex); }
  });
})();
</script>"""


def _html_head(title: str, depth: int, org_color: str = "#e8ff47",
               org_color_light: str = "#6e7e00",
               extra_scripts: str = "", live_count: int | None = None) -> str:
    # Two values are threaded through here instead of one: org_color is the
    # dark-theme accent (used by default and as the :root fallback),
    # org_color_light is the WCAG-corrected variant for light theme. Both are
    # audited per-org (see the color-pair table in ORG_MAP) since many of the
    # original hex values were picked to pop on a near-black background and
    # fail contrast badly once the page flips to a white surface.
    #
    # live_count: when the caller already knows the current sitewide live
    # count (write_index() computes it anyway for the hero stats), pass it
    # here so the nav's LIVE pill — including its pulse-strip bars — renders
    # correctly on first paint instead of staying hidden until _LIVE_PILL_JS's
    # async fetch of live-count.json resolves. Pages that don't have the
    # count on hand (org/channel/stream) pass None and keep the old
    # JS-only behavior.
    nav_pill_class = "nav-pill nav-live-pill"
    nav_pill_count = "0"
    if live_count is not None:
        nav_pill_count = str(live_count)
        if live_count > 0:
            nav_pill_class += " has-live"
    return (
        f'<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        f'<meta charset="UTF-8">\n'
        f'<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'<meta name="color-scheme" content="dark light">\n'
        f'<script>!function(){{var t=localStorage.getItem("idvt-theme")||'
        f'(window.matchMedia("(prefers-color-scheme: light)").matches?"light":"dark");'
        f'document.documentElement.setAttribute("data-theme",t)}}();</script>\n'
        f'<title>{esc(title)} — IDVTuber Tracker</title>\n'
        f'<link rel="icon" href="{"../" * depth}favicon.ico">\n'
        f'<script>window.SITE_DEPTH = {depth};</script>\n'
        f'{_FONTS}\n'
        f'{_GA_SNIPPET}'
        f'{extra_scripts}\n'
        # Shared chrome that's identical to privacy.html/terms.html (color
        # tokens, reset, .site-nav skeleton) lives in theme.css. Everything
        # that's specific to the *generated* dashboard — org/channel/stream
        # card styles, the search overlay, and the handful of rules that
        # intentionally differ from the standalone pages (.page width, h1
        # size, toggle accent colour) — lives in dashboard.css, loaded
        # after theme.css so its overrides win. Neither file is inlined
        # here anymore; see them directly for the ruleset. The only thing
        # that stays inline is --org-color, since it's computed per page
        # from ORG_MAP and can't be baked into a static stylesheet.
        f'<link rel="stylesheet" href="{"../" * depth}theme.css">\n'
        f'<link rel="stylesheet" href="{"../" * depth}dashboard.css">\n'
        f'<style>\n'
        f'  :root {{ --org-color: {org_color}; }}\n'
        f'  [data-theme="light"] {{ --org-color: {org_color_light}; }}\n'
        f'  @media (prefers-color-scheme: light) {{\n'
        f'    :root:not([data-theme="dark"]) {{ --org-color: {org_color_light}; }}\n'
        f'  }}\n'
        f'</style>\n'
        f'</head>\n<body>\n'
        f'<nav class="site-nav">\n'
        f'  <a class="site-nav-logo" href="{"../" * depth}index.html">\n'
        f'    <img class="site-nav-logo-icon" src="{"../" * depth}favicon.ico" alt="">\n'
        f'    <span class="site-nav-logo-word"><span class="lw1">IDVTuber</span><span class="lw2">//</span><span class="lw3">Tracker</span></span>\n'
        f'  </a>\n'
        f'  <span class="nav-spacer"></span>\n'
        + f'  <div class="nav-right-cluster">\n'
        f'    <a class="{nav_pill_class}" id="navLivePill" href="{"../" * depth}live.html">\n'
        f'      <span class="pulse-strip" id="navPulseStrip"></span>\n'
        f'      <span class="nav-live-dot"></span> LIVE <strong id="navLiveCount">{nav_pill_count}</strong>\n'
        f'    </a>\n'
        f'    <div class="search-trigger" id="searchTrigger">\n'
        f'      <span class="search-trigger-icon">&#128269;</span> Search orgs, channels, streams… <kbd>/</kbd>\n'
        f'    </div>\n'
        f'    {_TOGGLE_HTML}\n'
        f'  </div>\n'
        f'</nav>\n'
        f'<div class="page">\n'
    )


# ── Page-specific JS snippets ────────────────────────────────────────────────
# INDEX_JS: sort chips re-order org cards by the selected metric; range chips
# (7D/30D/ALL) swap which pre-baked window's numbers are visible on every
# card and in the stats-bar peak pill. The two interact — sorting by "Peak
# Viewers" always uses whichever range is currently selected, so switching
# ranges never silently un-sorts the grid.
# The old per-index-page name filter (navSearch input) is gone — that job
# now belongs to the global search overlay (_SEARCH_JS), which searches
# orgs/channels/streams everywhere, not just org names on this one page.
_INDEX_JS = """
<script>
(function () {
  var grid       = document.querySelector('.orgs-grid');
  var chips      = document.querySelectorAll('.filter-chip');
  var rangeWrap  = document.getElementById('homeRange');
  var rangeChips = rangeWrap ? rangeWrap.querySelectorAll('.range-chip') : [];
  var sitePeakEl = document.querySelector('.js-site-peak');
  var mode  = 'all';    // 'all' | 'az' | 'peak'
  var range = '30d';    // '7d' | '30d' | 'all'

  function applySort() {
    if (!grid) return;
    var cards = Array.from(grid.querySelectorAll('.org-card'));
    if (mode === 'az') {
      cards.sort(function (a, b) {
        return (a.getAttribute('data-name') || '').localeCompare(b.getAttribute('data-name') || '');
      });
      cards.forEach(function (c) { grid.appendChild(c); });
    } else if (mode === 'peak') {
      cards.sort(function (a, b) {
        var pa = parseFloat(a.getAttribute('data-peak-' + range) || '0') || 0;
        var pb = parseFloat(b.getAttribute('data-peak-' + range) || '0') || 0;
        return pb - pa;   // descending — highest peak viewers first
      });
      cards.forEach(function (c) { grid.appendChild(c); });
    }
    // 'all' = default document order (no re-sort needed on page load)
  }

  function fmtNum(n) {
    n = parseInt(n, 10) || 0;
    if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
    return String(n);
  }

  function applyRange() {
    if (grid) {
      Array.from(grid.querySelectorAll('.org-card')).forEach(function (card) {
        var streamsEl = card.querySelector('.js-streams');
        var peakEl    = card.querySelector('.js-peak');
        if (streamsEl) streamsEl.textContent = card.getAttribute('data-streams-' + range) || '0';
        if (peakEl) {
          var p = card.getAttribute('data-peak-' + range) || '0';
          peakEl.textContent = p === '0' ? '—' : fmtNum(p);
        }
      });
    }
    if (sitePeakEl && rangeWrap) {
      var sp = rangeWrap.getAttribute('data-peak-' + range) || '0';
      sitePeakEl.textContent = sp === '0' ? '—' : fmtNum(sp);
    }
    applySort();
  }

  chips.forEach(function (chip) {
    chip.addEventListener('click', function () {
      chips.forEach(function (c) { c.classList.remove('active'); });
      chip.classList.add('active');
      var label = chip.textContent.trim();
      if (label === 'A–Z') {
        mode = 'az';
      } else if (label === 'Peak Viewers') {
        mode = 'peak';
      } else {
        mode = 'all';
      }
      applySort();
    });
  });

  rangeChips.forEach(function (chip) {
    chip.addEventListener('click', function () {
      rangeChips.forEach(function (c) { c.classList.remove('active'); });
      chip.classList.add('active');
      range = chip.getAttribute('data-range') || '30d';
      applyRange();
    });
  });
})();
</script>"""

# ORG_JS: sort chips re-order channel cards by the selected metric; range
# chips swap which window (7d/30d/all) the hero and every card's Streams/
# Peak CCV numbers reflect. Peak-sort always uses whichever range is
# currently active, same pattern as _INDEX_JS.
_ORG_JS = """
<script>
(function () {
  var grid       = document.querySelector('.channels-grid');
  var chips      = document.querySelectorAll('.sort-chip');
  var rangeWrap  = document.getElementById('orgRange');
  var rangeChips = rangeWrap ? rangeWrap.querySelectorAll('.range-chip') : [];
  var heroStreamsEl = document.querySelector('.org-hero-stats .js-streams');
  var heroPeakEl    = document.querySelector('.org-hero-stats .js-peak');
  var mode  = 'subs';   // 'subs' | 'peak' | 'likes' | 'streams' | 'az'
  var range = '30d';    // '7d' | '30d' | 'all'

  function getVal(card, attr) {
    return parseFloat(card.getAttribute(attr) || '0') || 0;
  }

  function fmtNum(n) {
    n = parseInt(n, 10) || 0;
    if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
    return String(n);
  }

  function sortCards() {
    if (!grid) return;
    var cards = Array.from(grid.querySelectorAll('.channel-card'));
    if (mode === 'az') {
      cards.sort(function (a, b) {
        return (a.getAttribute('data-name') || '').localeCompare(b.getAttribute('data-name') || '');
      });
    } else {
      var map = { 'subs': 'data-subs', 'likes': 'data-likes',
                  'peak': 'data-peak-' + range, 'streams': 'data-streams-' + range };
      cards.sort(function (a, b) { return getVal(b, map[mode]) - getVal(a, map[mode]); });
    }
    cards.forEach(function (c) { grid.appendChild(c); });
  }

  function applyRange() {
    if (grid) {
      Array.from(grid.querySelectorAll('.channel-card')).forEach(function (card) {
        var streamsEl = card.querySelector('.js-streams');
        var peakEl    = card.querySelector('.js-peak');
        if (streamsEl) streamsEl.textContent = card.getAttribute('data-streams-' + range) || '0';
        if (peakEl) {
          var p = card.getAttribute('data-peak-' + range) || '0';
          peakEl.textContent = p === '0' ? '—' : fmtNum(p);
        }
      });
    }
    if (rangeWrap) {
      if (heroStreamsEl) heroStreamsEl.textContent = rangeWrap.getAttribute('data-streams-' + range) || '0';
      if (heroPeakEl) {
        var hp = rangeWrap.getAttribute('data-peak-' + range) || '0';
        heroPeakEl.textContent = hp === '0' ? '—' : fmtNum(hp);
      }
    }
    sortCards();
  }

  var keyMap = {
    'Subscribers': 'subs',
    'Peak CCV':    'peak',
    'Peak Likes':  'likes',
    'Streams':     'streams',
    'A–Z':   'az',
  };

  chips.forEach(function (chip) {
    chip.addEventListener('click', function () {
      chips.forEach(function (c) { c.classList.remove('active'); });
      chip.classList.add('active');
      mode = keyMap[chip.textContent.trim()] || 'subs';
      sortCards();
    });
  });

  rangeChips.forEach(function (chip) {
    chip.addEventListener('click', function () {
      rangeChips.forEach(function (c) { c.classList.remove('active'); });
      chip.classList.add('active');
      range = chip.getAttribute('data-range') || '30d';
      applyRange();
    });
  });
})();
</script>"""


def _html_foot(depth: int, page_type: str = '') -> str:
    rel = "../" * depth
    extra_js = ''
    if page_type == 'index':
        extra_js = _INDEX_JS + '\n'
    elif page_type == 'org':
        extra_js = _ORG_JS + '\n'
    return (
        f'\n  <footer>\n'
        f'    <span>&#169; 2026 IDVTuber Tracker &#8212; Non-commercial fan project</span>\n'
        f'    <nav class="footer-links">\n'
        f'      <a href="{rel}index.html">Home</a>\n'
        f'      <span class="footer-sep">·</span>\n'
        f'      <a href="{rel}privacy.html">Privacy Policy</a>\n'
        f'      <span class="footer-sep">·</span>\n'
        f'      <a href="{rel}terms.html">Terms of Use</a>\n'
        f'    </nav>\n'
        f'  </footer>\n'
        f'</div>\n'
        f'{_SEARCH_HTML}\n'
        f'{_THEME_JS}\n'
        f'{_LIVE_PILL_JS}\n'
        f'{_SEARCH_JS}\n'
        + extra_js
        + f'</body>\n</html>'
    )


def _breadcrumb(crumbs: list[tuple[str, str]]) -> str:
    parts = []
    for i, (label, href) in enumerate(crumbs):
        if i == len(crumbs) - 1:
            parts.append(f'<span class="current">{esc(label)}</span>')
        else:
            parts.append(f'<a href="{href}">{esc(label)}</a>')
        if i < len(crumbs) - 1:
            parts.append('<span class="sep">&#8250;</span>')
    return '<nav class="breadcrumb">' + " ".join(parts) + "</nav>\n"


# ══════════════════════════════════════════════════════════════════════════════
# PAGE WRITERS  (unchanged from original — all logic preserved)
# ══════════════════════════════════════════════════════════════════════════════

def write_index(total_streams: int, total_channels: int, generated_at: str,
                stream_counts: dict | None = None,
                all_streams_by_channel: dict | None = None,
                logos: dict[str, str] | None = None,
                channel_ids_map: dict[str, str] | None = None,
                subscribers: dict[str, int] | None = None) -> None:
    stream_counts          = stream_counts or {}
    all_streams_by_channel = all_streams_by_channel or {}
    logos                  = logos or {}
    channel_ids_map        = channel_ids_map or {}
    subscribers            = subscribers or {}

    # ── sitewide windowed stats (== org-wide, since there's one org here) ─────
    all_site_streams = [s for streams in all_streams_by_channel.values() for s in streams]
    site_windows = _window_stats(all_site_streams)
    w30 = site_windows["30d"]
    sitewide_live = sum(1 for s in all_site_streams if (s.get("stream_status") or "vod") == "live")
    total_subs = 0
    for org in ORG_MAP.values():
        for e in org["channels"]:
            ch_id = channel_ids_map.get(e[0], "")
            total_subs += subscribers.get(ch_id, 0) or 0

    if _SINGLE_ORG:
        # ── single-org build (indies): the org tier is a pass-through — an
        # org page with exactly one card is a dead-end click, so the channel
        # grid that would normally live at {org_slug}/index.html is folded
        # straight into the homepage instead. write_org_page() is simply
        # never called for this org (see regenerate_org_pages()).
        org_slug, org = next(iter(ORG_MAP.items()))
        cards = ""
        for entry in org["channels"]:
            ch_name   = entry[0]
            ch_type   = entry[1]
            ch_slug   = slugify(ch_name)
            ch_id     = channel_ids_map.get(ch_name, "")
            logo_url  = logos.get(ch_id, "")
            sub_count = subscribers.get(ch_id, 0) or 0

            ch_streams = all_streams_by_channel.get(ch_name, [])
            ch_windows = _window_stats(ch_streams)
            ch_likes = 0
            likes = [s.get("peak_likes") or 0 for s in ch_streams if s.get("peak_likes")]
            if likes:
                ch_likes = max(likes)
            ch_is_live = any((s.get("stream_status") or "vod") == "live" for s in ch_streams)
            ch_live_badge = (
                '<span class="live-badge-sm"><span class="live-dot-sm"></span>LIVE</span>'
                if ch_is_live else ''
            )
            n_str   = ch_windows["30d"]["streams"]
            ch_peak = ch_windows["30d"]["peak"]

            words    = ch_name.replace("【", " ").replace("〔", " ").replace("Ch.", "").split()
            initials = "".join(w[0].upper() for w in words if w)[:2] or "?"
            if logo_url:
                _oe = f"this.outerHTML='<div class=&quot;channel-avatar-placeholder&quot;>{initials}</div>'"
                avatar_html = (
                    f'<img class="channel-avatar" src="{logo_url}" alt="" '
                    f'loading="lazy" referrerpolicy="no-referrer" onerror="{_oe}">'
                )
            else:
                avatar_html = f'<div class="channel-avatar-placeholder">{initials}</div>'

            role_lbl = "Org Channel" if ch_type == "org" else "Talent"

            cards += (
                f'\n    <a class="channel-card" href="{org_slug}/{ch_slug}/index.html"'
                f' data-name="{esc(ch_name)}" data-subs="{sub_count}" data-likes="{ch_likes}"'
                f' data-streams-7d="{ch_windows["7d"]["streams"]}" data-streams-30d="{ch_windows["30d"]["streams"]}" data-streams-all="{ch_windows["all"]["streams"]}"'
                f' data-peak-7d="{ch_windows["7d"]["peak"]}" data-peak-30d="{ch_windows["30d"]["peak"]}" data-peak-all="{ch_windows["all"]["peak"]}">\n'
                f'      <div class="ch-card-top">\n'
                f'        {avatar_html}\n'
                f'        {ch_live_badge}\n'
                f'      </div>\n'
                f'      <div class="ch-card-name-wrap">\n'
                f'        <div class="ch-card-name">{esc(ch_name)}</div>\n'
                f'        <div class="ch-card-role">{role_lbl}</div>\n'
                f'      </div>\n'
                f'      <div class="ch-card-stat-grid">\n'
                f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Subscribers</div><div class="ch-stat-cell-val">{fmt_subs(sub_count)}</div></div>\n'
                f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Streams</div><div class="ch-stat-cell-val js-streams">{n_str}</div></div>\n'
                f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Peak CCV</div><div class="ch-stat-cell-val js-peak">{fmt_compact(ch_peak) if ch_peak else "—"}</div></div>\n'
                f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Peak Likes</div><div class="ch-stat-cell-val">{fmt(ch_likes) if ch_likes else "—"}</div></div>\n'
                f'      </div>\n'
                f'    </a>'
            )

        # ── header ──────────────────────────────────────────────────────────
        # Stat-led hero, same accent-bar/card treatment as .channel-hero one
        # level down. Leads with channel/stream counts instead of an org
        # count (there's only ever one org here) — "38 orgs. 233 channels."
        # becomes "12 channels. 143 streams." for the indie framing. Range
        # chips + KPI row on the right double as the org-level hero stats,
        # reusing _ORG_JS's selectors (.org-hero-stats / #orgRange) so no
        # separate JS variant is needed for this merged page.
        body = (
            f'  <div class="site-hero">\n'
            f'    <div class="site-hero-accent"></div>\n'
            f'    <div class="site-hero-body">\n'
            f'      <div class="site-hero-info">\n'
            f'        <p class="eyebrow">IDVTuber Tracker &#8212; Independent VTubers</p>\n'
            f'        <h1>{total_channels} channels. {total_streams} streams. One signal feed.</h1>\n'
            f'        <p class="page-meta">Independent Indonesian VTubers, tracked and recorded in one place &#8212; subs, streams, and peak viewership, refreshed automatically. No agency, no group roster, just the numbers.</p>\n'
            f'        <p class="site-hero-updated">&#128337; Updated {generated_at}</p>\n'
            f'      </div>\n'
            f'      <div class="site-hero-side">\n'
            f'        <div class="range-chips" id="orgRange"'
            f' data-peak-7d="{site_windows["7d"]["peak"] or 0}"'
            f' data-peak-30d="{w30["peak"] or 0}"'
            f' data-peak-all="{site_windows["all"]["peak"] or 0}"'
            f' data-streams-7d="{site_windows["7d"]["streams"]}"'
            f' data-streams-30d="{w30["streams"]}"'
            f' data-streams-all="{site_windows["all"]["streams"]}">\n'
            f'          <span class="range-chip" data-range="7d">7D</span>\n'
            f'          <span class="range-chip active" data-range="30d">30D</span>\n'
            f'          <span class="range-chip" data-range="all">ALL</span>\n'
            f'        </div>\n'
            f'        <div class="site-hero-stats org-hero-stats">\n'
            f'          <div class="ohs"><div class="ohs-val">{total_channels}</div><div class="ohs-lbl">Channels</div></div>\n'
            f'          <div class="ohs"><div class="ohs-val js-streams">{w30["streams"]}</div><div class="ohs-lbl">Streams</div></div>\n'
            f'          <div class="ohs"><div class="ohs-val" id="idxLiveCount">{sitewide_live}</div><div class="ohs-lbl">Live now</div></div>\n'
            f'          <div class="ohs"><div class="ohs-val js-peak">{fmt_compact(w30["peak"]) if w30["peak"] else "—"}</div><div class="ohs-lbl">Peak CCV</div></div>\n'
            f'          <div class="ohs"><div class="ohs-val">{fmt_subs(total_subs)}</div><div class="ohs-lbl">Combined subs</div></div>\n'
            f'        </div>\n'
            f'      </div>\n'
            f'    </div>\n'
            f'  </div>\n'
            f'  <div class="sort-strip">\n'
            f'    <span class="sort-lbl">Sort by:</span>\n'
            f'    <span class="sort-chip active">Subscribers</span>\n'
            f'    <span class="sort-chip">Peak CCV</span>\n'
            f'    <span class="sort-chip">Peak Likes</span>\n'
            f'    <span class="sort-chip">Streams</span>\n'
            f'    <span class="sort-chip">A&#8211;Z</span>\n'
            f'  </div>\n'
            f'  <div class="channels-grid">{cards}\n  </div>\n'
        )

        html = _html_head("Independent VTubers", 0, live_count=sitewide_live) + body + _html_foot(0, 'org')
        (OUTPUT_DIR / "index.html").write_text(html, encoding="utf-8")
        log.info("Written: index.html (single-org, channel grid folded in)")
        return

    # ── multi-org build: original org-card index page ─────────────────────────
    def _org_stats(org):
        org_streams = []
        for e in org["channels"]:
            org_streams.extend(all_streams_by_channel.get(e[0], []))
        windows = _window_stats(org_streams)
        live = sum(1 for s in org_streams if (s.get("stream_status") or "vod") == "live")
        return windows, live

    org_cards = ""
    for org_slug, org in ORG_MAP.items():
        n_ch = len(org["channels"])
        windows, n_live = _org_stats(org)
        ow30 = windows["30d"]
        peak_str  = fmt_compact(ow30["peak"]) if ow30["peak"] else "—"
        live_badge = (
            f'<span class="live-badge-sm"><span class="live-dot-sm"></span>LIVE {n_live}</span>'
            if n_live else ''
        )
        org_cards += (
            f'\n    <a class="org-card" href="{org_slug}/index.html"'
            f' style="--org-color-dark:{org["color"]};--org-color-light:{org["color_light"]}"'
            f' data-name="{esc(org["label"])}"'
            f' data-streams-7d="{windows["7d"]["streams"]}" data-streams-30d="{windows["30d"]["streams"]}" data-streams-all="{windows["all"]["streams"]}"'
            f' data-peak-7d="{windows["7d"]["peak"]}" data-peak-30d="{windows["30d"]["peak"]}" data-peak-all="{windows["all"]["peak"]}">\n'
            f'      <div class="org-accent-bar"></div>\n'
            f'      <div class="org-card-body">\n'
            f'        <div class="org-card-top">\n'
            f'          <div class="org-card-title">{esc(org["label"])}</div>\n'
            f'          {live_badge}\n'
            f'        </div>\n'
            f'        <div class="org-card-desc">{esc(org["desc"])}</div>\n'
            f'        <div class="org-card-stats">\n'
            f'          <span class="ocs">&#128100; <strong>{n_ch}</strong></span>\n'
            f'          <span class="ocs">&#9654; <strong class="js-streams">{ow30["streams"]}</strong></span>\n'
            f'          <span class="ocs">&#128065; <strong class="js-peak">{peak_str}</strong> peak</span>\n'
            f'        </div>\n'
            f'      </div>\n'
            f'    </a>'
        )

    site_peak_30d = fmt_compact(w30["peak"]) if w30["peak"] else "—"

    body = (
        f'  <div class="site-hero">\n'
        f'    <div class="site-hero-accent"></div>\n'
        f'    <div class="site-hero-body">\n'
        f'      <div class="site-hero-info">\n'
        f'        <p class="eyebrow">IDVTuber Tracker &#8212; Live Analytics</p>\n'
        f'        <h1>{len(ORG_MAP)} orgs. {total_channels} channels. One signal feed.</h1>\n'
        f'        <p class="page-meta">Indonesian VTuber groups numbers tracked, displayed, and recorded in one place &#8212; subs, streams, and peak viewership, refreshed automatically.</p>\n'
        f'        <p class="site-hero-updated">&#128337; Updated {generated_at}</p>\n'
        f'      </div>\n'
        f'      <div class="site-hero-side">\n'
        f'        <div class="range-chips" id="homeRange"'
        f' data-peak-7d="{site_windows["7d"]["peak"] or 0}"'
        f' data-peak-30d="{site_windows["30d"]["peak"] or 0}"'
        f' data-peak-all="{site_windows["all"]["peak"] or 0}">\n'
        f'          <span class="range-chip" data-range="7d">7D</span>\n'
        f'          <span class="range-chip active" data-range="30d">30D</span>\n'
        f'          <span class="range-chip" data-range="all">ALL</span>\n'
        f'        </div>\n'
        f'        <div class="site-hero-stats">\n'
        f'          <div class="ohs"><div class="ohs-val">{len(ORG_MAP)}</div><div class="ohs-lbl">Organisations</div></div>\n'
        f'          <div class="ohs"><div class="ohs-val">{total_channels}</div><div class="ohs-lbl">Channels</div></div>\n'
        f'          <div class="ohs"><div class="ohs-val">{total_streams}</div><div class="ohs-lbl">Streams</div></div>\n'
        f'          <div class="ohs"><div class="ohs-val" id="idxLiveCount">{sitewide_live}</div><div class="ohs-lbl">Live now</div></div>\n'
        f'          <div class="ohs"><div class="ohs-val js-site-peak">{site_peak_30d}</div><div class="ohs-lbl">Peak CCV</div></div>\n'
        f'        </div>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'  </div>\n'
        f'  <div class="sort-strip">\n'
        f'    <span class="sort-lbl">Sort:</span>\n'
        f'    <span class="filter-chip active">All</span>\n'
        f'    <span class="filter-chip">A&#8211;Z</span>\n'
        f'    <span class="filter-chip">Peak Viewers</span>\n'
        f'  </div>\n'
        f'  <div class="orgs-grid">{org_cards}\n  </div>\n'
    )

    html = _html_head("Stream Analytics", 0, live_count=sitewide_live) + body + _html_foot(0, 'index')
    (OUTPUT_DIR / "index.html").write_text(html, encoding="utf-8")
    log.info("Written: index.html")


def write_search_index(resolved_channels: dict,
                        all_streams_by_channel: dict) -> None:
    """
    Writes a flat search-index.json at the site root: one entry per org,
    channel, and stream. Backs the global search overlay (_SEARCH_JS).

    Paths are stored root-relative (e.g. "eon-of-stars/harris-caine/index.html")
    rather than depth-relative — a single index file is shared by every page
    regardless of how deep it lives, so the client prefixes the correct
    number of "../" segments at click time (see window.SITE_DEPTH in
    _html_head) instead of this function needing to know who's asking.

    Cheap to regenerate every run — pure serialization of data already in
    memory (ORG_MAP + all_streams_by_channel), no extra DB/API calls.
    """
    entries: list[dict] = []

    for org_slug, org in ORG_MAP.items():
        entries.append({
            "type": "org",
            "name": org["label"],
            "sub":  f'{len(org["channels"])} channels',
            "path": f"{org_slug}/index.html",
        })
        for entry in org["channels"]:
            ch_name = entry[0]
            if ch_name not in resolved_channels:
                continue
            ch_slug = slugify(ch_name)
            streams = all_streams_by_channel.get(ch_name, [])
            entries.append({
                "type": "channel",
                "name": ch_name,
                "sub":  f'{org["label"]} · {len(streams)} streams',
                "path": f"{org_slug}/{ch_slug}/index.html",
            })
            for stream in streams:
                vid    = stream["video_id"]
                v_slug = slugify(vid)
                title  = stream.get("video_title") or vid
                entries.append({
                    "type": "stream",
                    "name": title,
                    "sub":  f'{ch_name} · {org["label"]}',
                    "path": f"{org_slug}/{ch_slug}/{v_slug}.html",
                })

    try:
        (OUTPUT_DIR / "search-index.json").write_text(
            json.dumps(entries, ensure_ascii=False), encoding="utf-8"
        )
        log.info("Search index written — %d entries.", len(entries))
    except Exception as e:
        log.warning("Could not write search-index.json: %s", e)

    # Tiny companion file for the nav's "LIVE n" pill (see _LIVE_PILL_JS).
    # Deliberately NOT folded into search-index.json — that file is lazy
    # loaded only when someone opens the search overlay, but the live count
    # needs to be fetched unconditionally on every page view, so it gets its
    # own few-bytes file instead of forcing an eager load of the (much
    # larger) full search index on every single page.
    live_count = sum(
        1 for streams in all_streams_by_channel.values()
        for s in streams if (s.get("stream_status") or "vod") == "live"
    )
    try:
        (OUTPUT_DIR / "live-count.json").write_text(
            json.dumps({"count": live_count}), encoding="utf-8"
        )
    except Exception as e:
        log.warning("Could not write live-count.json: %s", e)


def write_live_page(all_streams_by_channel: dict,
                     logos: dict | None = None,
                     channel_ids_map: dict | None = None) -> None:
    """
    Writes live.html at the site root — every currently-live stream across
    every org, in one flat grid. Linked from the nav's "LIVE n" pill.

    Regenerated unconditionally every run, same as write_index() and
    write_search_index() — there's no dirty-tracking to do here, since
    "who's live right now" changes on exactly the cadence this already
    runs on. The whole page IS the dirty set every time.

    Multiple orgs appear on this page at once (same situation as the index
    grid), so each card gets its own --org-color-dark/--org-color-light
    pair inline rather than relying on the single :root-level value that
    single-org pages use — see the .live-card CSS rules in dashboard.css.
    """
    logos           = logos or {}
    channel_ids_map = channel_ids_map or {}

    cards = ""
    live_count = 0
    for ch_name, streams in all_streams_by_channel.items():
        org_result = _CH_TO_ORG.get(ch_name)
        if not org_result:
            continue
        org_slug, org = org_result

        for stream in streams:
            if (stream.get("stream_status") or "vod") != "live":
                continue
            live_count += 1

            vid      = stream["video_id"]
            ch_slug  = slugify(ch_name)
            v_slug   = slugify(vid)
            ch_id    = channel_ids_map.get(ch_name, "")
            logo_url = logos.get(ch_id, "")

            words    = ch_name.replace("【", " ").replace("〔", " ").replace("Ch.", "").split()
            initials = "".join(w[0].upper() for w in words if w)[:2] or "?"
            if logo_url:
                _oe = f"this.outerHTML='<div class=&quot;live-card-avatar-ph&quot;>{initials}</div>'"
                avatar_html = (
                    f'<img class="live-card-avatar" src="{logo_url}" alt="" '
                    f'loading="lazy" referrerpolicy="no-referrer" onerror="{_oe}">'
                )
            else:
                avatar_html = f'<div class="live-card-avatar-ph">{initials}</div>'

            title   = esc((stream.get("video_title") or vid)[:90])
            started = fmt_dt(stream.get("first_seen"), time_only=True)
            peak    = fmt(stream.get("peak_viewers"))

            cards += (
                f'\n    <a class="live-card" href="{org_slug}/{ch_slug}/{v_slug}.html"'
                f' style="--org-color-dark:{org["color"]};--org-color-light:{org["color_light"]}">\n'
                f'      <div class="live-card-top">\n'
                f'        {avatar_html}\n'
                f'        <span class="live-card-badge"><span class="dot"></span>LIVE</span>\n'
                f'      </div>\n'
                f'      <div class="live-card-title">{title}</div>\n'
                f'      <div class="live-card-meta">{esc(ch_name)} &#183; {esc(org["label"])}</div>\n'
                f'      <div class="live-card-stats">\n'
                f'        <div>Started<span>{started} WIB</span></div>\n'
                f'        <div>Peak CCV<span>{peak}</span></div>\n'
                f'      </div>\n'
                f'    </a>'
            )

    if cards:
        grid_html = f'  <div class="live-grid">{cards}\n  </div>\n'
    else:
        grid_html = (
            '  <p class="empty" style="padding:2rem 0;">'
            'Nothing is live right now — check back later, or browse the archive from the homepage.'
            '</p>\n'
        )

    bc = _breadcrumb([("Home", "index.html"), ("Live", "")])
    plural = "" if live_count == 1 else "s"
    body = (
        bc
        + f'  <header>\n'
        f'    <p class="eyebrow">IDVTuber Tracker &#8212; Live Now</p>\n'
        f'    <h1>Live <em>Right Now</em></h1>\n'
        f'    <p class="page-meta">{live_count} stream{plural} currently live, across every tracked org.</p>\n'
        f'  </header>\n'
        + grid_html
    )

    html = _html_head("Live Now", 0, live_count=live_count) + body + _html_foot(0)
    (OUTPUT_DIR / "live.html").write_text(html, encoding="utf-8")
    log.info("Written: live.html (%d live stream(s)).", live_count)


def write_org_page(org_slug: str, org: dict, stream_counts: dict,
                   logos: dict[str, str] | None = None,
                   channel_ids_map: dict[str, str] | None = None,
                   subscribers: dict[str, int] | None = None,
                   all_streams_by_channel: dict | None = None) -> None:
    org_dir = OUTPUT_DIR / org_slug
    org_dir.mkdir(exist_ok=True)
    logos                  = logos or {}
    channel_ids_map        = channel_ids_map or {}
    subscribers            = subscribers or {}
    all_streams_by_channel = all_streams_by_channel or {}

    # ── aggregate org-level windowed stats for hero ───────────────────────────
    org_streams = []
    for e in org["channels"]:
        org_streams.extend(all_streams_by_channel.get(e[0], []))
    org_windows = _window_stats(org_streams)
    total_subs = 0
    for e in org["channels"]:
        ch_id = channel_ids_map.get(e[0], "")
        total_subs += subscribers.get(ch_id, 0) or 0

    # ── channel cards ─────────────────────────────────────────────────────────
    cards = ""
    for entry in org["channels"]:
        ch_name   = entry[0]
        ch_type   = entry[1]
        ch_slug   = slugify(ch_name)
        ch_id     = channel_ids_map.get(ch_name, "")
        logo_url  = logos.get(ch_id, "")
        sub_count = subscribers.get(ch_id, 0) or 0

        # per-channel windowed peak/streams + all-time likes + live status
        ch_streams = all_streams_by_channel.get(ch_name, [])
        ch_windows = _window_stats(ch_streams)
        ch_likes = 0
        likes = [s.get("peak_likes") or 0 for s in ch_streams if s.get("peak_likes")]
        if likes:
            ch_likes = max(likes)
        ch_is_live = any((s.get("stream_status") or "vod") == "live" for s in ch_streams)
        ch_live_badge = (
            '<span class="live-badge-sm"><span class="live-dot-sm"></span>LIVE</span>'
            if ch_is_live else ''
        )
        n_str  = ch_windows["30d"]["streams"]
        ch_peak = ch_windows["30d"]["peak"]

        # avatar
        words    = ch_name.replace("【", " ").replace("〔", " ").replace("Ch.", "").split()
        initials = "".join(w[0].upper() for w in words if w)[:2] or "?"
        if logo_url:
            _oe = f"this.outerHTML='<div class=&quot;channel-avatar-placeholder&quot;>{initials}</div>'"
            avatar_html = (
                f'<img class="channel-avatar" src="{logo_url}" alt="" '
                f'loading="lazy" referrerpolicy="no-referrer" onerror="{_oe}">'
            )
        else:
            avatar_html = f'<div class="channel-avatar-placeholder">{initials}</div>'

        role_lbl = "Org Channel" if ch_type == "org" else "Talent"

        cards += (
            f'\n    <a class="channel-card" href="{ch_slug}/index.html"'
            f' data-name="{esc(ch_name)}" data-subs="{sub_count}" data-likes="{ch_likes}"'
            f' data-streams-7d="{ch_windows["7d"]["streams"]}" data-streams-30d="{ch_windows["30d"]["streams"]}" data-streams-all="{ch_windows["all"]["streams"]}"'
            f' data-peak-7d="{ch_windows["7d"]["peak"]}" data-peak-30d="{ch_windows["30d"]["peak"]}" data-peak-all="{ch_windows["all"]["peak"]}">\n'
            f'      <div class="ch-card-top">\n'
            f'        {avatar_html}\n'
            f'        {ch_live_badge}\n'
            f'      </div>\n'
            f'      <div class="ch-card-name-wrap">\n'
            f'        <div class="ch-card-name">{esc(ch_name)}</div>\n'
            f'        <div class="ch-card-role">{role_lbl}</div>\n'
            f'      </div>\n'
            f'      <div class="ch-card-stat-grid">\n'
            f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Subscribers</div><div class="ch-stat-cell-val">{fmt_subs(sub_count)}</div></div>\n'
            f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Streams</div><div class="ch-stat-cell-val js-streams">{n_str}</div></div>\n'
            f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Peak CCV</div><div class="ch-stat-cell-val js-peak">{fmt_compact(ch_peak) if ch_peak else "—"}</div></div>\n'
            f'        <div class="ch-stat-cell"><div class="ch-stat-cell-lbl">Peak Likes</div><div class="ch-stat-cell-val">{fmt(ch_likes) if ch_likes else "—"}</div></div>\n'
            f'      </div>\n'
            f'    </a>'
        )

    bc = _breadcrumb([("Home", "../index.html"), (org["label"], "")])
    w30 = org_windows["30d"]
    body = (
        bc
        # org hero
        + f'  <div class="org-hero">\n'
        f'    <div class="org-hero-accent"></div>\n'
        f'    <div class="org-hero-body">\n'
        f'      <div class="org-hero-info">\n'
        f'        <div class="org-hero-name">{esc(org["label"])}</div>\n'
        f'        <div class="org-hero-desc">{esc(org["desc"])}</div>\n'
        f'        <div class="org-hero-stats">\n'
        f'          <div class="ohs"><div class="ohs-val">{len(org["channels"])}</div><div class="ohs-lbl">Channels</div></div>\n'
        f'          <div class="ohs"><div class="ohs-val js-streams">{w30["streams"]}</div><div class="ohs-lbl">Streams</div></div>\n'
        f'          <div class="ohs"><div class="ohs-val js-peak">{fmt_compact(w30["peak"]) if w30["peak"] else "—"}</div><div class="ohs-lbl">Peak CCV</div></div>\n'
        f'          <div class="ohs"><div class="ohs-val">{fmt_subs(total_subs)}</div><div class="ohs-lbl">Combined subs</div></div>\n'
        f'        </div>\n'
        f'      </div>\n'
        f'      <div class="range-chips" id="orgRange"'
        f' data-peak-7d="{org_windows["7d"]["peak"] or 0}"'
        f' data-peak-30d="{w30["peak"] or 0}"'
        f' data-peak-all="{org_windows["all"]["peak"] or 0}"'
        f' data-streams-7d="{org_windows["7d"]["streams"]}"'
        f' data-streams-30d="{w30["streams"]}"'
        f' data-streams-all="{org_windows["all"]["streams"]}">\n'
        f'        <span class="range-chip" data-range="7d">7D</span>\n'
        f'        <span class="range-chip active" data-range="30d">30D</span>\n'
        f'        <span class="range-chip" data-range="all">ALL</span>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'  </div>\n'
        # sort strip
        + f'  <div class="sort-strip">\n'
        f'    <span class="sort-lbl">Sort by:</span>\n'
        f'    <span class="sort-chip active">Subscribers</span>\n'
        f'    <span class="sort-chip">Peak CCV</span>\n'
        f'    <span class="sort-chip">Peak Likes</span>\n'
        f'    <span class="sort-chip">Streams</span>\n'
        f'    <span class="sort-chip">A&#8211;Z</span>\n'
        f'  </div>\n'
        f'  <div class="channels-grid">{cards}\n  </div>\n'
    )

    html = _html_head(org["label"], 1, org["color"], org["color_light"]) + body + _html_foot(1, 'org')
    (org_dir / "index.html").write_text(html, encoding="utf-8")
    log.info("Written: %s/index.html", org_slug)


def _dur_str(first, last) -> str:
    """Return compact duration string e.g. '2h 07m'."""
    if not first or not last:
        return ""
    try:
        total = int((last - first).total_seconds())
        h, rem = divmod(total, 3600)
        m = rem // 60
        return f"{h}h {m:02d}m" if h else f"{m}m"
    except Exception:
        return ""


def write_channel_page(org_slug: str, org: dict, ch_name: str,
                       streams: list[dict],
                       logos: dict | None = None,
                       channel_ids_map: dict | None = None,
                       subscribers: dict | None = None) -> None:
    logos          = logos          or {}
    channel_ids_map = channel_ids_map or {}
    subscribers    = subscribers    or {}

    ch_slug  = slugify(ch_name)
    ch_dir   = OUTPUT_DIR / org_slug / ch_slug
    ch_dir.mkdir(parents=True, exist_ok=True)

    # ── resolve avatar + subscriber count ─────────────────────────────────────
    ch_id    = channel_ids_map.get(ch_name, "")
    logo_url = logos.get(ch_id, "")
    sub_raw  = subscribers.get(ch_id, 0) or 0
    sub_fmt  = fmt(sub_raw) if sub_raw else "—"

    # Build initials fallback (up to 2 chars from the display name)
    words    = ch_name.replace("【", " ").replace("〔", " ").replace("Ch.", "").split()
    initials = "".join(w[0].upper() for w in words if w)[:2] or "?"

    # Avatar HTML — real image with onerror fallback to initials placeholder
    if logo_url:
        _oe = f"this.outerHTML='<div class=&quot;hero-avatar-large-placeholder&quot;>{initials}</div>'"
        avatar_html = (
            f'<img class="hero-avatar-large"'
            f' src="{logo_url}" alt="{esc(ch_name)} avatar" loading="lazy"'
            f' onerror="{_oe}">\n'
        )
    else:
        avatar_html = f'<div class="hero-avatar-large-placeholder">{initials}</div>\n'

    # ── tracking window ────────────────────────────────────────────────────────
    def _stream_dt(s):
        v = s.get("first_seen")
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = datetime.fromisoformat(v.replace("Z", "+00:00"))
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.astimezone(_LOCAL_TZ)
        except Exception:
            return None

    dts = [d for d in (_stream_dt(s) for s in streams) if d]
    if dts:
        oldest  = min(dts)
        newest  = max(dts)
        if oldest.year == newest.year and oldest.month == newest.month:
            window_str = oldest.strftime("%b %Y")
        else:
            window_str = f'{oldest.strftime("%b %Y")} – {newest.strftime("%b %Y")}'
    else:
        window_str = "—"

    # ── aggregate stats (KPI strip) ───────────────────────────────────────────
    peak_ccvs   = [s["peak_viewers"] for s in streams if s.get("peak_viewers")]
    avg_peak    = round(sum(peak_ccvs) / len(peak_ccvs)) if peak_ccvs else 0
    total_views = sum(s.get("view_count") or 0 for s in streams)
    ch_windows  = _window_stats(streams)  # only Streams/Peak CCV cells are range-aware;
                                           # Avg peak CCV / Total views stay all-time, same
                                           # precedent as "Channels"/"Combined subs" at the
                                           # org level not being windowed either.

    # ── records ────────────────────────────────────────────────────────────────
    def _best(key):
        candidates = [(s.get(key) or 0, s) for s in streams if s.get(key)]
        return max(candidates, key=lambda x: x[0]) if candidates else (0, None)

    peak_ccv_val, peak_ccv_stream = _best("peak_viewers")
    peak_likes_val, peak_likes_stream = _best("peak_likes")
    peak_views_val, peak_views_stream = _best("view_count")

    def _rec_title(stream):
        if not stream:
            return "—"
        t = (stream.get("video_title") or "").strip()
        return t[:45] + "…" if len(t) > 45 else t or "—"

    def _rec_date(stream):
        if not stream:
            return "—"
        dt = _stream_dt(stream)
        return dt.strftime("%d %b %Y") if dt else "—"

    # ── group streams by month ─────────────────────────────────────────────────
    months: OrderedDict = OrderedDict()
    for stream in streams:
        dt = _stream_dt(stream)
        month_key = dt.strftime("%B %Y") if dt else "Unknown"
        months.setdefault(month_key, []).append(stream)

    # ── monthly summary table (for sidebar) ───────────────────────────────────
    monthly_peaks = {}
    for mk, ms in months.items():
        mp = max((s.get("peak_viewers") or 0 for s in ms), default=0)
        monthly_peaks[mk] = mp
    global_best_month = max(monthly_peaks, key=monthly_peaks.get) if monthly_peaks else None

    monthly_rows = ""
    for mk, ms in months.items():
        is_best = mk == global_best_month
        tr_cls  = ' class="month-best-row"' if is_best else ""
        pk      = monthly_peaks.get(mk, 0)
        monthly_rows += (
            f'      <tr{tr_cls}>\n'
            f'        <td><a class="month-a" href="#">{mk}</a></td>\n'
            f'        <td><span class="month-cnt">{len(ms)}</span></td>\n'
            f'        <td class="month-peak">{fmt(pk)}</td>\n'
            f'      </tr>\n'
        )

    # ── recent streams card grid (latest 8, full-width 4×2) ──────────────────
    recent_8 = streams[:8]

    def _rc_card(stream) -> str:
        vid    = stream["video_id"]
        v_slug = slugify(vid)
        status = stream.get("stream_status", "vod") or "vod"
        live   = status == "live"
        dt     = _stream_dt(stream)
        date_s = dt.strftime("%d %b %Y") if dt else "—"
        title  = esc((stream.get("video_title") or vid)[:70])
        thumb  = f"https://i.ytimg.com/vi/{vid}/mqdefault_live.jpg"
        _onerr = "this.parentNode.innerHTML='<div class=&quot;rc-placeholder&quot;&gt;&#9654;</div>'"
        live_b = '<span class="rc-live">Live</span>' if live else ""
        return (
            f'    <a class="rc" href="{v_slug}.html">\n'
            f'      <div class="rc-thumb">\n'
            f'        <img src="{thumb}" alt="" loading="lazy" onerror="{_onerr}">\n'
            f'        {live_b}\n'
            f'      </div>\n'
            f'      <div class="rc-body">\n'
            f'        <div class="rc-title">{title}</div>\n'
            f'        <div class="rc-date">{date_s}</div>\n'
            f'        <div class="rc-stats">\n'
            f'          <span>&#128065; <span class="rc-peak">{fmt(stream.get("peak_viewers"))}</span></span>\n'
            f'          <span>&#9825; {fmt(stream.get("peak_likes"))}</span>\n'
            f'          <span>&#9654; {fmt(stream.get("view_count"))}</span>\n'
            f'        </div>\n'
            f'      </div>\n'
            f'    </a>\n'
        )

    recent_cards_html = ""
    for s in recent_8:
        recent_cards_html += _rc_card(s)

    recent_section_html = (
        f'  <div class="recent-streams-section">\n'
        f'    <div class="recent-streams-hdr">Recent streams</div>\n'
        f'    <div class="recent-grid">\n'
        + recent_cards_html
        + f'    </div>\n'
        f'  </div>\n'
    ) if recent_8 else ""

    # ── chronological stream list — collapsible month groups ─────────────────
    def _row_item(stream) -> str:
        vid      = stream["video_id"]
        v_slug   = slugify(vid)
        status   = stream.get("stream_status", "vod") or "vod"
        live     = status == "live"
        badge    = '<div class="live-badge">Live</div>' if live else ""
        dt       = _stream_dt(stream)
        date_str = dt.strftime("%d %b %Y") if dt else "—"
        time_str = fmt_dt(stream.get("first_seen"), time_only=True)
        dur      = _dur_str(stream.get("first_seen"), stream.get("last_seen"))
        thumb    = f"https://i.ytimg.com/vi/{vid}/mqdefault_live.jpg"
        title    = esc((stream.get("video_title") or vid)[:90])
        _onerr   = "this.parentNode.innerHTML='<div class=&quot;th-placeholder&quot;&gt;&#9654;</div>'"
        dur_part = (
            f'        <span class="row-sep">·</span>\n'
            f'        <span>{dur}</span>\n'
        ) if dur else ""
        return (
            f'  <a class="stream-row-item" href="{v_slug}.html">\n'
            f'    <div class="stream-thumb-cell">\n'
            f'      <img src="{thumb}" alt="" loading="lazy" onerror="{_onerr}">\n'
            f'      {badge}\n'
            f'    </div>\n'
            f'    <div class="stream-row-body">\n'
            f'      <div class="stream-row-title">{title}</div>\n'
            f'      <div class="stream-row-meta">\n'
            f'        <span>{date_str}</span>\n'
            f'        <span class="row-sep">·</span>\n'
            f'        <span>{time_str} WIB</span>\n'
            + dur_part
            + f'      </div>\n'
            f'      <div class="stream-row-stats">\n'
            f'        <span class="rs">&#128065; <span class="rs-peak">{fmt(stream.get("peak_viewers"))}</span> peak</span>\n'
            f'        <span class="rs">&#9825; {fmt(stream.get("peak_likes"))}</span>\n'
            f'        <span class="rs">&#9654; {fmt(stream.get("view_count"))}</span>\n'
            f'      </div>\n'
            f'    </div>\n'
            f'  </a>\n'
        )

    # Build one collapsible group per month; first month open by default
    chron_groups = ""
    for i, (mk, ms) in enumerate(months.items()):
        open_cls = " is-open" if i == 0 else ""
        rows_html = "".join(_row_item(s) for s in ms)
        chron_groups += (
            f'  <div class="month-group{open_cls}">\n'
            f'    <button class="month-toggle" aria-expanded="{"true" if i == 0 else "false"}">\n'
            f'      <span class="month-toggle-left">{mk}</span>\n'
            f'      <span class="month-toggle-right">'
            f'<span class="month-cnt-badge">{len(ms)}</span>'
            f'<span class="month-chevron">▾</span>'
            f'</span>\n'
            f'    </button>\n'
            f'    <div class="month-body">\n'
            + rows_html
            + f'    </div>\n'
            f'  </div>\n'
        )

    if not months:
        chron_groups = '  <p class="empty" style="padding:1.25rem;">No streams recorded yet.</p>\n'

    chron_js = (
        '<script>\n'
        '(function(){\n'
        '  document.querySelectorAll(".month-toggle").forEach(function(btn){\n'
        '    btn.addEventListener("click", function(){\n'
        '      var grp = btn.closest(".month-group");\n'
        '      var open = grp.classList.toggle("is-open");\n'
        '      btn.setAttribute("aria-expanded", open ? "true" : "false");\n'
        '    });\n'
        '  });\n'
        '})();\n'
        '</script>\n'
    )

    # Range chips (7D/30D/ALL) swap Streams tracked / Peak CCV between the
    # three windows baked into #channelRange's data-* attributes at build
    # time — no re-fetch, no recomputation, just a text swap.
    range_js = (
        '<script>\n'
        '(function(){\n'
        '  var wrap = document.getElementById("channelRange");\n'
        '  if (!wrap) return;\n'
        '  var streamsEl = document.querySelector(".kpi-strip .js-streams");\n'
        '  var peakEl    = document.querySelector(".kpi-strip .js-peak");\n'
        '  function fmtNum(n) {\n'
        '    n = parseInt(n, 10) || 0;\n'
        '    if (n >= 1000000) return (n / 1000000).toFixed(1) + "M";\n'
        '    if (n >= 1000) return (n / 1000).toFixed(1) + "K";\n'
        '    return String(n);\n'
        '  }\n'
        '  wrap.querySelectorAll(".range-chip").forEach(function(chip){\n'
        '    chip.addEventListener("click", function(){\n'
        '      wrap.querySelectorAll(".range-chip").forEach(function(c){ c.classList.remove("active"); });\n'
        '      chip.classList.add("active");\n'
        '      var range = chip.getAttribute("data-range") || "30d";\n'
        '      if (streamsEl) streamsEl.textContent = wrap.getAttribute("data-streams-" + range) || "0";\n'
        '      if (peakEl) {\n'
        '        var p = wrap.getAttribute("data-peak-" + range) || "0";\n'
        '        peakEl.textContent = p === "0" ? "—" : fmtNum(p);\n'
        '      }\n'
        '    });\n'
        '  });\n'
        '})();\n'
        '</script>\n'
    )

    stream_list_html = (
        f'    <div class="stream-list-panel">\n'
        f'      <div class="panel-hdr">All streams — by month</div>\n'
        + chron_groups
        + f'    </div>\n'
    )

    # ── assemble page ─────────────────────────────────────────────────────────
    # Org crumb is dropped on single-org builds — there's no {org}/index.html
    # to link to (write_index() absorbed it), so it would be a dead link.
    # The org label still shows in the hero-org-badge just above for context.
    bc = _breadcrumb(
        [("Home", "../../index.html")]
        + ([] if _SINGLE_ORG else [(org["label"], "../index.html")])
        + [(ch_name, "")]
    )

    yt_url = f"https://youtube.com/channel/{ch_id}" if ch_id else "#"

    body = (
        bc
        # ── hero ──
        + f'  <div class="channel-hero">\n'
        f'    <div class="hero-shimmer"></div>\n'
        f'    <div class="hero-body">\n'
        f'      {avatar_html}'
        f'      <div class="hero-info">\n'
        f'        <div class="hero-org-badge">\n'
        f'          <div class="hero-org-dot"></div>\n'
        f'          {esc(org["label"])}\n'
        f'        </div>\n'
        f'        <div class="hero-name">{esc(ch_name)}</div>\n'
        f'        <div class="hero-meta-row">\n'
        f'          <div class="hero-meta-item"><span>Subscribers</span><strong>{sub_fmt}</strong></div>\n'
        f'          <div class="hero-meta-item"><span>Total views</span><strong>{fmt(sum(s.get("view_count") or 0 for s in streams))}</strong></div>\n'
        f'          <div class="hero-meta-item"><span>Tracking</span><strong>{window_str}</strong></div>\n'
        f'        </div>\n'
        f'      </div>\n'
        f'      <div class="hero-actions">\n'
        f'        <a class="external-link" href="{yt_url}" target="_blank" rel="noopener">View Channel &#8599;</a>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'  </div>\n'
        # ── range chips + kpi strip ──
        + f'  <div class="range-chips" id="channelRange"'
        f' data-streams-7d="{ch_windows["7d"]["streams"]}" data-streams-30d="{ch_windows["30d"]["streams"]}" data-streams-all="{ch_windows["all"]["streams"]}"'
        f' data-peak-7d="{ch_windows["7d"]["peak"] or 0}"'
        f' data-peak-30d="{ch_windows["30d"]["peak"] or 0}"'
        f' data-peak-all="{ch_windows["all"]["peak"] or 0}"'
        f' style="margin-bottom:0.75rem;">\n'
        f'    <span class="range-chip" data-range="7d">7D</span>\n'
        f'    <span class="range-chip active" data-range="30d">30D</span>\n'
        f'    <span class="range-chip" data-range="all">ALL</span>\n'
        f'  </div>\n'
        f'  <div class="kpi-strip">\n'
        f'    <div class="kpi-cell"><div class="kpi-label">Streams tracked</div><div class="kpi-value js-streams">{ch_windows["30d"]["streams"]}</div><div class="kpi-sub">{window_str}</div></div>\n'
        f'    <div class="kpi-cell"><div class="kpi-label">Peak CCV</div><div class="kpi-value js-peak">{fmt_compact(ch_windows["30d"]["peak"]) if ch_windows["30d"]["peak"] else "—"}</div><div class="kpi-sub">Selected range</div></div>\n'
        f'    <div class="kpi-cell"><div class="kpi-label">Avg peak CCV</div><div class="kpi-value">{fmt(avg_peak)}</div><div class="kpi-sub">Per stream, all-time</div></div>\n'
        f'    <div class="kpi-cell"><div class="kpi-label">Total views</div><div class="kpi-value">{fmt(total_views)}</div><div class="kpi-sub">All-time</div></div>\n'
        f'  </div>\n'
        # ── recent streams grid (full-width, above main grid) ──
        + recent_section_html
        # ── main grid ──
        + f'  <div class="ch-main-grid">\n'
        # left: collapsible chronological list
        + stream_list_html
        # right: sidebar
        + f'    <div class="sidebar-col">\n'
        # 1. monthly summary
        + f'      <div class="side-panel">\n'
        f'        <div class="panel-hdr">Monthly summary</div>\n'
        f'        <table class="monthly-tbl">\n'
        f'          <thead><tr>\n'
        f'            <th>Month</th><th>Streams</th><th>Peak CCV</th>\n'
        f'          </tr></thead>\n'
        f'          <tbody>\n'
        + monthly_rows
        + f'          </tbody>\n'
        f'        </table>\n'
        f'      </div>\n'
        # 2. current subscribers (count only, no graph)
        + f'      <div class="side-panel">\n'
        f'        <div class="panel-hdr">Subscribers</div>\n'
        f'        <div class="subs-body">\n'
        f'          <div class="subs-count">{sub_fmt}</div>\n'
        f'          <div class="subs-label">YouTube subscribers</div>\n'
        f'        </div>\n'
        f'      </div>\n'
        # 3. channel records
        + f'      <div class="side-panel">\n'
        f'        <div class="panel-hdr">Channel records</div>\n'
        f'        <div class="rec-row">\n'
        f'          <div><div class="rec-lbl">Peak CCV</div><div class="rec-val">{fmt_compact(peak_ccv_val)}</div><div class="rec-ctx">{esc(_rec_title(peak_ccv_stream))}</div></div>\n'
        f'          <div class="rec-right"><div class="rec-lbl">Date</div><div class="rec-date">{_rec_date(peak_ccv_stream)}</div></div>\n'
        f'        </div>\n'
        f'        <div class="rec-row">\n'
        f'          <div><div class="rec-lbl">Most liked</div><div class="rec-val">{fmt(peak_likes_val)}</div><div class="rec-ctx">{esc(_rec_title(peak_likes_stream))}</div></div>\n'
        f'          <div class="rec-right"><div class="rec-lbl">Date</div><div class="rec-date">{_rec_date(peak_likes_stream)}</div></div>\n'
        f'        </div>\n'
        f'        <div class="rec-row">\n'
        f'          <div><div class="rec-lbl">Most viewed</div><div class="rec-val">{fmt(peak_views_val)}</div><div class="rec-ctx">{esc(_rec_title(peak_views_stream))}</div></div>\n'
        f'          <div class="rec-right"><div class="rec-lbl">Date</div><div class="rec-date">{_rec_date(peak_views_stream)}</div></div>\n'
        f'        </div>\n'
        f'      </div>\n'
        f'    </div>\n'   # close sidebar-col
        f'  </div>\n'     # close ch-main-grid
        + chron_js
        + range_js
    )

    html = _html_head(ch_name, 2, org["color"], org["color_light"]) + body + _html_foot(2)
    (ch_dir / "index.html").write_text(html, encoding="utf-8")
    log.info("  Written: %s/%s/index.html", org_slug, ch_slug)


def write_stream_page(org_slug: str, org: dict, ch_name: str,
                      stream: dict, timeseries: list[dict]) -> None:
    vid     = stream["video_id"]
    v_slug  = slugify(vid)
    ch_slug = slugify(ch_name)
    ch_dir  = OUTPUT_DIR / org_slug / ch_slug
    ch_dir.mkdir(parents=True, exist_ok=True)

    status = stream.get("stream_status", "vod") or "vod"
    if status == "live":
        s_cls, s_lbl = "status-live",     "&#128308; Live"
    elif status == "upcoming":
        s_cls, s_lbl = "status-upcoming", "Upcoming"
    else:
        s_cls, s_lbl = "status-vod",      "VOD"

    labels   = [fmt_dt(r["collected_at"], time_only=True) for r in timeseries]
    viewers  = [int(r["concurrent_viewers"] or 0) for r in timeseries]
    likes    = [int(r["like_count"]         or 0) for r in timeseries]
    comments = [int(r["comment_count"]      or 0) for r in timeseries]

    title_text  = stream["video_title"] or vid
    short_title = (title_text[:40] + "…") if len(title_text) > 40 else title_text
    org_color       = org["color"]
    org_color_light = org["color_light"]

    bc = _breadcrumb(
        [("Home", "../../index.html")]
        + ([] if _SINGLE_ORG else [(org["label"], "../index.html")])
        + [(ch_name, "index.html"), (short_title, "")]
    )

    chart_script = (
        '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>\n'
        '<script src="https://cdnjs.cloudflare.com/ajax/libs/hammer.js/2.0.8/hammer.min.js"></script>\n'
        '<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>'
    )

    def _fmt_duration(first, last) -> str:
        if not first or not last:
            return "—"
        try:
            delta = last - first
            total = int(delta.total_seconds())
            h, rem = divmod(total, 3600)
            m, s   = divmod(rem, 60)
            return f"{h}h {m:02d}m {s:02d}s" if h else f"{m}m {s:02d}s"
        except Exception:
            return "—"

    duration_str = _fmt_duration(stream["first_seen"], stream["last_seen"])

    body = (
        bc
        + f'  <header>\n'
        f'    <p class="eyebrow">{esc(org["label"])} &nbsp;&#183;&nbsp; {esc(ch_name)}</p>\n'
        f'    <span class="stream-status {s_cls}" style="display:inline-block;margin-bottom:0.75rem;">{s_lbl}</span>\n'
        f'    <h1>{esc(title_text)}</h1>\n'
        f'    <p class="page-meta">Video ID: {esc(vid)}</p>\n'
        f'  </header>\n\n'
        f'  <div class="stream-hero">\n'
        f'    <div class="embed-side">\n'
        f'      <div class="embed-wrap">\n'
        f'        <iframe src="https://www.youtube.com/embed/{vid}" allowfullscreen loading="lazy"></iframe>\n'
        f'      </div>\n'
        f'      <div class="stream-thumb-meta">\n'
        f'        <span>{fmt_dt(stream["first_seen"])}</span>\n'
        f'        <a href="https://www.youtube.com/watch?v={vid}" target="_blank" rel="noopener">View Stream &#8599;</a>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'    <div class="kpi-side">\n'
        f'      <div class="kpi-grid">\n'
        f'        <div class="kpi"><div class="kpi-label">Peak Viewers</div><div class="kpi-value">{fmt(stream["peak_viewers"])}</div><div class="kpi-sub">concurrent</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Avg Viewers</div><div class="kpi-value">{fmt(stream.get("avg_viewers"))}</div><div class="kpi-sub">concurrent</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Peak Likes</div><div class="kpi-value">{fmt(stream["peak_likes"])}</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Peak Comments</div><div class="kpi-value">{fmt(stream["peak_comments"])}</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Stream Start</div><div class="kpi-value kpi-sm">{fmt_dt(stream["first_seen"])}</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Stream End</div><div class="kpi-value kpi-sm">{fmt_dt(stream["last_seen"])}</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">Duration</div><div class="kpi-value kpi-sm">{duration_str}</div></div>\n'
        f'        <div class="kpi"><div class="kpi-label">View Count</div><div class="kpi-value kpi-sm">{fmt(stream.get("view_count"))}</div><div class="kpi-sub">total plays</div></div>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'  </div>\n\n'
        f'  <div class="chart-box">\n'
        f'    <div class="chart-toolbar">\n'
        f'      <div class="chart-title">Concurrent Viewers over Time</div>\n'
        f'      <div class="chart-actions">\n'
        f'        <button class="chart-btn" onclick="resetZoom(\'viewerChart\')">Reset Zoom</button>\n'
        f'        <button class="chart-btn" onclick="downloadCSV(\'viewerChart\')">Download CSV</button>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'    <div class="chart-wrap"><canvas id="viewerChart"></canvas></div>\n'
        f'    <p class="chart-hint">Scroll to zoom &nbsp;&#183;&nbsp; Shift+drag to select range &nbsp;&#183;&nbsp; Drag to pan &nbsp;&#183;&nbsp; Double-click to reset</p>\n'
        f'  </div>\n\n'
        f'  <div class="chart-box">\n'
        f'    <div class="chart-toolbar">\n'
        f'      <div class="chart-title">Likes &amp; Comments over Time</div>\n'
        f'      <div class="chart-actions">\n'
        f'        <button class="chart-btn" onclick="resetZoom(\'engagementChart\')">Reset Zoom</button>\n'
        f'        <button class="chart-btn" onclick="downloadCSV(\'engagementChart\')">Download CSV</button>\n'
        f'      </div>\n'
        f'    </div>\n'
        f'    <div class="chart-wrap"><canvas id="engagementChart"></canvas></div>\n'
        f'    <p class="chart-hint">Scroll to zoom &nbsp;&#183;&nbsp; Shift+drag to select range &nbsp;&#183;&nbsp; Drag to pan &nbsp;&#183;&nbsp; Double-click to reset</p>\n'
        f'  </div>\n\n'
        f'  <p class="generated">Generated {_now_local().strftime("%Y-%m-%d %H:%M WIB")}'
        f' &nbsp;&#183;&nbsp; yt-livestream-tracker</p>\n\n'
        f'<script>\n'
        f'// ── Data ────────────────────────────────────────────────────────\n'
        f'const ts    = {json.dumps(labels)};\n'
        f'const views = {json.dumps(viewers)};\n'
        f'const likes = {json.dumps(likes)};\n'
        f'const comms = {json.dumps(comments)};\n'
        f'const VIDEO_ID = {json.dumps(vid)};\n'
        f'\n'
        f'// ── Theme-aware colours (read CSS variables at runtime) ─────────\n'
        f'// orgColor is intentionally NOT baked in as a fixed Python string —\n'
        f'// it is read live from --org-color, which _html_head() sets to the\n'
        f'// dark- or light-mode value depending on the current theme. This is\n'
        f'// what lets the chart line recolor when the toggle is flipped instead\n'
        f'// of staying stuck on whichever color happened to be baked in at\n'
        f'// generation time.\n'
        f'function getCSSVar(name) {{\n'
        f'  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();\n'
        f'}}\n'
        f'function chartColors() {{\n'
        f'  return {{\n'
        f'    org:  getCSSVar("--org-color") || "{org_color}",\n'
        f'    grid: getCSSVar("--border") || "rgba(40,50,74,0.35)",\n'
        f'    tick: getCSSVar("--muted")  || "#8992A8",\n'
        f'  }};\n'
        f'}}\n'
        f'let orgColor = chartColors().org;\n'
        f'\n'
        f'// ── Shared dataset defaults ──────────────────────────────────────\n'
        f'const LINE = {{\n'
        f'  borderWidth: 2,\n'
        f'  pointRadius: 0,          // no dots on the line\n'
        f'  pointHoverRadius: 4,     // dot appears only on hover\n'
        f'  pointHoverBorderWidth: 2,\n'
        f'  fill: true,\n'
        f'  tension: 0.4,            // smooth cubic bezier curve\n'
        f'}};\n'
        f'\n'
        f'// ── Base chart options ───────────────────────────────────────────\n'
        f'function makeOpts(extraPlugins) {{\n'
        f'  const c = chartColors();\n'
        f'  return {{\n'
        f'    responsive: true,\n'
        f'    maintainAspectRatio: false,\n'
        f'    interaction: {{ mode: "index", intersect: false }},\n'
        f'    plugins: {{\n'
        f'      legend: {{ labels: {{ color: c.tick, font: {{ family: "IBM Plex Mono", size: 11 }}, boxWidth: 12 }} }},\n'
        f'      zoom: {{\n'
        f'        pan: {{\n'
        f'          enabled: true,\n'
        f'          mode: "x",\n'
        f'        }},\n'
        f'        zoom: {{\n'
        f'          wheel:  {{ enabled: true }},\n'
        f'          pinch:  {{ enabled: true }},\n'
        f'          drag:   {{ enabled: true, modifierKey: "shift", backgroundColor: "rgba(255,255,255,0.05)", borderColor: "rgba(255,255,255,0.3)", borderWidth: 1 }},\n'
        f'          mode: "x",\n'
        f'        }},\n'
        f'      }},\n'
        f'      ...extraPlugins,\n'
        f'    }},\n'
        f'    scales: {{\n'
        f'      x: {{\n'
        f'        ticks: {{ color: c.tick, font: {{ family: "IBM Plex Mono", size: 10 }}, maxTicksLimit: 10, maxRotation: 0 }},\n'
        f'        grid:  {{ color: c.grid }},\n'
        f'      }},\n'
        f'      y: {{\n'
        f'        ticks: {{ color: c.tick, font: {{ family: "IBM Plex Mono", size: 10 }}, beginAtZero: true }},\n'
        f'        grid:  {{ color: c.grid }},\n'
        f'      }},\n'
        f'    }},\n'
        f'  }};\n'
        f'}}\n'
        f'\n'
        f'// ── Chart registry ───────────────────────────────────────────────\n'
        f'const CHARTS = {{}};\n'
        f'\n'
        f'// ── Viewer chart ─────────────────────────────────────────────────\n'
        f'CHARTS.viewerChart = new Chart(document.getElementById("viewerChart"), {{\n'
        f'  type: "line",\n'
        f'  data: {{\n'
        f'    labels: ts,\n'
        f'    datasets: [{{\n'
        f'      label: "Concurrent Viewers",\n'
        f'      data: views,\n'
        f'      borderColor: orgColor,\n'
        f'      backgroundColor: orgColor + "18",\n'
        f'      ...LINE,\n'
        f'    }}],\n'
        f'  }},\n'
        f'  options: makeOpts({{}}),\n'
        f'}});\n'
        f'\n'
        f'// ── Engagement chart ─────────────────────────────────────────────\n'
        f'CHARTS.engagementChart = new Chart(document.getElementById("engagementChart"), {{\n'
        f'  type: "line",\n'
        f'  data: {{\n'
        f'    labels: ts,\n'
        f'    datasets: [\n'
        f'      {{ label: "Likes",    data: likes, borderColor: "#ff4f6d", backgroundColor: "rgba(255,79,109,0.06)",  ...LINE }},\n'
        f'      {{ label: "Comments", data: comms, borderColor: "#4F9EFF", backgroundColor: "rgba(79,158,255,0.06)", ...LINE }},\n'
        f'    ],\n'
        f'  }},\n'
        f'  options: makeOpts({{}}),\n'
        f'}});\n'
        f'\n'
        f'// ── Reset zoom ───────────────────────────────────────────────────\n'
        f'function resetZoom(id) {{\n'
        f'  const c = CHARTS[id];\n'
        f'  if (c) c.resetZoom();\n'
        f'}}\n'
        f'\n'
        f'// Attach double-click reset to both canvases\n'
        f'document.getElementById("viewerChart").addEventListener("dblclick", function() {{ resetZoom("viewerChart"); }});\n'
        f'document.getElementById("engagementChart").addEventListener("dblclick", function() {{ resetZoom("engagementChart"); }});\n'
        f'\n'
        f'// ── Re-tint the viewer chart line on theme toggle ──────────────────\n'
        f'// _THEME_JS dispatches "idvt-theme-change" right after it flips the\n'
        f'// data-theme attribute, so --org-color has already resolved to the\n'
        f'// new dark/light value by the time this fires.\n'
        f'window.addEventListener("idvt-theme-change", function() {{\n'
        f'  orgColor = chartColors().org;\n'
        f'  const vc = CHARTS.viewerChart;\n'
        f'  vc.data.datasets[0].borderColor = orgColor;\n'
        f'  vc.data.datasets[0].backgroundColor = orgColor + "18";\n'
        f'  vc.update();\n'
        f'}});\n'
        f'\n'
        f'// ── CSV download ─────────────────────────────────────────────────\n'
        f'function downloadCSV(id) {{\n'
        f'  const chart = CHARTS[id];\n'
        f'  if (!chart) return;\n'
        f'  const datasets = chart.data.datasets;\n'
        f'  const labels   = chart.data.labels;\n'
        f'  // Header row: Timestamp + one column per dataset\n'
        f'  const header = ["Timestamp", ...datasets.map(function(d) {{ return d.label; }})];\n'
        f'  // Data rows\n'
        f'  const rows = labels.map(function(lbl, i) {{\n'
        f'    return [lbl, ...datasets.map(function(d) {{ return d.data[i] ?? ""; }})]\n'
        f'      .map(function(v) {{ return String(v).includes(",") ? \'"\' + v + \'"\' : v; }})\n'
        f'      .join(",");\n'
        f'  }});\n'
        f'  const csv  = [header.join(","), ...rows].join("\\n");\n'
        f'  const blob = new Blob([csv], {{ type: "text/csv" }});\n'
        f'  const url  = URL.createObjectURL(blob);\n'
        f'  const a    = document.createElement("a");\n'
        f'  a.href     = url;\n'
        f'  a.download = VIDEO_ID + "_" + id + ".csv";\n'
        f'  document.body.appendChild(a);\n'
        f'  a.click();\n'
        f'  document.body.removeChild(a);\n'
        f'  URL.revokeObjectURL(url);\n'
        f'}}\n'
        f'</script>\n'
    )

    html = _html_head(title_text, 2, org_color, org_color_light, chart_script) + body + _html_foot(2)
    (ch_dir / f"{v_slug}.html").write_text(html, encoding="utf-8")
    log.info("    Written: %s/%s/%s.html", org_slug, ch_slug, v_slug)


# ══════════════════════════════════════════════════════════════════════════════
# PARTIAL BUILD ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _enrich_stream(stream: dict, conn, table: str, hist) -> tuple[dict, list, list]:
    """
    Fetch timeseries and compute avg_viewers for a stream.
    Returns (enriched_stream, timeseries).
    all_rows is no longer fetched — the raw data table was removed from the stream page.
    """
    is_archived = stream.get("_source") == "history"

    if is_archived:
        ts = get_archived_timeseries(hist, stream["video_id"])
        if stream.get("avg_viewers") is None:
            viewer_vals = [int(r["concurrent_viewers"]) for r in ts if r["concurrent_viewers"]]
            stream["avg_viewers"] = round(sum(viewer_vals) / len(viewer_vals)) if viewer_vals else None
    else:
        ts = get_stream_timeseries(conn, table, stream["video_id"])
        viewer_vals = [int(r["concurrent_viewers"]) for r in ts if r["concurrent_viewers"]]
        stream = dict(stream)
        stream["avg_viewers"] = round(sum(viewer_vals) / len(viewer_vals)) if viewer_vals else None

    return stream, ts


# ══════════════════════════════════════════════════════════════════════════════
# SHARED ORCHESTRATION HELPERS
# (used by generate_live.py and generate_backfill.py)
# ══════════════════════════════════════════════════════════════════════════════

def setup_output_dirs() -> None:
    """Create OUTPUT_DIR and copy static legal pages + favicon. Cheap, run by both entry points."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for static_file in ["privacy.html", "terms.html", "favicon.ico","theme.css", "theme-toggle.js", "dashboard.css"]:
        src = Path(static_file)
        dst = OUTPUT_DIR / static_file
        if src.exists():
            shutil.copy2(src, dst)
            log.info("Copied %s to %s/", static_file, OUTPUT_DIR)


def load_channel_maps(conn) -> tuple[dict, dict, dict]:
    """
    Returns (channel_ids_map, logos, subscribers).
    channel_ids_map: {ch_name: channel_id} merged from ORG_MAP entries and DB rows.
    """
    db_channels = get_channel_rows(conn)
    db_by_name  = {ch["channel_name"]: ch for ch in db_channels}
    db_by_id    = {ch["channel_id"]:   ch for ch in db_channels}

    channel_ids_map: dict[str, str] = {}
    for org in ORG_MAP.values():
        for entry in org["channels"]:
            if len(entry) > 2 and entry[2]:
                channel_ids_map[entry[0]] = entry[2]
    for ch in db_channels:
        channel_ids_map[ch["channel_name"]] = ch["channel_id"]

    all_channel_ids = list(dict.fromkeys(channel_ids_map.values()))
    log.info("Fetching channel data from YouTube API…")
    logos, subscribers = get_channel_data(all_channel_ids)
    log.info("Fetched %d logo(s) and %d subscriber count(s).", len(logos), len(subscribers))

    _load_schema_cache(conn, [ch["table_name"] for ch in db_channels])

    return channel_ids_map, logos, subscribers, db_by_name, db_by_id


def resolve_channels(db_by_name: dict, db_by_id: dict) -> dict[str, dict]:
    """
    Resolve ORG_MAP channel names -> DB rows, with fallback by channel_id.
    Also ensures OUTPUT_DIR/{org_slug} directories exist.
    """
    resolved_channels: dict[str, dict] = {}
    for org_slug, org in ORG_MAP.items():
        (OUTPUT_DIR / org_slug).mkdir(exist_ok=True)
        for entry in org["channels"]:
            ch_name = entry[0]
            if ch_name in resolved_channels:
                continue
            db_row = db_by_name.get(ch_name)
            if not db_row:
                org_map_id = entry[2] if len(entry) > 2 else ""
                if org_map_id:
                    db_row = db_by_id.get(org_map_id)
                if not db_row:
                    log.warning(
                        "ORG_MAP channel '%s' (org: %s) not found in DB by name "
                        "or channel_id — no pages will be generated for it.",
                        ch_name, org_slug,
                    )
            if db_row:
                resolved_channels[ch_name] = db_row
    return resolved_channels


def fetch_all_streams(conn, hist, resolved_channels: dict) -> tuple[dict, dict, int, int]:
    """
    Bulk-fetch live + archived streams for all resolved channels in two
    round-trips total (one Postgres, one SQLite), merge them per channel.
    Returns (all_streams_by_channel, stream_counts, total_streams, total_channels).
    """
    table_infos = [
        (ch_name, row["table_name"])
        for ch_name, row in resolved_channels.items()
        if _table_exists(conn, row["table_name"])
    ]
    log.info("Fetching stream summaries for %d channel tables in bulk…", len(table_infos))
    bulk_live = get_all_streams_bulk(conn, table_infos) if table_infos else {}

    # Build a name-mapping that covers BOTH the ORG_MAP name (used as the key
    # throughout the dashboard) and the DB-stored name (what the archiver wrote
    # into history.db via ch["channel_name"] from the Postgres channels table).
    # These can differ for channels where the ORG_MAP entry was matched by
    # channel_id fallback (e.g. "Whicker Butler" vs "Whicker Butler - Vtuber
    # Agency", or "Li Mingshu Ch." vs "Li Mingshu Ch. " with a trailing space).
    # When the archiver runs it writes the DB name; get_all_archived_streams()
    # queries by name exactly — so without this mapping those channels' entire
    # archived history is silently invisible to the dashboard, producing the
    # "only 1 month of data" symptom (Postgres has ~30 days; SQLite has the
    # older history but under a key that never matches the ORG_MAP name).
    db_name_to_org_name: dict[str, str] = {}
    all_query_names: list[str] = []
    for org_map_name, db_row in resolved_channels.items():
        db_name = db_row.get("channel_name", org_map_name)
        all_query_names.append(org_map_name)
        if db_name != org_map_name:
            all_query_names.append(db_name)
            db_name_to_org_name[db_name] = org_map_name

    raw_archived = get_all_archived_streams(hist, all_query_names) if hist else {}

    # Re-key any results stored under the DB name back to the ORG_MAP name,
    # merging into whatever was already found under the ORG_MAP name directly.
    bulk_archived: dict[str, list] = {}
    for query_name, streams in raw_archived.items():
        canonical = db_name_to_org_name.get(query_name, query_name)
        bulk_archived.setdefault(canonical, [])
        bulk_archived[canonical].extend(streams)

    all_streams_by_channel: dict[str, list[dict]] = {}
    stream_counts: dict[str, int] = {}
    total_streams  = 0
    total_channels = 0

    for ch_name in resolved_channels:
        live_streams = bulk_live.get(ch_name, [])
        live_ids     = {s["video_id"] for s in live_streams}
        archived     = [s for s in bulk_archived.get(ch_name, [])
                        if s["video_id"] not in live_ids]
        merged = list(live_streams) + archived
        all_streams_by_channel[ch_name] = merged
        stream_counts[ch_name]          = len(merged)
        total_channels += 1
        total_streams  += len(merged)

    for org in ORG_MAP.values():
        for entry in org["channels"]:
            ch_name = entry[0]
            stream_counts.setdefault(ch_name, 0)
            all_streams_by_channel.setdefault(ch_name, [])

    log.info("DB query complete — %d streams across %d channels.", total_streams, total_channels)
    return all_streams_by_channel, stream_counts, total_streams, total_channels


def compute_dirty_set(all_streams_by_channel: dict, manifest: dict,
                       include_new_vods: bool = True) -> tuple[set, set, set]:
    """
    Determine which stream video_ids need (re)generation, and which parent
    channel/org pages need to follow.

    A stream is dirty if:
      - it is currently live (status == "live" in the freshly-fetched row), OR
      - it was live last run (manifest says "live") and needs one final
        regeneration to lock in its finished state, OR
      - it has never been generated before AND include_new_vods is True
        (set False in the fast/live loop so brand-new VODs that were never
        caught live are left for the backfill loop instead), OR
      - it IS in the manifest as "vod" but its .html file does not exist on
        disk — the file was generated and manifest was updated correctly, but
        the git commit step failed to persist it (or the Pages artifact was
        deployed but the committed baseline was missing the file), so the
        page returns 404. Treating it as dirty forces regeneration of the
        missing file without needing to wipe the manifest.

    Once a stream is recorded in the manifest with status "vod" AND its
    .html file exists on disk, it is permanently clean.

    """
    dirty_video_ids: set[str] = set()
    dirty_channels:  set[str] = set()
    dirty_orgs:      set[str] = set()

    for ch_name, streams in all_streams_by_channel.items():
        for stream in streams:
            vid          = stream["video_id"]
            in_manifest  = vid in manifest
            was_live     = manifest.get(vid, {}).get("status") == "live"
            is_live_now  = (stream.get("stream_status") or "vod") == "live"

            # Check whether the stream page file actually exists on disk.
            # A manifest entry saying "vod" only means we *tried* to generate
            # the file — it does not guarantee the file was committed to git
            # and is actually being served by Pages. Missing files cause 404s
            # even when the channel/org page correctly links to them.
            file_missing = False
            if in_manifest and not is_live_now and not was_live:
                entry      = manifest[vid]
                org_slug   = entry.get("org_slug", "")
                ch_slug    = entry.get("ch_slug", "") or slugify(ch_name)
                v_slug     = slugify(vid)
                page_path  = OUTPUT_DIR / org_slug / ch_slug / f"{v_slug}.html"
                file_missing = not page_path.exists()

            is_dirty = is_live_now or was_live or (not in_manifest and include_new_vods) or file_missing

            if is_dirty:
                dirty_video_ids.add(vid)
                dirty_channels.add(ch_name)
                org_result = _CH_TO_ORG.get(ch_name)
                if org_result:
                    dirty_orgs.add(org_result[0])

    return dirty_video_ids, dirty_channels, dirty_orgs


def generate_stream_pages(dirty_work: list[tuple], run_ts: str,
                           max_workers: int = 8) -> dict:
    """
    Generate stream pages in parallel for the given work list.
    Each item: (org_slug, org, ch_name, table, stream).
    Returns a dict of {video_id: manifest_entry} for successfully written pages.
    Failed pages are logged and simply omitted — they remain dirty next run.
    """
    import time as _time
    results: dict[str, dict] = {}

    def _write_one_stream(args):
        org_slug, org, ch_name, table, stream = args
        is_archived = stream.get("_source") == "history"
        for attempt in range(3):
            t_conn = None
            t_hist = None
            try:
                # Archived streams only need SQLite (history.db) for their
                # timeseries — opening a Postgres connection for them wastes
                # a PgBouncer slot and causes false connection-burst failures
                # when the backfill is processing thousands of archived streams.
                if not is_archived:
                    t_conn = get_conn()
                t_hist = get_history_conn()
                enriched, ts = _enrich_stream(stream, t_conn, table, t_hist)
                write_stream_page(org_slug, org, ch_name, enriched, ts)
                return enriched["video_id"], {
                    "org_slug":     org_slug,
                    "ch_slug":      slugify(ch_name),
                    "ch_name":      ch_name,
                    "status":       enriched.get("stream_status") or "vod",
                    "generated_at": run_ts,
                }
            except psycopg2.OperationalError as exc:
                if is_archived:
                    # Postgres errors should not happen for archived streams
                    # (they don't use t_conn) — re-raise immediately.
                    raise
                if attempt < 2:
                    wait = 2 ** attempt
                    log.warning(
                        "Connection error on stream %s (attempt %d/3), "
                        "retrying in %ds: %s",
                        stream.get("video_id", "?"), attempt + 1, wait, exc,
                    )
                    _time.sleep(wait)
                    continue
                raise
            finally:
                try:
                    if t_conn:
                        t_conn.close()
                except Exception:
                    pass
                try:
                    if t_hist:
                        t_hist.close()
                except Exception:
                    pass

    workers = min(max_workers, max(1, len(dirty_work)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_write_one_stream, item) for item in dirty_work]
        for fut in as_completed(futures):
            try:
                vid, entry = fut.result()
                results[vid] = entry
            except Exception as exc:
                log.error("Stream page generation failed: %s", exc)

    return results


def build_dirty_work_list(resolved_channels: dict, all_streams_by_channel: dict,
                           dirty_video_ids: set) -> list[tuple]:
    """Build the (org_slug, org, ch_name, table, stream) work list for dirty streams."""
    dirty_work: list[tuple] = []
    for org_slug, org in ORG_MAP.items():
        for entry in org["channels"]:
            ch_name = entry[0]
            db_row  = resolved_channels.get(ch_name)
            if not db_row:
                continue
            table = db_row["table_name"]
            for stream in all_streams_by_channel.get(ch_name, []):
                if stream["video_id"] in dirty_video_ids:
                    dirty_work.append((org_slug, org, ch_name, table, stream))
    return dirty_work


def regenerate_channel_pages(resolved_channels: dict, dirty_channels: set,
                              all_streams_by_channel: dict, logos: dict,
                              channel_ids_map: dict, subscribers: dict,
                              max_workers: int = 32) -> int:
    """Regenerate only channel pages whose dirty_channels membership says changed."""
    channel_write_args = []
    for org_slug, org in ORG_MAP.items():
        for entry in org["channels"]:
            ch_name = entry[0]
            if not resolved_channels.get(ch_name):
                continue
            if ch_name not in dirty_channels:
                continue
            streams = all_streams_by_channel.get(ch_name, [])
            channel_write_args.append(
                (org_slug, org, ch_name, streams, logos, channel_ids_map, subscribers)
            )

    if channel_write_args:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(channel_write_args))) as pool:
            futs = [pool.submit(write_channel_page, *a) for a in channel_write_args]
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception as exc:
                    log.error("Channel page generation failed: %s", exc)

    return len(channel_write_args)


def regenerate_org_pages(dirty_orgs: set, stream_counts: dict, logos: dict,
                          channel_ids_map: dict, subscribers: dict,
                          all_streams_by_channel: dict,
                          max_workers: int = 32) -> int:
    """Regenerate only org pages whose dirty_orgs membership says changed.

    No-ops entirely on a single-org build: write_index() already folds the
    one org's channel grid straight into the homepage (see _SINGLE_ORG), so
    there is no {org_slug}/index.html for anything to write — generating one
    here would just produce an orphan page nothing links to.
    """
    if _SINGLE_ORG:
        return 0

    org_write_args = [
        (org_slug, org, stream_counts, logos, channel_ids_map, subscribers,
         all_streams_by_channel)
        for org_slug, org in ORG_MAP.items()
        if org_slug in dirty_orgs
    ]
    if org_write_args:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(org_write_args))) as pool:
            futs = [pool.submit(write_org_page, *a) for a in org_write_args]
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception as exc:
                    log.error("Org page generation failed: %s", exc)

    return len(org_write_args)
