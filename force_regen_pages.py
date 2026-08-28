"""
force_regen_pages.py
ONE-OFF MAINTENANCE SCRIPT — not part of the regular live/backfill pipeline.

Why this exists:
Commit 877e4881 fixed fetch_all_streams() to correctly merge history.db
archived streams even when the archiver's stored channel_name differs from
the ORG_MAP display name. That fix is correct and already deployed, but
compute_dirty_set() has no way to know the merge logic changed — a channel
whose streams are all already 'vod' in manifest.json is permanently "clean"
and will never retrigger a channel/org page rebuild on its own. Those pages
are stuck showing whatever they showed before the fix (in the affected
cases: ~1 month of live-DB-only data, missing the archive).

What this script does:
  - Fetches the full corrected live+archive merge for every resolved channel
    (same fetch_all_streams() used by generate_live.py / generate_backfill.py).
  - Rebuilds EVERY channel page and EVERY org page unconditionally, ignoring
    dirty-tracking entirely.
  - Rebuilds the index page.

What this script deliberately does NOT do:
  - It does not touch stream (per-video) pages. Those are addressed by
    video_id, not channel_name, so they were never affected by this bug —
    regenerating ~3800 of them again would be pure wasted cost.
  - It does not read or write manifest.json. Manifest only tracks stream
    page state; leaving it untouched means generate_live.py / 
    generate_backfill.py continue exactly as before after this runs.

This is safe to run multiple times — it's idempotent and doesn't mutate any
shared state that the regular pipeline depends on. It's also safe to run
concurrently with the live/backfill loops (read-only against the DB and
history.db, no manifest writes) but for a clean first run it's simplest to
just wait for the current live-loop run to finish before triggering this.
"""

from dashboard_core import (
    AIVEN_DATABASE_URL, log, _now_local,
    get_conn, get_history_conn,
    setup_output_dirs, load_channel_maps, resolve_channels,
    fetch_all_streams, regenerate_channel_pages, regenerate_org_pages,
    write_index, ORG_MAP,
)


def force_regen() -> None:
    if not AIVEN_DATABASE_URL:
        print("ERROR: DATABASE_URL / AIVEN_DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    conn = get_conn()
    hist = get_history_conn()
    if hist is None:
        log.warning(
            "history.db not found/openable — this run would rebuild pages "
            "WITHOUT archived data, which defeats the point. Aborting."
        )
        raise SystemExit(1)

    setup_output_dirs()

    channel_ids_map, logos, subscribers, db_by_name, db_by_id = load_channel_maps(conn)
    resolved_channels = resolve_channels(db_by_name, db_by_id)

    all_streams_by_channel, stream_counts, total_streams, total_channels = \
        fetch_all_streams(conn, hist, resolved_channels)

    # Force EVERY channel/org to be treated as dirty, bypassing manifest-based
    # dirty tracking entirely — this is the whole point of this script.
    all_channels = set(resolved_channels.keys())
    all_orgs     = set(ORG_MAP.keys())

    log.info(
        "Force-regen: rebuilding all %d channel page(s) and %d org page(s) "
        "regardless of manifest/dirty-tracking state (%d total streams known).",
        len(all_channels), len(all_orgs), total_streams
    )

    channels_written = regenerate_channel_pages(
        resolved_channels, all_channels, all_streams_by_channel,
        logos, channel_ids_map, subscribers,
    )
    orgs_written = regenerate_org_pages(
        all_orgs, stream_counts, logos, channel_ids_map, subscribers,
        all_streams_by_channel,
    )

    generated_at = _now_local().strftime("%Y-%m-%d %H:%M WIB")
    write_index(total_streams, total_channels, generated_at,
                stream_counts, all_streams_by_channel)

    conn.close()
    hist.close()

    log.info(
        "Force-regen complete — %d channel page(s), %d org page(s), 1 index "
        "rebuilt. manifest.json was NOT touched.",
        channels_written, orgs_written
    )


if __name__ == "__main__":
    force_regen()
