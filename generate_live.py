"""
generate_live.py
FAST LOOP — intended to run every 15 minutes.

Scope is deliberately narrow: this script only regenerates stream pages for
streams that are CURRENTLY LIVE, or that just transitioned live -> vod since
the last run (one final regeneration to lock in their finished state).

It does NOT scan for "new VODs that were never seen live" — that is the job
of generate_backfill.py, which runs on a slower cadence (e.g. hourly) and can
afford a full reconciliation pass.

This split is what keeps the 15-minute window sustainable as the historical
archive grows: this script's cost is bounded by how many channels are
concurrently live right now, not by the total number of streams ever
recorded. A channel that streamed 5,000 hours of history costs the same as
one with 5 hours, because only its *current* live/just-ended state matters
here.

Stream pages are the only per-stream artifact regenerated. Channel and org
pages are regenerated for any channel/org that had a dirty stream, so the
"LIVE" badge and updated stream cards show up promptly. The index page is
always regenerated (cheap — no DB I/O). search-index.json is also always
regenerated here — it's just a serialization of data already fetched for
the index page, no extra cost, and it keeps the global search overlay from
drifting stale between backfill runs.
"""

from dashboard_core import (
    AIVEN_DATABASE_URL, log, _now_local,
    get_conn, get_history_conn,
    load_manifest, save_manifest,
    setup_output_dirs, load_channel_maps, resolve_channels,
    fetch_all_streams, compute_dirty_set, build_dirty_work_list,
    generate_stream_pages, regenerate_channel_pages, regenerate_org_pages,
    write_index, write_search_index, write_live_page,
)


def generate_live() -> None:
    if not AIVEN_DATABASE_URL:
        print("ERROR: DATABASE_URL / AIVEN_DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    conn = get_conn()
    hist = get_history_conn()
    setup_output_dirs()

    channel_ids_map, logos, subscribers, db_by_name, db_by_id = load_channel_maps(conn)
    manifest = load_manifest()
    log.info("Manifest loaded — %d stream pages previously generated.", len(manifest))

    resolved_channels = resolve_channels(db_by_name, db_by_id)

    all_streams_by_channel, stream_counts, total_streams, total_channels = \
        fetch_all_streams(conn, hist, resolved_channels)

    # include_new_vods=False: a stream that was never caught live and isn't
    # yet in the manifest is left for the backfill loop. This is the key
    # difference from the old single-script approach — it's what keeps this
    # loop's cost bounded by "currently live" rather than "everything ever".
    dirty_video_ids, dirty_channels, dirty_orgs = compute_dirty_set(
        all_streams_by_channel, manifest, include_new_vods=False
    )

    log.info(
        "Live-loop build plan: %d stream page(s), %d channel page(s), "
        "%d org page(s).",
        len(dirty_video_ids), len(dirty_channels), len(dirty_orgs)
    )

    dirty_work = build_dirty_work_list(resolved_channels, all_streams_by_channel, dirty_video_ids)

    run_ts = _now_local().strftime("%Y-%m-%d %H:%M WIB")
    # Worker count capped at 8 — see generate_backfill.py and prior incident
    # notes: Supabase/PgBouncer rejects bursts of simultaneous connections,
    # and the live loop's dirty set is small enough that 8 workers is plenty.
    new_entries = generate_stream_pages(dirty_work, run_ts, max_workers=8)
    manifest.update(new_entries)

    channels_written = regenerate_channel_pages(
        resolved_channels, dirty_channels, all_streams_by_channel,
        logos, channel_ids_map, subscribers,
    )
    orgs_written = regenerate_org_pages(
        dirty_orgs, stream_counts, logos, channel_ids_map, subscribers,
        all_streams_by_channel,
    )

    generated_at = _now_local().strftime("%Y-%m-%d %H:%M WIB")
    write_index(total_streams, total_channels, generated_at,
                stream_counts, all_streams_by_channel)
    write_search_index(resolved_channels, all_streams_by_channel)
    write_live_page(all_streams_by_channel, logos, channel_ids_map)

    save_manifest(manifest)
    conn.close()
    if hist:
        hist.close()

    log.info(
        "Live loop complete — %d stream page(s), %d channel page(s), "
        "%d org page(s), 1 index, 1 search index, 1 live page. (%d total streams known, most untouched.)",
        len(new_entries), channels_written, orgs_written, total_streams
    )


if __name__ == "__main__":
    generate_live()
