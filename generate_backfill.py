"""
generate_backfill.py
SLOW LOOP — intended to run hourly or daily, NOT on the 15-minute cycle.

This script's job is reconciliation: it catches streams that generate_live.py
deliberately skips — specifically, any stream that appears in the database
(live table or history archive) but has NEVER been given a manifest entry.
This happens when a stream goes live and ends entirely between two
15-minute live-loop runs (the live loop only ever sees it as already "vod"
and, by design, does not regenerate brand-new VODs — see
compute_dirty_set(include_new_vods=False) in generate_live.py).

Once a stream has a manifest entry with status "vod", this script will never
touch it again — VOD data is frozen once a stream ends (viewer timeseries,
peak/avg viewers do not change after the broadcast finishes). This is what
keeps this script's cost bounded too: it only ever processes the backlog of
genuinely-new-and-unseen streams, not the entire historical archive on every
run.

Because this runs infrequently, it can safely use a smaller worker count and
does not need to be fast — there is no 15-minute SLA here.

search-index.json is regenerated here too (same as generate_live.py) so a
backfill-only run still leaves the search overlay up to date even if the
live loop hasn't run in a while.
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


def generate_backfill() -> None:
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

    # include_new_vods=True: this is the whole point of the backfill loop —
    # catch anything that slipped through the live loop's narrower net.
    # Streams already correctly marked "vod" in the manifest are still
    # skipped; only never-seen video_ids and currently-live streams qualify.
    dirty_video_ids, dirty_channels, dirty_orgs = compute_dirty_set(
        all_streams_by_channel, manifest, include_new_vods=True
    )

    log.info(
        "Backfill build plan: %d stream page(s), %d channel page(s), "
        "%d org page(s).",
        len(dirty_video_ids), len(dirty_channels), len(dirty_orgs)
    )

    if not dirty_video_ids:
        log.info("Nothing to backfill for stream pages — manifest is fully reconciled.")
        # Still worth refreshing search-index.json/live.html even with no
        # dirty streams: channel/org metadata (subscriber counts, who's
        # live right now, etc.) can change without any stream itself being
        # dirty in the manifest sense.
        write_search_index(resolved_channels, all_streams_by_channel)
        write_live_page(all_streams_by_channel, logos, channel_ids_map)
        conn.close()
        if hist:
            hist.close()
        return

    # Log how many of the dirty set are missing-file recoveries vs genuinely new.
    in_manifest_count = sum(1 for v in dirty_video_ids if v in manifest)
    new_count = len(dirty_video_ids) - in_manifest_count
    if in_manifest_count:
        log.info(
            "Backfill includes %d stream page(s) that are in the manifest as 'vod' "
            "but missing from disk (404 recovery) and %d genuinely new stream(s).",
            in_manifest_count, new_count
        )

    dirty_work = build_dirty_work_list(resolved_channels, all_streams_by_channel, dirty_video_ids)

    run_ts = _now_local().strftime("%Y-%m-%d %H:%M WIB")
    # Lower worker count than the live loop: this runs infrequently and the
    # backlog can be large after a long gap (e.g. first run, or after an
    # outage), so a gentler concurrency level avoids any connection-burst
    # issues with Supabase/PgBouncer over a longer, more tolerant window.
    new_entries = generate_stream_pages(dirty_work, run_ts, max_workers=6)
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
        "Backfill complete — %d stream page(s), %d channel page(s), "
        "%d org page(s), 1 index, 1 search index, 1 live page. (%d total streams known.)",
        len(new_entries), channels_written, orgs_written, total_streams
    )


if __name__ == "__main__":
    generate_backfill()
