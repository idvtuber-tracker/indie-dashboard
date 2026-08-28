# 📊 IDVTuber Dashboard

The live analytics dashboard for the [IDVTuber Tracker](https://github.com/idvtuber-tracker/tracker) project. This repository holds the **generated static HTML site** deployed to GitHub Pages — it does not contain any application source code.

---

## What This Is

The dashboard provides a browsable, multi-level view of YouTube livestream analytics collected from Indonesian VTuber (IDVTuber) channels. It covers 180+ tracked channels across multiple organisations, tracking concurrent viewers, view counts, likes, and comments across live and past streams.

The site is rebuilt and pushed here automatically by the tracker's GitHub Actions workflows every few minutes. You do not need to run anything manually to keep it up to date.

---

## Site Structure

The site is organised as four levels of pages, each linking to the next:

```
index.html                             ← All tracked organisations
└── {org}/index.html                   ← Channels within an organisation
    └── {org}/{channel}/index.html     ← Stream history for a channel
        └── {org}/{channel}/{id}.html  ← Detail page for a single stream
```

**Organisation index** — Cards for every tracked org, showing active stream count and a summary of recent activity. Supports filtering and sorting via chips (by activity, name, and channel count).

**Channel list** — All channels belonging to an organisation, with subscriber counts, logos, and a count of tracked streams. Also supports filter/sort chips.

**Stream cards** — A chronological list of streams for a channel, including peak viewers, total view count, and stream duration. Live and upcoming streams are highlighted.

**Stream detail** — Full time-series charts for concurrent viewers, likes, and comments over the course of a single stream, alongside summary statistics. Charts support pinch-to-zoom and pan (via HammerJS + chartjs-plugin-zoom) for inspecting specific moments in a stream.

Dashboard pages also feature hero sections and KPI strips summarizing key stats at a glance.

All timestamps are displayed in **WIB (UTC+7)**. The site supports light and dark themes, following the system preference by default with a manual toggle available.

---

## How It Gets Updated

This repo is the deployment target for the tracker pipeline. The tracker runs as two separate workflows for better concurrency and reliability:

- **Live workflow** — runs frequently, updating currently-live and recently-ended streams.
- **Backfill workflow** — runs on a slower cadence, catching up on historical data and any streams missed by the live pass.

The flow on each run is:

1. The tracker collects analytics from the YouTube Data API and writes them to Supabase PostgreSQL.
2. The dashboard generator reads from PostgreSQL and the long-term SQLite archive (`history.db`), then writes a fresh `dashboard/` folder.
3. The tracker pushes the updated `dashboard/` folder to this repository via `git push`.
4. A `repository_dispatch` event is fired on this repo, matching which tracker workflow triggered the push.
5. The corresponding `deploy_live.yml` or `deploy_backfill.yml` workflow here picks up that event and deploys the site to GitHub Pages.

Only pages that have changed (new streams or currently live streams) are regenerated on each run. Completed VOD pages are written once and never touched again, keeping deploy times fast. A manifest-based tracking system prevents race conditions between concurrent live and backfill runs.

---

## Repository Contents

```
.
├── dashboard/                 ← Generated site output (auto-updated)
│   ├── index.html
│   ├── manifest.json          ← Partial-build state (managed by tracker)
│   └── {org}/...
└── .github/
    └── workflows/
        ├── deploy_live.yml        ← GitHub Pages deployment, triggered by live tracker runs
        └── deploy_backfill.yml    ← GitHub Pages deployment, triggered by backfill tracker runs
```

Everything inside `dashboard/` is machine-generated. Do not edit those files manually — they will be overwritten on the next tracker run.

---

## Related Repositories

| Repository | Purpose |
| --- | --- |
| [`idvtuber-tracker/tracker`](https://github.com/idvtuber-tracker/tracker) | Source code — tracker, dashboard generator, archiver |
| [`idvtuber-tracker/dashboard`](https://github.com/idvtuber-tracker/dashboard) | This repo — generated site + Pages deployment |
| `idvtuber-tracker/idvt-history` | Private repo, Long-term stream archive (`history.db`, SQLite) |

---

## Deployment

GitHub Pages is configured to serve from the `dashboard/` folder on the `main` branch. The `deploy_live.yml` and `deploy_backfill.yml` workflows are each triggered by their corresponding `repository_dispatch` event sent by the tracker after a successful push, ensuring the live site reflects the latest data shortly after every collection cycle.
