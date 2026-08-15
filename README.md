# Bd-algolia-sync-v3-

Syncs Brilliant Directories (learnwitheveryavenue.com) educator listings into the
Algolia index `BD_class_posts`, which powers the
[lwea-search](https://github.com/mindthegaptutoring/lwea-search) frontend
(GitHub Pages).

## Where this actually runs

**Render Cron Job:** https://dashboard.render.com/cron/crn-d7br66c50q8c73ffi170

- Runs `python bd_algolia_sync_v3.py` every 45 minutes.
- Created directly in the Render dashboard, not by syncing `render.yaml` as a
  Blueprint. If the job is ever deleted or needs to be recreated, use the
  settings above rather than assuming `render.yaml` is authoritative.
- Required env vars (set in Render, not in this repo): `BD_API_KEY`,
  `ALGOLIA_APP_ID`, `ALGOLIA_WRITE_KEY`, `ALGOLIA_INDEX_NAME`.

## What the script does

`bd_algolia_sync_v3.py`:

1. Pulls active, published educator listings from the BD API
   (`/user/search`, `/user/get`, `/users_portfolio_groups/get`).
2. Reshapes fields (delivery methods, grade levels, image URLs, etc.).
3. Pushes the full set to Algolia in one shot via
   `index.replace_all_objects(...)`.

## Files

| File | Purpose |
|---|---|
| `bd_algolia_sync_v3.py` | The sync script — this is what the Render Cron Job runs. |
| `render.yaml` | Blueprint reference config, kept in sync with the live cron job's schedule/command. Not currently used to deploy (the live job was created manually in Render). |
| `.github/workflows/sync.yml` | Manual-dispatch GitHub Action for running the sync on demand. Not used for the recurring schedule — Render owns that. |
| `requirements.txt` | Python dependencies. |
