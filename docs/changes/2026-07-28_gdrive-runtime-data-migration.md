# 2026-07-28 Google Drive runtime data migration

## Scope

- Moved all dashboard runtime JSON/data artifacts out of the GitHub Pages data
  bundle and into dashboard-specific Google Drive folders.
- Generated files are staged outside the OneDrive project under
  `%LOCALAPPDATA%\KMTC\3w-dashboard-runtime` (overridable with
  `DASHBOARD_RUNTIME_ROOT`). After every verified Drive sync, the pipeline
  removes `output` data, uncompressed runtime JSON, Drive-backed compressed
  runtime files, Sales Target chunks, OBT history, and the current `1.csv`/`2.csv`
  downloads. The immutable `_cache_2025.parquet` basis is restored and verified
  from the Drive `Source Artifacts` folder at the start of each run and is also
  removed after the completed sync.

## Drive layout

- `KMTC 3W Dashboard Data`
  - `Main Dashboard`
    - `data.json.gz`
    - `dashboard_summary.json`
    - `History/dashboard_summary_YYYYMMDD.json`
    - `Source Artifacts/` for parquet/BSA pipeline artifacts
  - `OBT Exception Monitor`
    - `history.json.gz`
    - `obt_exception_history.json`
  - `Sales Target`
    - `index.json`
    - `manifest.json`
    - `base2025.json`
    - `data/*.json`

The root folder preserves the previous access model: `ekmtc.com` domain reader,
not discoverable in search. Children inherit that permission.

## Runtime changes

- `dist/drive-config.js` stores stable Drive folder IDs.
- The main dashboard loads the current gzip JSON and historical summaries through
  the authenticated Drive API.
- OBT loads the main gzip JSON plus its own history gzip from Drive.
- Sales Target lists its Drive `data` folder once, caches the file-name-to-ID
  mapping, and lazy-loads the selected chunks through Drive.
- Production does not fall back to GitHub-hosted data. Localhost retains a local
  file fallback for development.

## Pipeline changes

- `scripts/sync_dashboard_data_to_gdrive.py` performs idempotent upserts using
  MD5 checks and preserves existing Drive file IDs. OAuth token refresh and
  Drive requests retry transient TLS/network failures.
- `--cleanup-local` removes local data only after all Drive group uploads finish
  successfully; a failed sync leaves local recovery files untouched.
- Before cleanup, every expected file is read back from Drive and matched by
  file ID, size, and MD5. `--verify-drive` repeats that check later from the
  manifest stored outside OneDrive under `%LOCALAPPDATA%\KMTC\3w-dashboard`.
- `run_daily.bat` calls the Drive sync after all main, OBT, and Sales Target
  artifacts are built and enables verified local cleanup.
- Runtime data files are ignored/untracked in `dist`; GitHub Pages now carries
  application code only.

## Verification

- On 2026-08-12, uploaded and verified 1,445 runtime files, including 1,434
  Sales Target chunks; 1,457 local staging files (1,230.1 MiB) were removed.
- Main dashboard: Drive requests returned HTTP 200 and rendered data date
  `20260728`.
- Sales Target: rendered `1,262` chunks / `324,445` BKG; quarter interaction
  changed from Q3 to Q2 with 149 table rows.
- OBT: rendered `161,143` rows and `92` days of Drive-backed history; horizon
  filter interaction completed without console errors.
