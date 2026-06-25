# OBT Exception Monitor Deployment Notes

## Current access model

The monitor follows the same client-side Google OAuth gate used by the existing `-3W Booking Dashboard`:

- OAuth client: `409330651463-giie223egsskdq10etn642gjtron1hq5.apps.googleusercontent.com`
- allowed domain: `ekmtc.com`
- shared session keys: `gtoken`, `guser`
- Pages repo: `jkpark-create/kmtc-3w-dashboard-web`

If a user has already signed in to the existing dashboard, `/obt-exception-monitor/` opens with the same session. If a user opens the monitor directly, it redirects through the existing Pages root OAuth callback and returns to `/obt-exception-monitor/` after sign-in.

`history.json` is generated data and is intentionally ignored in the source repo. The source copy is retained locally for Google Drive backup, while the Pages deployment copy can be size-capped to avoid GitHub's 100 MB file limit.

## Stronger protection options

The current approach matches the existing dashboard and protects the UI flow. For server-side file protection, use one of these stronger patterns:

1. Cloudflare Access in front of the static site, restricted to the company Google Workspace domain.

2. Google Cloud IAP in front of a Google-hosted static frontend.

3. GitHub Enterprise private/internal Pages with organization SSO tied to the company identity provider.

## Static deployment files

Minimum files for the dashboard:

- `index.html`
- `styles.css`
- `app.js`
- `guide.html`
- `auth.js`

Generated runtime data:

- existing dashboard data: `data.json`
- daily pace history: `history.json`
- Drive backup: `obt_exception_history.json`

When deployed under the existing Pages root as `/obt-exception-monitor/`, `app.js` reads dashboard data from `../data.json`. In the local project layout, it reads `../dist/data.json`.

The daily automation pre-generates history immediately after refreshing `dist/data.json` for the latest dataset, so each date change is stored before the monitor reads it. History build failures are non-fatal by default; the update continues with a warning and the previous history remains available. Use `OBT_HISTORY_STRICT=1` only for validation runs where history mismatches should stop the job.

The full local history is uploaded to Google Drive as `obt_exception_history.json`. The deployed Pages `history.json` is bounded by `OBT_HISTORY_DEPLOY_MAX_BYTES` (default 90 MB) and keeps the newest snapshots first when trimming is required.
