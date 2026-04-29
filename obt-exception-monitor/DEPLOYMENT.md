# OBT Exception Monitor Deployment Notes

## Current access model

The monitor follows the same client-side Google OAuth gate used by the existing `-3W Booking Dashboard`:

- OAuth client: `409330651463-giie223egsskdq10etn642gjtron1hq5.apps.googleusercontent.com`
- allowed domain: `ekmtc.com`
- shared session keys: `gtoken`, `guser`
- Pages repo: `jkpark-create/kmtc-3w-dashboard-web`

If a user has already signed in to the existing dashboard, `/obt-exception-monitor/` opens with the same session. If a user opens the monitor directly, it redirects through the existing Pages root OAuth callback and returns to `/obt-exception-monitor/` after sign-in.

`history.json` is generated data and is intentionally ignored in the source repo. It is copied only into the Pages deployment repo.

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
- optional daily pace history: `history.json`

When deployed under the existing Pages root as `/obt-exception-monitor/`, `app.js` reads dashboard data from `../data.json`. In the local project layout, it reads `../dist/data.json`.
