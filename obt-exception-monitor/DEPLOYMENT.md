# OBT Exception Monitor Deployment Notes

## Current decision

The monitor is ready as a static page, but the existing GitHub Pages target is public:

- source repo: `jkpark-create/kmtc-3w-dashboard`
- Pages repo: `jkpark-create/kmtc-3w-dashboard-web`

Because the requested access rule is "company Google account only", the monitor should not be deployed directly to the public Pages site with data artifacts. The generated daily speed file `history.json` is intentionally ignored in this repo.

## Safe deployment patterns

1. GitHub as source + Cloudflare Access
   - Deploy the static site from GitHub.
   - Put Cloudflare Access in front of the site.
   - Allow only the company Google Workspace domain.

2. GitHub as source + Google Cloud IAP
   - Deploy the static files to a Google-hosted frontend.
   - Protect the service with Identity-Aware Proxy.
   - Allow only company Google Workspace users/groups.

3. GitHub Enterprise private/internal Pages
   - Move the Pages site to an Enterprise organization that supports private or internal Pages.
   - This gates access by GitHub identity. If company Google login is required, connect the organization identity provider to Google Workspace/SAML.

## Static deployment files

Minimum files for the dashboard:

- `index.html`
- `styles.css`
- `app.js`
- `guide.html`

Generated runtime data:

- existing dashboard data: `data.json`
- optional daily pace history: `history.json`

When deployed under the existing Pages root as `/obt-exception-monitor/`, `app.js` reads dashboard data from `../data.json`. In the local project layout, it reads `../dist/data.json`.
