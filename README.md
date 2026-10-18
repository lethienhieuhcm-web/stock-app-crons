# stock-app-crons

Scheduled jobs for the StockApp (Vietnam stocks) moved off the Muse VM onto
GitHub Actions, so they survive VM replacements. Public repo = unlimited
Actions minutes.

## Workflows

| Workflow | Schedule | Does |
|---|---|---|
| `stockapp-alert-check` | every 10 min | Price alerts + TP/SL targets from R2 via worker internal API → Telegram on hit (one-shot) |

## Scripts

`scripts/check_alerts_r2.py` — vendored from `~/workspace/stock-app/scripts/`
on the VM (2026-10-18). One deliberate difference: the VM copy hardcodes
curl `-x http://hatch-egress-proxy:3128`; this copy reads the proxy from
`http_proxy`/`https_proxy` env (unset on Actions → direct connection).

## Secrets (repo Settings → Secrets → Actions)

- `TELEGRAM_BOT_TOKEN` — Deal Hunter Telegram bot token
- `TELEGRAM_CHAT_ID` — Telegram chat ID for alerts
- `STOCKAPP_PUBLISH_SECRET` — worker internal API secret (`~/.stockapp_publish` on VM)
