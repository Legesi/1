# Security Notes

This project is a paper-trading deployment by design.

- `config.json` keeps `system.mode` set to `paper`.
- Live and testnet order submission is disabled in the HTTP API.
- Binance credentials are not required for the local paper workflow.
- Optional notification credentials must be supplied through environment variables.
- Do not commit `.env`, exchange API keys, database files, logs, or process identifiers.
- Before enabling any future live adapter, review permissions, key scopes, network restrictions, and staged approval controls.

If a credential is accidentally committed, revoke it immediately and rotate the affected secret before rewriting repository history.
