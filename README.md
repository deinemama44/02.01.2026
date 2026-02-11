# Airdrop Farmer (Full-Auto Orchestrator)

A production-style project for automating **legitimate** airdrop workflows in one place.

## What it does

- Loads wallets and project tasks from JSON config
- Runs tasks in scheduled cycles (or one-shot)
- Supports `dry_run` for safe simulation and `--live` override
- Built-in retry/backoff and structured JSON logs
- Global safety controls (`kill_switch`, cooldown, timeout, stop-on-failure)
- Persists cooldown state between restarts via `state_file`
- Provider abstraction so you can plug real API/on-chain implementations

## Important compliance note

This project is for compliant automation only. Do not use it to bypass platform restrictions,
KYC requirements, anti-bot systems, or terms of service.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
cp config.example.json config.json
PYTHONPATH=src python -m airdrop_farmer.cli validate --config config.json
PYTHONPATH=src python -m airdrop_farmer.cli run --config config.json --once
```

Optional editable install if your environment allows package installation:

```bash
pip install -e .[dev]
airdrop-farmer validate --config config.json
airdrop-farmer run --config config.json --once
```

## Commands

- `airdrop-farmer validate --config config.json` → validate config
- `airdrop-farmer run --config config.json --once` → one cycle, exits with code `2` if any task failed
- `airdrop-farmer run --config config.json --max-cycles 10` → run loop with cycle limit
- `airdrop-farmer run --config config.json --live` → force live mode
- `airdrop-farmer run --config config.json --ignore-env-check` → skip key env var checks (dev use only)

## Config highlights

`config.example.json` includes:

- wallet list + env-var mapping for secrets
- per-project cooldown
- per-task retries
- global runtime controls (`kill_switch`, timeout, state file, stop behavior)

## Extending with real providers

Add provider classes in `airdrop_farmer/providers.py` and register them in `provider_factory`.
Each provider must implement `execute(wallet_name, wallet_address, action, params, dry_run)`.
