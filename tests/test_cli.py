from __future__ import annotations

import json

from airdrop_farmer.cli import main


def test_cli_run_once_returns_2_on_task_fail(tmp_path, monkeypatch):
    config = {
        "dry_run": True,
        "kill_switch": False,
        "check_wallet_env_vars": False,
        "state_file": str(tmp_path / "state.json"),
        "max_task_seconds": 5,
        "cycle_interval_seconds": 30,
        "wallets": [{"name": "w1", "address": "0x1", "private_key_env": "PK1"}],
        "projects": [
            {
                "name": "p1",
                "enabled": True,
                "cooldown_seconds": 0,
                "tasks": [{"id": "bad", "provider": "unknown", "action": "x"}],
            }
        ],
    }
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps(config), encoding="utf-8")

    monkeypatch.setattr(
        "sys.argv",
        ["airdrop-farmer", "run", "--config", str(cfg), "--once", "--ignore-env-check"],
    )
    code = main()
    assert code == 2
