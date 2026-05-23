from __future__ import annotations

import json

import pytest

from airdrop_farmer.config import ConfigError, load_config


def test_load_config_success(tmp_path, monkeypatch):
    monkeypatch.setenv("PK1", "secret")
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(
        json.dumps(
            {
                "dry_run": True,
                "kill_switch": False,
                "stop_on_first_task_failure": False,
                "check_wallet_env_vars": True,
                "state_file": str(tmp_path / "state.json"),
                "max_task_seconds": 20,
                "cycle_interval_seconds": 30,
                "wallets": [
                    {"name": "w1", "address": "0x1", "private_key_env": "PK1"},
                ],
                "projects": [
                    {
                        "name": "p1",
                        "enabled": True,
                        "cooldown_seconds": 0,
                        "tasks": [{"id": "t1", "provider": "mock", "action": "a1"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    cfg = load_config(cfg_path)
    assert cfg.wallets[0].name == "w1"
    assert cfg.projects[0].tasks[0].id == "t1"


def test_load_config_rejects_duplicate_wallet(tmp_path):
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(
        json.dumps(
            {
                "dry_run": True,
                "kill_switch": False,
                "state_file": str(tmp_path / "state.json"),
                "max_task_seconds": 10,
                "cycle_interval_seconds": 30,
                "check_wallet_env_vars": False,
                "wallets": [
                    {"name": "dup", "address": "0x1", "private_key_env": "PK1"},
                    {"name": "dup", "address": "0x2", "private_key_env": "PK2"},
                ],
                "projects": [
                    {
                        "name": "p1",
                        "enabled": True,
                        "cooldown_seconds": 0,
                        "tasks": [{"id": "t1", "provider": "mock", "action": "a1"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError):
        load_config(cfg_path)


def test_load_config_missing_wallet_env_fails(tmp_path):
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(
        json.dumps(
            {
                "dry_run": True,
                "kill_switch": False,
                "check_wallet_env_vars": True,
                "state_file": str(tmp_path / "state.json"),
                "max_task_seconds": 10,
                "cycle_interval_seconds": 30,
                "wallets": [
                    {"name": "w1", "address": "0x1", "private_key_env": "PK_MISSING"},
                ],
                "projects": [
                    {
                        "name": "p1",
                        "enabled": True,
                        "cooldown_seconds": 0,
                        "tasks": [{"id": "t1", "provider": "mock", "action": "a1"}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError):
        load_config(cfg_path)
