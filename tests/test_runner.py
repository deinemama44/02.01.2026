from __future__ import annotations

import json

from airdrop_farmer.logging_utils import build_logger
from airdrop_farmer.models import AppConfig, ProjectConfig, RetryConfig, TaskConfig, WalletConfig
from airdrop_farmer.runner import AirdropRunner


def _base_config(tmp_path) -> AppConfig:
    return AppConfig(
        dry_run=True,
        kill_switch=False,
        stop_on_first_task_failure=False,
        check_wallet_env_vars=False,
        state_file=str(tmp_path / "state.json"),
        max_task_seconds=5,
        cycle_interval_seconds=60,
        wallets=[WalletConfig(name="w1", address="0xabc", private_key_env="PK")],
        projects=[
            ProjectConfig(
                name="project",
                enabled=True,
                cooldown_seconds=0,
                tasks=[
                    TaskConfig(
                        id="t1",
                        provider="mock",
                        action="daily_checkin",
                        params={"points": 5},
                        retry=RetryConfig(attempts=2, base_delay_seconds=0),
                    )
                ],
            )
        ],
    )


def test_run_once_executes_task(tmp_path):
    runner = AirdropRunner(_base_config(tmp_path), build_logger())
    results, summary = runner.run_once()
    assert len(results) == 1
    assert results[0].status == "ok"
    assert results[0].attempts == 1
    assert summary.success == 1


def test_kill_switch_skips_all(tmp_path):
    cfg = _base_config(tmp_path)
    cfg.kill_switch = True
    runner = AirdropRunner(cfg, build_logger())
    results, summary = runner.run_once()
    assert results == []
    assert summary.total == 0


def test_state_file_written_and_reloaded(tmp_path):
    cfg = _base_config(tmp_path)
    runner1 = AirdropRunner(cfg, build_logger())
    _, summary = runner1.run_once()
    assert summary.total == 1

    state_path = tmp_path / "state.json"
    raw = json.loads(state_path.read_text(encoding="utf-8"))
    assert "w1:project" in raw

    # reload state in a new runner instance
    runner2 = AirdropRunner(cfg, build_logger())
    assert ("w1", "project") in runner2._last_run
