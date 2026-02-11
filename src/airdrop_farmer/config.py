from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .models import AppConfig, ProjectConfig, RetryConfig, TaskConfig, WalletConfig


class ConfigError(ValueError):
    pass


def _must_be_dict(name: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be an object")
    return value


def _as_retry(data: dict[str, Any]) -> RetryConfig:
    attempts = int(data.get("attempts", 1))
    delay = float(data.get("base_delay_seconds", 1.0))
    if attempts < 1:
        raise ConfigError("retry.attempts must be >= 1")
    if delay < 0:
        raise ConfigError("retry.base_delay_seconds must be >= 0")
    return RetryConfig(attempts=attempts, base_delay_seconds=delay)


def load_config(path: str | Path) -> AppConfig:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    raw = _must_be_dict("config", data)

    wallets_raw = raw.get("wallets", [])
    projects_raw = raw.get("projects", [])

    if not isinstance(wallets_raw, list):
        raise ConfigError("wallets must be a list")
    if not isinstance(projects_raw, list):
        raise ConfigError("projects must be a list")

    wallets: list[WalletConfig] = []
    for index, wallet in enumerate(wallets_raw):
        w = _must_be_dict(f"wallets[{index}]", wallet)
        wallets.append(
            WalletConfig(
                name=str(w["name"]).strip(),
                address=str(w["address"]).strip(),
                private_key_env=str(w["private_key_env"]).strip(),
            )
        )

    projects: list[ProjectConfig] = []
    for pindex, project in enumerate(projects_raw):
        p = _must_be_dict(f"projects[{pindex}]", project)
        tasks_raw = p.get("tasks", [])
        if not isinstance(tasks_raw, list):
            raise ConfigError(f"projects[{pindex}].tasks must be a list")

        tasks: list[TaskConfig] = []
        for tindex, task in enumerate(tasks_raw):
            t = _must_be_dict(f"projects[{pindex}].tasks[{tindex}]", task)
            params = t.get("params", {})
            if not isinstance(params, dict):
                raise ConfigError(
                    f"projects[{pindex}].tasks[{tindex}].params must be an object"
                )
            retry = _as_retry(_must_be_dict("retry", t.get("retry", {})))

            tasks.append(
                TaskConfig(
                    id=str(t["id"]).strip(),
                    provider=str(t["provider"]).strip(),
                    action=str(t["action"]).strip(),
                    params=params,
                    retry=retry,
                )
            )

        projects.append(
            ProjectConfig(
                name=str(p["name"]).strip(),
                enabled=bool(p.get("enabled", True)),
                cooldown_seconds=float(p.get("cooldown_seconds", 0)),
                tasks=tasks,
            )
        )

    cfg = AppConfig(
        dry_run=bool(raw.get("dry_run", True)),
        kill_switch=bool(raw.get("kill_switch", False)),
        stop_on_first_task_failure=bool(raw.get("stop_on_first_task_failure", False)),
        check_wallet_env_vars=bool(raw.get("check_wallet_env_vars", True)),
        state_file=str(raw.get("state_file", ".airdrop_farmer_state.json")),
        max_task_seconds=float(raw.get("max_task_seconds", 30)),
        cycle_interval_seconds=float(raw.get("cycle_interval_seconds", 60)),
        wallets=wallets,
        projects=projects,
    )

    validate_config(cfg)
    return cfg


def validate_config(cfg: AppConfig) -> None:
    if cfg.max_task_seconds <= 0:
        raise ConfigError("max_task_seconds must be > 0")
    if cfg.cycle_interval_seconds <= 0:
        raise ConfigError("cycle_interval_seconds must be > 0")
    if not cfg.state_file.strip():
        raise ConfigError("state_file cannot be empty")

    if not cfg.wallets:
        raise ConfigError("at least one wallet is required")
    if not cfg.projects:
        raise ConfigError("at least one project is required")

    seen_wallets: set[str] = set()
    for wallet in cfg.wallets:
        if not wallet.name:
            raise ConfigError("wallet.name cannot be empty")
        if not wallet.address:
            raise ConfigError(f"wallet '{wallet.name}' has empty address")
        if not wallet.private_key_env:
            raise ConfigError(f"wallet '{wallet.name}' has empty private_key_env")
        if wallet.name in seen_wallets:
            raise ConfigError(f"duplicate wallet name: {wallet.name}")
        seen_wallets.add(wallet.name)

        if cfg.check_wallet_env_vars and os.getenv(wallet.private_key_env) is None:
            raise ConfigError(
                f"missing env var for wallet '{wallet.name}': {wallet.private_key_env}"
            )

    seen_projects: set[str] = set()
    for project in cfg.projects:
        if not project.name:
            raise ConfigError("project.name cannot be empty")
        if project.cooldown_seconds < 0:
            raise ConfigError(f"project '{project.name}' cooldown_seconds must be >= 0")
        if not project.tasks:
            raise ConfigError(f"project '{project.name}' must contain at least one task")

        if project.name in seen_projects:
            raise ConfigError(f"duplicate project name: {project.name}")
        seen_projects.add(project.name)

        task_ids: set[str] = set()
        for task in project.tasks:
            if not task.id:
                raise ConfigError(f"task id cannot be empty in project '{project.name}'")
            if task.id in task_ids:
                raise ConfigError(f"duplicate task id '{task.id}' in project '{project.name}'")
            task_ids.add(task.id)
            if not task.provider:
                raise ConfigError(f"task '{task.id}' in project '{project.name}' needs provider")
            if not task.action:
                raise ConfigError(f"task '{task.id}' in project '{project.name}' needs action")
