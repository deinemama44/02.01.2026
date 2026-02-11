from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class RetryConfig:
    attempts: int = 1
    base_delay_seconds: float = 1.0


@dataclass(slots=True)
class TaskConfig:
    id: str
    provider: str
    action: str
    params: dict[str, Any] = field(default_factory=dict)
    retry: RetryConfig = field(default_factory=RetryConfig)


@dataclass(slots=True)
class ProjectConfig:
    name: str
    enabled: bool
    cooldown_seconds: float
    tasks: list[TaskConfig]


@dataclass(slots=True)
class WalletConfig:
    name: str
    address: str
    private_key_env: str


@dataclass(slots=True)
class AppConfig:
    dry_run: bool
    kill_switch: bool
    stop_on_first_task_failure: bool
    check_wallet_env_vars: bool
    state_file: str
    max_task_seconds: float
    cycle_interval_seconds: float
    wallets: list[WalletConfig]
    projects: list[ProjectConfig]


@dataclass(slots=True)
class TaskResult:
    wallet: str
    project: str
    task_id: str
    status: str
    message: str
    attempts: int


@dataclass(slots=True)
class RunSummary:
    total: int
    success: int
    failed: int

    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 1.0
        return self.success / self.total
