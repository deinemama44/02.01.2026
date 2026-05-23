from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import asdict
from pathlib import Path
from typing import Callable

from .models import AppConfig, ProjectConfig, RunSummary, TaskConfig, TaskResult, WalletConfig
from .providers import provider_factory


class AirdropRunner:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self._state_path = Path(self.config.state_file)
        self._last_run = self._load_state()

    def run_once(self) -> tuple[list[TaskResult], RunSummary]:
        if self.config.kill_switch:
            self.logger.warning("kill_switch enabled - skipping run")
            return [], RunSummary(total=0, success=0, failed=0)

        results: list[TaskResult] = []

        for wallet in self.config.wallets:
            for project in self.config.projects:
                if not project.enabled:
                    continue
                if not self._project_ready(wallet, project):
                    self.logger.info(
                        "project_cooldown_active",
                        extra={"extra": {"wallet": wallet.name, "project": project.name}},
                    )
                    continue

                for task in project.tasks:
                    result = self._run_task(wallet, project, task)
                    results.append(result)
                    if self.config.stop_on_first_task_failure and result.status != "ok":
                        self.logger.warning(
                            "stopping_due_to_task_failure",
                            extra={
                                "extra": {
                                    "wallet": wallet.name,
                                    "project": project.name,
                                    "task_id": task.id,
                                }
                            },
                        )
                        self._last_run[(wallet.name, project.name)] = time.time()
                        self._save_state()
                        summary = self._build_summary(results)
                        return results, summary

                self._last_run[(wallet.name, project.name)] = time.time()

        self._save_state()
        summary = self._build_summary(results)
        self.logger.info("cycle_summary", extra={"extra": asdict(summary)})
        return results, summary

    def run_forever(self, max_cycles: int | None = None) -> None:
        cycles = 0
        while True:
            self.run_once()
            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                return
            time.sleep(self.config.cycle_interval_seconds)

    def _project_ready(self, wallet: WalletConfig, project: ProjectConfig) -> bool:
        key = (wallet.name, project.name)
        last = self._last_run.get(key)
        if last is None:
            return True
        return (time.time() - last) >= project.cooldown_seconds

    def _run_task(self, wallet: WalletConfig, project: ProjectConfig, task: TaskConfig) -> TaskResult:
        attempts = 0
        try:
            provider = provider_factory(task.provider)
        except Exception as exc:  # noqa: BLE001
            self.logger.error(
                "provider_init_failed",
                extra={
                    "extra": {
                        "wallet": wallet.name,
                        "project": project.name,
                        "task_id": task.id,
                        "provider": task.provider,
                        "error": str(exc),
                    }
                },
            )
            return TaskResult(
                wallet=wallet.name,
                project=project.name,
                task_id=task.id,
                status="failed",
                message=f"provider init failed: {exc}",
                attempts=0,
            )

        while attempts < task.retry.attempts:
            attempts += 1
            try:
                response = self._run_with_timeout(
                    provider.execute,
                    wallet_name=wallet.name,
                    wallet_address=wallet.address,
                    action=task.action,
                    params=task.params,
                    dry_run=self.config.dry_run,
                )
                if response.ok:
                    self.logger.info(
                        "task_success",
                        extra={
                            "extra": {
                                "wallet": wallet.name,
                                "project": project.name,
                                "task_id": task.id,
                                "attempt": attempts,
                                "tx_ref": response.tx_ref,
                            }
                        },
                    )
                    return TaskResult(wallet.name, project.name, task.id, "ok", response.message, attempts)

                self.logger.warning(
                    "task_failed",
                    extra={
                        "extra": {
                            "wallet": wallet.name,
                            "project": project.name,
                            "task_id": task.id,
                            "attempt": attempts,
                            "reason": response.message,
                        }
                    },
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.error(
                    "task_exception",
                    extra={
                        "extra": {
                            "wallet": wallet.name,
                            "project": project.name,
                            "task_id": task.id,
                            "attempt": attempts,
                            "error": str(exc),
                        }
                    },
                )

            if attempts < task.retry.attempts:
                time.sleep(task.retry.base_delay_seconds * attempts)

        return TaskResult(
            wallet=wallet.name,
            project=project.name,
            task_id=task.id,
            status="failed",
            message=f"failed after {attempts} attempts",
            attempts=attempts,
        )

    def _run_with_timeout(self, fn: Callable, **kwargs):
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(fn, **kwargs)
            try:
                return future.result(timeout=self.config.max_task_seconds)
            except TimeoutError as exc:
                raise TimeoutError(f"task timed out after {self.config.max_task_seconds}s") from exc

    def _load_state(self) -> dict[tuple[str, str], float]:
        if not self._state_path.exists():
            return {}

        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return {}
            parsed: dict[tuple[str, str], float] = {}
            for key, value in raw.items():
                if ":" not in key:
                    continue
                wallet, project = key.split(":", 1)
                parsed[(wallet, project)] = float(value)
            return parsed
        except (OSError, ValueError, TypeError):
            return {}

    def _save_state(self) -> None:
        serializable = {f"{wallet}:{project}": ts for (wallet, project), ts in self._last_run.items()}
        self._state_path.write_text(json.dumps(serializable, indent=2), encoding="utf-8")

    @staticmethod
    def _build_summary(results: list[TaskResult]) -> RunSummary:
        total = len(results)
        success = sum(1 for r in results if r.status == "ok")
        failed = total - success
        return RunSummary(total=total, success=success, failed=failed)
