from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Protocol


@dataclass(slots=True)
class ProviderResponse:
    ok: bool
    message: str
    tx_ref: str | None = None


class Provider(Protocol):
    def execute(
        self,
        *,
        wallet_name: str,
        wallet_address: str,
        action: str,
        params: dict,
        dry_run: bool,
    ) -> ProviderResponse:
        ...


class MockProvider:
    """Deterministic-ish simulation provider for local testing and dry-runs."""

    def execute(
        self,
        *,
        wallet_name: str,
        wallet_address: str,
        action: str,
        params: dict,
        dry_run: bool,
    ) -> ProviderResponse:
        seed = f"{wallet_name}:{wallet_address}:{action}:{sorted(params.items())}"
        digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
        random_value = int(digest[:4], 16) % 100

        time.sleep(0.01)
        if dry_run:
            return ProviderResponse(True, f"[dry_run] simulated action={action}", f"dry-{digest[:10]}")

        if random_value < 90:
            return ProviderResponse(True, f"executed action={action}", f"tx-{digest[:12]}")
        return ProviderResponse(False, f"provider rejected action={action}")


def provider_factory(name: str) -> Provider:
    mapping = {
        "mock": MockProvider,
    }
    cls = mapping.get(name)
    if cls is None:
        raise ValueError(f"unknown provider: {name}")
    return cls()
