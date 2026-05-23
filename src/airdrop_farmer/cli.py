from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from .config import ConfigError, load_config, validate_config
from .logging_utils import build_logger
from .runner import AirdropRunner


def _cmd_validate(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.config)
        validate_config(cfg)
    except (OSError, ValueError, ConfigError) as exc:
        print(f"Config invalid: {exc}", file=sys.stderr)
        return 1

    print("Config valid")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    logger = build_logger()
    try:
        cfg = load_config(args.config)
    except (OSError, ValueError, ConfigError) as exc:
        print(f"Config invalid: {exc}", file=sys.stderr)
        return 1

    if args.live:
        cfg.dry_run = False
    if args.ignore_env_check:
        cfg.check_wallet_env_vars = False

    try:
        validate_config(cfg)
    except ConfigError as exc:
        print(f"Runtime config invalid: {exc}", file=sys.stderr)
        return 1

    runner = AirdropRunner(cfg, logger)
    if args.once:
        results, summary = runner.run_once()
        payload = {
            "summary": asdict(summary),
            "results": [asdict(r) for r in results],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if summary.failed == 0 else 2

    logger.info("starting_scheduler")
    runner.run_forever(max_cycles=args.max_cycles)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airdrop-farmer",
        description="Full-auto orchestrator for legitimate airdrop task workflows",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run one cycle or scheduler loop")
    run.add_argument("--config", required=True, help="path to config json")
    run.add_argument("--once", action="store_true", help="run only one cycle")
    run.add_argument("--max-cycles", type=int, default=None, help="optional cycle limit for scheduler mode")
    run.add_argument("--live", action="store_true", help="force live mode (override config dry_run)")
    run.add_argument(
        "--ignore-env-check",
        action="store_true",
        help="skip wallet private-key env var validation (mainly for local dry-run dev)",
    )
    run.set_defaults(func=_cmd_run)

    validate = sub.add_parser("validate", help="validate config")
    validate.add_argument("--config", required=True, help="path to config json")
    validate.set_defaults(func=_cmd_validate)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
