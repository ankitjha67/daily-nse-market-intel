from __future__ import annotations

import argparse
import sys

from market_intel.config import load_config
from market_intel.pipeline import run_pipeline


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="market-intel")
    sub = parser.add_subparsers(dest="cmd", required=True)

    runp = sub.add_parser("run", help="Run daily market intelligence pipeline")
    runp.add_argument("--config", required=True, help="Path to config.yaml")

    args = parser.parse_args(argv)

    if args.cmd == "run":
        cfg = load_config(args.config)
        run_pipeline(cfg)
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
