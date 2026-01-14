from __future__ import annotations

import asyncio
import sys

from loguru import logger
from packages.common.config import load_tbv8_config
from apps.runtime.main import run_simfill_backtest


def main() -> None:
    try:
        cfg = load_tbv8_config()
        logger.info("Starting Backtester App...")
        asyncio.run(run_simfill_backtest(cfg))
    except KeyboardInterrupt:
        logger.info("Backtester interrupted by user.")
    except Exception as e:
        logger.exception(f"Backtester failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()