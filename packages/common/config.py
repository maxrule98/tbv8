from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

import yaml
from pydantic import BaseModel, Field

from packages.common.constants import BASE_TIMEFRAME
from .types import MarketDataVenueId, RoutingMode, VenueId


class VenueConfig(BaseModel):
    venue: VenueId
    enabled: bool = True

    ws_url: str
    rest_url: str

    # CEX credentials (optional)
    api_key: str = ""
    api_secret: str = ""

    # DEX credentials (optional)
    wallet_address: str = ""
    private_key: str = ""

    symbols: List[str] = Field(default_factory=list)


class RiskConfig(BaseModel):
    max_position_btc: float = 0.01
    max_daily_loss_usd: float = 10.0
    max_leverage: float = 2.0


class RoutingConfig(BaseModel):
    primary: VenueId
    secondary: Optional[VenueId] = None
    mode: RoutingMode = RoutingMode.PRIMARY_LIVE_SECONDARY_SHADOW


class StrategyConfig(BaseModel):
    strategy_id: str
    symbol: str
    timeframe: str = BASE_TIMEFRAME
    risk: RiskConfig = Field(default_factory=RiskConfig)
    routing: RoutingConfig


class HistoryConfig(BaseModel):
    """
    Controls how much historical data we want locally.

    market_data_venue: which venue to use as the research/backfill source, e.g. "binance_spot"
    start_date: required for deterministic bootstraps
    end_date: optional; if null/empty -> 'now' at runtime
    """
    market_data_venue: MarketDataVenueId = "binance_spot"
    start_date: str = "2017-08-17T00:00:00Z"
    end_date: Optional[str] = None


class DataConfig(BaseModel):
    """
    Storage locations.
    """
    db_path: str = "data/tbv8.sqlite"


class TBV8Config(BaseModel):
    venues: List[VenueConfig]
    strategy: StrategyConfig
    history: HistoryConfig = Field(default_factory=HistoryConfig)
    data: DataConfig = Field(default_factory=DataConfig)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML structure in {path}")
    return data


def _maybe_load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text())
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML structure in {path}")
    return data


def load_tbv8_config(
    venues_dir: Path = Path("config/venues"),
    strategy_path: Path = Path("config/strategies/btc_usdt_perp_v1.yaml"),
    history_path: Path = Path("config/history.yaml"),
    data_path: Path = Path("config/data.yaml"),
) -> TBV8Config:
    # ---- venues (execution venues)
    venue_cfgs: List[VenueConfig] = []
    for p in sorted(venues_dir.glob("*.yaml")):
        raw = _load_yaml(p)
        venue_cfgs.append(VenueConfig.model_validate(raw))

    enabled = [v for v in venue_cfgs if v.enabled]

    # ---- strategy
    strat_raw = _load_yaml(strategy_path)
    strategy = StrategyConfig.model_validate(strat_raw)

    # ---- history + data (optional files)
    history_raw = _maybe_load_yaml(history_path)
    data_raw = _maybe_load_yaml(data_path)

    history = HistoryConfig.model_validate(history_raw) if history_raw else HistoryConfig()
    data_cfg = DataConfig.model_validate(data_raw) if data_raw else DataConfig()

    # ---- sanity checks for routing venues (execution-only)
    prim = strategy.routing.primary
    if not any(v.venue == prim for v in enabled):
        raise ValueError(f"Primary venue '{prim}' not enabled/found in {venues_dir}")

    sec = strategy.routing.secondary
    if sec and not any(v.venue == sec for v in enabled):
        raise ValueError(f"Secondary venue '{sec}' not enabled/found in {venues_dir}")

    # ---- sanity for history dates
    if not history.start_date:
        raise ValueError("history.start_date must be set (e.g. '2017-08-17T00:00:00Z')")

    # end_date may be None/"" to mean "now"
    if history.end_date is not None and history.end_date.strip() == "":
        history = HistoryConfig(
            market_data_venue=history.market_data_venue,
            start_date=history.start_date,
            end_date=None,
        )

    return TBV8Config(
        venues=enabled,
        strategy=strategy,
        history=history,
        data=data_cfg,
    )
