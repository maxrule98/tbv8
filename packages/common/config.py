from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

import yaml
from pydantic import BaseModel, Field

from .types import RoutingMode, VenueId


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
    timeframe: str = "1m"
    risk: RiskConfig = Field(default_factory=RiskConfig)
    routing: RoutingConfig


class TBV8Config(BaseModel):
    venues: List[VenueConfig]
    strategy: StrategyConfig


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML structure in {path}")
    return data


def load_tbv8_config(
    venues_dir: Path = Path("config/venues"),
    strategy_path: Path = Path("config/strategies/btc_usdt_perp_v1.yaml"),
) -> TBV8Config:
    venue_cfgs: List[VenueConfig] = []
    for p in sorted(venues_dir.glob("*.yaml")):
        raw = _load_yaml(p)
        venue_cfgs.append(VenueConfig.model_validate(raw))

    strat_raw = _load_yaml(strategy_path)
    strategy = StrategyConfig.model_validate(strat_raw)

    # Only include enabled venues
    enabled = [v for v in venue_cfgs if v.enabled]

    # Sanity: ensure primary exists
    prim = strategy.routing.primary
    if not any(v.venue == prim for v in enabled):
        raise ValueError(f"Primary venue '{prim}' not enabled/found in {venues_dir}")

    # Sanity: if secondary configured, ensure exists
    sec = strategy.routing.secondary
    if sec and not any(v.venue == sec for v in enabled):
        raise ValueError(f"Secondary venue '{sec}' not enabled/found in {venues_dir}")

    return TBV8Config(venues=enabled, strategy=strategy)
