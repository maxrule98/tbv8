from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

from .types import MarketDataVenueId, RoutingMode, VenueId
from .timeframes import timeframe_to_ms


def normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if "/" not in s:
        raise ValueError(f"symbol must be canonical like 'BTC/USDT' (got {symbol!r})")
    base, quote = s.split("/", 1)
    if not base or not quote:
        raise ValueError(f"symbol must be canonical like 'BTC/USDT' (got {symbol!r})")
    return f"{base}/{quote}"


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

    @field_validator("symbols", mode="before")
    @classmethod
    def _normalize_symbols(cls, v):
        if v is None:
            return []
        return [normalize_symbol(x) for x in v]


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
    timeframes: List[str] = Field(..., min_length=1)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    routing: RoutingConfig

    @field_validator("symbol")
    @classmethod
    def _validate_symbol(cls, v: str) -> str:
        return normalize_symbol(v)

    @field_validator("timeframes")
    @classmethod
    def _validate_timeframes(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("strategy.timeframes must be non-empty")
        out: List[str] = []
        seen: set[str] = set()
        for tf in v:
            tf2 = str(tf).strip()
            if not tf2:
                continue
            timeframe_to_ms(tf2)  # validates known timeframe
            if tf2 not in seen:
                seen.add(tf2)
                out.append(tf2)
        if not out:
            raise ValueError("strategy.timeframes must contain at least one valid timeframe")
        return out


class HistoryConfig(BaseModel):
    market_data_venue: MarketDataVenueId = "binance_spot"
    start_date: str = "2017-08-17T00:00:00Z"
    end_date: Optional[str] = None


class DataConfig(BaseModel):
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
    # ---- venues
    venue_cfgs: List[VenueConfig] = []
    for p in sorted(venues_dir.glob("*.yaml")):
        raw = _load_yaml(p)
        venue_cfgs.append(VenueConfig.model_validate(raw))

    enabled = [v for v in venue_cfgs if v.enabled]

    # ---- strategy
    strat_raw = _load_yaml(strategy_path)
    strategy = StrategyConfig.model_validate(strat_raw)

    # ---- optional history + data
    history_raw = _maybe_load_yaml(history_path)
    data_raw = _maybe_load_yaml(data_path)

    history = HistoryConfig.model_validate(history_raw) if history_raw else HistoryConfig()
    data_cfg = DataConfig.model_validate(data_raw) if data_raw else DataConfig()

    # ---- routing sanity
    prim = strategy.routing.primary
    if not any(v.venue == prim for v in enabled):
        raise ValueError(f"Primary venue '{prim}' not enabled/found in {venues_dir}")

    sec = strategy.routing.secondary
    if sec and not any(v.venue == sec for v in enabled):
        raise ValueError(f"Secondary venue '{sec}' not enabled/found in {venues_dir}")

    # ---- symbol sanity (optional but recommended)
    # This ensures your strategy symbol is actually configured on at least one enabled venue.
    if not any(strategy.symbol in v.symbols for v in enabled):
        raise ValueError(
            f"Strategy symbol '{strategy.symbol}' not found in any enabled venue symbols. "
            f"Add it to config/venues/*.yaml symbols."
        )

    if not history.start_date:
        raise ValueError("history.start_date must be set (e.g. '2017-08-17T00:00:00Z')")

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