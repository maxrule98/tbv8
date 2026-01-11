from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from packages.common.types import RoutingMode, VenueId


@dataclass
class VenueState:
    venue: VenueId
    position_btc: float = 0.0


@dataclass
class RouteDecision:
    primary: VenueId
    secondary: Optional[VenueId]
    mode: RoutingMode


class VenueRouter:
    """
    TBV8 routing: choose which venue(s) to execute on.
    Start simple:
      - PRIMARY_ONLY
      - PRIMARY_LIVE_SECONDARY_SHADOW
    """

    def __init__(self, primary: VenueId, secondary: Optional[VenueId], mode: RoutingMode):
        self.primary = primary
        self.secondary = secondary
        self.mode = mode

    def decide(self) -> RouteDecision:
        return RouteDecision(primary=self.primary, secondary=self.secondary, mode=self.mode)

    def compute_delta(self, target_btc: float, venue_states: Dict[VenueId, VenueState], venue: VenueId) -> float:
        st = venue_states.get(venue)
        if not st:
            return target_btc  # assume flat if unknown
        return target_btc - st.position_btc
