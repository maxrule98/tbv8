from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from packages.common.types import RoutingMode


@dataclass(frozen=True)
class RoutingPlan:
    primary: str
    secondary: Optional[str]
    mode: RoutingMode


class ExecutionRouter:
    """
    Pure routing policy.
    No side effects, no IO.
    """

    def __init__(self, plan: RoutingPlan):
        self.plan = plan

    def choose_primary(self) -> str:
        return self.plan.primary

    def choose_shadow(self) -> Optional[str]:
        if self.plan.mode == RoutingMode.PRIMARY_LIVE_SECONDARY_SHADOW and self.plan.secondary:
            return self.plan.secondary
        return None
