"""Expected value estimation for trading opportunities."""

from dataclasses import dataclass
from typing import Iterable


@dataclass
class Opportunity:
    """Represents a tradeable opportunity with probability and payoff."""

    probability: float
    payoff: float
    cost: float


class ExpectedValueCalculator:
    """Compute expected value for a set of opportunities."""

    def evaluate(self, opportunities: Iterable[Opportunity]) -> float:
        """Return the total expected value across opportunities."""

        ev = 0.0
        for opportunity in opportunities:
            if not 0 <= opportunity.probability <= 1:
                raise ValueError("Probability must be between 0 and 1")
            ev += opportunity.probability * (opportunity.payoff - opportunity.cost)
        return ev
