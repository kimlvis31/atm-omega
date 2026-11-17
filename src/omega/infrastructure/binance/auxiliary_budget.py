"""Public weighted admission plus auxiliary request-count admission, shared on disk."""

from pathlib import Path

from omega.domain.errors import ValidationError
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.shared_budget import SharedRequestBudget


class AuxiliaryPublicBudget(RequestBudget):
    """Keep the existing weighted ledger; don't invalidate its stored policy.

    Reservation failures may conservatively retain already-spent admission in
    the other ledger. No HTTP request follows a failed reservation. Auxiliary
    statistics with provider weight zero still spend one local weighted unit.
    """

    def __init__(self, root: Path, *, clock=None):
        options = {} if clock is None else {"clock": clock}
        self.primary = SharedRequestBudget(Path(root) / "binance-public-budget.json", **options)
        self.requests = SharedRequestBudget(
            Path(root) / "binance-public-aux-request-budget.json",
            (Window("binance-ip", 1000, 300),),
            **options,
        )

    def reserve(self, costs):
        if not isinstance(costs, dict) or set(costs) != {"binance-ip"}:
            raise ValidationError("auxiliary budget admits public IP requests only")
        self.primary.reserve(costs)
        self.requests.reserve({"binance-ip": 1})

    def observe_usage(self, scope, seconds, used):
        # Used-weight headers aren't counts of the statistical HTTP requests.
        self.primary.observe_usage(scope, seconds, used)

    def cooldown(self, seconds):
        self.primary.cooldown(seconds)
        self.requests.cooldown(seconds)
