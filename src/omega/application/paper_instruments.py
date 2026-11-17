"""Initial paper host settlement policy; exchange rules remain an adapter concern."""

from omega.domain.errors import ValidationError


def require_paper_settlement(instrument):
    # This host has one USDT wallet per account and no FX conversion. Do not
    # silently value BTC-, USDC- or KRW-quoted instruments in the same wallet.
    symbol = instrument.symbol.split("_", 1)[0]
    if instrument.provider != "binance" or not symbol.endswith("USDT") or len(symbol) <= 4:
        raise ValidationError(
            "paper v1 requires Binance USDT-quoted/settled instruments; "
            "currency conversion is not implemented"
        )
