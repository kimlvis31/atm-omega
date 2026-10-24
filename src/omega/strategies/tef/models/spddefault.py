"""SPDDEFAULT compatibility formula; source hash in tests/fixtures/tef-legacy.json."""

PARAMETERS = (
    ("shortDelta", 0.0, -1.0, 1.0),
    ("shortStrength", 1.0, 0.0, 1.0),
    ("shortLength", 1.0, 0.0, 1.0),
    ("longDelta", 0.0, -1.0, 1.0),
    ("longStrength", 1.0, 0.0, 1.0),
    ("longLength", 1.0, 0.0, 1.0),
)


def calculate(params: tuple, linearizedAnalysis: dict, tcTracker_model: dict) -> tuple[str, float]:
    (
        param_delta_SHORT,
        param_strength_SHORT,
        param_length_SHORT,
        param_delta_LONG,
        param_strength_LONG,
        param_length_LONG,
    ) = params
    la_swing0_lsPrice = linearizedAnalysis.get("0_SWING_0_LSPRICE", None)
    la_swing0_lsType = linearizedAnalysis.get("0_SWING_0_LSTYPE", None)
    if la_swing0_lsType is None:
        return (None, 0)
    if not tcTracker_model:
        tcTracker_model["tefVal_prev"] = 0
        tcTracker_model["la_swing0_lsType_prev"] = -1
    isShort_prev = tcTracker_model["la_swing0_lsType_prev"] == 1
    isShort_this = la_swing0_lsType == 1
    cycleReset = isShort_prev ^ isShort_this
    if isShort_this:
        param_delta_eff = param_delta_SHORT
        param_strength_eff = param_strength_SHORT
        param_length_eff = param_length_SHORT
    else:
        param_delta_eff = param_delta_LONG
        param_strength_eff = param_strength_LONG
        param_length_eff = param_length_LONG
    if isShort_this:
        pd = 1 - linearizedAnalysis["KLINE_CLOSEPRICE"] / la_swing0_lsPrice
    else:
        pd = linearizedAnalysis["KLINE_CLOSEPRICE"] / la_swing0_lsPrice - 1
    dist = pd - param_delta_eff
    if param_delta_eff <= pd:
        tefVal_abs = max((1 - dist / max(param_length_eff, 1e-06)) * param_strength_eff, 0.0)
    else:
        tefVal_abs = 0.0
    if param_length_eff == 0:
        tefVal_abs = 0.0
    tefVal_abs = round(tefVal_abs, 6)
    if not cycleReset:
        tefVal_abs = min(tefVal_abs, abs(tcTracker_model["tefVal_prev"]))
    if isShort_this:
        tefVal = -tefVal_abs
    else:
        tefVal = tefVal_abs
    tcTracker_model["tefVal_prev"] = tefVal
    tcTracker_model["la_swing0_lsType_prev"] = la_swing0_lsType
    tefDir = "SHORT" if isShort_this else "LONG"
    return (tefDir, tefVal)
