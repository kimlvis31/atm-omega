"""MMACDDEFAULT compatibility formula; source hash in tests/fixtures/tef-legacy.json."""

import math

PARAMETERS = (
    ("alpha", 1.0, 0.0001, 5.0),
    ("beta", 1.0, 1.0, 10.0),
    ("delta", 0.0, -1.0, 1.0),
    ("shortStrength", 1.0, 0.0, 1.0),
    ("longStrength", 1.0, 0.0, 1.0),
    ("downHillThreshold", 0.8, 0.0, 1.0),
)


def calculate(params: tuple, linearizedAnalysis: dict, tcTracker_model: dict) -> tuple[str, float]:
    (
        param_alpha,
        param_beta,
        param_delta,
        param_strength_SHORT,
        param_strength_LONG,
        param_downHillThreshold,
    ) = params
    la_mmacd_msDeltaAbsMARel = linearizedAnalysis.get("0_MMACD_MSDELTAABSMAREL", None)
    if la_mmacd_msDeltaAbsMARel is None:
        return (None, 0)
    if not tcTracker_model:
        tcTracker_model["tefVal_prev"] = 0
        tcTracker_model["tefVal_absMax"] = 0
        tcTracker_model["la_mmacd_msDeltaAbsMARel_prev"] = 0
        tcTracker_model["isDownhill"] = False
    isShort_prev = tcTracker_model["la_mmacd_msDeltaAbsMARel_prev"] < param_delta
    isShort_this = la_mmacd_msDeltaAbsMARel < param_delta
    cycleReset = isShort_prev ^ isShort_this
    if isShort_this:
        param_strength_eff = param_strength_SHORT
    else:
        param_strength_eff = param_strength_LONG
    x_sign = -1.0 if la_mmacd_msDeltaAbsMARel < 0 else 1.0
    x_abs = pow(abs(la_mmacd_msDeltaAbsMARel / param_alpha), param_beta)
    y_norm = math.tanh(x_abs) * x_sign
    width = param_delta + 1.0 if isShort_this else 1.0 - param_delta
    dist = abs(y_norm - param_delta)
    tefVal_abs = dist / max(width, 1e-09) * param_strength_eff
    if width == 0:
        tefVal_abs = 0.0
    tefVal_abs = round(tefVal_abs, 6)
    if cycleReset:
        tefVal_absMax = tefVal_abs
    else:
        tefVal_absMax = max(tcTracker_model["tefVal_absMax"], tefVal_abs)
    if cycleReset:
        isDownhill = False
    else:
        isDownhill = tcTracker_model["isDownhill"]
    if tefVal_abs < tefVal_absMax * param_downHillThreshold:
        isDownhill = True
    if isDownhill:
        tefVal_abs = min(tefVal_abs, abs(tcTracker_model["tefVal_prev"]))
    if isShort_this:
        tefVal = -tefVal_abs
    else:
        tefVal = tefVal_abs
    tcTracker_model["tefVal_prev"] = tefVal
    tcTracker_model["tefVal_absMax"] = tefVal_absMax
    tcTracker_model["la_mmacd_msDeltaAbsMARel_prev"] = la_mmacd_msDeltaAbsMARel
    tcTracker_model["isDownhill"] = isDownhill
    tefDir = "SHORT" if isShort_this else "LONG"
    return (tefDir, tefVal)
