"""BSCALPHA compatibility formula; source hash in tests/fixtures/tef-legacy.json."""

PARAMETERS = (
    ("HTF_Bias", 0.0, -1.0, 1.0),
    ("HTF_ReversalThreshold_S", 0.01, 0.0, 1.0),
    ("HTF_ReversalThreshold_L", 0.01, 0.0, 1.0),
    ("HTF_Strength_S", 1.0, 0.0, 1.0),
    ("HTF_Strength_L", 1.0, 0.0, 1.0),
    ("MTF_Bias_S", 0.0, -1.0, 1.0),
    ("MTF_Bias_L", 0.0, -1.0, 1.0),
    ("MTF_ReversalThreshold_Entry_S", 0.8, 0.0, 1.0),
    ("MTF_ReversalThreshold_Entry_L", 0.8, 0.0, 1.0),
    ("MTF_ReversalThreshold_Exit_S", 0.8, 0.0, 1.0),
    ("MTF_ReversalThreshold_Exit_L", 0.8, 0.0, 1.0),
)


def calculate(params: tuple, linearizedAnalysis: dict, tcTracker_model: dict) -> tuple[str, float]:
    (
        param_HTF_Bias,
        param_HTF_ReversalThreshold_S,
        param_HTF_ReversalThreshold_L,
        param_HTF_Strength_S,
        param_HTF_Strength_L,
        param_MTF_Bias_S,
        param_MTF_Bias_L,
        param_MTF_ReversalThreshold_Entry_S,
        param_MTF_ReversalThreshold_Entry_L,
        param_MTF_ReversalThreshold_Exit_S,
        param_MTF_ReversalThreshold_Exit_L,
    ) = params
    la_iClosed_1d = linearizedAnalysis["CLOSED_11"]
    la_iClosed_1h = linearizedAnalysis["CLOSED_5"]
    la_kline_cp = linearizedAnalysis["KLINE_CLOSEPRICE"]
    la_EMA_EMA = linearizedAnalysis.get("11_EMA_0_EMA", None)
    la_MMACD_MSDeltaAbsMARel = linearizedAnalysis.get("5_MMACD_MSDELTAABSMAREL", None)
    if not tcTracker_model:
        tcTracker_model["tefVal_prev"] = 0
        tcTracker_model["tefDir_prev"] = "LONG"
        tcTracker_model["EMA_EMA_last"] = 0
        tcTracker_model["MMACD_MSDELTAABSMAREL_last"] = 0
        tcTracker_model["HTF_ISSHORT"] = False
        tcTracker_model["MTF_CYCLICMAX"] = False
    tefVal_prev = tcTracker_model["tefVal_prev"]
    tefDir_prev = tcTracker_model["tefDir_prev"]
    ema_EMA_last = tcTracker_model["EMA_EMA_last"]
    mmacd_MsDeltaAbsMARel_last = tcTracker_model["MMACD_MSDELTAABSMAREL_last"]
    htf_isShort = tcTracker_model["HTF_ISSHORT"]
    mtf_CyclicMax = tcTracker_model["MTF_CYCLICMAX"]
    if la_kline_cp is None or la_EMA_EMA is None or la_MMACD_MSDeltaAbsMARel is None:
        return (tefDir_prev, tefVal_prev)
    ema_EMA = la_EMA_EMA if la_iClosed_1d else ema_EMA_last
    mmacd_MsDeltaAbsMARel = (
        la_MMACD_MSDeltaAbsMARel if la_iClosed_1h else mmacd_MsDeltaAbsMARel_last
    )
    ema_EMA_biased = ema_EMA * (1 + param_HTF_Bias)
    if htf_isShort:
        if ema_EMA_biased * (1 + param_HTF_ReversalThreshold_S) <= la_kline_cp:
            htf_isShort = False
    elif la_kline_cp <= ema_EMA_biased * (1 - param_HTF_ReversalThreshold_L):
        htf_isShort = True
    if htf_isShort:
        htf_strength = param_HTF_Strength_S
    else:
        htf_strength = param_HTF_Strength_L
    if htf_isShort:
        mtf_bias = param_MTF_Bias_S
    else:
        mtf_bias = param_MTF_Bias_L
    mtf_y_prev = mmacd_MsDeltaAbsMARel_last - mtf_bias
    mtf_y = mmacd_MsDeltaAbsMARel - mtf_bias
    mtf_isBelow_prev = mtf_y_prev < 0
    mtf_isBelow_this = mtf_y < 0
    mtf_cyclicDist = abs(mmacd_MsDeltaAbsMARel - mtf_bias)
    if mtf_isBelow_prev != mtf_isBelow_this:
        mtf_CyclicMax = mtf_cyclicDist
    else:
        mtf_CyclicMax = max(mtf_cyclicDist, mtf_CyclicMax)
    if htf_isShort:
        if mtf_isBelow_this:
            mtf_allowEntry = False
            mtf_allowExit = mtf_cyclicDist < mtf_CyclicMax * param_MTF_ReversalThreshold_Exit_S
        else:
            mtf_allowEntry = mtf_cyclicDist < mtf_CyclicMax * param_MTF_ReversalThreshold_Entry_S
            mtf_allowExit = False
    elif mtf_isBelow_this:
        mtf_allowEntry = mtf_cyclicDist < mtf_CyclicMax * param_MTF_ReversalThreshold_Entry_L
        mtf_allowExit = False
    else:
        mtf_allowEntry = False
        mtf_allowExit = mtf_cyclicDist < mtf_CyclicMax * param_MTF_ReversalThreshold_Exit_L
    if htf_isShort:
        if mtf_allowEntry:
            tefVal = -htf_strength
        elif mtf_allowExit:
            tefVal = 0
        else:
            tefVal = tefVal_prev
    elif mtf_allowEntry:
        tefVal = htf_strength
    elif mtf_allowExit:
        tefVal = 0
    else:
        tefVal = tefVal_prev
    tefVal = round(tefVal, 6)
    tefDir = "SHORT" if htf_isShort else "LONG"
    tcTracker_model["tefVal_prev"] = tefVal
    tcTracker_model["tefDir_prev"] = tefDir
    tcTracker_model["EMA_EMA_last"] = ema_EMA
    tcTracker_model["MMACD_MSDELTAABSMAREL_last"] = mmacd_MsDeltaAbsMARel
    tcTracker_model["HTF_ISSHORT"] = htf_isShort
    tcTracker_model["MTF_CYCLICMAX"] = mtf_CyclicMax
    return (tefDir, tefVal)
