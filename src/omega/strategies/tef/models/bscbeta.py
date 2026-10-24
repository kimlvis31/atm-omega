"""BSCBETA compatibility formula; source hash in tests/fixtures/tef-legacy.json."""

import math

PARAMETERS = (
    ("HTF_Bias", 0.0, -1.0, 1.0),
    ("HTF_ReversalThreshold_S", 0.01, 0.0, 1.0),
    ("HTF_ReversalThreshold_L", 0.01, 0.0, 1.0),
    ("HTF_Strength_S", 1.0, 0.0, 1.0),
    ("HTF_Strength_L", 1.0, 0.0, 1.0),
    ("MTF_Bias_S", 0.0, -1.0, 1.0),
    ("MTF_Bias_L", 0.0, -1.0, 1.0),
    ("MTF_Alpha_Entry_S", 1.0, 0.0, 10.0),
    ("MTF_Alpha_Entry_L", 1.0, 0.0, 10.0),
    ("MTF_Alpha_Exit_S", 1.0, 0.0, 10.0),
    ("MTF_Alpha_Exit_L", 1.0, 0.0, 10.0),
    ("MTF_Beta_Entry_S", 1.0, 0.0, 10.0),
    ("MTF_Beta_Entry_L", 1.0, 0.0, 10.0),
    ("MTF_Beta_Exit_S", 1.0, 0.0, 10.0),
    ("MTF_Beta_Exit_L", 1.0, 0.0, 10.0),
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
        param_MTF_Alpha_Entry_S,
        param_MTF_Alpha_Entry_L,
        param_MTF_Alpha_Exit_S,
        param_MTF_Alpha_Exit_L,
        param_MTF_Beta_Entry_S,
        param_MTF_Beta_Entry_L,
        param_MTF_Beta_Exit_S,
        param_MTF_Beta_Exit_L,
    ) = params
    la_iClosed_1h = linearizedAnalysis["CLOSED_5"]
    la_iClosed_15m = linearizedAnalysis["CLOSED_3"]
    la_iClosed_5m = linearizedAnalysis["CLOSED_2"]
    la_kline_cp = linearizedAnalysis["KLINE_CLOSEPRICE"]
    la_EMA_EMA = linearizedAnalysis.get("5_EMA_0_EMA", None)
    la_MMACD_MSDeltaAbsMARel = linearizedAnalysis.get("3_MMACD_MSDELTAABSMAREL", None)
    la_DMIxADX_DMIxADXAbsMARel = linearizedAnalysis.get("2_DMIxADX_0_DMIxADXABSMAREL", None)
    la_MFI_MFIDevAbsMARel = linearizedAnalysis.get("2_MFI_0_MFIDEVABSMAREL", None)
    if not tcTracker_model:
        tcTracker_model["tefVal_prev"] = 0
        tcTracker_model["tefDir_prev"] = "LONG"
        tcTracker_model["EMA_EMA_last"] = 0
        tcTracker_model["MMACD_MSDELTAABSMAREL_last"] = 0
        tcTracker_model["DMIxADX_DMIxADXABSMAREL_last"] = 0
        tcTracker_model["MFI_MFIDEVABSMAREL_last"] = 0
        tcTracker_model["HTF_ISSHORT"] = False
        tcTracker_model["MTF_ANCHOR"] = 0
        tcTracker_model["ENTRY_PRICE"] = 0.0
    tefVal_prev = tcTracker_model["tefVal_prev"]
    tefDir_prev = tcTracker_model["tefDir_prev"]
    ema_EMA_last = tcTracker_model["EMA_EMA_last"]
    mmacd_MsDeltaAbsMARel_last = tcTracker_model["MMACD_MSDELTAABSMAREL_last"]
    dmixadx_dmixadxAbsMARel_last = tcTracker_model["DMIxADX_DMIxADXABSMAREL_last"]
    mfi_mfiDevAbsMARel_last = tcTracker_model["MFI_MFIDEVABSMAREL_last"]
    htf_isShort = tcTracker_model["HTF_ISSHORT"]
    mtf_anchor = tcTracker_model["MTF_ANCHOR"]
    entry_price = tcTracker_model.get("ENTRY_PRICE", 0.0)
    if (
        la_kline_cp is None
        or la_EMA_EMA is None
        or la_MMACD_MSDeltaAbsMARel is None
        or (la_DMIxADX_DMIxADXAbsMARel is None)
        or (la_MFI_MFIDevAbsMARel is None)
    ):
        return (tefDir_prev, tefVal_prev)
    ema_EMA = la_EMA_EMA if la_iClosed_1h else ema_EMA_last
    mmacd_MsDeltaAbsMARel = (
        la_MMACD_MSDeltaAbsMARel if la_iClosed_15m else mmacd_MsDeltaAbsMARel_last
    )
    dmixadx_DMIxADXAbsMARel = (
        la_DMIxADX_DMIxADXAbsMARel if la_iClosed_5m else dmixadx_dmixadxAbsMARel_last
    )
    mfi_MFIDevAbsMARel = la_MFI_MFIDevAbsMARel if la_iClosed_5m else mfi_mfiDevAbsMARel_last
    ema_EMA_biased = ema_EMA * (1 + param_HTF_Bias)
    htf_isShort_prev = htf_isShort
    if htf_isShort:
        if ema_EMA_biased * (1 + param_HTF_ReversalThreshold_S) <= la_kline_cp:
            htf_isShort = False
    elif la_kline_cp <= ema_EMA_biased * (1 - param_HTF_ReversalThreshold_L):
        htf_isShort = True
    if htf_isShort:
        htf_strength = param_HTF_Strength_S
    else:
        htf_strength = param_HTF_Strength_L
    if htf_isShort_prev != htf_isShort:
        tefVal_prev = 0
        mtf_anchor = 0
    if htf_isShort:
        mtf_bias = param_MTF_Bias_S
    else:
        mtf_bias = param_MTF_Bias_L
    mtf_y_prev = mmacd_MsDeltaAbsMARel_last - mtf_bias
    mtf_y = mmacd_MsDeltaAbsMARel - mtf_bias
    mtf_isBelow_prev = mtf_y_prev < 0
    mtf_isBelow_this = mtf_y < 0
    if mtf_isBelow_prev != mtf_isBelow_this:
        mtf_anchor = tefVal_prev
    if htf_isShort:
        if mtf_isBelow_this:
            alpha = param_MTF_Alpha_Exit_S
            beta = param_MTF_Beta_Exit_S
            mtf_minDeviation = 0
            mtf_maxDeviation = math.tanh(pow(abs(mtf_y / alpha), beta))
        else:
            alpha = param_MTF_Alpha_Entry_S
            beta = param_MTF_Beta_Entry_S
            mtf_minDeviation = -math.tanh(pow(abs(mtf_y / alpha), beta))
            mtf_maxDeviation = 0
    elif mtf_isBelow_this:
        alpha = param_MTF_Alpha_Entry_L
        beta = param_MTF_Beta_Entry_L
        mtf_minDeviation = 0
        mtf_maxDeviation = math.tanh(pow(abs(mtf_y / alpha), beta))
    else:
        alpha = param_MTF_Alpha_Exit_L
        beta = param_MTF_Beta_Exit_L
        mtf_minDeviation = -math.tanh(pow(abs(mtf_y / alpha), beta))
        mtf_maxDeviation = 0
    _stf_signal = (dmixadx_DMIxADXAbsMARel + mfi_MFIDevAbsMARel) / 2
    if htf_isShort:
        if mtf_isBelow_this:
            stf_deviation = mtf_maxDeviation * htf_strength
            if entry_price > 0 and la_kline_cp < entry_price:
                tefVal = max(tefVal_prev, mtf_anchor + stf_deviation)
            else:
                tefVal = tefVal_prev
        else:
            stf_deviation = mtf_minDeviation * htf_strength
            tefVal = min(tefVal_prev, mtf_anchor + stf_deviation)
    elif mtf_isBelow_this:
        stf_deviation = mtf_maxDeviation * htf_strength
        tefVal = max(tefVal_prev, mtf_anchor + stf_deviation)
    else:
        stf_deviation = mtf_minDeviation * htf_strength
        if entry_price > 0 and la_kline_cp > entry_price:
            tefVal = min(tefVal_prev, mtf_anchor + stf_deviation)
        else:
            tefVal = tefVal_prev
    if htf_isShort:
        tefVal = max(-htf_strength, tefVal)
        tefVal = min(0, tefVal)
    else:
        tefVal = max(0, tefVal)
        tefVal = min(htf_strength, tefVal)
    tefVal = round(tefVal, 6)
    tefDir = "SHORT" if htf_isShort else "LONG"
    if tefVal == 0:
        tcTracker_model["ENTRY_PRICE"] = 0.0
    elif htf_isShort_prev != htf_isShort or entry_price == 0:
        tcTracker_model["ENTRY_PRICE"] = la_kline_cp
    elif abs(tefVal) > abs(tefVal_prev):
        added_qty = abs(tefVal) - abs(tefVal_prev)
        new_entry = (entry_price * abs(tefVal_prev) + la_kline_cp * added_qty) / abs(tefVal)
        tcTracker_model["ENTRY_PRICE"] = new_entry
    tcTracker_model["tefVal_prev"] = tefVal
    tcTracker_model["tefDir_prev"] = tefDir
    tcTracker_model["EMA_EMA_last"] = ema_EMA
    tcTracker_model["MMACD_MSDELTAABSMAREL_last"] = mmacd_MsDeltaAbsMARel
    tcTracker_model["DMIxADX_DMIxADXABSMAREL_last"] = dmixadx_DMIxADXAbsMARel
    tcTracker_model["MFI_MFIDEVABSMAREL_last"] = mfi_MFIDevAbsMARel
    tcTracker_model["HTF_ISSHORT"] = htf_isShort
    tcTracker_model["MTF_ANCHOR"] = mtf_anchor
    return (tefDir, tefVal)
