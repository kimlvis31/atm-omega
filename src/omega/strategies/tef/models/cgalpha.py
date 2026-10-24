"""CGALPHA compatibility formula; source hash in tests/fixtures/tef-legacy.json."""

import math

PARAMETERS = (
    ("HTF_Bias", 0.0, -1.0, 1.0),
    ("HTF_ReversalThreshold_S", 0.7, 0.0, 1.0),
    ("HTF_ReversalThreshold_L", 0.7, 0.0, 1.0),
    ("HTF_Strength_S", 1.0, 0.0, 1.0),
    ("HTF_Strength_L", 1.0, 0.0, 1.0),
    ("HTF_DMIxADX_Alpha", 1.0, 0.0001, 5.0),
    ("MTF_PSAR_DCC_Alpha", 10.0, 1.0, 100.0),
    ("MTF_PSAR_Weight", 0.5, 0.0, 1.0),
    ("MTF_Bias_S", 0.0, -1.0, 1.0),
    ("MTF_Bias_L", 0.0, -1.0, 1.0),
    ("MTF_Alpha_S_Entry", 1.0, 0.0001, 5.0),
    ("MTF_Alpha_L_Entry", 1.0, 0.0001, 5.0),
    ("MTF_Alpha_S_Exit", 1.0, 0.0001, 5.0),
    ("MTF_Alpha_L_Exit", 1.0, 0.0001, 5.0),
    ("MTF_Beta_S_Entry", 1.0, 0.0, 10.0),
    ("MTF_Beta_L_Entry", 1.0, 0.0, 10.0),
    ("MTF_Beta_S_Exit", 1.0, 0.0, 10.0),
    ("MTF_Beta_L_Exit", 1.0, 0.0, 10.0),
    ("IVP_Alpha", 0.03, 0.001, 0.5),
    ("STF_WOI_Weight", 0.5, 0.0, 1.0),
    ("STF_Bias_S", 0.0, -1.0, 1.0),
    ("STF_Bias_L", 0.0, -1.0, 1.0),
    ("STF_Alpha_S_Entry", 1.0, 0.0001, 5.0),
    ("STF_Alpha_L_Entry", 1.0, 0.0001, 5.0),
    ("STF_Alpha_S_Exit", 1.0, 0.0001, 5.0),
    ("STF_Alpha_L_Exit", 1.0, 0.0001, 5.0),
    ("STF_Beta_S_Entry", 1.0, 0.0, 10.0),
    ("STF_Beta_L_Entry", 1.0, 0.0, 10.0),
    ("STF_Beta_S_Exit", 1.0, 0.0, 10.0),
    ("STF_Beta_L_Exit", 1.0, 0.0, 10.0),
)


def calculate(params: tuple, linearizedAnalysis: dict, tcTracker_model: dict) -> tuple[str, float]:
    (
        param_HTF_Bias,
        param_HTF_ReversalThreshold_S,
        param_HTF_ReversalThreshold_L,
        param_HTF_Strength_S,
        param_HTF_Strength_L,
        param_HTF_DMIxADX_Alpha,
        param_MTF_PSAR_DCC_Alpha,
        param_MTF_PSAR_Weight,
        param_MTF_Bias_S,
        param_MTF_Bias_L,
        param_MTF_Alpha_S_Entry,
        param_MTF_Alpha_L_Entry,
        param_MTF_Alpha_S_Exit,
        param_MTF_Alpha_L_Exit,
        param_MTF_Beta_S_Entry,
        param_MTF_Beta_L_Entry,
        param_MTF_Beta_S_Exit,
        param_MTF_Beta_L_Exit,
        param_IVP_Alpha,
        param_STF_WOI_Weight,
        param_STF_Bias_S,
        param_STF_Bias_L,
        param_STF_Alpha_S_Entry,
        param_STF_Alpha_L_Entry,
        param_STF_Alpha_S_Exit,
        param_STF_Alpha_L_Exit,
        param_STF_Beta_S_Entry,
        param_STF_Beta_L_Entry,
        param_STF_Beta_S_Exit,
        param_STF_Beta_L_Exit,
    ) = params
    la_iClosed_4h = linearizedAnalysis["CLOSED_7"]
    la_MMACD_MSDeltaAbsMARel_4h = linearizedAnalysis.get("7_MMACD_MSDELTAABSMAREL", None)
    la_DMIxADX_DMIxADXAbsMARel_4h = linearizedAnalysis.get("7_DMIxADX_0_DMIxADXABSMAREL", None)
    la_MFI_MFIDevAbsMARel_1h = linearizedAnalysis.get("5_MFI_0_MFIDEVABSMAREL", None)
    la_PSAR_Price_1h = linearizedAnalysis.get("5_PSAR_0_PSAR", None)
    la_PSAR_DCC_1h = linearizedAnalysis.get("5_PSAR_0_DCC", None)
    la_IVP_NB4 = linearizedAnalysis.get("5_IVP_NB4", None)
    la_IVP_NB5 = linearizedAnalysis.get("5_IVP_NB5", None)
    la_NES_NESAbsMARel_1m = linearizedAnalysis.get("0_NES_0_NESABSMAREL", None)
    la_WOI_WOIAbsMARel_1m = linearizedAnalysis.get("0_WOI_0_WOIABSMAREL", None)
    la_closePrice = linearizedAnalysis.get("KLINE_CLOSEPRICE", None)
    if not tcTracker_model:
        tcTracker_model["tefVal_prev"] = 0
        tcTracker_model["tefDir_prev"] = "LONG"
        tcTracker_model["MMACD_MSDeltaAbsMARel_4h_prev"] = 0
        tcTracker_model["HTF_CyclicMax"] = 0
    tefVal_prev = tcTracker_model["tefVal_prev"]
    tefDir_prev = tcTracker_model["tefDir_prev"]
    mmacd_MsDeltaAbsMARel_4h_prev = tcTracker_model["MMACD_MSDeltaAbsMARel_4h_prev"]
    htf_CyclicMax = tcTracker_model["HTF_CyclicMax"]
    if (
        la_MMACD_MSDeltaAbsMARel_4h is None
        or la_DMIxADX_DMIxADXAbsMARel_4h is None
        or la_MFI_MFIDevAbsMARel_1h is None
        or (la_PSAR_Price_1h is None)
        or (la_PSAR_DCC_1h is None)
        or (la_NES_NESAbsMARel_1m is None)
        or (la_WOI_WOIAbsMARel_1m is None)
        or (la_closePrice is None)
    ):
        return (tefDir_prev, tefVal_prev)
    mmacd_MsDeltaAbsMARel_4h = (
        la_MMACD_MSDeltaAbsMARel_4h if la_iClosed_4h else mmacd_MsDeltaAbsMARel_4h_prev
    )
    dmixadx_AbsMARel_4h = la_DMIxADX_DMIxADXAbsMARel_4h
    mfi_DevAbsMARel_1h = la_MFI_MFIDevAbsMARel_1h
    psar_price_1h = la_PSAR_Price_1h
    psar_dcc_1h = la_PSAR_DCC_1h
    nes_AbsMARel_1m = la_NES_NESAbsMARel_1m
    woi_AbsMARel_1m = la_WOI_WOIAbsMARel_1m
    close_price = la_closePrice
    htf_y_prev = mmacd_MsDeltaAbsMARel_4h_prev - param_HTF_Bias
    htf_y = mmacd_MsDeltaAbsMARel_4h - param_HTF_Bias
    htf_isBelow_prev = htf_y_prev < 0
    htf_isBelow_this = htf_y < 0
    htf_cyclicDist = abs(htf_y)
    if htf_isBelow_prev != htf_isBelow_this:
        htf_CyclicMax = htf_cyclicDist
    else:
        htf_CyclicMax = max(htf_cyclicDist, htf_CyclicMax)
    if htf_isBelow_this:
        htf_isShort = htf_CyclicMax * param_HTF_ReversalThreshold_S < htf_cyclicDist
    else:
        htf_isShort = htf_cyclicDist < htf_CyclicMax * param_HTF_ReversalThreshold_L
    htf_dmixadx_confidence = math.tanh(abs(dmixadx_AbsMARel_4h) / param_HTF_DMIxADX_Alpha)
    htf_strength_base = param_HTF_Strength_S if htf_isShort else param_HTF_Strength_L
    htf_strength = htf_strength_base * htf_dmixadx_confidence
    price_above_psar = close_price > psar_price_1h
    psar_aligned = price_above_psar != htf_isShort
    psar_maturity = math.tanh(psar_dcc_1h / param_MTF_PSAR_DCC_Alpha)
    psar_contrib = psar_maturity if psar_aligned else -psar_maturity
    mtf_raw = (
        mfi_DevAbsMARel_1h * (1 - param_MTF_PSAR_Weight) + psar_contrib * param_MTF_PSAR_Weight
    )
    if htf_isShort:
        mtf_bias = param_MTF_Bias_S
        mtf_isEntry = mtf_raw <= mtf_bias
        mtf_alpha = param_MTF_Alpha_S_Entry if mtf_isEntry else param_MTF_Alpha_S_Exit
        mtf_beta = param_MTF_Beta_S_Entry if mtf_isEntry else param_MTF_Beta_S_Exit
    else:
        mtf_bias = param_MTF_Bias_L
        mtf_isEntry = mtf_bias <= mtf_raw
        mtf_alpha = param_MTF_Alpha_L_Entry if mtf_isEntry else param_MTF_Alpha_L_Exit
        mtf_beta = param_MTF_Beta_L_Entry if mtf_isEntry else param_MTF_Beta_L_Exit
    mtf_dist = abs(mtf_raw - mtf_bias)
    mtf_dist_norm = math.tanh(pow(mtf_dist / mtf_alpha, mtf_beta))
    mtf_limit = mtf_dist_norm if mtf_isEntry else 1 - mtf_dist_norm
    mtf_strength = mtf_limit * htf_strength
    ivp_gate = 1.0
    if la_IVP_NB4 is not None and la_IVP_NB5 is not None:
        if htf_isShort:
            if close_price > la_IVP_NB4:
                space_frac = (close_price - la_IVP_NB4) / close_price
                ivp_gate = math.tanh(space_frac / param_IVP_Alpha)
        elif close_price < la_IVP_NB5:
            space_frac = (la_IVP_NB5 - close_price) / close_price
            ivp_gate = math.tanh(space_frac / param_IVP_Alpha)
    stf_sAvg = nes_AbsMARel_1m * (1 - param_STF_WOI_Weight) + woi_AbsMARel_1m * param_STF_WOI_Weight
    if htf_isShort:
        stf_bias = param_STF_Bias_S
        stf_isEntry = stf_sAvg <= stf_bias
        stf_alpha = param_STF_Alpha_S_Entry if stf_isEntry else param_STF_Alpha_S_Exit
        stf_beta = param_STF_Beta_S_Entry if stf_isEntry else param_STF_Beta_S_Exit
    else:
        stf_bias = param_STF_Bias_L
        stf_isEntry = stf_bias <= stf_sAvg
        stf_alpha = param_STF_Alpha_L_Entry if stf_isEntry else param_STF_Alpha_L_Exit
        stf_beta = param_STF_Beta_L_Entry if stf_isEntry else param_STF_Beta_L_Exit
    stf_dist = abs(stf_sAvg - stf_bias)
    x_norm = math.tanh(pow(stf_dist / stf_alpha, stf_beta))
    if htf_isShort:
        stf_tefValue = -x_norm if stf_isEntry else x_norm - 1
    else:
        stf_tefValue = x_norm if stf_isEntry else 1 - x_norm
    tefVal = round(stf_tefValue * mtf_strength * ivp_gate, 6)
    tefDir = "SHORT" if htf_isShort else "LONG"
    tcTracker_model["tefVal_prev"] = tefVal
    tcTracker_model["tefDir_prev"] = tefDir
    tcTracker_model["MMACD_MSDeltaAbsMARel_4h_prev"] = mmacd_MsDeltaAbsMARel_4h
    tcTracker_model["HTF_CyclicMax"] = htf_CyclicMax
    return (tefDir, tefVal)
