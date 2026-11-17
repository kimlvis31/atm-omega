"""PATHFINDER compatibility formula; source hash in tests/fixtures/tef-legacy.json."""

import math

PARAMETERS = (
    ("MST_Deadzone", 0.1, 0.0, 1.0),
    ("MST_Bias", 0.0, -1.0, 1.0),
    ("Swap_Ratio_S", 0.0, 0.0, 1.0),
    ("Swap_Ratio_L", 0.0, 0.0, 1.0),
    ("Acc_Threshold_S", 0.3, 0.01, 1.0),
    ("Acc_Threshold_L", 0.3, 0.01, 1.0),
    ("TP_Threshold_S", 0.3, 0.01, 1.0),
    ("TP_Threshold_L", 0.3, 0.01, 1.0),
    ("Impulse_Span_S", 0.3, 0.01, 1.0),
    ("Impulse_Span_L", 0.3, 0.01, 1.0),
    ("Acc_Step_S", 0.2, 0.0, 1.0),
    ("Acc_Step_L", 0.2, 0.0, 1.0),
    ("TP_Step_S", 0.2, 0.0, 1.0),
    ("TP_Step_L", 0.2, 0.0, 1.0),
    ("Acc_MSTGain_S", 0.0, 0.0, 5.0),
    ("Acc_MSTGain_L", 0.0, 0.0, 5.0),
    ("TP_MSTGain_S", 0.0, 0.0, 1.0),
    ("TP_MSTGain_L", 0.0, 0.0, 1.0),
    ("Consec_Gain_S", 0.5, 0.0, 5.0),
    ("Consec_Gain_L", 0.5, 0.0, 5.0),
    ("Price_Gain_S", 0.5, 0.0, 5.0),
    ("Price_Gain_L", 0.5, 0.0, 5.0),
    ("Loss_Threshold_S", 0.02, 0.0, 0.2),
    ("Loss_Threshold_L", 0.02, 0.0, 0.2),
)


MST_IID = 5
OSC_IID = 0
LAKEY_CLOSED_MST = f"CLOSED_{MST_IID}"
LAKEY_CLOSED_OSC = f"CLOSED_{OSC_IID}"
LAKEY_CLOSEPRICE = "KLINE_CLOSEPRICE"
LAKEY_MST = f"{MST_IID}_MST_0_MST"
LAKEY_STS = f"{OSC_IID}_STS_S"
# These closed inputs are cached independently even while the other is unavailable.
CACHED_INPUTS = (
    (LAKEY_CLOSED_MST, LAKEY_MST, "MST_last"),
    (LAKEY_CLOSED_OSC, LAKEY_STS, "STS_last"),
)
PRICEFACTOR_MAX = 3.0
MAGNITUDE_STEPS = 4


def _reset_cycle_state(tc):
    tc["CYC_ACTIVE"] = False
    tc["CYC_SIDE"] = 0
    tc["CYC_PEAK"] = 0.0
    tc["CYC_BASE"] = 0.0
    tc["CYC_MAX"] = 0.0
    tc["NBUY"] = 0
    tc["NSELL"] = 0


def _round_position(val):
    return math.floor(val * 10000.0 + 0.5) / 10000.0


def _magnitude_quantized(peak, threshold, span):
    m = max(0.0, min(1.0, (peak - threshold) / span))
    return max(1.0, -math.floor(-(m * MAGNITUDE_STEPS) + 1e-09)) / MAGNITUDE_STEPS


def calculate(params: tuple, linearizedAnalysis: dict, tcTracker_model: dict) -> tuple[str, float]:
    (
        param_MST_Deadzone,
        param_MST_Bias,
        param_Swap_Ratio_S,
        param_Swap_Ratio_L,
        param_Acc_Threshold_S,
        param_Acc_Threshold_L,
        param_TP_Threshold_S,
        param_TP_Threshold_L,
        param_Impulse_Span_S,
        param_Impulse_Span_L,
        param_Acc_Step_S,
        param_Acc_Step_L,
        param_TP_Step_S,
        param_TP_Step_L,
        param_Acc_MSTGain_S,
        param_Acc_MSTGain_L,
        param_TP_MSTGain_S,
        param_TP_MSTGain_L,
        param_Consec_Gain_S,
        param_Consec_Gain_L,
        param_Price_Gain_S,
        param_Price_Gain_L,
        param_Loss_Threshold_S,
        param_Loss_Threshold_L,
    ) = params
    la_iClosed_mst = linearizedAnalysis.get(LAKEY_CLOSED_MST, False)
    la_iClosed_osc = linearizedAnalysis.get(LAKEY_CLOSED_OSC, False)
    la_kline_cp = linearizedAnalysis.get(LAKEY_CLOSEPRICE, None)
    la_mst = linearizedAnalysis.get(LAKEY_MST, None)
    la_sts = linearizedAnalysis.get(LAKEY_STS, None)
    tc = tcTracker_model
    if not tc:
        tc["tefVal_prev"] = 0.0
        tc["tefDir_prev"] = "LONG"
        tc["MODE"] = 0
        tc["LASTDIR"] = 0
        tc["MST_last"] = None
        tc["STS_last"] = None
        tc["AVGENTRY"] = None
        _reset_cycle_state(tc)
    tefVal_prev = tc["tefVal_prev"]
    tefDir_prev = tc["tefDir_prev"]
    mst = la_mst if la_iClosed_mst and la_mst is not None else tc["MST_last"]
    stsIsNew = la_iClosed_osc and la_sts is not None
    sts = la_sts if stsIsNew else tc["STS_last"]
    tc["MST_last"] = mst
    tc["STS_last"] = sts
    if la_kline_cp is None or mst is None or sts is None:
        return (tefDir_prev, tefVal_prev)
    mst_b = mst + param_MST_Bias
    if param_MST_Deadzone < mst_b:
        mode = 1
    elif mst_b < -param_MST_Deadzone:
        mode = -1
    else:
        mode = 0
    tc["MODE"] = mode
    position = abs(tefVal_prev)
    lastDir = tc["LASTDIR"]
    if mode != 0 and mode != lastDir:
        if lastDir != 0:
            swapRatio = param_Swap_Ratio_S if mode < 0 else param_Swap_Ratio_L
            position = _round_position(min(1.0, position * swapRatio))
            _reset_cycle_state(tc)
            tc["AVGENTRY"] = la_kline_cp if 0 < position else None
        lastDir = mode
        tc["LASTDIR"] = lastDir
    if lastDir == 0:
        tc["tefVal_prev"] = 0.0
        tc["tefDir_prev"] = "LONG"
        return ("LONG", 0.0)
    rSign = lastDir
    allowEntry = mode == lastDir
    if lastDir < 0:
        (
            p_accTh,
            p_tpTh,
            p_span,
            p_accStep,
            p_tpStep,
            p_accMG,
            p_tpMG,
            p_consec,
            p_price,
            p_lossTh,
        ) = (
            param_Acc_Threshold_S,
            param_TP_Threshold_S,
            param_Impulse_Span_S,
            param_Acc_Step_S,
            param_TP_Step_S,
            param_Acc_MSTGain_S,
            param_TP_MSTGain_S,
            param_Consec_Gain_S,
            param_Price_Gain_S,
            param_Loss_Threshold_S,
        )
    else:
        (
            p_accTh,
            p_tpTh,
            p_span,
            p_accStep,
            p_tpStep,
            p_accMG,
            p_tpMG,
            p_consec,
            p_price,
            p_lossTh,
        ) = (
            param_Acc_Threshold_L,
            param_TP_Threshold_L,
            param_Impulse_Span_L,
            param_Acc_Step_L,
            param_TP_Step_L,
            param_Acc_MSTGain_L,
            param_TP_MSTGain_L,
            param_Consec_Gain_L,
            param_Price_Gain_L,
            param_Loss_Threshold_L,
        )
    if allowEntry:
        mstStrength = max(
            0.0, min(1.0, (abs(mst_b) - param_MST_Deadzone) / max(1e-06, 1 - param_MST_Deadzone))
        )
    else:
        mstStrength = 0.0
    step_acc = p_accStep * (1 + p_accMG * mstStrength)
    step_tp = max(0.0, p_tpStep * (1 - p_tpMG * mstStrength))
    position_prev = position
    avgEntry = tc["AVGENTRY"]
    hasAvg = avgEntry is not None and 0 < position
    profit = rSign * (la_kline_cp - avgEntry) / avgEntry if hasAvg else 0.0
    if stsIsNew:
        s_rel = rSign * sts
        s_abs = abs(s_rel)
        side_now = 1 if 0 < s_rel else -1 if s_rel < 0 else 0
        if tc["CYC_ACTIVE"] and side_now * tc["CYC_SIDE"] < 0:
            tc["CYC_ACTIVE"] = False
        if not tc["CYC_ACTIVE"] and side_now != 0:
            th_now = p_accTh if side_now < 0 else p_tpTh
            if th_now <= s_abs:
                if side_now < 0:
                    tc["NBUY"] += 1
                    tc["NSELL"] = 0
                    consecFactor = 1 + p_consec * (tc["NBUY"] - 1)
                    priceFactor = (
                        max(0.0, min(PRICEFACTOR_MAX, 1 + p_price * -profit / 0.01))
                        if hasAvg
                        else 1.0
                    )
                else:
                    tc["NSELL"] += 1
                    tc["NBUY"] = 0
                    consecFactor = 1 + p_consec * (tc["NSELL"] - 1)
                    priceFactor = (
                        min(PRICEFACTOR_MAX, 1 + p_price * max(0.0, profit) / 0.01)
                        if hasAvg
                        else 1.0
                    )
                tc["CYC_ACTIVE"] = True
                tc["CYC_SIDE"] = side_now
                tc["CYC_PEAK"] = s_abs
                tc["CYC_BASE"] = position
                tc["CYC_MAX"] = (step_acc if side_now < 0 else step_tp) * consecFactor * priceFactor
        if tc["CYC_ACTIVE"]:
            tc["CYC_PEAK"] = max(tc["CYC_PEAK"], s_abs)
            th_cyc = p_accTh if tc["CYC_SIDE"] < 0 else p_tpTh
            m_q = _magnitude_quantized(peak=tc["CYC_PEAK"], threshold=th_cyc, span=p_span)
            if tc["CYC_SIDE"] < 0:
                if allowEntry:
                    position = max(position, min(1.0, tc["CYC_BASE"] + tc["CYC_MAX"] * m_q))
            else:
                lossHold = hasAvg and -p_lossTh < profit < 0
                if not lossHold:
                    position = min(position, max(0.0, tc["CYC_BASE"] - tc["CYC_MAX"] * m_q))
    position = _round_position(min(position, 1.0))
    if position == 0:
        tc["AVGENTRY"] = None
    elif position_prev < position:
        if avgEntry is None or position_prev == 0:
            tc["AVGENTRY"] = la_kline_cp
        else:
            tc["AVGENTRY"] = (
                avgEntry * position_prev + la_kline_cp * (position - position_prev)
            ) / position
    tefDir = "LONG" if 0 < lastDir else "SHORT"
    tefVal = position if 0 < lastDir else -position
    if tefVal == 0:
        tefVal = 0.0
    tc["tefVal_prev"] = tefVal
    tc["tefDir_prev"] = tefDir
    return (tefDir, tefVal)
