"""Inert notification text: provenance is distinct from acknowledgement/recovery."""

import html
import math
from datetime import UTC, datetime


def _text(ko, en, language):
    return ko if language == "ko" else en


def _time(value):
    if type(value) not in {int, float}:
        return "?"
    try:
        if not math.isfinite(value):
            return "?"
        return datetime.fromtimestamp(value, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, OverflowError, OSError):
        return "?"


def incident_headers(language):
    return (
        ["ID", "알림 시각", "최근 발생", "발생 범위 / 해결 여부", "심각도", "내용", "소리 확인"]
        if language == "ko"
        else [
            "ID",
            "Recorded",
            "Last occurrence",
            "Provenance / resolution",
            "Severity",
            "Summary",
            "Sound confirmed",
        ]
    )


def incident_row(item, language):
    summaries = {
        "CONTROL_TRANSPORT_OR_STORAGE_FAILURE": (
            "요청 처리 중 통신 또는 저장 오류 · 정확한 원인 미확인",
            "Request transport or storage failed; exact cause unconfirmed",
        ),
        "DIAGNOSTIC_LOG_UNAVAILABLE": (
            "진단 로그를 기록하지 못함",
            "Diagnostic log recording failed",
        ),
        "ALL_RECORDING_UNAVAILABLE": ("거래 기록 저장 불가", "Trading records unavailable"),
        "PAPER_RECORDING_UNAVAILABLE": ("가상 거래 기록 저장 불가", "Paper recording unavailable"),
        "CAUSAL_INFERENCE_UNAVAILABLE": (
            "NN 입력/추론 사용 불가",
            "Committed NN inference unavailable",
        ),
    }
    code = item.get("code", "")
    code = code if isinstance(code, str) else ""
    pair = summaries.get(
        code, ("코어 알림 · 상세 코드 확인", "Core incident; see details for code")
    )
    scope = {
        "current_core": ("현재 코어에서 관측", "Observed by current core"),
        "previous_core": ("이전 코어의 기록", "Previous core record"),
    }.get(str(item.get("observation_scope")), ("발생 코어 미확인", "Recording core unknown"))
    # Seen now does not mean currently failing; lack of a new occurrence is not recovery.
    status = _text(*scope, language) + _text(
        " · 해결 미확인", " · resolution unconfirmed", language
    )
    severity = {
        "critical": ("심각", "Critical"),
        "fatal": ("치명적", "Fatal"),
        "error": ("오류", "Error"),
        "warning": ("경고", "Warning"),
        "info": ("정보", "Info"),
    }.get(str(item.get("severity")), ("미확인", "Unknown"))
    return [
        str(item.get("id", "?")),
        _time(item.get("timestamp")),
        _time(item.get("last_seen_at")),
        status,
        _text(*severity, language),
        _text(*pair, language),
        _text("확인됨", "Confirmed", language)
        if item.get("acknowledged")
        else _text("미확인", "Unconfirmed", language),
    ]


def incident_details(item, language):
    context = item.get("context")
    context = context if isinstance(context, dict) else {}
    # Escape plain text before Qt's automatic rich-text tooltip detection.
    fields = [f"ID: {str(item.get('id', '?'))[:128]}", f"Code: {str(item.get('code', '?'))[:256]}"]
    fields.append(_text("알림 시각", "Recorded", language) + ": " + _time(item.get("timestamp")))
    fields.append(
        _text("최근 발생", "Last occurrence", language) + ": " + _time(item.get("last_seen_at"))
    )
    for key, choices in {
        "stage": {"headers", "body", "operation", "response", "unknown"},
        "operation": {"GET", "POST", "unknown"},
        "error_kind": {"timeout", "io"},
    }.items():
        value = context.get(key)
        if isinstance(value, str) and value in choices:
            fields.append(f"{key}: {value}")
    number = context.get("errno")
    if type(number) is int and -4096 <= number <= 4096:
        fields.append(f"errno: {number}")
    count = item.get("occurrences")
    if type(count) is int and 0 <= count <= 1_000_000_000:
        fields.append(_text("기록 횟수", "Occurrences", language) + f": {count}")
    fields.append(
        _text(
            "소리 확인은 장애 해결을 의미하지 않습니다.",
            "Sound acknowledgement does not confirm recovery.",
            language,
        )
    )
    return "<pre>" + html.escape("\n".join(fields)) + "</pre>"


def incident_connection_text(core, *, attached, last_received, language):
    received = _time(last_received.timestamp()) if isinstance(last_received, datetime) else "?"
    if not attached:
        return (
            _text(
                "코어 연결 끊김 · 현재 상태 확인 불가 · 마지막 수신: ",
                "Core disconnected; current status unavailable; last received: ",
                language,
            )
            + received
        )
    storage = core.get("storage_available")
    recording = (
        _text("현재 제어 기록 저장 가능", "Current control recording available", language)
        if storage is True
        else _text("현재 제어 기록 저장 불가", "Current control recording unavailable", language)
        if storage is False
        else _text("제어 기록 저장 상태 미확인", "Control recording status unknown", language)
    )
    return (
        _text("코어 연결됨 · ", "Core connected · ", language)
        + recording
        + _text(
            " · 과거 알림 해결을 보장하지 않음 · 마지막 수신: ",
            " · previous incident recovery unconfirmed · last received: ",
            language,
        )
        + received
    )


def incident_critical_summary(items, language):
    """Count unacknowledged critical facts without merging their ACK identities."""
    counts = {"current_core": 0, "previous_core": 0, "unknown": 0}
    for item in items:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("severity"), str)
            or item["severity"].lower() not in {"critical", "fatal"}
            or item.get("acknowledged") is True
            or item.get("resolved") is True
            or isinstance(item.get("status"), str)
            and item.get("status") in {"resolved", "closed"}
        ):
            continue
        scope = item.get("observation_scope")
        counts[scope if isinstance(scope, str) and scope in counts else "unknown"] += 1
    return _text(
        f"미확인 중요 알림 {sum(counts.values())}건 · 현재 관측 {counts['current_core']} · "
        f"이전 기록 {counts['previous_core']} · 발생 범위 미확인 {counts['unknown']}",
        f"Unacknowledged critical: {sum(counts.values())} · "
        f"current observations {counts['current_core']} · "
        f"previous records {counts['previous_core']} · provenance unknown {counts['unknown']}",
        language,
    )


def incident_guidance(item, core, *, attached, last_received, language):
    """Plain safe categories only; current recording is not evidence of recovery."""
    if not isinstance(item, dict):
        return _text(
            "기록을 선택하면 발생 범위, 알려진 내용과 다음 조치를 확인할 수 있습니다.",
            "Select a record to see its provenance, known facts and next step.",
            language,
        )
    core = core if isinstance(core, dict) else {}
    row = incident_row(item, language)
    fields = [row[4] + " · " + row[5]]
    fields.append(
        _text(
            "최초 기록: " + row[1] + " · 최근 발생: " + row[2],
            "First recorded: " + row[1] + " · last occurrence: " + row[2],
            language,
        )
    )
    count = item.get("occurrences")
    if type(count) is int and 0 <= count <= 1_000_000_000:
        fields.append(_text("기록 횟수: ", "Occurrences: ", language) + str(count))
    scope = item.get("observation_scope")
    provenance = {
        "current_core": (
            "현재 코어에서 관측된 기록입니다. 지금도 계속 실패하는지와 해결 여부는 미확인입니다.",
            "The current core observed this occurrence. "
            "Ongoing failure and recovery are unconfirmed.",
        ),
        "previous_core": (
            "이전 코어에서 저장된 기록입니다. 이번 실행의 재발 여부와 해결 여부는 미확인입니다.",
            "This record was retained from a previous core. "
            "Recurrence in this run and recovery are unconfirmed.",
        ),
    }.get(
        scope if isinstance(scope, str) else "unknown",
        (
            "발생 코어가 기록되지 않았습니다. 신규 장애나 해결 상태로 단정할 수 없습니다.",
            "The recording core is unknown. Neither a new failure nor recovery is established.",
        ),
    )
    fields.append(_text(*provenance, language))
    context = item.get("context")
    context = context if isinstance(context, dict) else {}
    stage = context.get("stage")
    stage_text = {
        "headers": ("요청 머리말 읽기", "reading request headers"),
        "body": ("요청 내용 읽기", "reading the request body"),
        "operation": ("요청 처리", "processing a request"),
        "response": ("응답 전송", "sending a response"),
    }.get(stage if isinstance(stage, str) else "unknown")
    error_kind = context.get("error_kind")
    error_text = {
        "timeout": ("시간 초과", "timeout"),
        "io": ("입출력 오류", "input/output error"),
    }.get(error_kind if isinstance(error_kind, str) else "unknown")
    if stage_text or error_text:
        categories = [pair for pair in (stage_text, error_text) if pair]
        fields.append(
            _text("기록된 분류: ", "Recorded category: ", language)
            + " · ".join(_text(*pair, language) for pair in categories)
        )
    number = context.get("errno")
    if type(number) is int and -4096 <= number <= 4096:
        fields.append(
            _text("기록된 오류 번호: ", "Recorded error number: ", language) + str(number)
        )
    fields.append(
        _text(
            "정확한 실패 대상과 원인은 이 기록만으로 확정할 수 없습니다.",
            "The exact failed target and cause cannot be established from this record alone.",
            language,
        )
    )
    if not attached:
        fields.append(
            incident_connection_text(
                core, attached=False, last_received=last_received, language=language
            )
        )
        fields.append(
            _text(
                "다음 조치: 기존 연결 절차로 코어 연결을 확인한 뒤 현재 상태와 대조하세요.",
                "Next step: check the core connection through the existing connection workflow, "
                "then compare current status.",
                language,
            )
        )
    elif core.get("storage_available") is False:
        fields.append(
            _text(
                "다음 조치: 현재 제어 기록을 저장할 수 없습니다. "
                "저장 장치와 접근 권한의 진단을 확인하세요.",
                "Next step: current control recording is unavailable. "
                "Review storage and access-permission diagnostics.",
                language,
            )
        )
    else:
        fields.append(
            _text(
                "다음 조치: 최근 발생 시각을 현재 연결·기록 상태와 대조하세요. "
                "제어 기록 상태만으로 "
                "시장 DB, 로그 또는 응답 전송의 복구를 판단할 수 없습니다.",
                "Next step: compare the last occurrence with current connection and "
                "recording status. Control recording status alone does not establish recovery "
                "of the market DB, logs or response transport.",
                language,
            )
        )
    fields.append(
        _text(
            "소리 확인은 장애 해결이나 거래 재개가 아닙니다. 저장된 사실과 재개 조건은 유지됩니다.",
            "Acknowledging sound does not resolve the incident or resume trading. "
            "Recorded facts and resume gates remain.",
            language,
        )
    )
    return "\n".join(fields)


def incident_acknowledgement_text(state, language):
    choices = {
        "pending": (
            "선택한 기록의 소리 확인 결과를 기다리는 중입니다.",
            "Waiting for sound acknowledgement of the selected record.",
        ),
        "durable": (
            "선택한 기록의 소리 확인이 저장되었습니다. 해결·재개를 뜻하지 않습니다.",
            "Sound acknowledgement was saved for this record. "
            "This does not mean recovery or resume.",
        ),
        "volatile": (
            "소리 확인은 임시 반영됐지만 저장되지 않았습니다. 재시작 후 다시 알릴 수 있습니다.",
            "Sound acknowledgement is temporary and was not saved. "
            "It may alert again after restart.",
        ),
        "rejected": (
            "소리 확인 요청이 거절되었습니다. 기록과 반복 알림은 유지됩니다.",
            "Sound acknowledgement was rejected. The record and repeat eligibility remain.",
        ),
        "changed": (
            "이전 판에 대한 소리 확인 요청입니다. 현재 판의 저장·확인은 별도로 확인해야 합니다.",
            "This acknowledgement concerns an earlier revision. "
            "Confirmation of the current revision remains unconfirmed.",
        ),
        "unconfirmed": (
            "소리 확인 결과나 저장 여부가 미확인입니다. 기존 요청 상태를 확인하세요.",
            "Sound acknowledgement or its persistence is unconfirmed. "
            "Check the existing operation status.",
        ),
    }
    pair = choices.get(state)
    return _text(*pair, language) if pair else ""
