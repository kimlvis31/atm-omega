"""Read-only text for the host's cached evidence accounting observation."""

COUNTS = (
    "charged_bytes",
    "charged_records",
    "active_pins",
    "uncertain_reservations",
    "uncertain_reserved_bytes",
    "remaining_bytes",
    "remaining_records",
    "remaining_pins",
)


def evidence_status(value, *, current=False, language="ko"):
    """Unknown accounting is never converted to zero or filesystem free space."""
    ko = language == "ko"
    missing = "미확인" if ko else "unknown"
    valid = (
        type(value) is dict
        and type(value.get("schema_version")) is int
        and value["schema_version"] == 1
        and value.get("scope") == "host_causal_evidence_accounting"
        and type(value.get("state")) is str
        and value.get("state") in {"ready", "warning", "full", "unavailable"}
        and all(
            type(value.get(key)) is bool
            for key in ("ready", "accepts_new_reservation", "accepts_new_pin")
        )
        and type(value.get("limits")) is dict
    )
    if valid:
        limits = value["limits"]
        valid = (
            all(
                type(limits.get(key)) is int and limits[key] > 0
                for key in ("max_bytes", "max_records", "max_pins", "warning_percent")
            )
            and limits["warning_percent"] <= 100
            and all(
                key in value
                and (value[key] is None or (type(value[key]) is int and value[key] >= 0))
                for key in COUNTS
            )
            and type(value.get("reservation_headroom_bytes")) is int
            and value["reservation_headroom_bytes"] == 2_097_152
            and type(value.get("consumed_receipt_allowance_bytes")) is int
            and value["consumed_receipt_allowance_bytes"] == limits["max_pins"] * 2048
            and type(value.get("consumed_receipt_allowance_records")) is int
            and value["consumed_receipt_allowance_records"] == limits["max_pins"]
        )
        if valid:
            valid = (
                value["ready"] is False
                and value["accepts_new_reservation"] is False
                and value["accepts_new_pin"] is False
                and all(value[key] is None for key in COUNTS)
                if value["state"] == "unavailable"
                else value["ready"] is True and all(value[key] is not None for key in COUNTS)
            )
    scope = (
        "호스트 증거 기록 누계 · 전체 디스크/모델/학습 용량·디스크 여유 공간이 아닙니다. "
        "설정은 master의 neural_evidence에서 다음 코어 시작에 적용 · 자동 삭제/환급 없음"
        if ko
        else "Charged host evidence only; not total disk/model/training size or disk free space. "
        "Master neural_evidence settings apply at next core start; no automatic deletion/refund."
    )
    if not valid:
        return ("증거 저장 상태 미확인\n" if ko else "Evidence storage unknown\n") + scope

    def number(key):
        return missing if value[key] is None else str(value[key])

    freshness = (
        ("현재 조회" if ko else "current observation")
        if current
        else ("이전 조회 · 재조회 필요" if ko else "previous observation; refresh required")
    )
    yes, no = ("가능", "불가") if ko else ("yes", "no")
    return (
        f"Evidence · {value['state']} · {freshness} · ready {yes if value['ready'] else no}\n"
        f"Bytes {number('charged_bytes')} / {limits['max_bytes']} · "
        f"Records {number('charged_records')} / {limits['max_records']} · "
        f"Pins {number('active_pins')} / {limits['max_pins']}\n"
        f"{'남은 일반 한도' if ko else 'Ordinary headroom'}: "
        f"bytes {number('remaining_bytes')}, records {number('remaining_records')}, "
        f"pins {number('remaining_pins')} · "
        f"{'미확정 예약' if ko else 'Uncertain reservations'} "
        f"{number('uncertain_reservations')} / {number('uncertain_reserved_bytes')} bytes\n"
        f"{'새 예약' if ko else 'New reservation'} "
        f"{yes if value['accepts_new_reservation'] else no} · "
        f"{'새 pin' if ko else 'New pin'} {yes if value['accepts_new_pin'] else no} · "
        f"{'경고' if ko else 'Warning'} {limits['warning_percent']}% · "
        f"reservation headroom {value['reservation_headroom_bytes']} bytes · "
        f"consumed receipt allowance {value['consumed_receipt_allowance_bytes']} bytes / "
        f"{value['consumed_receipt_allowance_records']} records\n{scope}"
    )
