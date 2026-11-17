"""Bounded UTC warmup planning, independent of strategies, storage and workers."""

from omega.domain.errors import ValidationError

MINUTE = 60_000
SHORT_WARMUP = 2000
MAX_WARMUP = 525_600
CHUNK_MINUTES = 512


def aligned_window(target_end_ms, minutes, alignment_minutes):
    if (
        type(target_end_ms) is not int
        or target_end_ms < 0
        or target_end_ms % MINUTE
        or type(minutes) is not int
        or not 1 <= minutes <= MAX_WARMUP
        or type(alignment_minutes) is not int
        or not 1 <= alignment_minutes <= 1440
    ):
        raise ValidationError("invalid bounded warmup window")
    start = target_end_ms - minutes * MINUTE
    if start < 0:
        raise ValidationError("warmup history would precede the epoch")
    start = start // (alignment_minutes * MINUTE) * (alignment_minutes * MINUTE)
    if (target_end_ms - start) // MINUTE > MAX_WARMUP:
        raise ValidationError("aligned warmup exceeds the one-year preparation limit")
    return start, target_end_ms


def summary(plan):
    start, end, cursor = (plan[key] for key in ("start_ms", "target_end_ms", "next_open_ms"))
    return {
        "plan_id": plan["plan_id"],
        "state": plan["state"],
        "start_ms": start,
        "target_end_ms": end,
        "next_open_ms": cursor,
        "processed_minutes": (cursor - start) // MINUTE,
        "total_minutes": (end - start) // MINUTE,
        "alignment_minutes": plan["alignment_minutes"],
        "chunk_minutes": CHUNK_MINUTES,
    }
