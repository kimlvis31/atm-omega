"""Worker-owned immutable auxiliary selection and durable paper input references.

Candle delivery remains independent of auxiliary IO. A failed selection provides
no usable auxiliary values; it never stops already submitted fills/local stops.
"""

import json
import time
from copy import deepcopy
from uuid import uuid4

from omega.application.market_frame_builder import MarketFrameBuilder
from omega.application.paper_inputs import PaperInputBatch, input_receipt
from omega.domain.errors import StorageError, ValidationError
from omega.domain.market_inputs import MarketFeatureFrame, market_frame_checksum
from omega.infrastructure.analysis_frames import CommittedAnalysisFrameSource
from omega.infrastructure.postgres.config import database_destination_id, load_database_settings
from omega.runtime.analysis_manifest import AnalysisInputManifest, InputPolicy
from omega.runtime.analysis_replay import replay


class _EmptyStore:
    def get(self, *args, **kwargs):
        return None

    def latest_sample(self, *args, **kwargs):
        return None


class PaperAnalysisSource:
    def __init__(
        self,
        root,
        *,
        source_factory=None,
        manifest_factory=None,
        policy_factory=None,
        clock_ms=None,
    ):
        self.root = root
        self.source_factory = source_factory or (lambda: CommittedAnalysisFrameSource(root))
        self.manifest_factory = manifest_factory or (
            lambda: AnalysisInputManifest(
                root, namespace="paper-inputs", consumer_kind="paper_input"
            )
        )
        self.policy_factory = policy_factory or self._policy
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.source = self.manifest = None
        self.execution_id = uuid4().hex
        self.verified = {}
        self.closed = False

    def _policy(self, item):
        settings = load_database_settings(self.root / "database.config.toml")
        if settings.host not in {"localhost", "127.0.0.1", "::1"}:
            raise ValidationError("paper auxiliary inputs require a local database")
        return InputPolicy(
            database_destination_id(settings),
            tuple(item["channels"]),
            item["maximum_metric_age_ms"],
        )

    def _source(self):
        if self.closed:
            raise StorageError("paper analysis source is closed")
        if self.source is None:
            self.source = self.source_factory()
        return self.source

    def read(self, keys, start_ms, end_ms, *, limit=4096):
        return self._source().read(keys, start_ms, end_ms, limit=limit, as_of_ms=self.clock_ms())

    @staticmethod
    def _unrecorded(candles, as_of_ms, reason, retrospective):
        return PaperInputBatch(
            tuple(
                MarketFeatureFrame(
                    c, as_of_ms, reference_time_ms=c.close_time_ms if retrospective else None
                )
                for c in candles
            ),
            None,
            reason,
        )

    def prepare_inputs(self, plan, candles, *, as_of_ms, names=None, retrospective=False):
        """Stage before the paper CAS; only its successful commit consumes a head.

        Orphan segments from a rejected/failed paper transaction are preserved.
        One batch invocation shares identical selections across strategy instances.
        """
        if self.closed:
            raise StorageError("paper analysis source is closed")
        items = plan.get("analysis_inputs", {})
        selected, cache = {}, {}
        for name, item in sorted(items.items()):
            if names is not None and name not in names:
                continue
            if not retrospective and "live_receipt" in item:
                item = {
                    **item,
                    "receipt": item["live_receipt"],
                    "manual_recovery_required": item["live_manual_recovery_required"],
                }
            rows = tuple(c for c in candles if c.instrument.key == item["instrument"])
            if not rows:
                continue
            if item.get("manual_recovery_required"):
                selected[name] = self._unrecorded(
                    rows, as_of_ms, "analysis_input_recovery_required", retrospective
                )
                continue
            reason = "analysis_input_policy_unavailable"
            try:
                policy = self.policy_factory(deepcopy(item))
                if (
                    not isinstance(policy, InputPolicy)
                    or policy.channels != tuple(item["channels"])
                    or policy.maximum_metric_age_ms != item["maximum_metric_age_ms"]
                ):
                    raise ValidationError("paper input policy differs from the active graph")
                source = self._source()
                source.check_destination(policy.destination_id)
                if self.manifest is None:
                    self.manifest = self.manifest_factory()
                prior = item.get("receipt")
                if prior is not None:
                    reason = "analysis_input_replay_unavailable"
                    input_receipt(prior)
                    if (
                        prior["binding"] != item["binding"]
                        or InputPolicy.from_document(prior["policy"]) != policy
                        or prior["end_ms"] != rows[0].open_time_ms
                    ):
                        raise ValidationError("paper input head is not the preceding bound minute")
                    if self.verified.get(name) != prior:
                        restored = replay(
                            self.manifest,
                            prior["head"],
                            source,
                            expected_analysis_id=item["binding"][:32],
                            expected_policy=policy,
                            expected_kind="paper_input",
                        )
                        if (
                            restored[-1].frame.candle.close_time_ms != prior["end_ms"]
                            or market_frame_checksum(restored[-1].frame) != prior["frame_digest"]
                        ):
                            raise ValidationError(
                                "paper input head differs from its committed receipt"
                            )
                        self.verified[name] = deepcopy(prior)
                key = (
                    rows[0].instrument,
                    rows[0].open_time_ms,
                    rows[-1].close_time_ms,
                    as_of_ms,
                    policy,
                    retrospective,
                )
                selection_reason = ""
                if key not in cache:
                    try:
                        frames = source.read_frames(
                            rows[0].instrument,
                            rows[0].open_time_ms,
                            rows[-1].close_time_ms,
                            as_of_ms=as_of_ms,
                            channels=policy.channels,
                            maximum_metric_age_ms=policy.maximum_metric_age_ms,
                            retrospective_until_ms=rows[-1].close_time_ms
                            if retrospective
                            else None,
                            limit=512,
                        )
                        if (
                            type(frames) is not tuple
                            or tuple(p.frame.candle for p in frames) != rows
                        ):
                            raise ValidationError(
                                "paper auxiliary source changed its committed candles"
                            )
                        cache[key] = (frames, "")
                    except Exception:
                        # Unknown query outcome supplies no values. Durable empty
                        # selections let the next minute recover automatically;
                        # corruption of an already consumed head takes the stricter
                        # replay/manual-recovery path above.
                        builder = MarketFrameBuilder(
                            _EmptyStore(), maximum_metric_age_ms=policy.maximum_metric_age_ms
                        )
                        cache[key] = (
                            tuple(
                                builder.prepare(
                                    c,
                                    as_of_ms=as_of_ms,
                                    channels=policy.channels,
                                    retrospective=retrospective,
                                )
                                for c in rows
                            ),
                            "analysis_source_unavailable",
                        )
                frames, selection_reason = cache[key]
                reason = "analysis_input_recording_unavailable"
                head = self.manifest.stage(
                    item["binding"][:32],
                    self.execution_id,
                    0,
                    frames,
                    policy,
                    previous=prior["head"] if prior else None,
                )
                receipt = {
                    "version": 1,
                    "binding": item["binding"],
                    "policy": policy.document(),
                    "head": head.document(),
                    "frame_digest": market_frame_checksum(frames[-1].frame),
                    "end_ms": rows[-1].close_time_ms,
                }
                selected[name] = PaperInputBatch(
                    tuple(p.frame for p in frames),
                    json.dumps(receipt, sort_keys=True, separators=(",", ":")),
                    selection_reason,
                )
                # This is only a read-validation cache, never the consumed cursor.
                # The next invocation always uses the head from the paper plan.
                self.verified[name] = deepcopy(receipt)
            except Exception:
                selected[name] = self._unrecorded(rows, as_of_ms, reason, retrospective)
        return selected

    def close(self):
        if self.closed:
            return
        self.closed = True
        failure = None
        for resource in (self.manifest, self.source):
            if resource is not None:
                try:
                    resource.close()
                except Exception as exc:
                    failure = failure or exc
        if failure is not None:
            raise failure
