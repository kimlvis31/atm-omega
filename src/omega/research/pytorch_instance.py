"""Optional external-instance example: next-minute return MLP or stateful GRU.

Select this file in the NN page after installing the compatible optional PyTorch
environment. Import/inspection never imports torch; create() requires it. This
example is research only and is not connected to an account or trading strategy.
Preprocessing, model shape, optimizer, checkpoint and evaluation belong here.
"""

# Imports
import math
import os
from collections import defaultdict

from omega import __version__

# Constants
INPUT_SIZE = 4


def describe():
    return {
        "module_id": "omega.example.pytorch_return",
        "module_version": "1.1",
        "api_version": 1,
        "core_version": __version__,
        "framework": "pytorch",
        "capabilities": {
            "train": True,
            "predict": True,
            "prepare_inference": True,
            "resume_training": True,
            "stateful_inference": True,
            "online_learning": False,
            "dynamic_topology": False,
        },
        "resume_config_overrides": ["epochs"],
        "input": {
            "schema_id": "omega.ohlcv.1m.v1",
            "preprocessing_owner": "module",
            "fit_scope": "train_only",
            "features": ["close/open-1", "high/open-1", "low/open-1", "log_volume"],
            "sequence_boundary": "instrument, split, gap; no cross-boundary windows",
        },
        "output": {
            "task": "regression",
            "target": "next_close/current_close-1",
            "available_at": "last input minute close",
            "purpose": "research example, no profitability or live inference guarantee",
        },
        "config_schema": {
            "architecture": ["mlp", "gru"],
            "lookback": [2, 256],
            "hidden_size": [1, 512],
            "epochs": [1, 10000],
            "batch_size": [1, 4096],
            "checkpoint_every": [1, 10000],
        },
        "configuration_schema": {
            "type": "object",
            "properties": {
                "architecture": {"type": "string", "enum": ["mlp", "gru"], "default": "mlp"},
                "lookback": {"type": "integer", "minimum": 2, "maximum": 256, "default": 4},
                "hidden_size": {"type": "integer", "minimum": 1, "maximum": 512, "default": 8},
                "epochs": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 10},
                "batch_size": {"type": "integer", "minimum": 1, "maximum": 4096, "default": 32},
                "learning_rate": {
                    "type": "number",
                    "minimum": 0.00000001,
                    "maximum": 1,
                    "default": 0.001,
                },
                "checkpoint_every": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 10000,
                    "default": 1,
                },
                "max_samples": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100000,
                    "default": 20000,
                },
            },
        },
        "inference": {
            "input": "normalized features [batch, sequence, 4]; use instance.features(rows)",
            "state": "GRU hidden tensor as JSON [1,batch,hidden]; None resets",
            "state_owner": (
                "caller keeps separate states per instrument and resets on gap/model change"
            ),
            "training_state": "independent sample windows; no hidden carry between samples",
        },
    }


def create(config, context):
    return TorchReturnInstance(config, context)


def _integer(config, name, default, maximum):
    value = config.get(name, default)
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer in [1,{maximum}]")
    return value


class TorchReturnInstance:
    def __init__(self, config, context):
        try:
            import torch
        except ImportError as exc:
            raise RuntimeError(
                "PyTorch is not installed in the core-compatible Python environment"
            ) from exc
        self.torch = torch
        torch.set_num_threads(1)
        torch.manual_seed(context.seed)
        requested = context.device_requested
        if requested == "gpu" and not torch.cuda.is_available():
            raise ValueError("GPU explicitly requested but CUDA is unavailable")
        self.device = "cuda" if requested != "cpu" and torch.cuda.is_available() else "cpu"
        self.device_reason = (
            "explicit CPU"
            if requested == "cpu"
            else (
                "CUDA available" if self.device == "cuda" else "CUDA unavailable; auto selected CPU"
            )
        )
        self.architecture = config.get("architecture", "mlp")
        if self.architecture not in {"mlp", "gru"}:
            raise ValueError("architecture must be mlp or gru")
        self.lookback = _integer(config, "lookback", 4, 256)
        if self.lookback < 2:
            raise ValueError("lookback must be at least two")
        self.hidden_size = _integer(config, "hidden_size", 8, 512)
        self.epochs = _integer(config, "epochs", 10, 10000)
        self.batch_size = _integer(config, "batch_size", 32, 4096)
        self.checkpoint_every = _integer(config, "checkpoint_every", 1, 10000)
        self.max_samples = _integer(config, "max_samples", 20000, 100000)
        self.learning_rate = config.get("learning_rate", 0.001)
        if (
            type(self.learning_rate) not in {int, float}
            or not math.isfinite(self.learning_rate)
            or not 0 < self.learning_rate <= 1
        ):
            raise ValueError("learning_rate must be finite and in (0,1]")
        self.volume_scale = None
        self.completed_epochs = 0
        self.last_checkpoint = None
        self.generator = torch.Generator(device="cpu").manual_seed(context.seed)
        if self.architecture == "mlp":
            self.network = torch.nn.Sequential(
                torch.nn.Flatten(),
                torch.nn.Linear(self.lookback * INPUT_SIZE, self.hidden_size),
                torch.nn.ReLU(),
                torch.nn.Linear(self.hidden_size, 1),
            ).to(self.device)
            self.head = None
        else:
            self.network = torch.nn.GRU(INPUT_SIZE, self.hidden_size, batch_first=True).to(
                self.device
            )
            self.head = torch.nn.Linear(self.hidden_size, 1).to(self.device)
        self.optimizer = torch.optim.Adam(self.parameters(), lr=self.learning_rate)
        self.loss_function = torch.nn.MSELoss()

    def parameters(self):
        return list(self.network.parameters()) + (list(self.head.parameters()) if self.head else [])

    def _structure(self, context, step=0):
        context.emit_structure(
            {
                "id": f"pytorch-{self.architecture}-v1",
                "nodes": [
                    {"id": "input", "type": "input", "shape": [self.lookback, INPUT_SIZE]},
                    {"id": "hidden", "type": self.architecture, "shape": [self.hidden_size]},
                    {
                        "id": "output",
                        "type": "linear",
                        "shape": [1],
                        "parameters": sum(parameter.numel() for parameter in self.parameters()),
                    },
                ],
                "edges": [
                    {"source": "input", "target": "hidden"},
                    {"source": "hidden", "target": "output"},
                ],
            },
            step=step,
        )

    def prepare(self, rows, context):
        self.volume_scale = max(math.log1p(float(row.volume)) for row in rows) or 1.0
        context.emit_event("preprocessing_fit", split="train", volume_scale=self.volume_scale)
        self._structure(context)
        return self.transform(rows, "train", context)

    def features(self, rows):
        if self.volume_scale is None:
            raise ValueError("preprocessing is not fitted/restored")
        result = []
        for row in rows:
            values = [
                float(row.close / row.open - 1),
                float(row.high / row.open - 1),
                float(row.low / row.open - 1),
                math.log1p(float(row.volume)) / self.volume_scale,
            ]
            if not all(math.isfinite(value) for value in values):
                raise ValueError("non-finite model input")
            result.append(values)
        return result

    def transform(self, rows, purpose, context):
        # [1]: Build complete windows separately within each instrument/split.
        by_instrument = defaultdict(list)
        for row in rows:
            by_instrument[row.instrument].append(row)
        samples = []
        for instrument in sorted(by_instrument):
            series = sorted(by_instrument[instrument], key=lambda row: row.open_time_ms)
            values = self.features(series)
            for end in range(self.lookback, len(series)):
                context.check_cancelled()
                window = series[end - self.lookback : end + 1]
                if any(
                    right.open_time_ms - left.open_time_ms != 60000
                    for left, right in zip(window, window[1:], strict=False)
                ):
                    continue
                if len(samples) >= self.max_samples:
                    raise ValueError("example sample budget exceeded; choose a smaller dataset")
                target = float(series[end].close / series[end - 1].close - 1)
                samples.append(
                    (
                        values[end - self.lookback : end],
                        target,
                        series[end - 1].open_time_ms + 60000,
                        series[end].open_time_ms,
                        instrument,
                    )
                )
        if not samples:
            raise ValueError(f"{purpose} has no complete lookback/next-minute windows")
        return samples

    def _forward(self, inputs, state=None):
        if self.architecture == "mlp":
            if state is not None:
                raise ValueError("MLP has no recurrent state")
            return self.network(inputs), None
        sequence, hidden = self.network(inputs, state)
        return self.head(sequence[:, -1, :]), hidden

    def _tensors(self, samples):
        torch = self.torch
        inputs = torch.tensor(
            [sample[0] for sample in samples], dtype=torch.float32, device=self.device
        )
        targets = torch.tensor(
            [[sample[1]] for sample in samples], dtype=torch.float32, device=self.device
        )
        if not torch.isfinite(inputs).all() or not torch.isfinite(targets).all():
            raise ValueError("input/target cannot be represented as finite float32")
        return inputs, targets

    def train(self, train, validation, config, context):
        if config.get("online_learning", False):
            raise ValueError("this example does not implement online learning")
        if self.epochs <= self.completed_epochs:
            raise ValueError("epochs must exceed the completed checkpoint epoch")
        inputs, targets = self._tensors(train)
        validation_inputs, validation_targets = self._tensors(validation)
        torch = self.torch
        for epoch in range(self.completed_epochs, self.epochs):
            context.check_cancelled()
            self.network.train()
            if self.head:
                self.head.train()
            order = torch.randperm(len(train), generator=self.generator)
            for start in range(0, len(train), self.batch_size):
                context.check_cancelled()
                indices = order[start : start + self.batch_size].to(self.device)
                self.optimizer.zero_grad(set_to_none=True)
                predictions, _ = self._forward(inputs[indices])
                loss = self.loss_function(predictions, targets[indices])
                if not torch.isfinite(loss):
                    raise ValueError("non-finite training loss")
                loss.backward()
                self.optimizer.step()
            self.network.eval()
            if self.head:
                self.head.eval()
            with torch.no_grad():
                for split, values, expected in (
                    ("train", inputs, targets),
                    ("validation", validation_inputs, validation_targets),
                ):
                    predictions, _ = self._forward(values)
                    loss = float(self.loss_function(predictions, expected).cpu().item())
                    context.emit_metric("mse", loss, step=epoch + 1, split=split)
            for index, parameter in enumerate(self.parameters()):
                tensor = parameter.detach()
                summary = {
                    "shape": list(tensor.shape),
                    "mean": float(tensor.mean().cpu()),
                    "minimum": float(tensor.min().cpu()),
                    "maximum": float(tensor.max().cpu()),
                }
                if parameter.grad is not None:
                    summary["gradient_norm"] = float(parameter.grad.detach().norm().cpu())
                if context.recording["profile"] == "detailed":
                    summary["sample"] = tensor.flatten()[:64].cpu().tolist()
                context.emit_tensor_summary(f"parameter.{index}", summary, step=epoch + 1)
            self.completed_epochs = epoch + 1
            if self.completed_epochs % self.checkpoint_every == 0:
                self._checkpoint(context)
        return {
            "epochs": self.completed_epochs,
            "device": self.device,
            "device_reason": self.device_reason,
            "architecture": self.architecture,
            "topology": "fixed",
            "resume_boundary": "completed_epoch",
        }

    def predict(self, batch, state=None):
        torch = self.torch
        inputs = torch.tensor(batch, dtype=torch.float32, device=self.device)
        if inputs.ndim != 3 or inputs.shape[0] < 1 or inputs.shape[2] != INPUT_SIZE:
            raise ValueError("prediction input must have shape [batch,sequence,4]")
        if not 1 <= inputs.shape[1] <= self.lookback or (
            self.architecture == "mlp" and inputs.shape[1] != self.lookback
        ):
            raise ValueError("prediction sequence length is incompatible")
        if not torch.isfinite(inputs).all():
            raise ValueError("prediction input must be finite")
        hidden = (
            None if state is None else torch.tensor(state, dtype=torch.float32, device=self.device)
        )
        if hidden is not None and (
            list(hidden.shape) != [1, inputs.shape[0], self.hidden_size]
            or not torch.isfinite(hidden).all()
        ):
            raise ValueError("recurrent state has incompatible shape/values")
        self.network.eval()
        if self.head:
            self.head.eval()
        with torch.no_grad():
            values, next_state = self._forward(inputs, hidden)
        if not torch.isfinite(values).all() or (
            next_state is not None and not torch.isfinite(next_state).all()
        ):
            raise ValueError("non-finite inference output")
        return {
            "values": values[:, 0].cpu().tolist(),
            "state": None if next_state is None else next_state.cpu().tolist(),
        }

    def prepare_inference(self, rows, request, context):
        # Inference has no future target row and never fits volume_scale again.
        if len(rows) != self.lookback or request["contract"]["input_shape"] != [
            None,
            self.lookback,
            INPUT_SIZE,
        ]:
            raise ValueError("PyTorch raw inference requires the complete trained lookback")
        context.check_cancelled()
        return {"batch": [self.features(rows)], "used_rows": list(range(len(rows)))}

    def evaluate(self, rows, context):
        values = self.predict([row[0] for row in rows])["values"]
        errors = []
        for prediction, (_, target, available, timestamp, instrument) in zip(
            values, rows, strict=True
        ):
            errors.append((prediction - target) ** 2)
            context.emit_evaluation(
                {
                    "timestamp_ms": timestamp,
                    "available_at_ms": available,
                    "instrument": instrument,
                    "prediction": prediction,
                    "target": target,
                }
            )
        result = {"mse": sum(errors) / len(errors), "samples": len(rows)}
        context.emit_metric("mse", result["mse"], step=self.completed_epochs, split="test")
        return result

    def _checkpoint(self, context):
        name = f"checkpoints/epoch_{self.completed_epochs:05d}.pt"
        if self.last_checkpoint and self.last_checkpoint["file"] == name:
            return self.last_checkpoint
        self.torch.save(
            {
                "schema_version": 1,
                "architecture": self.architecture,
                "lookback": self.lookback,
                "hidden_size": self.hidden_size,
                "completed_epochs": self.completed_epochs,
                "volume_scale": self.volume_scale,
                "torch_version": str(self.torch.__version__),
                "network": self.network.state_dict(),
                "head": self.head.state_dict() if self.head else None,
                "optimizer": self.optimizer.state_dict(),
                "sampler_rng": self.generator.get_state(),
                "cpu_rng": self.torch.get_rng_state(),
                "cuda_rng": self.torch.cuda.get_rng_state_all() if self.device == "cuda" else [],
                "device": self.device,
            },
            context.artifact_path(name),
        )
        # A unique epoch file is registered only after its completed bytes are flushed.
        with context.artifact_path(name).open("r+b") as checkpoint:
            checkpoint.flush()
            os.fsync(checkpoint.fileno())
        self.last_checkpoint = context.register_artifact(
            name,
            kind="checkpoint",
            completed_epochs=self.completed_epochs,
            resume_capability="weights_optimizer_preprocessor_sampler_and_rng",
            boundary="epoch",
            framework_version=str(self.torch.__version__),
        )
        return self.last_checkpoint

    def save(self, context):
        return self._checkpoint(context)

    def restore(self, checkpoint, context):
        # Host verifies/snapshots registered artifact hash before this call.
        state = self.torch.load(checkpoint, map_location="cpu", weights_only=True)
        if (
            state["schema_version"] != 1
            or state["architecture"] != self.architecture
            or state["lookback"] != self.lookback
            or state["hidden_size"] != self.hidden_size
            or state["torch_version"] != str(self.torch.__version__)
        ):
            raise ValueError("checkpoint model/framework compatibility mismatch")
        if state["device"] != self.device:
            raise ValueError("exact resume requires the original device type")
        self.network.load_state_dict(state["network"], strict=True)
        if self.head:
            self.head.load_state_dict(state["head"], strict=True)
        self.optimizer.load_state_dict(state["optimizer"])
        for optimizer_state in self.optimizer.state.values():
            for name, value in optimizer_state.items():
                if self.torch.is_tensor(value):
                    optimizer_state[name] = value.to(self.device)
        self.generator.set_state(state["sampler_rng"])
        self.torch.set_rng_state(state["cpu_rng"])
        if self.device == "cuda":
            self.torch.cuda.set_rng_state_all(state["cuda_rng"])
        self.completed_epochs = state["completed_epochs"]
        self.volume_scale = state["volume_scale"]
        self._structure(context, self.completed_epochs)

    def close(self, context):
        self.optimizer.zero_grad(set_to_none=True)
