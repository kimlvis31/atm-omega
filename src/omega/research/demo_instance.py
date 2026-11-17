"""Dependency-free example NN instance: a single trainable linear neuron.

Real PyTorch/Triton CNN/RNN modules implement the same describe/create methods.
This small sample exists to verify host/plugin/recording behavior on every PC.
"""

# Imports
import json

from omega import __version__


def describe():
    return {
        "module_id": "omega.demo.linear_neuron",
        "module_version": "1.1",
        "api_version": 1,
        "core_version": __version__,
        "capabilities": {
            "train": True,
            "predict": True,
            "prepare_inference": True,
            "stateful_inference": False,
            "online_learning": False,
            "dynamic_topology": False,
            "resume_training": True,
        },
        "input": {
            "schema_id": "omega.ohlcv.1m.v1",
            "preprocessing_owner": "module",
            "fit_scope": "train_only",
        },
        "output": {
            "task": "regression",
            "target": "same_bar_close_open_return",
            "purpose": "host demonstration only; not a trading model",
        },
        "framework": "stdlib-demo",
        "resume_config_overrides": ["epochs"],
        "configuration_schema": {
            "type": "object",
            "properties": {
                "epochs": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 10},
                "learning_rate": {
                    "type": "number",
                    "minimum": 0.00000001,
                    "maximum": 1,
                    "default": 0.01,
                },
            },
        },
    }


def create(config, context):
    return LinearNeuron(config)


class LinearNeuron:
    def __init__(self, config):
        self.weight = 0.0
        self.bias = 0.0
        self.scale = 1.0
        self.completed_epochs = 0
        self.epochs = int(config.get("epochs", 10))
        self.learning_rate = float(config.get("learning_rate", 0.01))
        if not 1 <= self.epochs <= 10000 or not 0 < self.learning_rate <= 1:
            raise ValueError("invalid demo epoch/learning_rate")

    def prepare(self, rows, context):
        self.scale = max(float(row.volume) for row in rows) or 1.0
        context.emit_event("preprocessing_fit", split="train", volume_scale=self.scale)
        context.emit_structure(
            {
                "id": "linear-v1",
                "nodes": [
                    {"id": "volume", "type": "input", "shape": [1]},
                    {"id": "neuron", "type": "linear", "shape": [1], "parameters": 2},
                ],
                "edges": [{"source": "volume", "target": "neuron"}],
            }
        )
        return self.transform(rows, "train", context)

    def transform(self, rows, purpose, context):
        return [
            (
                float(row.volume) / self.scale,
                float(row.close / row.open - 1),
                row.open_time_ms,
                row.instrument,
            )
            for row in rows
        ]

    def train(self, train, validation, config, context):
        if self.epochs <= self.completed_epochs:
            raise ValueError("epochs must exceed the completed checkpoint epoch")
        for epoch in range(self.completed_epochs, self.epochs):
            context.check_cancelled()
            for value, target, _, _ in train:
                error = self.weight * value + self.bias - target
                self.weight -= self.learning_rate * error * value
                self.bias -= self.learning_rate * error
            for split, rows in (("train", train), ("validation", validation)):
                loss = sum(
                    (self.weight * value + self.bias - target) ** 2 for value, target, _, _ in rows
                ) / len(rows)
                context.emit_metric("mse", loss, step=epoch, split=split)
            context.emit_tensor_summary(
                "neuron", {"weight": self.weight, "bias": self.bias}, step=epoch
            )
            self.completed_epochs = epoch + 1
        return {
            "epochs": self.epochs,
            "topology": "fixed",
            "device": "cpu",
            "device_reason": "dependency-free demo implements CPU only",
        }

    def predict(self, batch, state=None):
        return {"values": [self.weight * value + self.bias for value in batch], "state": state}

    def prepare_inference(self, rows, request, context):
        # Restored training scale only; transform() also constructs targets.
        if len(rows) != 1 or request["contract"]["input_shape"] != [None]:
            raise ValueError("demo raw inference requires one volume input")
        return {"batch": [float(rows[0].volume) / self.scale], "used_rows": [0]}

    def evaluate(self, rows, context):
        errors = []
        for value, target, timestamp, instrument in rows:
            prediction = self.weight * value + self.bias
            errors.append((prediction - target) ** 2)
            context.emit_evaluation(
                {
                    "timestamp_ms": timestamp,
                    "instrument": instrument,
                    "prediction": prediction,
                    "target": target,
                    "error": prediction - target,
                }
            )
        result = {"mse": sum(errors) / len(errors), "samples": len(rows)}
        context.emit_metric("mse", result["mse"], step=self.epochs, split="test")
        return result

    def save(self, context):
        return context.checkpoint_json(
            "model.json",
            {
                "weight": self.weight,
                "bias": self.bias,
                "volume_scale": self.scale,
                "completed_epochs": self.completed_epochs,
            },
            structure_id="linear-v1",
            resume_capability="weights_preprocessor_epoch_stateless_sgd",
        )

    def restore(self, checkpoint, context):
        model = json.loads(checkpoint.read_text(encoding="utf-8"))
        self.weight = model["weight"]
        self.bias = model["bias"]
        self.scale = model["volume_scale"]
        self.completed_epochs = model["completed_epochs"]

    def close(self, context):
        pass
