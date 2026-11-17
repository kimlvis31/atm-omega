"""NN source, structured example configuration and explicit epoch-resume draft.

The GUI edits data only. Inspecting/loading a trusted .py module is delegated to
the core's research child process, never performed in the GUI process.
"""

import json
from pathlib import Path

from PySide6.QtCore import QSignalBlocker
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from omega.gui.configuration import ConfigurationEditor


class NeuralControls(QWidget):
    def __init__(self, document, parent=None):
        super().__init__(parent)
        self.document = document
        self._busy = False
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.source = QComboBox()
        self.source.addItem("외부 neuralnetwork.py / External instance", "external")
        self.source.addItem("PyTorch 예제 · MLP / GRU", "pytorch_return")
        self.source.addItem("호스트 데모 · Linear neuron", "demo")
        layout.addWidget(self.source)
        self.notice = QLabel()
        self.notice.setWordWrap(True)
        self.notice.setProperty("role", "muted")
        layout.addWidget(self.notice)
        self.editor = ConfigurationEditor(document)
        layout.addWidget(self.editor)
        self.resume = QGroupBox("이전 checkpoint에서 학습 재개 / Resume")
        self.resume.setCheckable(True)
        self.resume.setChecked(False)
        resume_form = QFormLayout(self.resume)
        resume_form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.run_directory = QLineEdit()
        self.run_directory.setPlaceholderText("이전 run.json이 있는 training 폴더")
        self.checkpoint = QLineEdit()
        self.checkpoint.setPlaceholderText("예: checkpoints/epoch_00002.pt 또는 model.json")
        for title, field, callback in (
            ("Training run", self.run_directory, self._browse_run),
            ("Checkpoint", self.checkpoint, self._browse_checkpoint),
        ):
            row = QHBoxLayout()
            row.addWidget(field)
            button = QPushButton("…")
            button.clicked.connect(callback)
            row.addWidget(button)
            resume_form.addRow(title, row)
        hint = QLabel(
            "원본은 보존합니다. 같은 데이터·분할·모듈·seed·설정만 재개할 수 있고, "
            "epochs는 추가 횟수가 아닌 총 목표 횟수입니다. "
            "종료 확인 전 running/unknown 작업은 재개하지 않습니다."
        )
        hint.setWordWrap(True)
        resume_form.addRow(hint)
        layout.addWidget(self.resume)
        self.resume.toggled.connect(self._resume_changed)
        self.run_directory.editingFinished.connect(self._resume_changed)
        self.checkpoint.editingFinished.connect(self._resume_changed)
        self.source.currentIndexChanged.connect(self._source_changed)
        document.textChanged.connect(self._document_changed)
        self._source_changed()

    def _read(self):
        try:
            value = json.loads(self.document.toPlainText())
            return value if isinstance(value, dict) else None
        except (ValueError, TypeError):
            return None

    def _source_changed(self, *_args):
        selected = self.source.currentData()
        properties = {}
        if selected in {"pytorch_return", "demo"}:
            fields = {
                "epochs": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 10000,
                    "default": 10,
                    "title": "총 Epochs",
                },
                "learning_rate": {
                    "type": "number",
                    "minimum": 0.00000001,
                    "maximum": 1,
                    "default": 0.001 if selected == "pytorch_return" else 0.01,
                    "title": "Learning rate",
                },
            }
            if selected == "pytorch_return":
                fields |= {
                    "architecture": {
                        "type": "string",
                        "enum": ["mlp", "gru"],
                        "default": "mlp",
                        "title": "모델 구조 / Architecture",
                    },
                    "lookback": {"type": "integer", "minimum": 2, "maximum": 256, "default": 4},
                    "hidden_size": {"type": "integer", "minimum": 1, "maximum": 512, "default": 8},
                    "batch_size": {"type": "integer", "minimum": 1, "maximum": 4096, "default": 32},
                    "checkpoint_every": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10000,
                        "default": 1,
                        "title": "Checkpoint 간격 (epochs)",
                    },
                    "max_samples": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100000,
                        "default": 20000,
                        "title": "분할별 최대 표본 수",
                    },
                }
            properties = {"config": {"type": "object", "properties": fields}}
        self.editor.set_schema({"type": "object", "properties": properties})
        self.notice.setText(
            "PyTorch 선택 설치가 필요합니다. 코어와 같은 Python 환경에서 실행합니다. "
            "이 예제는 다음 1분 수익률 연구용이며 실거래와 연결되지 않습니다. "
            "GRU 상태는 종목별로 분리하고 결측·모델 교체 시 초기화해야 합니다."
            if selected == "pytorch_return"
            else "설치 없이 호스트·기록·재개를 확인하는 데모이며 트레이딩 모델이 아닙니다."
            if selected == "demo"
            else "신뢰하는 외부 .py가 전처리·모델·학습·평가를 정의합니다. "
            "모듈별 config는 고급 설정에서 편집하며, 코드 실행은 연구 프로세스에서만 합니다."
        )

    def selection(self, external_path: str, *, demo_checked: bool = False) -> dict:
        selected = "demo" if demo_checked else self.source.currentData()
        if selected == "demo":
            return {"demo": True}
        if selected == "pytorch_return":
            return {"builtin": "pytorch_return"}
        if not external_path.strip():
            raise ValueError("신뢰하는 외부 모듈을 선택하거나 PyTorch/데모를 선택하세요.")
        return {"module": external_path.strip()}

    def apply_descriptor(self, descriptor: dict) -> None:
        """Use data-only module metadata returned by the research process."""
        schema = descriptor.get("configuration_schema")
        if isinstance(schema, dict) and isinstance(schema.get("properties"), dict):
            self.editor.set_schema(
                {
                    "type": "object",
                    "properties": {
                        "config": {"type": "object", "properties": schema["properties"]},
                    },
                }
            )

    def reset_descriptor(self) -> None:
        self._source_changed()

    def apply(self, parameters: dict) -> dict:
        if self.resume.isChecked():
            directory = self.run_directory.text().strip()
            checkpoint = self.checkpoint.text().strip()
            if not directory or not checkpoint:
                raise ValueError("재개할 training 폴더와 checkpoint 상대 경로를 모두 선택하세요.")
            if Path(checkpoint).is_absolute() or ".." in Path(checkpoint).parts:
                raise ValueError("Checkpoint는 training 폴더 아래 상대 경로여야 합니다.")
            parameters["resume"] = {"run": directory, "checkpoint": checkpoint}
            mc_options = parameters.get("monte_carlo", {})
            if not isinstance(mc_options, dict):
                raise ValueError("Monte Carlo 설정은 object여야 합니다.")
            if mc_options.get("enabled", False):
                raise ValueError(
                    "재개는 원래 생성된 dataset을 선택하고 Monte Carlo 재생성은 해제하세요."
                )
        return parameters

    def _resume_changed(self, *_args):
        if self._busy:
            return
        parameters = self._read()
        if parameters is None:
            return
        if self.resume.isChecked():
            parameters["resume"] = {
                "run": self.run_directory.text().strip(),
                "checkpoint": self.checkpoint.text().strip(),
            }
        else:
            parameters.pop("resume", None)
        self.document.setPlainText(json.dumps(parameters, ensure_ascii=False, indent=2))

    def _document_changed(self):
        parameters = self._read()
        if parameters is None:
            return
        options = parameters.get("resume")
        if options is not None and not isinstance(options, dict):
            return
        self._busy = True
        with QSignalBlocker(self.resume):
            self.resume.setChecked(options is not None)
        if options is not None:
            self.run_directory.setText(str(options.get("run", "")))
            self.checkpoint.setText(str(options.get("checkpoint", "")))
        self._busy = False

    def _browse_run(self):
        directory = QFileDialog.getExistingDirectory(
            self, "Training run", self.run_directory.text()
        )
        if directory:
            self.run_directory.setText(directory)
            self.resume.setChecked(True)
            self._resume_changed()

    def _browse_checkpoint(self):
        root = self.run_directory.text().strip()
        if not root:
            self.notice.setText("Training run 폴더를 먼저 선택하세요.")
            return
        path, _ = QFileDialog.getOpenFileName(
            self, "Checkpoint", root, "Checkpoint (*.pt *.pth *.json)"
        )
        if path:
            try:
                name = Path(path).resolve().relative_to(Path(root).resolve()).as_posix()
            except ValueError:
                self.notice.setText("선택한 checkpoint가 training 폴더 밖에 있습니다.")
                return
            self.checkpoint.setText(name)
            self.resume.setChecked(True)
            self._resume_changed()
