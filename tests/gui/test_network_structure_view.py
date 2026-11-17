"""Recorded structure summaries never invent network connections or tensor contents."""

import importlib.util
import json
import os
from copy import deepcopy

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional PySide6 is not installed"
)


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["omega-recorded-structure-tests"])


@pytest.fixture
def view(qt_app):
    from omega.gui.charts import NetworkStructureView

    widget = NetworkStructureView()
    widget.resize(800, 500)
    widget.language = "en"
    widget.show()
    yield widget
    widget.close()


def structure(edges=None):
    result = {
        "nodes": [
            {"id": "input", "shape": [None, 4], "type": "Input"},
            {"id": "hidden", "units": 8, "type": "Linear", "parameters": 40},
            {"id": "output", "units": 1, "type": "Linear"},
        ]
    }
    if edges is not None:
        result["edges"] = edges
    return result


def test_missing_edges_are_explicit_and_no_adjacent_connections_invented(view, qt_app):
    record = structure()
    before = deepcopy(record)
    view.set_structure(record)
    qt_app.processEvents()
    view.grab()
    assert view.recorded_edges == view._rendered_edges == ()
    assert "No recorded edges" in view._edge_hint()
    assert record == before
    view.set_structure(structure([]))
    assert view.edge_recording == "recorded"
    assert "Recorded edges 0" in view._edge_hint()


def test_only_recorded_skip_recurrent_and_self_edges_render(view, qt_app):
    view.set_structure(
        structure(
            [
                {"source": "input", "target": "output"},
                {"source": "output", "target": "hidden"},
                {"source": "hidden", "target": "hidden"},
                {"source": "input", "target": "output"},
            ]
        )
    )
    qt_app.processEvents()
    assert not view.grab().isNull()
    assert view._rendered_edges == ((0, 2), (2, 1), (1, 1))
    assert (0, 1) not in view._rendered_edges


def test_ambiguous_unknown_and_over_capacity_edges_are_hidden(view):
    record = structure([{"source": "input", "target": "hidden"}] * 514)
    record["nodes"][1]["id"] = "input"
    view.set_structure(record)
    assert view.recorded_edges == ()
    assert view.omitted_edges == 514
    assert "edges=514" in view._edge_hint()
    view.set_structure({"nodes": [{"id": str(i), "units": 100} for i in range(18)]})
    assert len(view.node_details) == 16
    assert all(detail["display_points"] == 24 for detail in view.node_details)
    assert view.omitted_nodes == 2


def test_invalid_metadata_is_symbolic_and_bounded(view):
    view.set_structure(
        {"nodes": [{"id": "x", "units": 2**80, "shape": [2**90], "parameters": True}]}
    )
    assert view.layers == [("x", 1)]
    assert view.node_details[0]["reported_units"] is None
    assert view.node_details[0]["parameters"] is None
    assert view.node_details[0]["shape"] is None


def test_click_inspects_recorded_node_and_drag_preserves_selection(view, qt_app):
    from PySide6.QtCore import QPoint, Qt
    from PySide6.QtTest import QTest

    view.set_structure(structure())
    qt_app.processEvents()
    view.grab()
    point, index = view._hit_nodes[0]
    results = []
    view.nodeSelected.connect(results.append)
    QTest.mouseClick(view, Qt.MouseButton.LeftButton, pos=point.toPoint())
    assert results[-1]["id"] == view.node_details[index]["id"]
    assert view.selected_node == index
    angle = view.angle
    QTest.mousePress(view, Qt.MouseButton.LeftButton, pos=QPoint(100, 100))
    QTest.mouseMove(view, QPoint(150, 130))
    QTest.mouseRelease(view, Qt.MouseButton.LeftButton, pos=QPoint(150, 130))
    assert view.angle != angle
    assert view.selected_node == index


def test_artifact_step_selection_and_bounded_history(qt_app, tmp_path):
    from omega.gui.pages import ArtifactPage

    page = ArtifactPage()
    try:
        path = tmp_path / "structures.jsonl"
        records = [
            {"step": 0, "structure": structure()},
            {"step": 3, "structure": structure([{"source": "input", "target": "output"}])},
        ]
        path.write_text("\n".join(json.dumps(row) for row in records), encoding="utf-8")
        assert page.open_path(path)
        assert page.structure_select.currentIndex() == 1
        assert page.graph.recorded_edges == ((0, 2),)
        page.structure_select.setCurrentIndex(0)
        assert "step=0" in page.structure_note.text()
        assert page.graph.recorded_edges == ()
        page._set_structures([{"step": i, "structure": structure()} for i in range(1003)])
        assert page.structure_select.count() == 1000
        assert page.structure_records[0][0:2] == (4, 3)
        assert page.structure_records[-1][0:2] == (1003, 1002)
        page._set_structures([])
        assert page.graph.layers == []
        assert page.structure_select.count() == 0
    finally:
        page.close()


@pytest.mark.parametrize("width", [800, 1360])
def test_artifact_structure_controls_remain_accessible(qt_app, width):
    from omega.gui.pages import ArtifactPage

    page = ArtifactPage()
    try:
        page.resize(width, 900)
        page._set_structures([{"step": 1, "structure": structure()}])
        page.tabs.setCurrentIndex(2)
        page.show()
        qt_app.processEvents()
        assert page.structure_select.isVisible()
        assert page.structure_select.width() > 250
        assert page.node_detail.isVisible()
        assert not page.grab().isNull()
        assert page.minimumSizeHint().width() <= width
    finally:
        page.close()
