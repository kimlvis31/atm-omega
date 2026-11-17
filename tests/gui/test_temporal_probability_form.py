"""TPD's three independent windows survive the structured analysis form."""

import pytest

pytest.importorskip("PySide6")

from omega.analysis.catalog import validate_configuration
from omega.gui.analysis_page import configuration_rows, form_configuration


def test_tpd_structured_roundtrip_preserves_windows_and_line():
    definition = {
        "kind": "TPD",
        "interval_id": 2,
        "viewLength": 7,
        "nSamples": 13,
        "nSamplesMA": 5,
        "line_index": 4,
    }
    encoded = form_configuration([definition])
    assert encoded["temporal_probability"] == [
        {key: value for key, value in definition.items() if key != "kind"}
    ]
    assert configuration_rows(validate_configuration(encoded)) == [definition]


@pytest.mark.parametrize(
    "change", [{"viewLength": 1}, {"nSamplesMA": True}, {"nSamples": 20001}, {"line_index": 10}]
)
def test_tpd_form_rejects_unrepresentable_parameters(change):
    with pytest.raises(ValueError):
        form_configuration([{"kind": "TPD", "interval_id": 0, **change}])
