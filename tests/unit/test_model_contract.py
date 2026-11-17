"""The inward contract remains the identical legacy class and pickle target."""

import pickle

from omega.domain.model_contract import ShadowModelContract
from omega.research.deployment import ShadowModelContract as LegacyContract


def test_reexport_document_and_process_pickle_preserve_contract_identity():
    contract = ShadowModelContract("omega.ohlcv.1m.v1", ("close",), 1, (None, 1))
    assert LegacyContract is ShadowModelContract
    assert LegacyContract.parse(contract.document()) == contract
    assert pickle.loads(pickle.dumps(contract)) == contract
    assert contract.__class__.__module__ == "omega.domain.model_contract"
