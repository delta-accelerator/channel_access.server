import pytest

import channel_access.common as ca
import channel_access.server as cas
from . import common


def test_alias(server):
    pv = server.createPV('CAS:Test', ca.Type.CHAR, attributes = {
        'value': 42
    })
    server.addAlias('CAS:Alias', 'CAS:Test')
    value = int(common.caget('CAS:Alias'))
    assert(value == 42)
    server.removeAlias('CAS:Alias')
    with pytest.raises(common.CagetError):
        common.caget('CAS:Alias')

def test_dynamic_size(server):
    pv = server.createPV('CAS:Test', ca.Type.CHAR, attributes = {
        'value': 42
    })
    assert(not pv.is_array)
    assert(pv.count == 1)
    value = int(common.caget('CAS:Test'))

    pv.value = [1, 2, 3]
    assert(pv.is_array)
    assert(pv.count == 3)
    value = list(map(int, common.caget('CAS:Test', array=True)))
    assert(len(value) == 3)

