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
