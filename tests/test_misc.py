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

def test_monitor_intern(server):
    received = None
    def handler(pv, attributes):
        nonlocal received
        received = attributes['value']

    pv = server.createPV('CAS:Test', ca.Type.CHAR, monitor=handler)
    pv.value = 1
    assert(received == 1)

def test_monitor_extern(server):
    received = None
    def handler(pv, attributes):
        nonlocal received
        received = attributes['value']

    pv = server.createPV('CAS:Test', ca.Type.CHAR, monitor=handler)
    common.caput('CAS:Test', 1)
    assert(received == 1)

def test_read_only(server):
    pv = server.createPV('CAS:Test', ca.Type.CHAR, read_only=True)
    with pytest.raises(common.CaputError):
        common.caput('CAS:Test', 1)
