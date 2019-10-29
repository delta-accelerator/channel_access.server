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
    assert(pv.count is None)
    value = int(common.caget('CAS:Test'))
    assert(value == 42)

    pv.value = [1, 2, 3]
    assert(pv.is_array)
    assert(pv.count == 3)
    value = list(map(int, common.caget('CAS:Test', array=True)))
    assert(len(value) == 3)

    pv.value = 23
    assert(not pv.is_array)
    assert(pv.count is None)
    value = int(common.caget('CAS:Test'))
    assert(value == 23)

@pytest.mark.skipif(not cas.numpy, reason="No numpy support")
def test_dynamic_size_numpy(server):
    import numpy

    pv = server.createPV('CAS:Test', ca.Type.CHAR, use_numpy=True, attributes = {
        'value': 42
    })
    assert(not pv.is_array)
    assert(pv.count is None)
    value = int(common.caget('CAS:Test'))
    assert(value == 42)

    pv.value = numpy.array([1, 2, 3])
    assert(pv.is_array)
    assert(pv.count == 3)
    value = list(map(int, common.caget('CAS:Test', array=True)))
    assert(len(value) == 3)

    pv.value = 23
    assert(not pv.is_array)
    assert(pv.count is None)
    value = int(common.caget('CAS:Test'))
    assert(value == 23)

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

@pytest.mark.parametrize("type_", common.ARRAY_TYPES)
def test_different_length_array(server, type_):
    pv = server.createPV('CAS:Test', type_, count=10, use_numpy=False)
    other_values = tuple(range(5))
    pv.value = other_values
    assert(pv.count == 5)

@pytest.mark.skipif(not cas.numpy, reason="No numpy support")
@pytest.mark.parametrize("type_", common.ARRAY_TYPES)
def test_different_length_array_numpy(server, type_):
    import numpy

    pv = server.createPV('CAS:Test', type_, count=10, use_numpy=True)
    other_values = numpy.arange(5)
    pv.value = other_values
    assert(pv.count == 5)
