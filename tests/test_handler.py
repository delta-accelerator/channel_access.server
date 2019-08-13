import pytest

import threading
import channel_access.common as ca
import channel_access.server as cas
from . import common


def test_write_handler(server):
    executed = False
    def handler(pv, value, timestamp, context):
        nonlocal executed
        executed = True
        return (value, timestamp)

    pv = server.createPV('CAS:Test', ca.Type.CHAR, write_handler=handler)
    common.caput('CAS:Test', 1)
    assert(executed)
    assert(pv.value == 1)

def test_noop_write_handler(server):
    executed = False
    def handler(pv, value, timestamp, context):
        nonlocal executed
        executed = True
        return True

    pv = server.createPV('CAS:Test', ca.Type.CHAR, write_handler=handler)
    common.caput('CAS:Test', 1)
    assert(executed)
    assert(pv.value == 1)

def test_changing_write_handler(server):
    executed = False
    def handler(pv, value, timestamp, context):
        nonlocal executed
        executed = True
        return (value + 1, timestamp)

    pv = server.createPV('CAS:Test', ca.Type.CHAR, write_handler=handler)
    common.caput('CAS:Test', 1)
    assert(executed)
    assert(pv.value == 2)

def test_failing_write_handler(server):
    pv = server.createPV('CAS:Test', ca.Type.CHAR, write_handler=cas.failing_write_handler)
    with pytest.raises(common.CaputError):
        common.caput('CAS:Test', 1)

def test_async_write_handler(server):
    executed = False
    completion = None
    args = None

    def handler(pv, value, timestamp, context):
        nonlocal executed, completion, args
        executed = True
        completion = cas.AsyncWrite(pv, context)
        args = (value, timestamp)
        return completion

    def complete():
        nonlocal completion, args
        completion.complete(*args)

    pv = server.createPV('CAS:Test', ca.Type.CHAR, write_handler=handler)
    timer = threading.Timer(1.0, complete)
    timer.start()
    common.caput('CAS:Test', 1, timeout=2)
    assert(executed)
    timer.join()
    assert(pv.value == 1)

def test_failing_async_write_handler(server):
    executed = False
    completion = None

    def handler(pv, value, timestamp, context):
        nonlocal executed, completion
        executed = True
        completion = cas.AsyncWrite(pv, context)
        return completion

    def complete():
        nonlocal completion
        completion.fail()

    pv = server.createPV('CAS:Test', ca.Type.CHAR, write_handler=handler)
    timer = threading.Timer(1.0, complete)
    timer.start()
    with pytest.raises(common.CaputError):
        common.caput('CAS:Test', 1, timeout=2)
    assert(executed)
    timer.join()
    assert(pv.value == 0)
