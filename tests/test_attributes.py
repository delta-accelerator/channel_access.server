import pytest
import datetime

import channel_access.common as ca
import channel_access.server as cas
from . import common


def test_attribute_timestamp(server):
    dt = datetime.datetime(2019, 5, 21, 15, 43, 44, tzinfo=datetime.timezone.utc)
    pv = server.createPV('CAS:Test', ca.Type.CHAR, attributes={
        'timestamp': dt
    })
    assert(pv.timestamp == dt)

def test_attribute_value(server):
    pv = server.createPV('CAS:Test', ca.Type.CHAR)
    pv.value = 42
    assert(pv.value == 42)

def test_attribute_status(server):
    pv = server.createPV('CAS:Test', ca.Type.FLOAT)
    pv.status = ca.Status.SOFT
    assert(pv.status == ca.Status.SOFT)

def test_attribute_severity(server):
    pv = server.createPV('CAS:Test', ca.Type.FLOAT)
    pv.severity = ca.Severity.MAJOR
    assert(pv.severity == ca.Severity.MAJOR)

def test_attribute_precision(server):
    pv = server.createPV('CAS:Test', ca.Type.FLOAT)
    pv.precision = 3
    assert(pv.precision == 3)

def test_attribute_unit(server):
    pv = server.createPV('CAS:Test', ca.Type.FLOAT)
    pv.unit = 'test'
    assert(pv.unit == 'test')

def test_attribute_enum_strings(server):
    pv = server.createPV('CAS:Test', ca.Type.ENUM)
    pv.enum_strings = ('a', 'bc')
    assert(pv.enum_strings == ('a', 'bc'))

def test_attribute_display_limits(server):
    pv = server.createPV('CAS:Test', ca.Type.CHAR)
    pv.display_limits = (-10, 33)
    assert(pv.display_limits == (-10, 33))

def test_attribute_control_limits(server):
    pv = server.createPV('CAS:Test', ca.Type.CHAR)
    pv.control_limits = (-10, 33)
    assert(pv.control_limits == (-10, 33))

def test_attribute_warning_limits(server):
    pv = server.createPV('CAS:Test', ca.Type.CHAR)
    pv.warning_limits = (-10, 33)
    assert(pv.warning_limits == (-10, 33))

def test_attribute_alarm_limits(server):
    pv = server.createPV('CAS:Test', ca.Type.CHAR)
    pv.alarm_limits = (-10, 33)
    assert(pv.alarm_limits == (-10, 33))


def test_attribute_status_severity(server):
    pv = server.createPV('CAS:Test', ca.Type.FLOAT)
    pv.status_severity = (ca.Status.SOFT, ca.Severity.MAJOR)
    assert(pv.status_severity == (ca.Status.SOFT, ca.Severity.MAJOR))
    pv = server.createPV('CAS:Test', ca.Type.FLOAT)
    pv.status_severity = (ca.Status.SOFT, ca.Severity.MAJOR)
    assert(pv.status_severity == (ca.Status.SOFT, ca.Severity.MAJOR))

def test_attribute_value_timestamp(server):
    dt = datetime.datetime(2019, 5, 21, 15, 43, 44, tzinfo=datetime.timezone.utc)
    pv = server.createPV('CAS:Test', ca.Type.CHAR, attributes={
        'timestamp': dt,
        'value': 42
    })
    assert(pv.value_timestamp == (42, dt))
